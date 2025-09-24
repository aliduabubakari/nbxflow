import time
import atexit
from contextlib import AbstractContextManager
from contextvars import ContextVar
from typing import Optional, Dict, List, Any, Callable
import threading

from .datasets import DatasetRef
from .registry import FlowRegistry, TaskSpec
from .lineage import get_emitter
from .facets import (
    performance_from_metrics_row, reliability_from_metrics_row,
    ClassificationFacet, ContractsFacet
)
from .metrics_adapter import find_metrics_row, attach_metrics as attach_metrics_fn
from .otel import TracingContext, get_prometheus_metrics
from ..utils.ids import generate_run_id, generate_job_name
from ..utils.time import now_iso_zulu
from ..utils.logging import get_logger
from ..config import settings

logger = get_logger(__name__)

# Context variables for tracking current flow and step
_current_flow: ContextVar[Optional[FlowRegistry]] = ContextVar("nbx_current_flow", default=None)
_current_step: ContextVar[Optional["Step"]] = ContextVar("nbx_current_step", default=None)

# Component types
COMPONENT_TYPES = [
    "DataLoader", "Transformer", "Reconciliator", "Enricher", 
    "Exporter", "QualityCheck", "Splitter", "Merger", 
    "Orchestrator", "Other"
]

class Flow(AbstractContextManager):
    """Context manager for tracking a complete flow execution."""
    
    def __init__(self, name: str, run_id: Optional[str] = None, 
                 auto_export: bool = True, export_path: Optional[str] = None):
        self.name = name
        self.run_id = run_id or generate_run_id()
        self.auto_export = auto_export
        self.export_path = export_path
        self.registry = FlowRegistry(name=name, run_id=self.run_id)
        self._token = None
        self._cleanup_registered = False
        
        # Update Prometheus metrics
        prom_metrics = get_prometheus_metrics()
        if 'active_flows' in prom_metrics:
            prom_metrics['active_flows'].inc()
    
    def __enter__(self) -> FlowRegistry:
        self._token = _current_flow.set(self.registry)
        self.registry.started_at = now_iso_zulu()
        
        # Register cleanup function
        if self.auto_export and not self._cleanup_registered:
            atexit.register(self._export_on_exit)
            self._cleanup_registered = True
        
        logger.info(f"Started flow: {self.name} (run_id: {self.run_id})")
        return self.registry
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        try:
            # Update registry status
            self.registry.finished_at = now_iso_zulu()
            if exc_type is None:
                self.registry.status = "SUCCESS"
            else:
                self.registry.status = "FAILED"
                logger.error(f"Flow {self.name} failed: {exc_val}")
            
            # Update Prometheus metrics
            prom_metrics = get_prometheus_metrics()
            if 'active_flows' in prom_metrics:
                prom_metrics['active_flows'].dec()
            
            logger.info(f"Completed flow: {self.name} with status: {self.registry.status}")
            
        finally:
            if self._token:
                _current_flow.reset(self._token)
        
        return False  # Don't suppress exceptions
    
    def _export_on_exit(self):
        """Export flow registry on exit."""
        try:
            if not self.export_path:
                self.export_path = f"{self.name}_flow_{self.run_id}.json"
            
            self.registry.to_json(self.export_path)
            logger.info(f"Flow registry exported to: {self.export_path}")
        except Exception as e:
            logger.error(f"Failed to export flow registry: {e}")

class Step(AbstractContextManager):
    """Context manager for tracking individual step execution."""
    
    def __init__(self, name: str, component_type: str = "Other", 
                 job_namespace: Optional[str] = None, tags: Optional[Dict[str, Any]] = None):
        
        if component_type not in COMPONENT_TYPES and component_type != "auto":
            raise ValueError(f"Invalid component_type: {component_type}. Must be one of: {COMPONENT_TYPES}")
        
        self.name = name
        self.component_type = component_type
        self.job_namespace = job_namespace or settings.ol_namespace
        self.tags = tags or {}
        self.run_id = generate_run_id()
        
        # I/O tracking
        self.inputs: List[DatasetRef] = []
        self.outputs: List[DatasetRef] = []
        self.contracts: List[Dict[str, Any]] = []
        
        # Timing and context
        self._start_time = None
        self._tracing_context = None
        self._step_token = None
        
        # Get current flow
        self.flow_registry = _current_flow.get()
        
        # OpenLineage emitter
        self._emitter = get_emitter()
    
    def __enter__(self) -> "Step":
        self._start_time = time.perf_counter()
        self._step_token = _current_step.set(self)
        
        # Start tracing
        trace_attributes = {
            "flow.name": self.flow_registry.name if self.flow_registry else "unknown",
            "step.name": self.name,
            "step.component_type": self.component_type,
            "step.run_id": self.run_id
        }
        trace_attributes.update(self.tags)
        
        self._tracing_context = TracingContext(
            span_name=f"{self.flow_registry.name if self.flow_registry else 'flow'}.{self.name}",
            attributes=trace_attributes
        )
        self._tracing_context.__enter__()
        
        # Add to flow registry
        if self.flow_registry:
            task_spec = TaskSpec(
                name=self.name,
                component_type=self.component_type,
                tags=self.tags,
                started_at=now_iso_zulu(),
                run_id=self.run_id,
                status="RUNNING",
                parent=self.flow_registry._task_stack[-1] if self.flow_registry._task_stack else None
            )
            self.flow_registry.add_task(task_spec)
            self.flow_registry._task_stack.append(self.name)
        
        # Emit OpenLineage START event
        job_name = generate_job_name(
            self.flow_registry.name if self.flow_registry else "unknown_flow", 
            self.name
        )
        
        self._emitter.emit_event(
            event_type="START",
            run_id=self.run_id,
            job_name=job_name,
            inputs=[],
            outputs=[]
        )
        
        # Update Prometheus metrics
        prom_metrics = get_prometheus_metrics()
        if 'active_steps' in prom_metrics:
            prom_metrics['active_steps'].inc()
        
        logger.info(f"Started step: {self.name} (component_type: {self.component_type})")
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        success = exc_type is None
        wall_time = time.perf_counter() - self._start_time if self._start_time else 0
        
        try:
            # Build run facets from metrics and other sources
            run_facets = self._build_run_facets(success, wall_time)
            
            # Emit OpenLineage COMPLETE/FAIL event
            job_name = generate_job_name(
                self.flow_registry.name if self.flow_registry else "unknown_flow", 
                self.name
            )
            
            event_type = "COMPLETE" if success else "FAIL"
            self._emitter.emit_event(
                event_type=event_type,
                run_id=self.run_id,
                job_name=job_name,
                inputs=list(self.inputs),
                outputs=list(self.outputs),
                run_facets=run_facets
            )
            
            # Update flow registry
            if self.flow_registry:
                status = "SUCCESS" if success else "FAILED"
                self.flow_registry.update_task_status(
                    name=self.name,
                    run_id=self.run_id,
                    status=status,
                    inputs=[ds.to_dict() for ds in self.inputs],
                    outputs=[ds.to_dict() for ds in self.outputs],
                    contracts=self.contracts
                )
                
                # Remove from stack
                if (self.flow_registry._task_stack and 
                    self.flow_registry._task_stack[-1] == self.name):
                    self.flow_registry._task_stack.pop()
            
            # Check for missing I/O and warn
            if settings.warn_on_missing_io:
                if not self.inputs:
                    logger.warning(f"Step '{self.name}' has no marked inputs. Use mark_input() to track data lineage.")
                if not self.outputs:
                    logger.warning(f"Step '{self.name}' has no marked outputs. Use mark_output() to track data lineage.")
            
            # Update Prometheus metrics
            prom_metrics = get_prometheus_metrics()
            flow_name = self.flow_registry.name if self.flow_registry else "unknown"
            
            if 'active_steps' in prom_metrics:
                prom_metrics['active_steps'].dec()
            if 'steps_total' in prom_metrics:
                prom_metrics['steps_total'].labels(
                    flow=flow_name, 
                    step=self.name, 
                    status='success' if success else 'failed'
                ).inc()
            if 'step_duration_seconds' in prom_metrics:
                prom_metrics['step_duration_seconds'].labels(
                    flow=flow_name, 
                    step=self.name
                ).observe(wall_time)
            
            status_msg = "succeeded" if success else f"failed: {exc_val}"
            logger.info(f"Step {self.name} {status_msg} (duration: {wall_time:.2f}s)")
            
        except Exception as e:
            logger.error(f"Error in step cleanup: {e}")
        
        finally:
            # Clean up tracing context
            if self._tracing_context:
                self._tracing_context.__exit__(exc_type, exc_val, exc_tb)
            
            # Reset step context
            if self._step_token:
                _current_step.reset(self._step_token)
        
        return False  # Don't suppress exceptions
    
    def _build_run_facets(self, success: bool, wall_time: float) -> Dict[str, Any]:
        """Build run facets from various sources."""
        facets = {}
        
        # Classification facet
        facets["classification"] = ClassificationFacet(
            component_type=self.component_type,
            method="manual" if self.component_type != "auto" else "auto",
            rationale="Manually specified" if self.component_type != "auto" else "Auto-classified"
        )
        
        # Performance facet from metrics
        metrics_row = find_metrics_row(self.name)
        if metrics_row:
            perf_facet = performance_from_metrics_row(metrics_row)
            if perf_facet:
                # Add wall time if not already present
                if perf_facet.wall_time_seconds is None:
                    perf_facet.wall_time_seconds = wall_time
                facets["performance"] = perf_facet
            
            # Reliability facet from metrics
            rel_facet = reliability_from_metrics_row(metrics_row)
            if rel_facet:
                # Update success status
                rel_facet.success = success
                facets["reliability"] = rel_facet
        else:
            # Minimal performance facet with wall time
            from .facets import PerformanceFacet
            facets["performance"] = PerformanceFacet(wall_time_seconds=wall_time)
        
        # Contracts facet
        if self.contracts:
            facets["contracts"] = ContractsFacet(contracts=self.contracts)
        
        return facets
    
    def mark_input(self, dataset: DatasetRef) -> None:
        """Mark a dataset as input to this step."""
        self.inputs.append(dataset)
        logger.debug(f"Marked input dataset: {dataset.namespace}:{dataset.name}")
    
    def mark_output(self, dataset: DatasetRef) -> None:
        """Mark a dataset as output from this step."""
        self.outputs.append(dataset)
        logger.debug(f"Marked output dataset: {dataset.namespace}:{dataset.name}")
    
    def add_contract(self, contract: Dict[str, Any]) -> None:
        """Add a data contract to this step."""
        self.contracts.append(contract)
        logger.debug(f"Added contract: {contract.get('suite', 'unknown')}")

# Factory functions and utilities
def flow(name: str, run_id: Optional[str] = None, auto_export: bool = True, 
         export_path: Optional[str] = None) -> Flow:
    """Create a new flow context."""
    return Flow(name=name, run_id=run_id, auto_export=auto_export, export_path=export_path)

def step(name: str, component_type: str = "Other", 
         job_namespace: Optional[str] = None, tags: Optional[Dict[str, Any]] = None) -> Step:
    """Create a new step context."""
    return Step(name=name, component_type=component_type, 
                job_namespace=job_namespace, tags=tags)

def current_step() -> Optional[Step]:
    """Get the currently active step."""
    return _current_step.get()

def current_flow() -> Optional[FlowRegistry]:
    """Get the currently active flow registry."""
    return _current_flow.get()

# Convenience functions for marking I/O
def mark_input(dataset: DatasetRef) -> None:
    """Mark an input dataset for the current step."""
    current = current_step()
    if current:
        current.mark_input(dataset)
    else:
        logger.warning("mark_input() called outside of step context")

def mark_output(dataset: DatasetRef) -> None:
    """Mark an output dataset for the current step."""
    current = current_step()
    if current:
        current.mark_output(dataset)
    else:
        logger.warning("mark_output() called outside of step context")

def attach_metrics(metrics_obj) -> bool:
    """Attach metrics object to current step."""
    return attach_metrics_fn(metrics_obj)

# Decorator for function-based steps
def task(name: Optional[str] = None, component_type: str = "Other", 
         tags: Optional[Dict[str, Any]] = None):
    """Decorator to convert a function into a step."""
    def decorator(func: Callable) -> Callable:
        def wrapper(*args, **kwargs):
            step_name = name or func.__name__
            with step(step_name, component_type=component_type, tags=tags):
                return func(*args, **kwargs)
        
        # Preserve function metadata
        wrapper.__name__ = func.__name__
        wrapper.__doc__ = func.__doc__
        wrapper.__module__ = func.__module__
        return wrapper
    
    return decorator