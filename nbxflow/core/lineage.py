from typing import Dict, List, Optional, Any
from datetime import datetime, timezone
import json

from ..config import settings
from ..utils.logging import get_logger
from .datasets import DatasetRef

logger = get_logger(__name__)

class OpenLineageEmitter:
    """Emits OpenLineage events."""
    
    def __init__(self, namespace: Optional[str] = None, url: Optional[str] = None):
        self.namespace = namespace or settings.ol_namespace
        self.url = url or settings.ol_url
        self.client = None
        self._init_client()
    
    def _init_client(self):
        """Initialize OpenLineage client if available and configured."""
        if not self.url:
            logger.debug("OpenLineage URL not configured, events will be logged only")
            return
        
        try:
            from openlineage.client import OpenLineageClient
            from openlineage.client.transport import HttpTransport, HttpConfig
            
            # Create transport with optional API key
            transport_config = HttpConfig(
                url=self.url,
                auth_token=settings.ol_api_key if settings.ol_api_key else None
            )
            transport = HttpTransport(transport_config)
            self.client = OpenLineageClient(transport=transport)
            logger.info(f"OpenLineage client initialized for {self.url}")
            
        except ImportError:
            logger.warning("OpenLineage client not available. Install with: pip install 'nbxflow[ol]'")
        except Exception as e:
            logger.error(f"Failed to initialize OpenLineage client: {e}")
    
    def emit_event(self, 
                   event_type: str,
                   run_id: str,
                   job_name: str,
                   inputs: Optional[List[DatasetRef]] = None,
                   outputs: Optional[List[DatasetRef]] = None,
                   run_facets: Optional[Dict[str, Any]] = None,
                   job_facets: Optional[Dict[str, Any]] = None) -> bool:
        """Emit an OpenLineage event."""
        
        event_time = datetime.now(timezone.utc).isoformat()
        
        # Convert datasets to OpenLineage format
        ol_inputs = []
        if inputs:
            for ds in inputs:
                ol_inputs.append(ds.to_openlineage_dataset())
        
        ol_outputs = []
        if outputs:
            for ds in outputs:
                ol_outputs.append(ds.to_openlineage_dataset())
        
        # Create event data
        event_data = {
            "eventType": event_type,
            "eventTime": event_time,
            "run": {
                "runId": run_id,
                "facets": self._prepare_facets(run_facets)
            },
            "job": {
                "namespace": self.namespace,
                "name": job_name,
                "facets": self._prepare_facets(job_facets)
            },
            "inputs": ol_inputs if ol_inputs else [],
            "outputs": ol_outputs if ol_outputs else [],
            "producer": f"nbxflow/{settings.__version__}" if hasattr(settings, '__version__') else "nbxflow/0.1.0",
            "schemaURL": "https://openlineage.io/spec/1-0-5/OpenLineage.json#/definitions/RunEvent"
        }
        
        # Log the event
        logger.debug(f"OpenLineage event: {event_type} for job {job_name}")
        
        # Emit via client if available
        if self.client:
            try:
                self.client.emit(self._create_run_event(event_data))
                return True
            except Exception as e:
                logger.error(f"Failed to emit OpenLineage event: {e}")
                return False
        else:
            # Fallback: log as JSON
            logger.info(f"OpenLineage event (no client): {json.dumps(event_data, indent=2)}")
            return True
    
    def _prepare_facets(self, facets: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
        """Prepare facets for OpenLineage emission."""
        if not facets:
            return None
        
        prepared = {}
        for key, value in facets.items():
            if hasattr(value, 'to_dict'):
                # Handle our custom facet classes
                facet_data = value.to_dict()
                facet_data["_producer"] = "https://github.com/your-org/nbxflow"
                facet_data["_schemaURL"] = f"https://github.com/your-org/nbxflow/facets/{key}"
                prepared[key] = facet_data
            elif isinstance(value, dict):
                prepared[key] = value
            else:
                # Try to serialize simple values
                try:
                    prepared[key] = {"value": value, "_producer": "https://github.com/your-org/nbxflow"}
                except:
                    logger.warning(f"Could not serialize facet {key}: {value}")
        
        return prepared if prepared else None
    
    def _create_run_event(self, event_data: Dict[str, Any]):
        """Create OpenLineage RunEvent object if client is available."""
        try:
            from openlineage.client.event_v2 import RunEvent, Run, Job, RunState
            from openlineage.client.uuid import generate_new_uuid
            
            # Map event type to RunState
            state_map = {
                "START": RunState.START,
                "RUNNING": RunState.RUNNING,
                "COMPLETE": RunState.COMPLETE,
                "ABORT": RunState.ABORT,
                "FAIL": RunState.FAIL
            }
            
            event_type = state_map.get(event_data["eventType"], RunState.COMPLETE)
            
            return RunEvent(
                eventType=event_type,
                eventTime=event_data["eventTime"],
                run=Run(runId=event_data["run"]["runId"], facets=event_data["run"]["facets"]),
                job=Job(namespace=event_data["job"]["namespace"], name=event_data["job"]["name"], facets=event_data["job"]["facets"]),
                inputs=event_data["inputs"],
                outputs=event_data["outputs"],
                producer=event_data["producer"],
                schemaURL=event_data["schemaURL"]
            )
        except ImportError:
            # Return dict format for logging
            return event_data

# Global emitter instance
_default_emitter = None

def get_emitter() -> OpenLineageEmitter:
    """Get the default OpenLineage emitter."""
    global _default_emitter
    if _default_emitter is None:
        _default_emitter = OpenLineageEmitter()
    return _default_emitter