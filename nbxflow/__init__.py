"""
nbxflow: Notebook Ops for Dataflow, Taskflow, and PerfFlow

A library for transforming exploratory notebooks into production-ready data pipelines
with comprehensive observability through OpenLineage, OpenTelemetry, and Great Expectations.
"""

from .version import __version__

# Core functionality
from .core.step import (
    flow, step, task, current_step, current_flow,
    mark_input, mark_output, attach_metrics
)

from .core.datasets import (
    DatasetRef, dataset_file, dataset_api, dataset_semt, 
    dataset_from_semt_table, add_schema_facet_from_dataframe
)

from .core.registry import FlowRegistry, TaskSpec

# Contracts
from .contracts.ge import (
    infer_contract_from_dataframe as ge_infer_contract_from_dataframe,
    validate_dataframe as ge_validate_dataframe
)

from .contracts.registry import ContractRegistry

# LLM helpers (optional)
try:
    from .llm.classifier import auto_classify_component, auto_type
    from .llm.refine_contracts import refine_ge_suite as llm_refine_contract
    _has_llm = True
except ImportError:
    _has_llm = False
    auto_classify_component = None
    auto_type = None
    llm_refine_contract = None

# Exporters
from .exporters import (
    generate_airflow_dag, generate_prefect_flow, generate_dagster_assets,
    to_mermaid, export_flow
)

# Configuration
from .config import settings

# Public API
__all__ = [
    # Version
    "__version__",
    
    # Core workflow
    "flow", "step", "task", "current_step", "current_flow",
    "mark_input", "mark_output", "attach_metrics",
    
    # Datasets
    "DatasetRef", "dataset_file", "dataset_api", "dataset_semt",
    "dataset_from_semt_table", "add_schema_facet_from_dataframe",
    
    # Registry
    "FlowRegistry", "TaskSpec",
    
    # Contracts
    "ge_infer_contract_from_dataframe", "ge_validate_dataframe",
    "ContractRegistry",
    
    # LLM helpers (if available)
    "auto_classify_component", "auto_type", "llm_refine_contract",
    
    # Exporters
    "generate_airflow_dag", "generate_prefect_flow", "generate_dagster_assets",
    "to_mermaid", "export_flow",
    
    # Configuration
    "settings",
]

# Convenience functions for common patterns
def quick_step(name: str, inputs=None, outputs=None, component_type="Other"):
    """
    Quick step decorator for simple cases.
    
    Example:
        @nbxflow.quick_step("load_data", component_type="DataLoader")
        def load_data():
            df = pd.read_csv("data.csv")
            nbxflow.mark_output(nbxflow.dataset_file("data.csv"))
            return df
    """
    def decorator(func):
        def wrapper(*args, **kwargs):
            with step(name, component_type=component_type):
                if inputs:
                    for inp in inputs:
                        mark_input(inp)
                
                result = func(*args, **kwargs)
                
                if outputs:
                    for out in outputs:
                        mark_output(out)
                
                return result
        
        wrapper.__name__ = func.__name__
        wrapper.__doc__ = func.__doc__
        return wrapper
    return decorator

def simple_flow(name: str):
    """
    Simple flow context manager that auto-exports on completion.
    
    Example:
        with nbxflow.simple_flow("data_pipeline"):
            # ... your steps ...
            pass
    """
    return flow(name, auto_export=True)

# Add convenience functions to __all__
__all__.extend(["quick_step", "simple_flow"])

# Module-level configuration helpers
def configure_openlineage(url: str, namespace: str = None, api_key: str = None):
    """Configure OpenLineage settings."""
    settings.ol_url = url
    if namespace:
        settings.ol_namespace = namespace
    if api_key:
        settings.ol_api_key = api_key

def configure_llm(provider: str, model: str = None, api_key: str = None, endpoint: str = None):
    """Configure LLM settings."""
    settings.llm_provider = provider
    if model:
        settings.llm_model = model
    if api_key:
        settings.llm_api_key = api_key
    if endpoint:
        settings.llm_endpoint = endpoint

def configure_otel(service_name: str = None, endpoint: str = None, enable_console: bool = None):
    """Configure OpenTelemetry settings."""
    if service_name:
        settings.otel_service_name = service_name
    if endpoint:
        settings.otel_endpoint = endpoint
    if enable_console is not None:
        settings.otel_enable_console = enable_console

# Add configuration helpers to __all__
__all__.extend(["configure_openlineage", "configure_llm", "configure_otel"])

# Version and capability reporting
def get_capabilities():
    """Get information about available capabilities."""
    capabilities = {
        "version": __version__,
        "core": True,
        "openlineage": True,
        "opentelemetry": True,
        "great_expectations": False,
        "llm": _has_llm,
        "prometheus": False,
        "exporters": True
    }
    
    # Check optional dependencies
    try:
        import great_expectations
        capabilities["great_expectations"] = True
    except ImportError:
        pass
    
    try:
        import prometheus_client
        capabilities["prometheus"] = True
    except ImportError:
        pass
    
    return capabilities

def print_capabilities():
    """Print available capabilities in a nice format."""
    caps = get_capabilities()
    
    print(f"nbxflow {caps['version']} - Available Capabilities:")
    print("=" * 50)
    
    status_map = {True: "✅", False: "❌"}
    
    print(f"{status_map[caps['core']]} Core functionality (flow, step, datasets)")
    print(f"{status_map[caps['openlineage']]} OpenLineage integration (dataflow)")
    print(f"{status_map[caps['opentelemetry']]} OpenTelemetry integration (perfflow)")
    print(f"{status_map[caps['great_expectations']]} Great Expectations (contracts)")
    print(f"{status_map[caps['llm']]} LLM helpers (classification, refinement)")
    print(f"{status_map[caps['prometheus']]} Prometheus metrics")
    print(f"{status_map[caps['exporters']]} Flow exporters (Airflow, Prefect, Dagster)")
    
    if not caps['great_expectations']:
        print("\n💡 Install Great Expectations: pip install 'nbxflow[ge]'")
    
    if not caps['llm']:
        print("💡 Install LLM support: pip install 'nbxflow[llm]'")
    
    if not caps['prometheus']:
        print("💡 Install Prometheus support: pip install 'nbxflow[prometheus]'")

__all__.extend(["get_capabilities", "print_capabilities"])

# Backwards compatibility aliases
# These match the original API design
infer_contract_from_dataframe = ge_infer_contract_from_dataframe
validate_dataframe = ge_validate_dataframe

if _has_llm:
    llm_refine_ge_suite = llm_refine_contract

__all__.extend(["infer_contract_from_dataframe", "validate_dataframe"])
if _has_llm:
    __all__.append("llm_refine_ge_suite")