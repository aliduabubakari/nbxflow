from typing import Optional, Dict, Any, List
import importlib
import sys

from ..utils.logging import get_logger

logger = get_logger(__name__)

def attach_metrics(metrics_obj) -> bool:
    """
    Attach a metrics object to the current step.
    This is called by users inside their Metrics context.
    """
    # This function serves as a marker that the user wants to link
    # their Metrics object to nbxflow. The actual linking happens
    # when we look up metrics by operation name.
    return True

def find_metrics_row(operation_name: str) -> Optional[Dict[str, Any]]:
    """
    Find metrics row for a given operation name.
    This looks for a Metrics class with an all_metrics class attribute.
    """
    try:
        # Try to find Metrics class in __main__ module (notebook context)
        main_module = sys.modules.get('__main__')
        if main_module and hasattr(main_module, 'Metrics'):
            metrics_class = getattr(main_module, 'Metrics')
            if hasattr(metrics_class, 'all_metrics'):
                all_metrics = getattr(metrics_class, 'all_metrics')
                if isinstance(all_metrics, list):
                    # Find the most recent metrics row for this operation
                    matching_rows = [
                        row for row in all_metrics 
                        if isinstance(row, dict) and row.get('operation') == operation_name
                    ]
                    if matching_rows:
                        return matching_rows[-1]  # Return most recent
        
        # Try alternative locations
        for module_name in ['metrics', 'utils.metrics', 'common.metrics']:
            try:
                module = importlib.import_module(module_name)
                if hasattr(module, 'Metrics'):
                    metrics_class = getattr(module, 'Metrics')
                    if hasattr(metrics_class, 'all_metrics'):
                        all_metrics = getattr(metrics_class, 'all_metrics')
                        if isinstance(all_metrics, list):
                            matching_rows = [
                                row for row in all_metrics 
                                if isinstance(row, dict) and row.get('operation') == operation_name
                            ]
                            if matching_rows:
                                return matching_rows[-1]
            except ImportError:
                continue
        
        logger.debug(f"No metrics found for operation: {operation_name}")
        return None
        
    except Exception as e:
        logger.warning(f"Error finding metrics for operation {operation_name}: {e}")
        return None

def get_all_metrics() -> List[Dict[str, Any]]:
    """Get all metrics rows."""
    try:
        main_module = sys.modules.get('__main__')
        if main_module and hasattr(main_module, 'Metrics'):
            metrics_class = getattr(main_module, 'Metrics')
            if hasattr(metrics_class, 'all_metrics'):
                all_metrics = getattr(metrics_class, 'all_metrics')
                if isinstance(all_metrics, list):
                    return all_metrics
        return []
    except Exception as e:
        logger.warning(f"Error getting all metrics: {e}")
        return []

def extract_metrics_summary(operation_name: str) -> Dict[str, Any]:
    """Extract key metrics for an operation."""
    row = find_metrics_row(operation_name)
    if not row:
        return {}
    
    summary = {}
    
    # Performance metrics
    perf_fields = [
        'wall_time_seconds', 'cpu_hours', 'memory_delta_mb', 
        'throughput_records_per_sec', 'input_tokens', 'output_tokens', 'llm_cost'
    ]
    for field in perf_fields:
        if field in row:
            summary[field] = row[field]
    
    # Reliability metrics
    reliability_fields = [
        'reliability_attempts', 'reliability_retries', 'reliability_succeeded_after_retry',
        'reliability_success', 'reliability_mttr_seconds', 'reliability_wasted_seconds'
    ]
    for field in reliability_fields:
        if field in row:
            summary[field] = row[field]
    
    # Additional fields
    for key, value in row.items():
        if key not in summary and key != 'operation':
            # Include other numeric or simple values
            if isinstance(value, (int, float, bool, str)):
                summary[key] = value
    
    return summary