from typing import Dict, Any, List, Optional
import os
from datetime import datetime

from ..utils.logging import get_logger

logger = get_logger(__name__)

PREFECT_FLOW_TEMPLATE = '''"""
Generated Prefect Flow from nbxflow FlowSpec
Flow: {flow_name}
Generated at: {generated_at}
"""

from prefect import flow, task, get_run_logger
from prefect.task_runners import SequentialTaskRunner
from typing import Dict, Any, List
import json

{task_definitions}

@flow(
    name="{flow_name}",
    description="Generated from nbxflow FlowSpec: {flow_description}",
    task_runner=SequentialTaskRunner(),
    retries=1,
    retry_delay_seconds=60,
)
def {flow_function_name}():
    """
    Main flow function for {flow_name}
    
    This flow was generated from an nbxflow FlowSpec and contains
    {task_count} tasks with the following component types:
    {component_summary}
    """
    logger = get_run_logger()
    logger.info("Starting flow: {flow_name}")
    
    # Execute tasks
{task_execution}
    
    logger.info("Flow {flow_name} completed successfully")
    return {{"status": "success", "tasks_executed": {task_count}}}

if __name__ == "__main__":
    # Run the flow
    result = {flow_function_name}()
    print(f"Flow execution result: {{result}}")
'''

PREFECT_TASK_TEMPLATE = '''
@task(
    name="{original_name}",
    description="{component_type} task generated from nbxflow",
    retries=2,
    retry_delay_seconds=30,
    tags=["{component_type_lower}", "nbxflow", "generated"]
)
def {task_function_name}() -> Dict[str, Any]:
    """
    Task: {original_name}
    Component Type: {component_type}
    
    Inputs: {input_count} datasets
    Outputs: {output_count} datasets
    """
    logger = get_run_logger()
    
    # Task metadata
    task_info = {{
        'name': '{original_name}',
        'component_type': '{component_type}',
        'inputs': {inputs},
        'outputs': {outputs},
        'contracts': {contracts},
        'tags': {tags}
    }}
    
    logger.info(f"Executing task: {{task_info['name']}}")
    logger.info(f"Component type: {{task_info['component_type']}}")
    
    # Log inputs and outputs
    if task_info['inputs']:
        logger.info("Input datasets:")
        for i, input_ds in enumerate(task_info['inputs']):
            logger.info(f"  {{i+1}}. {{input_ds.get('namespace', 'unknown')}}:{{input_ds.get('name', 'unknown')}}")
    
    if task_info['outputs']:
        logger.info("Expected output datasets:")
        for i, output_ds in enumerate(task_info['outputs']):
            logger.info(f"  {{i+1}}. {{output_ds.get('namespace', 'unknown')}}:{{output_ds.get('name', 'unknown')}}")
    
    # TODO: Implement actual task logic here
    # This is a placeholder - replace with your actual implementation
    
    # Example task processing based on component type
    if task_info['component_type'] == 'DataLoader':
        logger.info("Simulating data loading...")
        # Add your data loading logic here
        result = {{'status': 'loaded', 'records': 1000}}
    
    elif task_info['component_type'] == 'Transformer':
        logger.info("Simulating data transformation...")
        # Add your transformation logic here
        result = {{'status': 'transformed', 'records_processed': 1000}}
    
    elif task_info['component_type'] == 'Enricher':
        logger.info("Simulating data enrichment...")
        # Add your enrichment logic here
        result = {{'status': 'enriched', 'api_calls': 100}}
    
    elif task_info['component_type'] == 'Exporter':
        logger.info("Simulating data export...")
        # Add your export logic here
        result = {{'status': 'exported', 'records_written': 1000}}
    
    else:
        logger.info(f"Generic processing for {{task_info['component_type']}}...")
        result = {{'status': 'completed'}}
    
    logger.info(f"Task {{task_info['name']}} completed: {{result}}")
    return result
'''

def sanitize_function_name(name: str) -> str:
    """Convert name to valid Python function name."""
    import re
    # Replace spaces and special chars with underscores
    sanitized = re.sub(r'[^a-zA-Z0-9_]', '_', name.lower())
    # Remove multiple consecutive underscores
    sanitized = re.sub(r'_+', '_', sanitized)
    # Remove leading/trailing underscores
    sanitized = sanitized.strip('_')
    # Ensure it doesn't start with a number
    if sanitized and sanitized[0].isdigit():
        sanitized = f'task_{sanitized}'
    return sanitized or 'unknown_task'

def generate_task_execution_order(tasks: List[Dict[str, Any]]) -> List[str]:
    """
    Generate the execution order for tasks based on dependencies.
    
    Returns:
        List of task execution statements in order
    """
    execution_lines = []
    
    # For now, execute tasks in the order they appear in the FlowSpec
    # TODO: Implement proper dependency resolution
    
    task_results = []
    
    for i, task in enumerate(tasks):
        task_name = sanitize_function_name(task['name'])
        result_var = f"{task_name}_result"
        
        execution_lines.append(f"    {result_var} = {task_name}()")
        task_results.append(result_var)
    
    # Add a summary line
    if task_results:
        results_dict = ", ".join([f'"{tasks[i]["name"]}": {result}' for i, result in enumerate(task_results)])
        execution_lines.append(f"    results = {{{results_dict}}}")
        execution_lines.append(f'    logger.info(f"All tasks completed: {{list(results.keys())}}")')
    
    return execution_lines

def generate_prefect_flow(flow_spec: Dict[str, Any], output_path: str, 
                         flow_name: Optional[str] = None) -> str:
    """
    Generate a Prefect flow from a FlowSpec.
    
    Args:
        flow_spec: FlowSpec dictionary from nbxflow
        output_path: Path to write the generated flow file
        flow_name: Optional flow name override
        
    Returns:
        Path to the generated flow file
    """
    logger.info(f"Generating Prefect flow from FlowSpec: {flow_spec.get('flow', 'unknown')}")
    
    # Extract information from FlowSpec
    original_flow_name = flow_spec.get('flow', 'unknown_flow')
    tasks = flow_spec.get('tasks', [])
    
    if not tasks:
        logger.warning("No tasks found in FlowSpec")
    
    # Set defaults
    if flow_name is None:
        flow_name = original_flow_name
    
    flow_function_name = sanitize_function_name(flow_name)
    
    # Generate component type summary
    component_types = {}
    for task in tasks:
        comp_type = task.get('component_type', 'Other')
        component_types[comp_type] = component_types.get(comp_type, 0) + 1
    
    component_summary = ", ".join([f"{count} {comp_type}" for comp_type, count in component_types.items()])
    
    # Generate task definitions
    task_definitions = []
    
    for task in tasks:
        original_name = task['name']
        task_function_name = sanitize_function_name(original_name)
        component_type = task.get('component_type', 'Other')
        
        task_def = PREFECT_TASK_TEMPLATE.format(
            task_function_name=task_function_name,
            original_name=original_name,
            component_type=component_type,
            component_type_lower=component_type.lower(),
            input_count=len(task.get('inputs', [])),
            output_count=len(task.get('outputs', [])),
            inputs=task.get('inputs', []),
            outputs=task.get('outputs', []),
            contracts=task.get('contracts', []),
            tags=task.get('tags', {})
        )
        task_definitions.append(task_def)
    
    # Generate task execution order
    task_execution_lines = generate_task_execution_order(tasks)
    task_execution = '\n'.join(task_execution_lines) if task_execution_lines else "    pass"
    
    # Generate the complete flow
    flow_content = PREFECT_FLOW_TEMPLATE.format(
        flow_name=flow_name,
        generated_at=datetime.now().isoformat(),
        flow_function_name=flow_function_name,
        flow_description=f"Converted from nbxflow FlowSpec: {original_flow_name}",
        task_count=len(tasks),
        component_summary=component_summary or "No tasks",
        task_definitions='\n'.join(task_definitions),
        task_execution=task_execution
    )
    
    # Write to file
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    with open(output_path, 'w') as f:
        f.write(flow_content)
    
    logger.info(f"Generated Prefect flow: {output_path}")
    logger.info(f"Tasks: {len(tasks)}, Component types: {len(component_types)}")
    
    return output_path

def validate_prefect_export(flow_spec: Dict[str, Any]) -> List[str]:
    """
    Validate a FlowSpec for Prefect export and return any warnings.
    
    Returns:
        List of validation warnings
    """
    warnings = []
    
    tasks = flow_spec.get('tasks', [])
    if not tasks:
        warnings.append("No tasks found in FlowSpec")
        return warnings
    
    # Check for invalid function names
    function_names = set()
    for task in tasks:
        name = task.get('name', '')
        sanitized = sanitize_function_name(name)
        
        if sanitized != name.lower().replace(' ', '_'):
            warnings.append(f"Task name '{name}' will be sanitized to '{sanitized}'")
        
        if sanitized in function_names:
            warnings.append(f"Duplicate function name after sanitization: '{sanitized}'")
        function_names.add(sanitized)
    
    # Check for tasks without clear data flow
    isolated_tasks = []
    for task in tasks:
        inputs = task.get('inputs', [])
        outputs = task.get('outputs', [])
        component_type = task.get('component_type', 'Other')
        
        # Warn about tasks that might be isolated
        if not inputs and not outputs and component_type not in ['Orchestrator', 'QualityCheck']:
            isolated_tasks.append(task['name'])
    
    if isolated_tasks:
        warnings.append(f"Tasks with no inputs or outputs: {', '.join(isolated_tasks)}")
    
    # Check for very long task names
    for task in tasks:
        if len(task.get('name', '')) > 50:
            warnings.append(f"Task name is very long: '{task['name'][:30]}...'")
    
    return warnings

def estimate_prefect_resources(flow_spec: Dict[str, Any]) -> Dict[str, Any]:
    """
    Estimate resource requirements for the Prefect flow.
    
    Returns:
        Dictionary with resource estimates
    """
    tasks = flow_spec.get('tasks', [])
    
    # Categorize tasks by type for resource estimation
    resource_intensive_types = ['DataLoader', 'Transformer', 'Enricher']
    io_intensive_types = ['DataLoader', 'Exporter']
    api_intensive_types = ['Enricher', 'Reconciliator']
    
    resource_estimate = {
        'total_tasks': len(tasks),
        'estimated_cpu_intensive': 0,
        'estimated_io_intensive': 0,
        'estimated_api_calls': 0,
        'recommended_concurrency': 1,
        'estimated_runtime_minutes': 0
    }
    
    for task in tasks:
        comp_type = task.get('component_type', 'Other')
        
        if comp_type in resource_intensive_types:
            resource_estimate['estimated_cpu_intensive'] += 1
        
        if comp_type in io_intensive_types:
            resource_estimate['estimated_io_intensive'] += 1
        
        if comp_type in api_intensive_types:
            resource_estimate['estimated_api_calls'] += 1
        
        # Estimate runtime based on component type
        if comp_type == 'DataLoader':
            resource_estimate['estimated_runtime_minutes'] += 5
        elif comp_type in ['Transformer', 'Enricher']:
            resource_estimate['estimated_runtime_minutes'] += 10
        elif comp_type == 'Exporter':
            resource_estimate['estimated_runtime_minutes'] += 3
        else:
            resource_estimate['estimated_runtime_minutes'] += 2
    
    # Recommend concurrency based on task types
    if resource_estimate['estimated_api_calls'] > 3:
        resource_estimate['recommended_concurrency'] = min(5, resource_estimate['estimated_api_calls'])
    elif len(tasks) > 5:
        resource_estimate['recommended_concurrency'] = min(3, len(tasks) // 2)
    
    return resource_estimate