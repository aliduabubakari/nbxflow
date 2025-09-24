from typing import Dict, Any, List, Tuple, Optional
import os
from datetime import datetime

from ..utils.logging import get_logger

logger = get_logger(__name__)

AIRFLOW_DAG_TEMPLATE = '''"""
Generated Airflow DAG from nbxflow FlowSpec
Flow: {flow_name}
Generated at: {generated_at}
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.decorators import task
from airflow.operators.python import PythonOperator
import json

# Default arguments for the DAG
default_args = {{
    'owner': 'nbxflow',
    'depends_on_past': False,
    'start_date': datetime({start_year}, {start_month}, {start_day}),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}}

# Create the DAG
dag = DAG(
    dag_id='{dag_id}',
    default_args=default_args,
    description='Generated from nbxflow: {flow_description}',
    schedule_interval={schedule_interval},
    catchup=False,
    max_active_runs=1,
    tags=['nbxflow', 'generated'],
)

{task_functions}

# Task instances
{task_instances}

# Dependencies
{dependencies}
'''

AIRFLOW_TASK_TEMPLATE = '''
@task(dag=dag)
def {task_name}_task(**context):
    """
    Task: {original_name}
    Component Type: {component_type}
    """
    import logging
    
    logger = logging.getLogger(__name__)
    
    # Task metadata
    task_info = {{
        'name': '{original_name}',
        'component_type': '{component_type}',
        'inputs': {inputs},
        'outputs': {outputs},
        'contracts': {contracts},
        'tags': {tags}
    }}
    
    logger.info(f"Starting task: {{task_info['name']}}")
    logger.info(f"Component type: {{task_info['component_type']}}")
    logger.info(f"Inputs: {{task_info['inputs']}}")
    logger.info(f"Outputs: {{task_info['outputs']}}")
    
    # TODO: Implement actual task logic here
    # This is a placeholder - replace with your actual task implementation
    
    # Example of how to access inputs/outputs:
    # for input_ds in task_info['inputs']:
    #     logger.info(f"Processing input: {{input_ds['namespace']}}:{{input_ds['name']}}")
    
    # Example of basic data processing:
    result = {{
        'status': 'success',
        'processed_inputs': len(task_info['inputs']),
        'generated_outputs': len(task_info['outputs']),
        'execution_time': context.get('ts'),
    }}
    
    logger.info(f"Task {{task_info['name']}} completed successfully")
    return result
'''

def sanitize_task_name(name: str) -> str:
    """Convert task name to valid Python identifier."""
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

def derive_task_dependencies(tasks: List[Dict[str, Any]]) -> List[Tuple[str, str]]:
    """
    Derive dependencies between tasks based on dataset inputs/outputs.
    
    Returns:
        List of (upstream_task, downstream_task) tuples
    """
    dependencies = []
    
    # Create a mapping of datasets to producing tasks
    dataset_producers = {}
    for task in tasks:
        task_name = sanitize_task_name(task['name'])
        for output in task.get('outputs', []):
            dataset_key = f"{output.get('namespace', '')}:{output.get('name', '')}"
            dataset_producers[dataset_key] = task_name
    
    # Find dependencies
    for task in tasks:
        task_name = sanitize_task_name(task['name'])
        for input_ds in task.get('inputs', []):
            dataset_key = f"{input_ds.get('namespace', '')}:{input_ds.get('name', '')}"
            if dataset_key in dataset_producers:
                upstream_task = dataset_producers[dataset_key]
                if upstream_task != task_name:  # Avoid self-dependencies
                    dependencies.append((upstream_task, task_name))
    
    return dependencies

def generate_airflow_dag(flow_spec: Dict[str, Any], output_path: str, 
                        dag_id: Optional[str] = None,
                        schedule_interval: str = "None",
                        start_date: Optional[datetime] = None) -> str:
    """
    Generate an Airflow DAG from a FlowSpec.
    
    Args:
        flow_spec: FlowSpec dictionary from nbxflow
        output_path: Path to write the generated DAG file
        dag_id: Optional DAG ID (defaults to flow name)
        schedule_interval: Airflow schedule interval
        start_date: DAG start date (defaults to today)
        
    Returns:
        Path to the generated DAG file
    """
    logger.info(f"Generating Airflow DAG from FlowSpec: {flow_spec.get('flow', 'unknown')}")
    
    # Extract information from FlowSpec
    flow_name = flow_spec.get('flow', 'unknown_flow')
    tasks = flow_spec.get('tasks', [])
    
    if not tasks:
        logger.warning("No tasks found in FlowSpec")
    
    # Set defaults
    if dag_id is None:
        dag_id = sanitize_task_name(flow_name)
    
    if start_date is None:
        start_date = datetime.now()
    
    # Generate task functions
    task_functions = []
    task_instances = []
    
    for task in tasks:
        original_name = task['name']
        sanitized_name = sanitize_task_name(original_name)
        
        # Generate task function
        task_function = AIRFLOW_TASK_TEMPLATE.format(
            task_name=sanitized_name,
            original_name=original_name,
            component_type=task.get('component_type', 'Other'),
            inputs=task.get('inputs', []),
            outputs=task.get('outputs', []),
            contracts=task.get('contracts', []),
            tags=task.get('tags', {})
        )
        task_functions.append(task_function)
        
        # Generate task instance
        task_instances.append(f"{sanitized_name} = {sanitized_name}_task()")
    
    # Generate dependencies
    dependencies = derive_task_dependencies(tasks)
    dependency_lines = []
    
    if dependencies:
        for upstream, downstream in dependencies:
            dependency_lines.append(f"{upstream} >> {downstream}")
    else:
        # If no dependencies found, chain tasks in order
        if len(tasks) > 1:
            task_names = [sanitize_task_name(task['name']) for task in tasks]
            for i in range(len(task_names) - 1):
                dependency_lines.append(f"{task_names[i]} >> {task_names[i+1]}")
    
    # Generate the complete DAG
    dag_content = AIRFLOW_DAG_TEMPLATE.format(
        flow_name=flow_name,
        generated_at=datetime.now().isoformat(),
        start_year=start_date.year,
        start_month=start_date.month,
        start_day=start_date.day,
        dag_id=dag_id,
        flow_description=f"Converted from nbxflow FlowSpec: {flow_name}",
        schedule_interval=schedule_interval,
        task_functions='\n'.join(task_functions),
        task_instances='\n'.join(task_instances),
        dependencies='\n'.join(dependency_lines) if dependency_lines else '# No dependencies detected'
    )
    
    # Write to file
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    with open(output_path, 'w') as f:
        f.write(dag_content)
    
    logger.info(f"Generated Airflow DAG: {output_path}")
    logger.info(f"Tasks: {len(tasks)}, Dependencies: {len(dependencies)}")
    
    return output_path

def validate_airflow_export(flow_spec: Dict[str, Any]) -> List[str]:
    """
    Validate a FlowSpec for Airflow export and return any warnings.
    
    Returns:
        List of validation warnings
    """
    warnings = []
    
    tasks = flow_spec.get('tasks', [])
    if not tasks:
        warnings.append("No tasks found in FlowSpec")
        return warnings
    
    # Check for invalid task names
    for task in tasks:
        name = task.get('name', '')
        sanitized = sanitize_task_name(name)
        if sanitized != name.lower().replace(' ', '_'):
            warnings.append(f"Task name '{name}' will be sanitized to '{sanitized}'")
    
    # Check for missing I/O on tasks that should have them
    for task in tasks:
        component_type = task.get('component_type', 'Other')
        inputs = task.get('inputs', [])
        outputs = task.get('outputs', [])
        
        if component_type == 'DataLoader' and not outputs:
            warnings.append(f"DataLoader task '{task['name']}' has no outputs")
        elif component_type == 'Exporter' and not inputs:
            warnings.append(f"Exporter task '{task['name']}' has no inputs")
        elif component_type in ['Transformer', 'Enricher'] and not inputs:
            warnings.append(f"{component_type} task '{task['name']}' has no inputs")
    
    # Check for circular dependencies (basic check)
    dependencies = derive_task_dependencies(tasks)
    task_names = {sanitize_task_name(task['name']) for task in tasks}
    
    # Simple cycle detection
    graph = {name: [] for name in task_names}
    for upstream, downstream in dependencies:
        graph[upstream].append(downstream)
    
    # DFS for cycle detection
    visited = set()
    rec_stack = set()
    
    def has_cycle(node):
        if node not in visited:
            visited.add(node)
            rec_stack.add(node)
            
            for neighbor in graph[node]:
                if neighbor not in visited:
                    if has_cycle(neighbor):
                        return True
                elif neighbor in rec_stack:
                    return True
        
        rec_stack.remove(node)
        return False
    
    for node in task_names:
        if has_cycle(node):
            warnings.append("Circular dependency detected in task graph")
            break
    
    return warnings