from typing import Dict, Any, List, Optional
import os
from datetime import datetime

from ..utils.logging import get_logger

logger = get_logger(__name__)

DAGSTER_ASSETS_TEMPLATE = '''"""
Generated Dagster assets from nbxflow FlowSpec
Flow: {flow_name}
Generated at: {generated_at}
"""

from dagster import (
    asset, AssetIn, AssetOut, multi_asset, 
    get_dagster_logger, DagsterEventType,
    MetadataValue, MaterializeResult
)
from typing import Dict, Any, List
import json

{asset_definitions}

# Asset group for the entire flow
{flow_name}_assets = [
{asset_list}
]
'''

DAGSTER_ASSET_TEMPLATE = '''
@asset(
    name="{asset_name}",
    description="{description}",
    group_name="{group_name}",
    metadata={{
        "component_type": "{component_type}",
        "original_task_name": "{original_name}",
        "nbxflow_generated": True,
        "inputs_count": {input_count},
        "outputs_count": {output_count}
    }},
    ins={asset_ins},
    tags={tags}
)
def {asset_function_name}({function_params}) -> Dict[str, Any]:
    """
    Asset: {original_name}
    Component Type: {component_type}
    
    Generated from nbxflow task with {input_count} inputs and {output_count} outputs.
    """
    logger = get_dagster_logger()
    
    # Asset metadata
    asset_info = {{
        'name': '{original_name}',
        'component_type': '{component_type}',
        'inputs': {inputs},
        'outputs': {outputs},
        'contracts': {contracts}
    }}
    
    logger.info(f"Materializing asset: {{asset_info['name']}}")
    logger.info(f"Component type: {{asset_info['component_type']}}")
    
    # TODO: Implement actual asset logic here
    # This is a placeholder - replace with your actual implementation
    
    # Example processing based on component type
    result_metadata = {{}}
    
    if asset_info['component_type'] == 'DataLoader':
        logger.info("Simulating data loading...")
        # Add your data loading logic here
        records_loaded = 1000  # Placeholder
        result_metadata["records_loaded"] = MetadataValue.int(records_loaded)
        result = {{'status': 'loaded', 'records': records_loaded}}
    
    elif asset_info['component_type'] == 'Transformer':
        logger.info("Simulating data transformation...")
        # Add your transformation logic here
        records_processed = 1000  # Placeholder
        result_metadata["records_processed"] = MetadataValue.int(records_processed)
        result = {{'status': 'transformed', 'records_processed': records_processed}}
    
    elif asset_info['component_type'] == 'Enricher':
        logger.info("Simulating data enrichment...")
        # Add your enrichment logic here
        api_calls = 100  # Placeholder
        result_metadata["api_calls_made"] = MetadataValue.int(api_calls)
        result = {{'status': 'enriched', 'api_calls': api_calls}}
    
    elif asset_info['component_type'] == 'QualityCheck':
        logger.info("Simulating data quality check...")
        # Add your quality check logic here
        passed_checks = 15  # Placeholder
        total_checks = 20
        result_metadata["checks_passed"] = MetadataValue.int(passed_checks)
        result_metadata["total_checks"] = MetadataValue.int(total_checks)
        result = {{'status': 'validated', 'passed': passed_checks, 'total': total_checks}}
    
    else:
        logger.info(f"Generic processing for {{asset_info['component_type']}}...")
        result = {{'status': 'completed'}}
    
    # Add execution metadata
    result_metadata["execution_time"] = MetadataValue.timestamp(datetime.now().timestamp())
    result_metadata["component_type"] = MetadataValue.text(asset_info['component_type'])
    
    logger.info(f"Asset {{asset_info['name']}} materialized successfully")
    
    return MaterializeResult(
        metadata=result_metadata,
        asset_key="{asset_name}"
    )
'''

def sanitize_asset_name(name: str) -> str:
    """Convert name to valid Dagster asset name."""
    import re
    # Replace spaces and special chars with underscores
    sanitized = re.sub(r'[^a-zA-Z0-9_]', '_', name.lower())
    # Remove multiple consecutive underscores
    sanitized = re.sub(r'_+', '_', sanitized)
    # Remove leading/trailing underscores
    sanitized = sanitized.strip('_')
    # Ensure it doesn't start with a number
    if sanitized and sanitized[0].isdigit():
        sanitized = f'asset_{sanitized}'
    return sanitized or 'unknown_asset'

def derive_asset_dependencies(tasks: List[Dict[str, Any]]) -> Dict[str, List[str]]:
    """
    Derive asset dependencies based on dataset inputs/outputs.
    
    Returns:
        Dictionary mapping asset names to their dependencies
    """
    dependencies = {}
    dataset_producers = {}
    
    # Map datasets to their producing assets
    for task in tasks:
        asset_name = sanitize_asset_name(task['name'])
        for output in task.get('outputs', []):
            dataset_key = f"{output.get('namespace', '')}:{output.get('name', '')}"
            dataset_producers[dataset_key] = asset_name
    
    # Find dependencies for each asset
    for task in tasks:
        asset_name = sanitize_asset_name(task['name'])
        asset_dependencies = []
        
        for input_ds in task.get('inputs', []):
            dataset_key = f"{input_ds.get('namespace', '')}:{input_ds.get('name', '')}"
            if dataset_key in dataset_producers:
                upstream_asset = dataset_producers[dataset_key]
                if upstream_asset != asset_name:  # Avoid self-dependencies
                    asset_dependencies.append(upstream_asset)
        
        dependencies[asset_name] = asset_dependencies
    
    return dependencies

def generate_dagster_assets(flow_spec: Dict[str, Any], output_path: str, 
                           group_name: Optional[str] = None) -> str:
    """
    Generate Dagster assets from a FlowSpec.
    
    Args:
        flow_spec: FlowSpec dictionary from nbxflow
        output_path: Path to write the generated assets file
        group_name: Optional asset group name
        
    Returns:
        Path to the generated assets file
    """
    logger.info(f"Generating Dagster assets from FlowSpec: {flow_spec.get('flow', 'unknown')}")
    
    # Extract information from FlowSpec
    flow_name = flow_spec.get('flow', 'unknown_flow')
    tasks = flow_spec.get('tasks', [])
    
    if not tasks:
        logger.warning("No tasks found in FlowSpec")
        return output_path
    
    # Set defaults
    if group_name is None:
        group_name = sanitize_asset_name(flow_name)
    
    # Get asset dependencies
    dependencies = derive_asset_dependencies(tasks)
    
    # Generate asset definitions
    asset_definitions = []
    asset_names = []
    
    for task in tasks:
        original_name = task['name']
        asset_name = sanitize_asset_name(original_name)
        asset_function_name = f"materialize_{asset_name}"
        component_type = task.get('component_type', 'Other')
        
        # Build asset inputs (dependencies)
        task_dependencies = dependencies.get(asset_name, [])
        asset_ins = "{}"
        function_params = ""
        
        if task_dependencies:
            ins_dict = {}
            params = []
            for dep in task_dependencies:
                ins_dict[dep] = f'AssetIn(key="{dep}")'
                params.append(f"{dep}: Dict[str, Any]")
            
            asset_ins = "{" + ", ".join([f'"{k}": {v}' for k, v in ins_dict.items()]) + "}"
            function_params = ", ".join(params)
        
        # Generate asset definition
        asset_def = DAGSTER_ASSET_TEMPLATE.format(
            asset_name=asset_name,
            asset_function_name=asset_function_name,
            original_name=original_name,
            description=f"{component_type} asset converted from nbxflow task: {original_name}",
            group_name=group_name,
            component_type=component_type,
            input_count=len(task.get('inputs', [])),
            output_count=len(task.get('outputs', [])),
            asset_ins=asset_ins,
            function_params=function_params,
            inputs=task.get('inputs', []),
            outputs=task.get('outputs', []),
            contracts=task.get('contracts', []),
            tags=task.get('tags', {})
        )
        
        asset_definitions.append(asset_def)
        asset_names.append(f"    materialize_{asset_name}")
    
    # Generate the complete assets file
    assets_content = DAGSTER_ASSETS_TEMPLATE.format(
        flow_name=group_name,
        generated_at=datetime.now().isoformat(),
        asset_definitions='\n'.join(asset_definitions),
        asset_list=',\n'.join(asset_names)
    )
    
    # Write to file
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    with open(output_path, 'w') as f:
        f.write(assets_content)
    
    logger.info(f"Generated Dagster assets: {output_path}")
    logger.info(f"Assets: {len(tasks)}, Dependencies: {sum(len(deps) for deps in dependencies.values())}")
    
    return output_path

def validate_dagster_export(flow_spec: Dict[str, Any]) -> List[str]:
    """
    Validate a FlowSpec for Dagster export and return any warnings.
    
    Returns:
        List of validation warnings
    """
    warnings = []
    
    tasks = flow_spec.get('tasks', [])
    if not tasks:
        warnings.append("No tasks found in FlowSpec")
        return warnings
    
    # Check for invalid asset names
    asset_names = set()
    for task in tasks:
        name = task.get('name', '')
        sanitized = sanitize_asset_name(name)
        
        if sanitized != name.lower().replace(' ', '_'):
            warnings.append(f"Asset name '{name}' will be sanitized to '{sanitized}'")
        
        if sanitized in asset_names:
            warnings.append(f"Duplicate asset name after sanitization: '{sanitized}'")
        asset_names.add(sanitized)
    
    # Check for assets without clear materialization
    for task in tasks:
        component_type = task.get('component_type', 'Other')
        outputs = task.get('outputs', [])
        
        if component_type in ['QualityCheck', 'Orchestrator'] and not outputs:
            warnings.append(f"Asset '{task['name']}' of type {component_type} might not produce materializable outputs")
    
    # Check dependency complexity
    dependencies = derive_asset_dependencies(tasks)
    max_dependencies = max(len(deps) for deps in dependencies.values()) if dependencies else 0
    
    if max_dependencies > 5:
        warnings.append(f"Some assets have many dependencies (max: {max_dependencies}), consider breaking them down")
    
    # Check for very complex flows
    if len(tasks) > 20:
        warnings.append(f"Large number of assets ({len(tasks)}), consider organizing into multiple asset groups")
    
    return warnings

def generate_dagster_job(flow_spec: Dict[str, Any], assets_module: str = "assets") -> str:
    """
    Generate a Dagster job definition for the assets.
    
    Returns:
        String containing the job definition
    """
    flow_name = flow_spec.get('flow', 'unknown_flow')
    sanitized_name = sanitize_asset_name(flow_name)
    
    job_template = f'''
from dagster import define_asset_job, AssetSelection
from .{assets_module} import {sanitized_name}_assets

# Job to materialize all assets in the flow
{sanitized_name}_job = define_asset_job(
    name="{sanitized_name}_job",
    description="Job to materialize all assets from nbxflow FlowSpec: {flow_name}",
    selection=AssetSelection.assets(*{sanitized_name}_assets),
    tags={{"nbxflow": "generated", "flow": "{flow_name}"}}
)
'''
    
    return job_template