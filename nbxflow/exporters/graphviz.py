import os
from typing import Dict, Any, List, Tuple, Optional


def to_mermaid(flow_spec: Dict[str, Any], direction: str = "TD") -> str:
    """
    Convert FlowSpec to Mermaid flowchart syntax.
    
    Args:
        flow_spec: FlowSpec dictionary from nbxflow
        direction: Flowchart direction ("TD", "LR", "BT", "RL")
        
    Returns:
        Mermaid flowchart as string
    """
    tasks = flow_spec.get('tasks', [])
    flow_name = flow_spec.get('flow', 'Unknown Flow')
    
    if not tasks:
        return f"flowchart {direction}\n    Start[No tasks in {flow_name}]"
    
    lines = [f"flowchart {direction}"]
    lines.append(f"    %% Generated from nbxflow FlowSpec: {flow_name}")
    lines.append("")
    
    # Define nodes with styling based on component type
    node_styles = {
        'DataLoader': 'fill:#e1f5fe,stroke:#01579b',
        'Transformer': 'fill:#f3e5f5,stroke:#4a148c',
        'Enricher': 'fill:#e8f5e8,stroke:#1b5e20',
        'Reconciliator': 'fill:#fff3e0,stroke:#e65100',
        'Exporter': 'fill:#fce4ec,stroke:#880e4f',
        'QualityCheck': 'fill:#fffde7,stroke:#f57f17',
        'Splitter': 'fill:#e0f2f1,stroke:#00695c',
        'Merger': 'fill:#f1f8e9,stroke:#33691e',
        'Orchestrator': 'fill:#f5f5f5,stroke:#424242',
        'Other': 'fill:#ffffff,stroke:#757575'
    }
    
    # Add nodes
    for i, task in enumerate(tasks):
        task_name = task.get('name', f'task_{i}')
        component_type = task.get('component_type', 'Other')
        
        # Sanitize node ID
        node_id = _sanitize_mermaid_id(task_name)
        
        # Create node with component type styling
        if component_type == 'DataLoader':
            node_shape = f'{node_id}[("🔽 {task_name}")]'
        elif component_type == 'Transformer':
            node_shape = f'{node_id}["⚙️ {task_name}"]'
        elif component_type == 'Enricher':
            node_shape = f'{node_id}["➕ {task_name}"]'
        elif component_type == 'Reconciliator':
            node_shape = f'{node_id}["🔗 {task_name}"]'
        elif component_type == 'Exporter':
            node_shape = f'{node_id}[("🔼 {task_name}")]'
        elif component_type == 'QualityCheck':
            node_shape = f'{node_id}{"✅ " + task_name + "}"}'
        elif component_type == 'Splitter':
            node_shape = f'{node_id}{"↗️ " + task_name + "}"}'
        elif component_type == 'Merger':
            node_shape = f'{node_id}["↙️ {task_name}"]'
        elif component_type == 'Orchestrator':
            node_shape = f'{node_id}["🎯 {task_name}"]'
        else:
            node_shape = f'{node_id}["{task_name}"]'
        
        lines.append(f"    {node_shape}")
    
    lines.append("")
    
    # Add edges based on dataset dependencies
    edges = _derive_mermaid_edges(tasks)
    if edges:
        lines.append("    %% Dependencies")
        for source, target, label in edges:
            source_id = _sanitize_mermaid_id(source)
            target_id = _sanitize_mermaid_id(target)
            if label:
                lines.append(f"    {source_id} -.->|{label}| {target_id}")
            else:
                lines.append(f"    {source_id} --> {target_id}")
    else:
        # If no dependencies found, create a sequential flow
        lines.append("    %% Sequential flow (no dependencies detected)")
        for i in range(len(tasks) - 1):
            source_id = _sanitize_mermaid_id(tasks[i]['name'])
            target_id = _sanitize_mermaid_id(tasks[i + 1]['name'])
            lines.append(f"    {source_id} --> {target_id}")
    
    lines.append("")
    
    # Add styling
    lines.append("    %% Styling")
    for i, task in enumerate(tasks):
        node_id = _sanitize_mermaid_id(task['name'])
        component_type = task.get('component_type', 'Other')
        style = node_styles.get(component_type, node_styles['Other'])
        lines.append(f"    style {node_id} {style}")
    
    return "\n".join(lines)

def to_graphviz_dot(flow_spec: Dict[str, Any]) -> str:
    """
    Convert FlowSpec to Graphviz DOT format.
    
    Args:
        flow_spec: FlowSpec dictionary from nbxflow
        
    Returns:
        DOT format string
    """
    tasks = flow_spec.get('tasks', [])
    flow_name = flow_spec.get('flow', 'Unknown Flow')
    
    lines = [
        'digraph nbxflow_graph {',
        f'    label="{flow_name}";',
        '    labelloc=t;',
        '    fontsize=16;',
        '    rankdir=TD;',
        '    node [shape=box, style=filled];',
        ''
    ]
    
    # Define color scheme for component types
    colors = {
        'DataLoader': 'lightblue',
        'Transformer': 'lightpink',
        'Enricher': 'lightgreen',
        'Reconciliator': 'orange',
        'Exporter': 'lightcoral',
        'QualityCheck': 'lightyellow',
        'Splitter': 'lightcyan',
        'Merger': 'lightgray',
        'Orchestrator': 'lavender',
        'Other': 'white'
    }
    
    # Add nodes
    for i, task in enumerate(tasks):
        task_name = task.get('name', f'task_{i}')
        component_type = task.get('component_type', 'Other')
        
        # Sanitize node ID for DOT
        node_id = _sanitize_dot_id(task_name)
        color = colors.get(component_type, colors['Other'])
        
        # Add component type icon/prefix
        display_name = f"{component_type}\\n{task_name}"
        
        lines.append(f'    {node_id} [label="{display_name}", fillcolor={color}];')
    
    lines.append('')
    
    # Add edges
    edges = _derive_mermaid_edges(tasks)  # Same logic works for both
    if edges:
        for source, target, label in edges:
            source_id = _sanitize_dot_id(source)
            target_id = _sanitize_dot_id(target)
            if label:
                lines.append(f'    {source_id} -> {target_id} [label="{label}"];')
            else:
                lines.append(f'    {source_id} -> {target_id};')
    else:
        # Sequential edges if no dependencies
        for i in range(len(tasks) - 1):
            source_id = _sanitize_dot_id(tasks[i]['name'])
            target_id = _sanitize_dot_id(tasks[i + 1]['name'])
            lines.append(f'    {source_id} -> {target_id};')
    
    lines.append('}')
    
    return '\n'.join(lines)

def to_ascii_flow(flow_spec: Dict[str, Any]) -> str:
    """
    Convert FlowSpec to simple ASCII flow diagram.
    
    Args:
        flow_spec: FlowSpec dictionary from nbxflow
        
    Returns:
        ASCII art flow diagram
    """
    tasks = flow_spec.get('tasks', [])
    flow_name = flow_spec.get('flow', 'Unknown Flow')
    
    if not tasks:
        return f"Flow: {flow_name}\n(No tasks)"
    
    lines = [
        f"Flow: {flow_name}",
        "=" * (len(flow_name) + 6),
        ""
    ]
    
    # Simple vertical flow
    for i, task in enumerate(tasks):
        task_name = task.get('name', f'task_{i}')
        component_type = task.get('component_type', 'Other')
        
        # Add task box
        box_width = max(len(task_name), len(component_type)) + 4
        box_line = "+" + "-" * (box_width - 2) + "+"
        name_line = f"|{task_name:^{box_width - 2}}|"
        type_line = f"|{component_type:^{box_width - 2}}|"
        
        lines.extend([box_line, name_line, type_line, box_line])
        
        # Add arrow to next task (except for last)
        if i < len(tasks) - 1:
            lines.extend(["    |", "    v", ""])
    
    return '\n'.join(lines)

def _sanitize_mermaid_id(name: str) -> str:
    """Sanitize name for Mermaid node ID."""
    import re
    # Replace spaces and special chars with underscores
    sanitized = re.sub(r'[^a-zA-Z0-9_]', '_', name)
    # Remove multiple consecutive underscores
    sanitized = re.sub(r'_+', '_', sanitized)
    # Remove leading/trailing underscores
    sanitized = sanitized.strip('_')
    return sanitized or 'node'

def _sanitize_dot_id(name: str) -> str:
    """Sanitize name for Graphviz DOT node ID."""
    import re
    # Replace spaces and special chars with underscores
    sanitized = re.sub(r'[^a-zA-Z0-9_]', '_', name)
    # Ensure it starts with a letter
    if sanitized and sanitized[0].isdigit():
        sanitized = f'n_{sanitized}'
    return sanitized or 'node'

def _derive_mermaid_edges(tasks: List[Dict[str, Any]]) -> List[Tuple[str, str, str]]:
    """
    Derive edges for visualization from task dependencies.
    
    Returns:
        List of (source_task, target_task, edge_label) tuples
    """
    edges = []
    dataset_producers = {}
    
    # Map datasets to their producing tasks
    for task in tasks:
        task_name = task.get('name', 'unknown')
        for output in task.get('outputs', []):
            dataset_key = f"{output.get('namespace', '')}:{output.get('name', '')}"
            dataset_producers[dataset_key] = task_name
    
    # Find dependencies
    for task in tasks:
        task_name = task.get('name', 'unknown')
        for input_ds in task.get('inputs', []):
            dataset_key = f"{input_ds.get('namespace', '')}:{input_ds.get('name', '')}"
            if dataset_key in dataset_producers:
                upstream_task = dataset_producers[dataset_key]
                if upstream_task != task_name:  # Avoid self-loops
                    # Create edge label from dataset name
                    dataset_name = input_ds.get('name', '').split('/')[-1]  # Get last part of path
                    if len(dataset_name) > 20:
                        dataset_name = dataset_name[:17] + "..."
                    edges.append((upstream_task, task_name, dataset_name))
    
    return edges

def export_visualization(flow_spec: Dict[str, Any], format: str, output_path: str) -> str:
    """
    Export flow visualization in the specified format.
    
    Args:
        flow_spec: FlowSpec dictionary from nbxflow
        format: Output format ("mermaid", "dot", "ascii")
        output_path: Path to write the output file
        
    Returns:
        Path to the generated file
    """
    if format == "mermaid":
        content = to_mermaid(flow_spec)
    elif format == "dot":
        content = to_graphviz_dot(flow_spec)
    elif format == "ascii":
        content = to_ascii_flow(flow_spec)
    else:
        raise ValueError(f"Unsupported visualization format: {format}")
    
    # Write to file
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    with open(output_path, 'w') as f:
        f.write(content)
    
    return output_path