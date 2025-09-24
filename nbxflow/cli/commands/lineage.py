"""Lineage command implementation."""

import json
import os
from argparse import ArgumentParser, Namespace
from typing import Dict, Any, List

from ...exporters.graphviz import to_mermaid, to_ascii_flow, export_visualization
from ...core.datasets import DatasetRef
from ...utils.logging import get_logger

logger = get_logger(__name__)

class LineageCommand:
    """Command for analyzing and displaying data lineage."""
    
    @staticmethod
    def add_parser(subparsers) -> ArgumentParser:
        """Add the lineage command parser."""
        parser = subparsers.add_parser(
            "lineage",
            help="Analyze and display data lineage",
            description="Show data lineage graphs and analyze dataset dependencies"
        )
        
        parser.add_argument(
            "--flow-json",
            required=True,
            help="Path to FlowSpec JSON file"
        )
        
        parser.add_argument(
            "--format",
            choices=["ascii", "mermaid", "dot", "json"],
            default="ascii",
            help="Output format for lineage graph (default: ascii)"
        )
        
        parser.add_argument(
            "--output", "-o",
            help="Output file path (prints to console if not specified)"
        )
        
        parser.add_argument(
            "--show-datasets",
            action="store_true",
            help="Show detailed dataset information"
        )
        
        parser.add_argument(
            "--show-edges",
            action="store_true", 
            help="Show detailed edge information"
        )
        
        parser.add_argument(
            "--analyze",
            action="store_true",
            help="Perform lineage analysis and show insights"
        )
        
        parser.add_argument(
            "--filter-namespace",
            help="Filter datasets by namespace"
        )
        
        parser.add_argument(
            "--filter-type",
            help="Filter tasks by component type"
        )
        
        return parser
    
    @staticmethod
    def run(args: Namespace) -> int:
        """Execute the lineage command."""
        try:
            # Load FlowSpec
            logger.info(f"Loading FlowSpec from: {args.flow_json}")
            with open(args.flow_json, 'r') as f:
                flow_spec = json.load(f)
            
            # Apply filters
            filtered_spec = LineageCommand._apply_filters(flow_spec, args)
            
            # Generate lineage output
            if args.format == "json":
                output = LineageCommand._generate_json_lineage(filtered_spec, args)
            elif args.format == "ascii":
                output = to_ascii_flow(filtered_spec)
            elif args.format == "mermaid":
                output = to_mermaid(filtered_spec)
            elif args.format == "dot":
                from ...exporters.graphviz import to_graphviz_dot
                output = to_graphviz_dot(filtered_spec)
            else:
                raise ValueError(f"Unsupported format: {args.format}")
            
            # Output results
            if args.output:
                with open(args.output, 'w') as f:
                    f.write(output)
                logger.info(f"Lineage written to: {args.output}")
            else:
                print(output)
            
            # Show analysis if requested
            if args.analyze:
                analysis = LineageCommand._analyze_lineage(filtered_spec)
                print("\n" + "="*50)
                print("LINEAGE ANALYSIS")
                print("="*50)
                print(analysis)
            
            return 0
            
        except FileNotFoundError as e:
            logger.error(f"File not found: {e}")
            return 1
        except json.JSONDecodeError as e:
            logger.error(f"Invalid JSON: {e}")
            return 1
        except Exception as e:
            logger.error(f"Lineage command failed: {e}")
            return 1
    
    @staticmethod
    def _apply_filters(flow_spec: Dict[str, Any], args: Namespace) -> Dict[str, Any]:
        """Apply filters to the FlowSpec."""
        filtered_spec = flow_spec.copy()
        tasks = flow_spec.get('tasks', [])
        filtered_tasks = []
        
        for task in tasks:
            # Filter by component type
            if args.filter_type:
                if task.get('component_type', '').lower() != args.filter_type.lower():
                    continue
            
            # Filter by namespace (check inputs/outputs)
            if args.filter_namespace:
                has_namespace = False
                for dataset in task.get('inputs', []) + task.get('outputs', []):
                    if dataset.get('namespace') == args.filter_namespace:
                        has_namespace = True
                        break
                if not has_namespace:
                    continue
            
            filtered_tasks.append(task)
        
        filtered_spec['tasks'] = filtered_tasks
        
        if len(filtered_tasks) != len(tasks):
            logger.info(f"Applied filters: {len(tasks)} -> {len(filtered_tasks)} tasks")
        
        return filtered_spec
    
    @staticmethod
    def _generate_json_lineage(flow_spec: Dict[str, Any], args: Namespace) -> str:
        """Generate JSON lineage representation."""
        lineage_data = {
            "flow": flow_spec.get('flow', 'unknown'),
            "run_id": flow_spec.get('run_id', 'unknown'),
            "nodes": [],
            "edges": [],
            "datasets": [],
            "summary": {}
        }
        
        tasks = flow_spec.get('tasks', [])
        
        # Extract nodes (tasks)
        for task in tasks:
            node = {
                "id": task.get('name', 'unknown'),
                "type": "task",
                "component_type": task.get('component_type', 'Other'),
                "status": task.get('status', 'unknown'),
                "inputs": len(task.get('inputs', [])),
                "outputs": len(task.get('outputs', []))
            }
            lineage_data["nodes"].append(node)
        
        # Extract datasets if requested
        if args.show_datasets:
            all_datasets = {}
            for task in tasks:
                task_name = task.get('name', 'unknown')
                
                for dataset in task.get('inputs', []):
                    ds_key = f"{dataset.get('namespace', '')}:{dataset.get('name', '')}"
                    if ds_key not in all_datasets:
                        all_datasets[ds_key] = {
                            "namespace": dataset.get('namespace', ''),
                            "name": dataset.get('name', ''),
                            "consumers": [],
                            "producers": []
                        }
                    all_datasets[ds_key]["consumers"].append(task_name)
                
                for dataset in task.get('outputs', []):
                    ds_key = f"{dataset.get('namespace', '')}:{dataset.get('name', '')}"
                    if ds_key not in all_datasets:
                        all_datasets[ds_key] = {
                            "namespace": dataset.get('namespace', ''),
                            "name": dataset.get('name', ''),
                            "consumers": [],
                            "producers": []
                        }
                    all_datasets[ds_key]["producers"].append(task_name)
            
            lineage_data["datasets"] = list(all_datasets.values())
        
        # Extract edges
        dataset_producers = {}
        for task in tasks:
            task_name = task.get('name', 'unknown')
            for output in task.get('outputs', []):
                ds_key = f"{output.get('namespace', '')}:{output.get('name', '')}"
                dataset_producers[ds_key] = task_name
        
        for task in tasks:
            task_name = task.get('name', 'unknown')
            for input_ds in task.get('inputs', []):
                ds_key = f"{input_ds.get('namespace', '')}:{input_ds.get('name', '')}"
                if ds_key in dataset_producers:
                    producer = dataset_producers[ds_key]
                    if producer != task_name:
                        edge = {
                            "source": producer,
                            "target": task_name,
                            "dataset": ds_key,
                            "type": "data_dependency"
                        }
                        if args.show_edges:
                            edge.update({
                                "dataset_namespace": input_ds.get('namespace', ''),
                                "dataset_name": input_ds.get('name', ''),
                                "facets": input_ds.get('facets', {})
                            })
                        lineage_data["edges"].append(edge)
        
        # Add summary
        lineage_data["summary"] = {
            "total_tasks": len(tasks),
            "total_edges": len(lineage_data["edges"]),
            "total_datasets": len(lineage_data.get("datasets", [])),
            "component_types": list(set(task.get('component_type', 'Other') for task in tasks)),
            "namespaces": list(set(
                ds.get('namespace', '') 
                for task in tasks 
                for ds in task.get('inputs', []) + task.get('outputs', [])
            ))
        }
        
        return json.dumps(lineage_data, indent=2)
    
    @staticmethod
    def _analyze_lineage(flow_spec: Dict[str, Any]) -> str:
        """Analyze the lineage and provide insights."""
        tasks = flow_spec.get('tasks', [])
        
        if not tasks:
            return "No tasks found for analysis."
        
        analysis_lines = []
        
        # Basic statistics
        analysis_lines.append(f"📊 BASIC STATISTICS")
        analysis_lines.append(f"Total Tasks: {len(tasks)}")
        
        # Component type distribution
        component_types = {}
        for task in tasks:
            comp_type = task.get('component_type', 'Other')
            component_types[comp_type] = component_types.get(comp_type, 0) + 1
        
        analysis_lines.append(f"Component Types:")
        for comp_type, count in sorted(component_types.items()):
            analysis_lines.append(f"  - {comp_type}: {count}")
        
        # Dataset analysis
        all_datasets = set()
        namespace_stats = {}
        
        for task in tasks:
            for dataset in task.get('inputs', []) + task.get('outputs', []):
                ds_key = f"{dataset.get('namespace', '')}:{dataset.get('name', '')}"
                all_datasets.add(ds_key)
                
                namespace = dataset.get('namespace', 'unknown')
                namespace_stats[namespace] = namespace_stats.get(namespace, 0) + 1
        
        analysis_lines.append(f"Total Datasets: {len(all_datasets)}")
        analysis_lines.append(f"Namespaces:")
        for namespace, count in sorted(namespace_stats.items()):
            analysis_lines.append(f"  - {namespace}: {count} references")
        
        # Dependency analysis
        analysis_lines.append(f"\n🔗 DEPENDENCY ANALYSIS")
        
        # Find source and sink tasks
        has_inputs = set()
        has_outputs = set()
        
        for task in tasks:
            task_name = task.get('name', 'unknown')
            if task.get('inputs'):
                has_inputs.add(task_name)
            if task.get('outputs'):
                has_outputs.add(task_name)
        
        sources = [task['name'] for task in tasks if task['name'] not in has_inputs and task['name'] in has_outputs]
        sinks = [task['name'] for task in tasks if task['name'] in has_inputs and task['name'] not in has_outputs]
        isolated = [task['name'] for task in tasks if task['name'] not in has_inputs and task['name'] not in has_outputs]
        
        analysis_lines.append(f"Source Tasks (no inputs): {len(sources)}")
        if sources:
            for source in sources[:5]:  # Show first 5
                analysis_lines.append(f"  - {source}")
            if len(sources) > 5:
                analysis_lines.append(f"  ... and {len(sources) - 5} more")
        
        analysis_lines.append(f"Sink Tasks (no outputs): {len(sinks)}")
        if sinks:
            for sink in sinks[:5]:
                analysis_lines.append(f"  - {sink}")
            if len(sinks) > 5:
                analysis_lines.append(f"  ... and {len(sinks) - 5} more")
        
        if isolated:
            analysis_lines.append(f"⚠️  Isolated Tasks (no I/O): {len(isolated)}")
            for task in isolated:
                analysis_lines.append(f"  - {task}")
        
        # Flow complexity
        analysis_lines.append(f"\n📈 COMPLEXITY ANALYSIS")
        
        total_edges = 0
        dataset_producers = {}
        
        for task in tasks:
            task_name = task.get('name', 'unknown')
            for output in task.get('outputs', []):
                ds_key = f"{output.get('namespace', '')}:{output.get('name', '')}"
                dataset_producers[ds_key] = task_name
        
        for task in tasks:
            task_name = task.get('name', 'unknown')
            for input_ds in task.get('inputs', []):
                ds_key = f"{input_ds.get('namespace', '')}:{input_ds.get('name', '')}"
                if ds_key in dataset_producers:
                    producer = dataset_producers[ds_key]
                    if producer != task_name:
                        total_edges += 1
        
        analysis_lines.append(f"Total Dependencies: {total_edges}")
        
        # Estimate parallelization potential
        parallelizable_types = {"Transformer", "Enricher", "Reconciliator", "QualityCheck", "Splitter"}
        parallelizable_count = sum(1 for task in tasks if task.get('component_type') in parallelizable_types)
        
        analysis_lines.append(f"Potentially Parallelizable Tasks: {parallelizable_count}/{len(tasks)}")
        
        # Recommendations
        analysis_lines.append(f"\n💡 RECOMMENDATIONS")
        
        if isolated:
            analysis_lines.append(f"⚠️  Fix isolated tasks by adding input/output datasets")
        
        if len(sources) == 0:
            analysis_lines.append(f"⚠️  No clear data source tasks found")
        
        if len(sinks) == 0:
            analysis_lines.append(f"⚠️  No clear data sink tasks found")
        
        if total_edges == 0 and len(tasks) > 1:
            analysis_lines.append(f"⚠️  No dependencies detected - tasks may run in arbitrary order")
        
        if parallelizable_count > len(tasks) * 0.7:
            analysis_lines.append(f"✅ Good parallelization potential ({parallelizable_count} tasks)")
        
        return "\n".join(analysis_lines)