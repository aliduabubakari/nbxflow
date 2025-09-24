"""Export command implementation."""

import json
import os
from argparse import ArgumentParser, Namespace
from typing import Dict, Any

from ...exporters import (
    get_available_exporters, get_available_visualizations,
    export_flow, validate_export, export_visualization
)
from ...core.registry import FlowRegistry
from ...utils.logging import get_logger

logger = get_logger(__name__)

class ExportCommand:
    """Command for exporting FlowSpec to various formats."""
    
    @staticmethod
    def add_parser(subparsers) -> ArgumentParser:
        """Add the export command parser."""
        parser = subparsers.add_parser(
            "export",
            help="Export FlowSpec to orchestration platforms",
            description="Export nbxflow FlowSpec to Airflow, Prefect, Dagster, or visualization formats"
        )
        
        parser.add_argument(
            "--flow-json",
            required=True,
            help="Path to FlowSpec JSON file"
        )
        
        parser.add_argument(
            "--to",
            choices=get_available_exporters() + get_available_visualizations(),
            required=True,
            help="Target format for export"
        )
        
        parser.add_argument(
            "--out",
            required=True,
            help="Output file path"
        )
        
        # Format-specific options
        parser.add_argument(
            "--dag-id",
            help="DAG ID for Airflow export"
        )
        
        parser.add_argument(
            "--schedule",
            default="None",
            help="Schedule interval for Airflow DAG (default: None)"
        )
        
        parser.add_argument(
            "--group-name",
            help="Asset group name for Dagster export"
        )
        
        parser.add_argument(
            "--flow-name",
            help="Flow name override for Prefect export"
        )
        
        parser.add_argument(
            "--validate",
            action="store_true",
            help="Validate FlowSpec before export"
        )
        
        parser.add_argument(
            "--dry-run",
            action="store_true",
            help="Show what would be exported without writing files"
        )
        
        return parser
    
    @staticmethod
    def run(args: Namespace) -> int:
        """Execute the export command."""
        try:
            # Load FlowSpec
            logger.info(f"Loading FlowSpec from: {args.flow_json}")
            flow_spec = ExportCommand._load_flow_spec(args.flow_json)
            
            # Validate if requested
            if args.validate:
                warnings = validate_export(flow_spec, args.to)
                if warnings:
                    logger.warning("Validation warnings:")
                    for warning in warnings:
                        logger.warning(f"  - {warning}")
                    
                    if len(warnings) > 5:
                        response = input("Continue with export? [y/N]: ")
                        if response.lower() != 'y':
                            logger.info("Export cancelled")
                            return 1
            
            # Show dry run info
            if args.dry_run:
                return ExportCommand._dry_run(flow_spec, args)
            
            # Perform export
            output_path = ExportCommand._export_flow(flow_spec, args)
            
            logger.info(f"✅ Export completed successfully!")
            logger.info(f"📁 Output written to: {output_path}")
            
            # Show next steps
            ExportCommand._show_next_steps(args.to, output_path)
            
            return 0
            
        except FileNotFoundError as e:
            logger.error(f"File not found: {e}")
            return 1
        except json.JSONDecodeError as e:
            logger.error(f"Invalid JSON in FlowSpec file: {e}")
            return 1
        except ValueError as e:
            logger.error(f"Validation error: {e}")
            return 1
        except Exception as e:
            logger.error(f"Export failed: {e}")
            return 1
    
    @staticmethod
    def _load_flow_spec(file_path: str) -> Dict[str, Any]:
        """Load FlowSpec from JSON file."""
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"FlowSpec file not found: {file_path}")
        
        with open(file_path, 'r') as f:
            flow_spec = json.load(f)
        
        # Validate basic structure
        if not isinstance(flow_spec, dict):
            raise ValueError("FlowSpec must be a JSON object")
        
        if 'flow' not in flow_spec:
            raise ValueError("FlowSpec missing 'flow' field")
        
        if 'tasks' not in flow_spec:
            raise ValueError("FlowSpec missing 'tasks' field")
        
        logger.info(f"Loaded FlowSpec: {flow_spec['flow']} with {len(flow_spec['tasks'])} tasks")
        return flow_spec
    
    @staticmethod
    def _export_flow(flow_spec: Dict[str, Any], args: Namespace) -> str:
        """Perform the actual export."""
        target_format = args.to
        
        # Check if it's a visualization format
        if target_format in get_available_visualizations():
            return export_visualization(flow_spec, target_format, args.out)
        
        # Otherwise, it's an orchestrator export
        kwargs = {}
        
        # Add format-specific arguments
        if target_format == "airflow":
            if args.dag_id:
                kwargs["dag_id"] = args.dag_id
            kwargs["schedule_interval"] = args.schedule
        elif target_format == "prefect":
            if args.flow_name:
                kwargs["flow_name"] = args.flow_name
        elif target_format == "dagster":
            if args.group_name:
                kwargs["group_name"] = args.group_name
        
        return export_flow(flow_spec, target_format, args.out, **kwargs)
    
    @staticmethod
    def _dry_run(flow_spec: Dict[str, Any], args: Namespace) -> int:
        """Show what would be exported without writing files."""
        print(f"\n🔍 Dry Run - Export to {args.to}")
        print("=" * 50)
        
        print(f"Flow: {flow_spec.get('flow', 'unknown')}")
        print(f"Tasks: {len(flow_spec.get('tasks', []))}")
        print(f"Output: {args.out}")
        
        tasks = flow_spec.get('tasks', [])
        if tasks:
            print("\nTasks:")
            for i, task in enumerate(tasks, 1):
                name = task.get('name', f'task_{i}')
                component_type = task.get('component_type', 'Other')
                inputs = len(task.get('inputs', []))
                outputs = len(task.get('outputs', []))
                print(f"  {i:2d}. {name} ({component_type}) - {inputs} inputs, {outputs} outputs")
        
        # Show validation warnings
        warnings = validate_export(flow_spec, args.to)
        if warnings:
            print(f"\n⚠️  Validation Warnings ({len(warnings)}):")
            for warning in warnings:
                print(f"  - {warning}")
        else:
            print(f"\n✅ No validation warnings for {args.to} export")
        
        print(f"\n📝 Would write to: {args.out}")
        print("Use --validate to see detailed warnings, remove --dry-run to proceed")
        
        return 0
    
    @staticmethod
    def _show_next_steps(format_type: str, output_path: str) -> None:
        """Show next steps after successful export."""
        print(f"\n📋 Next Steps for {format_type}:")
        
        if format_type == "airflow":
            print(f"1. Copy {output_path} to your Airflow DAGs folder")
            print("2. Update the generated tasks with your actual logic")
            print("3. Test the DAG: airflow dags test <dag_id> <execution_date>")
            print("4. Enable the DAG in Airflow UI")
            
        elif format_type == "prefect":
            print(f"1. Install dependencies: pip install prefect")
            print(f"2. Update tasks in {output_path} with your actual logic")
            print(f"3. Test the flow: python {output_path}")
            print("4. Deploy: prefect deployment build & prefect deployment apply")
            
        elif format_type == "dagster":
            print(f"1. Install dependencies: pip install dagster")
            print(f"2. Update asset materializations in {output_path}")
            print("3. Add to your Dagster project's __init__.py")
            print("4. Launch: dagster dev")
            
        elif format_type == "mermaid":
            print(f"1. View {output_path} in a Mermaid-compatible editor")
            print("2. Use in documentation or Markdown files")
            print("3. Render: mermaid-cli or online editor")
            
        elif format_type == "dot":
            print(f"1. Render with Graphviz: dot -Tpng {output_path} -o graph.png")
            print("2. Or use online Graphviz viewers")
            
        elif format_type == "ascii":
            print(f"1. View or include {output_path} in documentation")
            print("2. Use in terminal or plain text contexts")

# Convenience function for programmatic use
def export_command(flow_json: str, target_format: str, output_path: str, **kwargs) -> str:
    """Programmatically export a FlowSpec."""
    with open(flow_json, 'r') as f:
        flow_spec = json.load(f)
    
    if target_format in get_available_visualizations():
        return export_visualization(flow_spec, target_format, output_path)
    else:
        return export_flow(flow_spec, target_format, output_path, **kwargs)