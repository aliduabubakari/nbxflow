"""Main CLI entry point for nbxflow."""

import sys
import argparse
from typing import List, Optional

from .commands.export import ExportCommand
from .commands.lineage import LineageCommand
from .commands.contracts import ContractsCommand
from .commands.classify import ClassifyCommand
from ..version import __version__
from ..utils.logging import get_logger

logger = get_logger(__name__)

def create_parser() -> argparse.ArgumentParser:
    """Create the main argument parser."""
    parser = argparse.ArgumentParser(
        prog="nbxflow",
        description="nbxflow: Notebook Ops for Dataflow, Taskflow, and PerfFlow",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Export FlowSpec to Airflow DAG
  nbxflow export --flow-json my_flow.json --to airflow --out airflow_dag.py
  
  # Show lineage graph from FlowSpec
  nbxflow lineage --flow-json my_flow.json --format mermaid
  
  # Classify a component
  nbxflow classify --name "geocode_locations" --doc "Add coordinates using HERE API"
  
  # Infer contract from CSV
  nbxflow contracts infer --csv data.csv --suite-name "data_quality_v1"

For more information, visit: https://github.com/your-org/nbxflow
        """
    )
    
    parser.add_argument(
        "--version", 
        action="version", 
        version=f"nbxflow {__version__}"
    )
    
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Enable verbose logging"
    )
    
    parser.add_argument(
        "--quiet", "-q",
        action="store_true", 
        help="Suppress non-error output"
    )
    
    # Create subparsers
    subparsers = parser.add_subparsers(
        dest="command",
        title="commands",
        description="Available commands",
        help="Command to run"
    )
    
    # Add command parsers
    ExportCommand.add_parser(subparsers)
    LineageCommand.add_parser(subparsers)
    ContractsCommand.add_parser(subparsers)
    ClassifyCommand.add_parser(subparsers)
    
    return parser

def setup_logging(verbose: bool = False, quiet: bool = False) -> None:
    """Set up logging based on command line flags."""
    import logging
    
    if quiet:
        logging.getLogger("nbxflow").setLevel(logging.ERROR)
    elif verbose:
        logging.getLogger("nbxflow").setLevel(logging.DEBUG)
    else:
        logging.getLogger("nbxflow").setLevel(logging.INFO)

def main(argv: Optional[List[str]] = None) -> int:
    """Main CLI entry point."""
    parser = create_parser()
    args = parser.parse_args(argv)
    
    # Set up logging
    setup_logging(args.verbose, args.quiet)
    
    if not args.command:
        parser.print_help()
        return 1
    
    try:
        # Dispatch to command handlers
        if args.command == "export":
            return ExportCommand.run(args)
        elif args.command == "lineage":
            return LineageCommand.run(args)
        elif args.command == "contracts":
            return ContractsCommand.run(args)
        elif args.command == "classify":
            return ClassifyCommand.run(args)
        else:
            logger.error(f"Unknown command: {args.command}")
            return 1
            
    except KeyboardInterrupt:
        logger.info("Operation cancelled by user")
        return 130
    except Exception as e:
        if args.verbose:
            logger.exception(f"Command failed: {e}")
        else:
            logger.error(f"Command failed: {e}")
        return 1

if __name__ == "__main__":
    sys.exit(main())