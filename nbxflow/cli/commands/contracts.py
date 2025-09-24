"""Contracts command implementation."""

import json
import os
import sys
from argparse import ArgumentParser, Namespace
from typing import Dict, Any, Optional

from ...contracts.ge import (
    infer_contract_from_dataframe, validate_dataframe, 
    is_available as ge_available
)
from ...contracts.registry import ContractRegistry
from ...contracts.utils import to_human_summary, validate_contract_structure
from ...utils.logging import get_logger

logger = get_logger(__name__)

class ContractsCommand:
    """Command for managing data contracts."""
    
    @staticmethod
    def add_parser(subparsers) -> ArgumentParser:
        """Add the contracts command parser."""
        parser = subparsers.add_parser(
            "contracts",
            help="Manage data contracts and expectations",
            description="Create, validate, and manage data quality contracts"
        )
        
        # Add subcommands
        contracts_subparsers = parser.add_subparsers(
            dest="contracts_command",
            title="contracts commands",
            help="Contracts operations"
        )
        
        # Infer command
        infer_parser = contracts_subparsers.add_parser(
            "infer",
            help="Infer contract from data",
            description="Automatically infer data quality expectations from sample data"
        )
        infer_parser.add_argument("--csv", help="CSV file to analyze")
        infer_parser.add_argument("--json", help="JSON file to analyze")
        infer_parser.add_argument("--suite-name", required=True, help="Name for the contract suite")
        infer_parser.add_argument("--mode", choices=["strict", "loose"], default="loose", 
                                help="Inference mode (default: loose)")
        infer_parser.add_argument("--output", "-o", help="Output file for contract")
        infer_parser.add_argument("--save", action="store_true", help="Save to contract registry")
        
        # Validate command
        validate_parser = contracts_subparsers.add_parser(
            "validate",
            help="Validate data against contract",
            description="Validate data against existing contract"
        )
        validate_parser.add_argument("--csv", help="CSV file to validate")
        validate_parser.add_argument("--json", help="JSON file to validate")
        validate_parser.add_argument("--contract", required=True, help="Contract file or name")
        validate_parser.add_argument("--version", help="Contract version (if using registry)")
        validate_parser.add_argument("--fail-on-error", action="store_true", 
                                   help="Exit with error code if validation fails")
        
        # List command
        list_parser = contracts_subparsers.add_parser(
            "list",
            help="List contracts in registry",
            description="List all contracts in the registry"
        )
        list_parser.add_argument("--detailed", action="store_true", help="Show detailed information")
        
        # Show command
        show_parser = contracts_subparsers.add_parser(
            "show",
            help="Show contract details",
            description="Show detailed information about a specific contract"
        )
        show_parser.add_argument("suite-name", help="Contract suite name")
        show_parser.add_argument("--version", help="Specific version to show")
        show_parser.add_argument("--format", choices=["json", "summary"], default="summary",
                               help="Output format")
        
        # Compare command
        compare_parser = contracts_subparsers.add_parser(
            "compare",
            help="Compare contract versions",
            description="Compare two versions of a contract"
        )
        compare_parser.add_argument("suite-name", help="Contract suite name")
        compare_parser.add_argument("--version1", required=True, help="First version")
        compare_parser.add_argument("--version2", required=True, help="Second version")
        
        # Delete command  
        delete_parser = contracts_subparsers.add_parser(
            "delete",
            help="Delete contract or version",
            description="Delete a contract or specific version"
        )
        delete_parser.add_argument("suite-name", help="Contract suite name")
        delete_parser.add_argument("--version", help="Specific version to delete")
        delete_parser.add_argument("--force", action="store_true", help="Skip confirmation")
        
        return parser
    
    @staticmethod
    def run(args: Namespace) -> int:
        """Execute the contracts command."""
        if not args.contracts_command:
            logger.error("No contracts subcommand specified")
            return 1
        
        try:
            if args.contracts_command == "infer":
                return ContractsCommand._infer(args)
            elif args.contracts_command == "validate":
                return ContractsCommand._validate(args)
            elif args.contracts_command == "list":
                return ContractsCommand._list(args)
            elif args.contracts_command == "show":
                return ContractsCommand._show(args)
            elif args.contracts_command == "compare":
                return ContractsCommand._compare(args)
            elif args.contracts_command == "delete":
                return ContractsCommand._delete(args)
            else:
                logger.error(f"Unknown contracts command: {args.contracts_command}")
                return 1
                
        except Exception as e:
            logger.error(f"Contracts command failed: {e}")
            return 1
    
    @staticmethod
    def _infer(args: Namespace) -> int:
        """Infer contract from data."""
        if not ge_available():
            logger.error("Great Expectations not available. Install with: pip install 'nbxflow[ge]'")
            return 1
        
        # Load data
        df = ContractsCommand._load_dataframe(args)
        if df is None:
            return 1
        
        logger.info(f"Analyzing data: {len(df)} rows, {len(df.columns)} columns")
        
        # Infer contract
        contract = infer_contract_from_dataframe(df, args.suite_name, args.mode)
        
        if contract.get('status') != 'CREATED':
            logger.error(f"Contract inference failed: {contract.get('reason', 'unknown error')}")
            return 1
        
        logger.info(f"✅ Contract inferred successfully")
        logger.info(f"Suite: {contract['suite']}")
        logger.info(f"Expectations: {len(contract.get('expectations', []))}")
        
        # Save to file if requested
        if args.output:
            with open(args.output, 'w') as f:
                json.dump(contract, f, indent=2)
            logger.info(f"Contract saved to: {args.output}")
        
        # Save to registry if requested
        if args.save:
            registry = ContractRegistry()
            version = registry.save_contract(args.suite_name, contract)
            logger.info(f"Contract saved to registry as version {version}")
        
        # Show summary
        print("\n" + "="*50)
        print("CONTRACT SUMMARY")
        print("="*50)
        print(to_human_summary(contract))
        
        return 0
    
    @staticmethod
    def _validate(args: Namespace) -> int:
        """Validate data against contract."""
        if not ge_available():
            logger.error("Great Expectations not available. Install with: pip install 'nbxflow[ge]'")
            return 1
        
        # Load data
        df = ContractsCommand._load_dataframe(args)
        if df is None:
            return 1
        
        # Load contract
        contract = ContractsCommand._load_contract(args.contract, args.version)
        if contract is None:
            return 1
        
        logger.info(f"Validating {len(df)} rows against contract: {contract.get('suite', 'unknown')}")
        
        # Perform validation
        validation_result = validate_dataframe(df, contract)
        
        # Show results
        print("\n" + "="*50)
        print("VALIDATION RESULTS")
        print("="*50)
        print(f"Suite: {validation_result.suite_name}")
        print(f"Status: {validation_result.status}")
        
        if validation_result.statistics:
            stats = validation_result.statistics
            print(f"Evaluated: {stats.get('evaluated_expectations', 0)} expectations")
            print(f"Successful: {stats.get('successful_expectations', 0)}")
            print(f"Failed: {stats.get('unsuccessful_expectations', 0)}")
            if 'success_percent' in stats:
                print(f"Success Rate: {stats['success_percent']:.1f}%")
        
        if validation_result.failures:
            print(f"\n❌ FAILURES ({len(validation_result.failures)}):")
            for i, failure in enumerate(validation_result.failures, 1):
                print(f"  {i}. {failure}")
        else:
            print(f"\n✅ All expectations passed!")
        
        # Return appropriate exit code
        if validation_result.status == "FAILED" and args.fail_on_error:
            return 1
        elif validation_result.status == "ERROR":
            return 1
        else:
            return 0
    
    @staticmethod
    def _list(args: Namespace) -> int:
        """List contracts in registry."""
        registry = ContractRegistry()
        contracts = registry.list_contracts()
        
        if not contracts:
            print("No contracts found in registry")
            return 0
        
        print(f"📋 Contracts in Registry ({len(contracts)}):")
        print("="*50)
        
        for contract_name in sorted(contracts):
            versions = registry.list_versions(contract_name)
            latest = registry.get_latest_version(contract_name)
            
            if args.detailed:
                info = registry.get_contract_info(contract_name)
                created = info.get('created_at', 'unknown') if info else 'unknown'
                print(f"\n📄 {contract_name}")
                print(f"   Versions: {len(versions)} ({', '.join(versions)})")
                print(f"   Latest: {latest}")
                print(f"   Created: {created}")
            else:
                print(f"  📄 {contract_name} (v{latest}, {len(versions)} versions)")
        
        return 0
    
    @staticmethod
    def _show(args: Namespace) -> int:
        """Show contract details."""
        suite_name = getattr(args, 'suite-name')
        registry = ContractRegistry()
        
        contract = registry.load_contract(suite_name, args.version)
        if contract is None:
            logger.error(f"Contract not found: {suite_name}")
            return 1
        
        if args.format == "json":
            print(json.dumps(contract, indent=2))
        else:
            print(to_human_summary(contract))
            
            # Show metadata if available
            metadata = contract.get('metadata', {})
            if metadata:
                print("\nMetadata:")
                for key, value in metadata.items():
                    print(f"  {key}: {value}")
        
        return 0
    
    @staticmethod
    def _compare(args: Namespace) -> int:
        """Compare contract versions."""
        suite_name = getattr(args, 'suite-name')
        registry = ContractRegistry()
        
        comparison = registry.compare_contracts(suite_name, args.version1, args.version2)
        
        if "error" in comparison:
            logger.error(comparison["error"])
            return 1
        
        print(f"📊 Comparison: {suite_name} v{args.version1} vs v{args.version2}")
        print("="*60)
        
        exp_count = comparison.get('expectations_count', {})
        print(f"Expectations: {exp_count.get('v1', 0)} → {exp_count.get('v2', 0)} (Δ{exp_count.get('diff', 0):+d})")
        
        exp_types = comparison.get('expectation_types', {})
        added = exp_types.get('added', [])
        removed = exp_types.get('removed', [])
        
        if added:
            print(f"\n✅ Added expectation types:")
            for exp_type in added:
                print(f"  + {exp_type}")
        
        if removed:
            print(f"\n❌ Removed expectation types:")
            for exp_type in removed:
                print(f"  - {exp_type}")
        
        if comparison.get('breaking_changes'):
            print(f"\n⚠️  This appears to be a BREAKING CHANGE")
        else:
            print(f"\n✅ Non-breaking change")
        
        return 0
    
    @staticmethod
    def _delete(args: Namespace) -> int:
        """Delete contract or version."""
        suite_name = getattr(args, 'suite-name')
        registry = ContractRegistry()
        
        if not args.force:
            if args.version:
                confirm = input(f"Delete {suite_name} version {args.version}? [y/N]: ")
            else:
                confirm = input(f"Delete ALL versions of {suite_name}? [y/N]: ")
            
            if confirm.lower() != 'y':
                logger.info("Deletion cancelled")
                return 0
        
        success = registry.delete_contract(suite_name, args.version)
        
        if success:
            if args.version:
                logger.info(f"Deleted {suite_name} version {args.version}")
            else:
                logger.info(f"Deleted all versions of {suite_name}")
            return 0
        else:
            logger.error("Deletion failed")
            return 1
    
    @staticmethod
    def _load_dataframe(args: Namespace):
        """Load dataframe from CSV or JSON file."""
        try:
            import pandas as pd
        except ImportError:
            logger.error("pandas required for data operations. Install with: pip install pandas")
            return None
        
        if args.csv:
            if not os.path.exists(args.csv):
                logger.error(f"CSV file not found: {args.csv}")
                return None
            try:
                return pd.read_csv(args.csv)
            except Exception as e:
                logger.error(f"Error reading CSV: {e}")
                return None
        
        elif args.json:
            if not os.path.exists(args.json):
                logger.error(f"JSON file not found: {args.json}")
                return None
            try:
                return pd.read_json(args.json)
            except Exception as e:
                logger.error(f"Error reading JSON: {e}")
                return None
        
        else:
            logger.error("No data file specified (use --csv or --json)")
            return None
    
    @staticmethod
    def _load_contract(contract_path_or_name: str, version: Optional[str] = None) -> Optional[Dict[str, Any]]:
        """Load contract from file or registry."""
        # Check if it's a file path
        if os.path.exists(contract_path_or_name):
            try:
                with open(contract_path_or_name, 'r') as f:
                    contract = json.load(f)
                
                # Validate structure
                errors = validate_contract_structure(contract)
                if errors:
                    logger.warning("Contract validation warnings:")
                    for error in errors:
                        logger.warning(f"  - {error}")
                
                return contract
            except Exception as e:
                logger.error(f"Error loading contract file: {e}")
                return None
        
        # Try loading from registry
        else:
            registry = ContractRegistry()
            contract = registry.load_contract(contract_path_or_name, version)
            if contract is None:
                logger.error(f"Contract not found in registry: {contract_path_or_name}")
            return contract