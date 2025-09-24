"""Classify command implementation."""

import inspect
import json
from argparse import ArgumentParser, Namespace
from typing import Optional

from ...llm.classifier import auto_classify_component, COMPONENT_TYPES
from ...llm.client import is_llm_available
from ...utils.logging import get_logger

logger = get_logger(__name__)

class ClassifyCommand:
    """Command for classifying pipeline components."""
    
    @staticmethod
    def add_parser(subparsers) -> ArgumentParser:
        """Add the classify command parser."""
        parser = subparsers.add_parser(
            "classify",
            help="Classify pipeline components",
            description="Automatically classify pipeline components using rules or LLM"
        )
        
        parser.add_argument(
            "--name",
            required=True,
            help="Component name"
        )
        
        parser.add_argument(
            "--doc",
            default="",
            help="Component documentation/docstring"
        )
        
        parser.add_argument(
            "--code",
            help="Code excerpt or file path"
        )
        
        parser.add_argument(
            "--hints",
            default="",
            help="Additional hints for classification"
        )
        
        parser.add_argument(
            "--method",
            choices=["auto", "llm", "rules"],
            default="auto",
            help="Classification method (default: auto)"
        )
        
        parser.add_argument(
            "--output-format",
            choices=["text", "json"],
            default="text",
            help="Output format (default: text)"
        )
        
        parser.add_argument(
            "--show-all-scores",
            action="store_true",
            help="Show all classification scores (rules method only)"
        )
        
        parser.add_argument(
            "--confidence-threshold",
            type=float,
            default=0.5,
            help="Minimum confidence threshold (default: 0.5)"
        )
        
        return parser
    
    @staticmethod
    def run(args: Namespace) -> int:
        """Execute the classify command."""
        try:
            # Load code if it's a file path
            code = ClassifyCommand._load_code(args.code) if args.code else ""
            
            # Determine classification method
            if args.method == "auto":
                prefer_llm = is_llm_available()
            elif args.method == "llm":
                if not is_llm_available():
                    logger.error("LLM not available. Configure LLM settings or use --method rules")
                    return 1
                prefer_llm = True
            else:  # rules
                prefer_llm = False
            
            # Perform classification
            logger.info(f"Classifying component: {args.name}")
            if prefer_llm:
                logger.info("Using LLM-based classification")
            else:
                logger.info("Using rule-based classification")
            
            result = auto_classify_component(
                name=args.name,
                docstring=args.doc,
                code=code,
                hints=args.hints,
                prefer_llm=prefer_llm
            )
            
            # Check confidence threshold
            confidence = result.get('confidence', 0.0)
            if confidence < args.confidence_threshold:
                logger.warning(f"Low confidence ({confidence:.2f}) - consider reviewing the classification")
            
            # Output results
            if args.output_format == "json":
                print(json.dumps(result, indent=2))
            else:
                ClassifyCommand._print_text_result(result, args)
            
            return 0
            
        except Exception as e:
            logger.error(f"Classification failed: {e}")
            return 1
    
    @staticmethod
    def _load_code(code_arg: str) -> str:
        """Load code from file or return as-is."""
        # Check if it looks like a file path
        if len(code_arg) < 200 and ('/' in code_arg or '\\' in code_arg or code_arg.endswith('.py')):
            try:
                with open(code_arg, 'r') as f:
                    return f.read()
            except FileNotFoundError:
                logger.warning(f"Code file not found: {code_arg}, treating as literal code")
                return code_arg
            except Exception as e:
                logger.warning(f"Error reading code file: {e}, treating as literal code")
                return code_arg
        else:
            return code_arg
    
    @staticmethod
    def _print_text_result(result: dict, args: Namespace) -> None:
        """Print classification result in human-readable format."""
        print("\n" + "="*50)
        print("COMPONENT CLASSIFICATION")
        print("="*50)
        
        print(f"Component: {args.name}")
        print(f"Classified as: {result.get('component_type', 'Unknown')}")
        print(f"Confidence: {result.get('confidence', 0.0):.2f}")
        print(f"Method: {result.get('method', 'unknown')}")
        
        rationale = result.get('rationale', '')
        if rationale:
            print(f"\nRationale:")
            print(f"  {rationale}")
        
        # Show fallback reason if present
        fallback_reason = result.get('fallback_reason')
        if fallback_reason:
            print(f"\nNote: {fallback_reason}")
        
        # Show all scores if rule-based and requested
        if args.show_all_scores and result.get('method') == 'rule-based':
            all_scores = result.get('all_scores', {})
            if all_scores:
                print(f"\nAll Scores:")
                for comp_type, score in sorted(all_scores.items(), key=lambda x: x[1], reverse=True):
                    print(f"  {comp_type}: {score}")
        
        # Show component type description
        component_type = result.get('component_type', 'Other')
        description = ClassifyCommand._get_component_description(component_type)
        if description:
            print(f"\nDescription:")
            print(f"  {description}")
        
        # Show confidence interpretation
        confidence = result.get('confidence', 0.0)
        print(f"\nConfidence Interpretation:")
        if confidence >= 0.8:
            print("  🟢 High confidence - classification is likely accurate")
        elif confidence >= 0.5:
            print("  🟡 Medium confidence - review recommended")
        else:
            print("  🔴 Low confidence - manual review strongly recommended")
        
        # Show available component types
        print(f"\nAvailable Component Types:")
        for comp_type in COMPONENT_TYPES:
            marker = "👉 " if comp_type == component_type else "   "
            print(f"  {marker}{comp_type}")
    
    @staticmethod
    def _get_component_description(component_type: str) -> Optional[str]:
        """Get description for a component type."""
        descriptions = {
            "DataLoader": "Loads data from files, databases, or APIs into the pipeline",
            "Transformer": "Transforms, cleans, or processes data without adding external information",
            "Reconciliator": "Matches, deduplicates, or reconciles data entities (often using external services)",
            "Enricher": "Adds new information to existing data using external APIs or datasets",
            "Exporter": "Outputs or saves data to external systems, files, or databases",
            "QualityCheck": "Validates data quality, runs tests, or checks data contracts",
            "Splitter": "Splits datasets into multiple outputs based on criteria",
            "Merger": "Combines multiple datasets into a single output",
            "Orchestrator": "Coordinates other components or manages workflow execution",
            "Other": "Components that don't fit the above categories"
        }
        return descriptions.get(component_type)

# Convenience function for programmatic use
def classify_component(name: str, docstring: str = "", code: str = "", hints: str = "", 
                      method: str = "auto") -> dict:
    """Programmatically classify a component."""
    prefer_llm = method == "llm" or (method == "auto" and is_llm_available())
    
    return auto_classify_component(
        name=name,
        docstring=docstring,
        code=code,
        hints=hints,
        prefer_llm=prefer_llm
    )