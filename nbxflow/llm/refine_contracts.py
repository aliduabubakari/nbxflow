from typing import Dict, Any, List, Optional
import json
import copy

from .client import get_llm_client, is_llm_available
from .prompts import REFINE_CONTRACT_PROMPT, SCHEMA_GENERALIZATION_PROMPT
from ..utils.logging import get_logger

logger = get_logger(__name__)

def refine_ge_suite(suite_dict: Dict[str, Any], samples: List[Dict[str, Any]], 
                   guidance: str = "") -> Dict[str, Any]:
    """
    Refine a Great Expectations suite using LLM analysis of sample data.
    
    Args:
        suite_dict: Dictionary representation of GE suite
        samples: Sample data records to analyze
        guidance: Additional guidance for refinement
    
    Returns:
        Dictionary with refinement suggestions and updated suite
    """
    if not is_llm_available():
        logger.warning("LLM not available for contract refinement")
        return {
            "suggestions": [],
            "updated_suite": suite_dict,
            "changes_summary": "LLM not available - no changes made",
            "risk_assessment": "No risk - original suite unchanged",
            "method": "fallback"
        }
    
    try:
        # Prepare data for LLM (limit size to avoid token limits)
        suite_json = json.dumps(suite_dict, indent=2)[:4000]
        samples_json = json.dumps(samples[:10], indent=2)[:4000]  # Limit to 10 samples
        guidance_text = guidance[:1000]  # Limit guidance length
        
        # Prepare prompt
        prompt = REFINE_CONTRACT_PROMPT.format(
            suite_json=suite_json,
            samples_json=samples_json,
            guidance=guidance_text
        )
        
        # Make LLM request
        client = get_llm_client()
        response = client.chat(
            messages=[{"role": "user", "content": prompt}],
            temperature=0.2,
            response_format={"type": "json_object"}
        )
        
        # Parse response
        try:
            result = json.loads(response)
            
            # Validate response structure
            required_fields = ["suggestions", "updated_suite", "changes_summary", "risk_assessment"]
            for field in required_fields:
                if field not in result:
                    result[field] = f"Missing {field} in LLM response"
            
            # Ensure updated_suite has the right structure
            if not isinstance(result.get("updated_suite"), dict):
                logger.warning("Invalid updated_suite in LLM response, using original")
                result["updated_suite"] = suite_dict
            
            result["method"] = "llm"
            return result
            
        except json.JSONDecodeError as e:
            logger.error(f"Error parsing LLM response for contract refinement: {e}")
            return _fallback_refinement(suite_dict, samples)
    
    except Exception as e:
        logger.error(f"LLM contract refinement failed: {e}")
        return _fallback_refinement(suite_dict, samples)

def _fallback_refinement(suite_dict: Dict[str, Any], samples: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Fallback refinement using simple heuristics."""
    suggestions = []
    updated_suite = copy.deepcopy(suite_dict)
    
    expectations = updated_suite.get("expectations", [])
    
    # Simple heuristic refinements
    for i, expectation in enumerate(expectations):
        exp_type = expectation.get("expectation_type")
        kwargs = expectation.get("kwargs", {})
        
        # Expand string length ranges
        if exp_type == "expect_column_value_lengths_to_be_between":
            column = kwargs.get("column")
            if column:
                # Add 20% buffer to string lengths
                min_val = kwargs.get("min_value", 0)
                max_val = kwargs.get("max_value", 100)
                
                new_min = max(0, int(min_val * 0.8))
                new_max = int(max_val * 1.2)
                
                if new_min != min_val or new_max != max_val:
                    expectations[i]["kwargs"]["min_value"] = new_min
                    expectations[i]["kwargs"]["max_value"] = new_max
                    suggestions.append({
                        "type": "length_buffer",
                        "field": column,
                        "change": f"Expanded length range from [{min_val}, {max_val}] to [{new_min}, {new_max}]",
                        "rationale": "Added 20% buffer for string length variations"
                    })
        
        # Expand numeric ranges
        elif exp_type == "expect_column_values_to_be_between":
            column = kwargs.get("column")
            if column:
                min_val = kwargs.get("min_value")
                max_val = kwargs.get("max_value")
                
                if min_val is not None and max_val is not None:
                    range_size = max_val - min_val
                    buffer = max(range_size * 0.1, 1)  # 10% buffer or minimum 1
                    
                    new_min = min_val - buffer
                    new_max = max_val + buffer
                    
                    expectations[i]["kwargs"]["min_value"] = new_min
                    expectations[i]["kwargs"]["max_value"] = new_max
                    suggestions.append({
                        "type": "range_buffer",
                        "field": column,
                        "change": f"Expanded range from [{min_val}, {max_val}] to [{new_min}, {new_max}]",
                        "rationale": "Added buffer for numeric value variations"
                    })
    
    return {
        "suggestions": suggestions,
        "updated_suite": updated_suite,
        "changes_summary": f"Applied {len(suggestions)} heuristic refinements",
        "risk_assessment": "Low risk - conservative buffer adjustments only",
        "method": "heuristic"
    }

def generalize_schema(schema_dict: Dict[str, Any], samples: List[Dict[str, Any]], 
                     context: str = "") -> Dict[str, Any]:
    """
    Generalize a data schema based on sample analysis.
    
    Args:
        schema_dict: Current schema dictionary
        samples: Sample data records
        context: Domain or context information
    
    Returns:
        Dictionary with generalized schema and analysis
    """
    if not is_llm_available():
        logger.warning("LLM not available for schema generalization")
        return _fallback_schema_generalization(schema_dict, samples)
    
    try:
        # Prepare data for LLM
        schema_json = json.dumps(schema_dict, indent=2)[:3000]
        samples_json = json.dumps(samples[:10], indent=2)[:4000]
        context_text = context[:500]
        
        # Prepare prompt
        prompt = SCHEMA_GENERALIZATION_PROMPT.format(
            schema_json=schema_json,
            samples_json=samples_json,
            context=context_text
        )
        
        # Make LLM request
        client = get_llm_client()
        response = client.chat(
            messages=[{"role": "user", "content": prompt}],
            temperature=0.2,
            response_format={"type": "json_object"}
        )
        
        # Parse response
        result = json.loads(response)
        
        # Validate response
        required_fields = ["generalized_schema", "changes", "flexibility_score", "quality_score"]
        for field in required_fields:
            if field not in result:
                if field in ["flexibility_score", "quality_score"]:
                    result[field] = 5  # Default middle score
                else:
                    result[field] = [] if field == "changes" else schema_dict
        
        result["method"] = "llm"
        return result
        
    except Exception as e:
        logger.error(f"LLM schema generalization failed: {e}")
        return _fallback_schema_generalization(schema_dict, samples)

def _fallback_schema_generalization(schema_dict: Dict[str, Any], samples: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Fallback schema generalization using simple analysis."""
    generalized = copy.deepcopy(schema_dict)
    changes = []
    
    if not samples:
        return {
            "generalized_schema": generalized,
            "changes": changes,
            "flexibility_score": 5,
            "quality_score": 5,
            "method": "fallback"
        }
    
    # Analyze field presence across samples
    all_fields = set()
    field_presence = {}
    
    for sample in samples:
        sample_fields = set(sample.keys())
        all_fields.update(sample_fields)
        
        for field in sample_fields:
            field_presence[field] = field_presence.get(field, 0) + 1
    
    # Update schema based on field presence analysis
    if "fields" in generalized:
        for field_def in generalized["fields"]:
            field_name = field_def.get("name")
            if field_name in field_presence:
                presence_rate = field_presence[field_name] / len(samples)
                
                # Make field nullable if not always present
                if presence_rate < 1.0 and not field_def.get("nullable", False):
                    field_def["nullable"] = True
                    changes.append({
                        "field": field_name,
                        "change": "made nullable",
                        "reason": f"Field present in only {presence_rate:.1%} of samples"
                    })
    
    flexibility_score = min(10, 5 + len(changes))
    quality_score = max(1, 8 - len(changes))
    
    return {
        "generalized_schema": generalized,
        "changes": changes,
        "flexibility_score": flexibility_score,
        "quality_score": quality_score,
        "method": "fallback"
    }

# Convenience functions
def llm_refine_contract(suite_dict: Dict[str, Any], samples: List[Dict[str, Any]], 
                       guidance: str = "") -> Dict[str, Any]:
    """Convenience function for contract refinement."""
    return refine_ge_suite(suite_dict, samples, guidance)

def llm_generalize_schema(schema_dict: Dict[str, Any], samples: List[Dict[str, Any]], 
                         context: str = "") -> Dict[str, Any]:
    """Convenience function for schema generalization."""
    return generalize_schema(schema_dict, samples, context)