from typing import Dict, Any, Optional
import json
import re

from .client import get_llm_client, is_llm_available
from .prompts import CLASSIFY_COMPONENT_PROMPT
from ..utils.logging import get_logger

logger = get_logger(__name__)

# Rule-based classification patterns
CLASSIFICATION_RULES = {
    "DataLoader": [
        r"\b(load|read|fetch|import|ingest)\b.*\b(file|csv|json|database|api)\b",
        r"\b(pandas\.read|read_csv|read_json|read_sql|fetch|download)\b",
        r"\b(load|loading).*\b(data|dataset)\b"
    ],
    "Transformer": [
        r"\b(transform|convert|clean|process|normalize)\b",
        r"\b(apply|map|filter|groupby|pivot|melt)\b",
        r"\b(preprocess|postprocess)\b"
    ],
    "Reconciliator": [
        r"\b(reconcile|match|dedupe|deduplicat|merge|join)\b",
        r"\b(entity.*match|record.*link)\b",
        r"\b(fuzzy.*match|string.*match)\b"
    ],
    "Enricher": [
        r"\b(enrich|extend|augment|enhance)\b",
        r"\b(add|append).*\b(data|information|features)\b",
        r"\b(geocod|weather|lookup)\b"
    ],
    "Exporter": [
        r"\b(export|save|write|output|publish)\b",
        r"\b(to_csv|to_json|to_sql|write_file)\b",
        r"\b(upload|send|post).*\b(api|service)\b"
    ],
    "QualityCheck": [
        r"\b(validate|check|test|verify|assert)\b",
        r"\b(quality|expectation|contract)\b",
        r"\b(great.expectation|ge\.)\b"
    ],
    "Splitter": [
        r"\b(split|partition|divide|separate)\b",
        r"\b(train.*test.*split|sample)\b"
    ],
    "Merger": [
        r"\b(merge|combine|concat|union|join)\b",
        r"\b(aggregate|consolidate)\b"
    ],
    "Orchestrator": [
        r"\b(orchestrat|coordinat|manag|schedul)\b",
        r"\b(workflow|pipeline|dag)\b"
    ]
}

def rule_based_classify(name: str, docstring: str = "", code: str = "", hints: str = "") -> Dict[str, Any]:
    """
    Classify component using rule-based approach.
    
    Returns:
        Dictionary with component_type, rationale, and confidence
    """
    # Combine all text for analysis
    text = f"{name} {docstring} {code} {hints}".lower()
    
    # Score each component type
    scores = {}
    matches = {}
    
    for component_type, patterns in CLASSIFICATION_RULES.items():
        score = 0
        type_matches = []
        
        for pattern in patterns:
            if re.search(pattern, text, re.IGNORECASE):
                score += 1
                type_matches.append(pattern)
        
        if score > 0:
            scores[component_type] = score
            matches[component_type] = type_matches
    
    if not scores:
        return {
            "component_type": "Other",
            "rationale": "No clear patterns matched for classification",
            "confidence": 0.1,
            "method": "rule-based"
        }
    
    # Get best match
    best_type = max(scores.keys(), key=lambda k: scores[k])
    max_score = scores[best_type]
    
    # Calculate confidence based on score and uniqueness
    total_score = sum(scores.values())
    confidence = min(0.9, max_score / total_score if total_score > 0 else 0.1)
    
    # Build rationale
    matched_patterns = matches[best_type]
    rationale = f"Rule-based classification found {max_score} pattern matches: {', '.join(matched_patterns[:2])}"
    if len(matched_patterns) > 2:
        rationale += f" and {len(matched_patterns) - 2} more"
    
    return {
        "component_type": best_type,
        "rationale": rationale,
        "confidence": confidence,
        "method": "rule-based",
        "all_scores": scores
    }

def llm_classify(name: str, docstring: str = "", code: str = "", hints: str = "") -> Dict[str, Any]:
    """
    Classify component using LLM.
    
    Returns:
        Dictionary with component_type, rationale, and confidence
    """
    if not is_llm_available():
        logger.warning("LLM not available, falling back to rule-based classification")
        return rule_based_classify(name, docstring, code, hints)
    
    try:
        # Prepare prompt
        # Truncate code to avoid token limits
        truncated_code = code[:2000] if len(code) > 2000 else code
        
        prompt = CLASSIFY_COMPONENT_PROMPT.format(
            name=name,
            docstring=docstring,
            code=truncated_code,
            hints=hints
        )
        
        # Make LLM request
        client = get_llm_client()
        response = client.chat(
            messages=[{"role": "user", "content": prompt}],
            temperature=0.1,
            response_format={"type": "json_object"}
        )
        
        # Parse response
        try:
            result = json.loads(response)
            
            # Validate required fields
            required_fields = ["component_type", "rationale", "confidence"]
            for field in required_fields:
                if field not in result:
                    raise ValueError(f"Missing required field: {field}")
            
            # Validate component type
            if result["component_type"] not in CLASSIFICATION_RULES.keys() and result["component_type"] != "Other":
                logger.warning(f"LLM returned invalid component type: {result['component_type']}, using 'Other'")
                result["component_type"] = "Other"
            
            # Ensure confidence is in valid range
            confidence = float(result["confidence"])
            if not (0.0 <= confidence <= 1.0):
                confidence = max(0.0, min(1.0, confidence))
                result["confidence"] = confidence
            
            result["method"] = "llm"
            return result
            
        except (json.JSONDecodeError, ValueError, KeyError) as e:
            logger.error(f"Error parsing LLM response: {e}. Response: {response}")
            # Fall back to rule-based
            fallback = rule_based_classify(name, docstring, code, hints)
            fallback["fallback_reason"] = f"LLM response parsing failed: {str(e)}"
            return fallback
    
    except Exception as e:
        logger.error(f"LLM classification failed: {e}")
        # Fall back to rule-based
        fallback = rule_based_classify(name, docstring, code, hints)
        fallback["fallback_reason"] = f"LLM request failed: {str(e)}"
        return fallback

def auto_classify_component(name: str, docstring: str = "", code: str = "", 
                          hints: str = "", prefer_llm: bool = True) -> Dict[str, Any]:
    """
    Classify a component automatically using the best available method.
    
    Args:
        name: Component name
        docstring: Component documentation
        code: Component code
        hints: Additional hints for classification
        prefer_llm: Whether to prefer LLM over rule-based when available
    
    Returns:
        Classification result dictionary
    """
    if prefer_llm and is_llm_available():
        result = llm_classify(name, docstring, code, hints)
        
        # If LLM confidence is very low, try rule-based as backup
        if result.get("confidence", 0) < 0.3:
            logger.info("LLM confidence low, trying rule-based classification")
            rule_result = rule_based_classify(name, docstring, code, hints)
            
            # Use rule-based if it has higher confidence
            if rule_result.get("confidence", 0) > result.get("confidence", 0):
                rule_result["note"] = "Rule-based used due to higher confidence than LLM"
                return rule_result
        
        return result
    else:
        return rule_based_classify(name, docstring, code, hints)

# Convenience functions for backward compatibility
def auto_type(name: str, docstring: str = "", code: str = "", hints: str = "") -> Dict[str, Any]:
    """Legacy function name for auto_classify_component."""
    return auto_classify_component(name, docstring, code, hints)