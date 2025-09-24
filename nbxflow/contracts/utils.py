from typing import Dict, Any, List, Optional
import json

def to_human_summary(contract: Dict[str, Any]) -> str:
    """
    Convert a contract to a human-readable summary.
    
    Args:
        contract: Contract dictionary
        
    Returns:
        Human-readable string summary
    """
    if not isinstance(contract, dict):
        return "Invalid contract format"
    
    suite_name = contract.get("suite", "Unknown")
    expectations = contract.get("expectations", [])
    
    if not expectations:
        return f"Contract '{suite_name}': No expectations defined"
    
    lines = [f"Contract: {suite_name}"]
    lines.append(f"Total Expectations: {len(expectations)}")
    lines.append("")
    
    # Group expectations by type
    by_type = {}
    for exp in expectations:
        exp_type = exp.get("expectation_type", "unknown")
        if exp_type not in by_type:
            by_type[exp_type] = []
        by_type[exp_type].append(exp)
    
    # Create summary by type
    for exp_type, exp_list in by_type.items():
        lines.append(f"{exp_type}: {len(exp_list)} expectations")
        
        # Show details for first few expectations of each type
        for i, exp in enumerate(exp_list[:3]):
            kwargs = exp.get("kwargs", {})
            if exp_type == "expect_column_to_exist":
                lines.append(f"  - Column '{kwargs.get('column', '?')}' must exist")
            elif exp_type == "expect_column_values_to_not_be_null":
                lines.append(f"  - Column '{kwargs.get('column', '?')}' must not be null")
            elif exp_type == "expect_column_values_to_be_between":
                col = kwargs.get('column', '?')
                min_val = kwargs.get('min_value', '?')
                max_val = kwargs.get('max_value', '?')
                lines.append(f"  - Column '{col}' values must be between {min_val} and {max_val}")
            elif exp_type == "expect_column_values_to_be_in_set":
                col = kwargs.get('column', '?')
                value_set = kwargs.get('value_set', [])
                if len(value_set) <= 5:
                    lines.append(f"  - Column '{col}' values must be in {value_set}")
                else:
                    lines.append(f"  - Column '{col}' values must be in set of {len(value_set)} values")
            elif exp_type == "expect_column_value_lengths_to_be_between":
                col = kwargs.get('column', '?')
                min_len = kwargs.get('min_value', '?')
                max_len = kwargs.get('max_value', '?')
                lines.append(f"  - Column '{col}' text length must be between {min_len} and {max_len}")
            elif exp_type == "expect_table_row_count_to_be_between":
                min_rows = kwargs.get('min_value', '?')
                max_rows = kwargs.get('max_value', '?')
                lines.append(f"  - Table must have between {min_rows} and {max_rows} rows")
            else:
                lines.append(f"  - {exp_type} with {len(kwargs)} parameters")
        
        if len(exp_list) > 3:
            lines.append(f"  ... and {len(exp_list) - 3} more")
        lines.append("")
    
    return "\n".join(lines)

def validate_contract_structure(contract: Dict[str, Any]) -> List[str]:
    """
    Validate the structure of a contract dictionary.
    
    Args:
        contract: Contract dictionary to validate
        
    Returns:
        List of validation error messages (empty if valid)
    """
    errors = []
    
    if not isinstance(contract, dict):
        errors.append("Contract must be a dictionary")
        return errors
    
    # Check required fields
    required_fields = ["type", "suite"]
    for field in required_fields:
        if field not in contract:
            errors.append(f"Missing required field: {field}")
    
    # Validate contract type
    contract_type = contract.get("type")
    if contract_type and contract_type not in ["GE", "custom"]:
        errors.append(f"Unknown contract type: {contract_type}")
    
    # Validate expectations structure
    expectations = contract.get("expectations")
    if expectations is not None:
        if not isinstance(expectations, list):
            errors.append("Expectations must be a list")
        else:
            for i, exp in enumerate(expectations):
                if not isinstance(exp, dict):
                    errors.append(f"Expectation {i} must be a dictionary")
                    continue
                
                if "expectation_type" not in exp:
                    errors.append(f"Expectation {i} missing expectation_type")
                
                if "kwargs" not in exp:
                    errors.append(f"Expectation {i} missing kwargs")
                elif not isinstance(exp["kwargs"], dict):
                    errors.append(f"Expectation {i} kwargs must be a dictionary")
    
    return errors

def merge_contracts(contracts: List[Dict[str, Any]], strategy: str = "union") -> Dict[str, Any]:
    """
    Merge multiple contracts into a single contract.
    
    Args:
        contracts: List of contract dictionaries
        strategy: Merge strategy ("union", "intersection", "latest")
        
    Returns:
        Merged contract dictionary
    """
    if not contracts:
        return {"type": "GE", "suite": "empty", "expectations": []}
    
    if len(contracts) == 1:
        return contracts[0].copy()
    
    if strategy == "latest":
        return contracts[-1].copy()
    
    # Merge based on union or intersection
    merged = {
        "type": contracts[0].get("type", "GE"),
        "suite": f"merged_{'_'.join(c.get('suite', 'unknown') for c in contracts[:3])}",
        "expectations": []
    }
    
    if strategy == "union":
        # Combine all expectations, removing duplicates
        seen_expectations = set()
        for contract in contracts:
            for exp in contract.get("expectations", []):
                exp_key = (exp.get("expectation_type"), str(sorted(exp.get("kwargs", {}).items())))
                if exp_key not in seen_expectations:
                    merged["expectations"].append(exp)
                    seen_expectations.add(exp_key)
    
    elif strategy == "intersection":
        # Only include expectations that appear in all contracts
        if not contracts:
            return merged
        
        # Start with expectations from first contract
        candidate_expectations = contracts[0].get("expectations", [])
        
        for exp in candidate_expectations:
            exp_key = (exp.get("expectation_type"), str(sorted(exp.get("kwargs", {}).items())))
            
            # Check if this expectation appears in all other contracts
            found_in_all = True
            for contract in contracts[1:]:
                found = False
                for other_exp in contract.get("expectations", []):
                    other_key = (other_exp.get("expectation_type"), str(sorted(other_exp.get("kwargs", {}).items())))
                    if exp_key == other_key:
                        found = True
                        break
                if not found:
                    found_in_all = False
                    break
            
            if found_in_all:
                merged["expectations"].append(exp)
    
    return merged

def contract_diff(contract1: Dict[str, Any], contract2: Dict[str, Any]) -> Dict[str, Any]:
    """
    Compare two contracts and return differences.
    
    Args:
        contract1: First contract
        contract2: Second contract
        
    Returns:
        Dictionary describing the differences
    """
    diff = {
        "added_expectations": [],
        "removed_expectations": [],
        "modified_expectations": [],
        "metadata_changes": {}
    }
    
    # Get expectations
    exp1 = {_exp_key(exp): exp for exp in contract1.get("expectations", [])}
    exp2 = {_exp_key(exp): exp for exp in contract2.get("expectations", [])}
    
    # Find added and removed expectations
    keys1 = set(exp1.keys())
    keys2 = set(exp2.keys())
    
    for key in keys2 - keys1:
        diff["added_expectations"].append(exp2[key])
    
    for key in keys1 - keys2:
        diff["removed_expectations"].append(exp1[key])
    
    # Find modified expectations (same type but different kwargs)
    for key in keys1 & keys2:
        if exp1[key].get("kwargs") != exp2[key].get("kwargs"):
            diff["modified_expectations"].append({
                "expectation_type": exp1[key].get("expectation_type"),
                "old_kwargs": exp1[key].get("kwargs"),
                "new_kwargs": exp2[key].get("kwargs")
            })
    
    # Check metadata changes
    for field in ["suite", "type"]:
        val1 = contract1.get(field)
        val2 = contract2.get(field)
        if val1 != val2:
            diff["metadata_changes"][field] = {"old": val1, "new": val2}
    
    return diff

def _exp_key(expectation: Dict[str, Any]) -> str:
    """Generate a unique key for an expectation."""
    exp_type = expectation.get("expectation_type", "")
    kwargs = expectation.get("kwargs", {})
    
    # Create a stable key from type and main identifying kwargs
    key_parts = [exp_type]
    
    # Add column name if present (most common identifier)
    if "column" in kwargs:
        key_parts.append(f"col:{kwargs['column']}")
    
    return "|".join(key_parts)

def extract_schema_from_contract(contract: Dict[str, Any]) -> Dict[str, Any]:
    """
    Extract a basic schema representation from a contract.
    
    Args:
        contract: Contract dictionary
        
    Returns:
        Schema dictionary with field definitions
    """
    schema = {
        "fields": [],
        "table_constraints": []
    }
    
    expectations = contract.get("expectations", [])
    columns = {}
    
    for exp in expectations:
        exp_type = exp.get("expectation_type")
        kwargs = exp.get("kwargs", {})
        column = kwargs.get("column")
        
        if column and column not in columns:
            columns[column] = {"name": column, "constraints": []}
        
        # Extract field-level constraints
        if column and exp_type == "expect_column_values_to_not_be_null":
            columns[column]["nullable"] = False
        elif column and exp_type == "expect_column_values_to_be_in_set":
            columns[column]["enum"] = kwargs.get("value_set", [])
        elif column and exp_type == "expect_column_values_to_be_between":
            columns[column]["min_value"] = kwargs.get("min_value")
            columns[column]["max_value"] = kwargs.get("max_value")
        elif column and exp_type == "expect_column_value_lengths_to_be_between":
            columns[column]["min_length"] = kwargs.get("min_value")
            columns[column]["max_length"] = kwargs.get("max_value")
        
        # Extract table-level constraints
        elif exp_type == "expect_table_row_count_to_be_between":
            schema["table_constraints"].append({
                "type": "row_count",
                "min_rows": kwargs.get("min_value"),
                "max_rows": kwargs.get("max_value")
            })
    
    schema["fields"] = list(columns.values())
    return schema