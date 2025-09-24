from typing import Dict, Any, Optional, List
import json
import os

from ..core.facets import GEValidationFacet
from ..config import settings
from ..utils.logging import get_logger

logger = get_logger(__name__)

try:
    import great_expectations as gx
    from great_expectations.core.expectation_suite import ExpectationSuite
    from great_expectations.core.expectation_configuration import ExpectationConfiguration
    HAVE_GE = True
except ImportError:
    gx = None
    ExpectationSuite = None
    ExpectationConfiguration = None
    HAVE_GE = False

def is_available() -> bool:
    """Check if Great Expectations is available."""
    return HAVE_GE

def infer_contract_from_dataframe(df, suite_name: str, mode: str = "loose") -> Dict[str, Any]:
    """
    Infer a Great Expectations suite from a pandas DataFrame.
    
    Args:
        df: pandas DataFrame
        suite_name: Name for the expectation suite
        mode: 'strict' or 'loose' - how strict the inferred expectations should be
    
    Returns:
        Dictionary representation of the contract
    """
    if not HAVE_GE:
        return {
            "type": "GE",
            "suite": suite_name,
            "status": "SKIPPED",
            "reason": "great_expectations not installed",
            "expectations": []
        }
    
    try:
        import pandas as pd
        
        if not isinstance(df, pd.DataFrame):
            return {
                "type": "GE",
                "suite": suite_name,
                "status": "ERROR",
                "reason": "Input is not a pandas DataFrame",
                "expectations": []
            }
        
        expectations = []
        
        # Basic existence expectations for all columns
        for column in df.columns:
            expectations.append({
                "expectation_type": "expect_column_to_exist",
                "kwargs": {"column": column}
            })
            
            # Non-null expectations (if column has non-null values)
            if not df[column].isnull().all():
                if mode == "strict" or df[column].isnull().sum() == 0:
                    expectations.append({
                        "expectation_type": "expect_column_values_to_not_be_null",
                        "kwargs": {"column": column}
                    })
            
            # Type-based expectations
            dtype = df[column].dtype
            
            if pd.api.types.is_numeric_dtype(dtype):
                # Numeric constraints
                if mode == "loose":
                    # Wide range based on data
                    min_val = float(df[column].min()) * 0.8  # 20% buffer
                    max_val = float(df[column].max()) * 1.2  # 20% buffer
                else:
                    min_val = float(df[column].min())
                    max_val = float(df[column].max())
                
                if not pd.isna(min_val):
                    expectations.append({
                        "expectation_type": "expect_column_values_to_be_between",
                        "kwargs": {
                            "column": column,
                            "min_value": min_val,
                            "max_value": max_val
                        }
                    })
            
            elif pd.api.types.is_string_dtype(dtype):
                # String length expectations
                lengths = df[column].dropna().str.len()
                if not lengths.empty:
                    if mode == "loose":
                        min_length = max(0, int(lengths.min()) - 5)
                        max_length = int(lengths.max()) + 50
                    else:
                        min_length = int(lengths.min())
                        max_length = int(lengths.max())
                    
                    expectations.append({
                        "expectation_type": "expect_column_value_lengths_to_be_between",
                        "kwargs": {
                            "column": column,
                            "min_value": min_length,
                            "max_value": max_length
                        }
                    })
                
                # Unique value expectations for categorical-like columns
                unique_values = df[column].dropna().unique()
                if len(unique_values) <= 20:  # Likely categorical
                    expectations.append({
                        "expectation_type": "expect_column_values_to_be_in_set",
                        "kwargs": {
                            "column": column,
                            "value_set": list(unique_values)
                        }
                    })
        
        # Row count expectation
        if mode == "loose":
            min_rows = max(0, len(df) - 1000)  # Allow some variation
            max_rows = len(df) + 1000
        else:
            min_rows = len(df)
            max_rows = len(df)
        
        expectations.append({
            "expectation_type": "expect_table_row_count_to_be_between",
            "kwargs": {
                "min_value": min_rows,
                "max_value": max_rows
            }
        })
        
        return {
            "type": "GE",
            "suite": suite_name,
            "status": "CREATED",
            "mode": mode,
            "row_count": len(df),
            "column_count": len(df.columns),
            "expectations": expectations
        }
        
    except Exception as e:
        logger.error(f"Error inferring contract from DataFrame: {e}")
        return {
            "type": "GE",
            "suite": suite_name,
            "status": "ERROR",
            "reason": str(e),
            "expectations": []
        }

def validate_dataframe(df, contract: Dict[str, Any]) -> GEValidationFacet:
    """
    Validate a DataFrame against a contract.
    
    Args:
        df: pandas DataFrame to validate
        contract: Contract dictionary (from infer_contract_from_dataframe)
    
    Returns:
        GEValidationFacet with validation results
    """
    suite_name = contract.get("suite", "unknown")
    
    if not HAVE_GE:
        return GEValidationFacet(
            suite_name=suite_name,
            status="SKIPPED",
            statistics={"reason": "great_expectations not installed"},
            failures=[]
        )
    
    try:
        import pandas as pd
        
        if not isinstance(df, pd.DataFrame):
            return GEValidationFacet(
                suite_name=suite_name,
                status="ERROR",
                statistics={"reason": "Input is not a pandas DataFrame"},
                failures=["Input validation failed"]
            )
        
        expectations = contract.get("expectations", [])
        if not expectations:
            return GEValidationFacet(
                suite_name=suite_name,
                status="SKIPPED",
                statistics={"reason": "No expectations in contract"},
                failures=[]
            )
        
        failures = []
        successful = 0
        
        # Simple validation logic (without full GE context)
        for expectation in expectations:
            exp_type = expectation.get("expectation_type")
            kwargs = expectation.get("kwargs", {})
            
            try:
                if exp_type == "expect_column_to_exist":
                    column = kwargs.get("column")
                    if column not in df.columns:
                        failures.append(f"Column '{column}' does not exist")
                    else:
                        successful += 1
                
                elif exp_type == "expect_column_values_to_not_be_null":
                    column = kwargs.get("column")
                    if column in df.columns:
                        null_count = df[column].isnull().sum()
                        if null_count > 0:
                            failures.append(f"Column '{column}' has {null_count} null values")
                        else:
                            successful += 1
                    else:
                        failures.append(f"Column '{column}' does not exist for null check")
                
                elif exp_type == "expect_column_values_to_be_between":
                    column = kwargs.get("column")
                    min_val = kwargs.get("min_value")
                    max_val = kwargs.get("max_value")
                    if column in df.columns:
                        out_of_range = ((df[column] < min_val) | (df[column] > max_val)).sum()
                        if out_of_range > 0:
                            failures.append(f"Column '{column}' has {out_of_range} values outside range [{min_val}, {max_val}]")
                        else:
                            successful += 1
                    else:
                        failures.append(f"Column '{column}' does not exist for range check")
                
                elif exp_type == "expect_column_value_lengths_to_be_between":
                    column = kwargs.get("column")
                    min_length = kwargs.get("min_value")
                    max_length = kwargs.get("max_value")
                    if column in df.columns:
                        lengths = df[column].dropna().str.len()
                        out_of_range = ((lengths < min_length) | (lengths > max_length)).sum()
                        if out_of_range > 0:
                            failures.append(f"Column '{column}' has {out_of_range} values with length outside range [{min_length}, {max_length}]")
                        else:
                            successful += 1
                    else:
                        failures.append(f"Column '{column}' does not exist for length check")
                
                elif exp_type == "expect_column_values_to_be_in_set":
                    column = kwargs.get("column")
                    value_set = set(kwargs.get("value_set", []))
                    if column in df.columns:
                        unexpected = df[column].dropna()[~df[column].dropna().isin(value_set)]
                        if len(unexpected) > 0:
                            failures.append(f"Column '{column}' has {len(unexpected)} unexpected values")
                        else:
                            successful += 1
                    else:
                        failures.append(f"Column '{column}' does not exist for value set check")
                
                elif exp_type == "expect_table_row_count_to_be_between":
                    min_rows = kwargs.get("min_value")
                    max_rows = kwargs.get("max_value")
                    actual_rows = len(df)
                    if actual_rows < min_rows or actual_rows > max_rows:
                        failures.append(f"Table has {actual_rows} rows, expected between {min_rows} and {max_rows}")
                    else:
                        successful += 1
                
                else:
                    logger.warning(f"Unsupported expectation type: {exp_type}")
                    
            except Exception as e:
                failures.append(f"Error validating {exp_type}: {str(e)}")
        
        status = "SUCCESS" if not failures else "FAILED"
        statistics = {
            "evaluated_expectations": len(expectations),
            "successful_expectations": successful,
            "unsuccessful_expectations": len(failures),
            "success_percent": (successful / len(expectations)) * 100 if expectations else 0
        }
        
        return GEValidationFacet(
            suite_name=suite_name,
            status=status,
            statistics=statistics,
            failures=failures
        )
        
    except Exception as e:
        logger.error(f"Error validating DataFrame: {e}")
        return GEValidationFacet(
            suite_name=suite_name,
            status="ERROR",
            statistics={"reason": str(e)},
            failures=[f"Validation error: {str(e)}"]
        )

def create_ge_suite_from_contract(contract: Dict[str, Any]) -> Optional[Any]:
    """
    Convert a contract dictionary to a Great Expectations ExpectationSuite.
    """
    if not HAVE_GE:
        return None
    
    try:
        suite_name = contract.get("suite", "default")
        expectations = contract.get("expectations", [])
        
        # Create expectation suite
        suite = ExpectationSuite(expectation_suite_name=suite_name)
        
        for exp_dict in expectations:
            exp_config = ExpectationConfiguration(
                expectation_type=exp_dict.get("expectation_type"),
                kwargs=exp_dict.get("kwargs", {})
            )
            suite.add_expectation(exp_config)
        
        return suite
        
    except Exception as e:
        logger.error(f"Error creating GE suite: {e}")
        return None

# Convenience functions for the main API
def ge_infer_contract_from_dataframe(df, suite_name: str, mode: str = "loose") -> Dict[str, Any]:
    """Convenience function for contract inference."""
    return infer_contract_from_dataframe(df, suite_name, mode)

def ge_validate_dataframe(df, contract: Dict[str, Any]) -> GEValidationFacet:
    """Convenience function for validation."""
    return validate_dataframe(df, contract)