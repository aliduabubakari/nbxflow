"""
Notebook-style code snippets showing typical nbxflow usage patterns.

These examples show how nbxflow would be used in actual Jupyter notebooks.
"""

import nbxflow
import pandas as pd
import numpy as np
from pathlib import Path

# Example 1: Basic notebook flow
print("=" * 60)
print("EXAMPLE 1: Basic Notebook Flow")
print("=" * 60)

with nbxflow.flow("air_quality_analysis"):
    
    # Cell 1: Load data
    with nbxflow.step("load_air_quality_data", component_type="DataLoader"):
        print("Loading air quality data...")
        
        # Simulate loading from CSV
        air_quality_data = pd.DataFrame({
            'timestamp': pd.date_range('2024-01-01', periods=100, freq='H'),
            'pm25': np.random.normal(25, 10, 100),
            'pm10': np.random.normal(40, 15, 100),
            'location': np.random.choice(['Station_A', 'Station_B', 'Station_C'], 100)
        })
        
        nbxflow.mark_input(nbxflow.dataset_file("data/air_quality_raw.csv"))
        nbxflow.mark_output(nbxflow.dataset_file("staging/air_quality_loaded.parquet"))
        
        print(f"Loaded {len(air_quality_data)} records")
    
    # Cell 2: Clean and transform
    with nbxflow.step("clean_air_quality", component_type="Transformer"):
        print("Cleaning air quality data...")
        
        nbxflow.mark_input(nbxflow.dataset_file("staging/air_quality_loaded.parquet"))
        
        # Remove outliers
        cleaned_data = air_quality_data[
            (air_quality_data['pm25'] > 0) & (air_quality_data['pm25'] < 100) &
            (air_quality_data['pm10'] > 0) & (air_quality_data['pm10'] < 200)
        ]
        
        # Add derived columns
        cleaned_data['aqi_estimate'] = cleaned_data['pm25'] * 2 + cleaned_data['pm10'] * 0.5
        
        nbxflow.mark_output(nbxflow.dataset_file("staging/air_quality_cleaned.parquet"))
        
        print(f"Cleaned data: {len(air_quality_data)} -> {len(cleaned_data)} records")

print("\n" + "=" * 60)
print("EXAMPLE 2: API Integration with SEMT")
print("=" * 60)

# Example 2: Working with SEMT tables and APIs
with nbxflow.flow("semt_integration_example"):
    
    # Simulate SEMT table structure
    semt_table = {
        "table": {
            "id": "weather_data",
            "datasetId": 123,
            "schema": {
                "fields": [
                    {"name": "timestamp", "type": "datetime"},
                    {"name": "temperature", "type": "float"},
                    {"name": "humidity", "type": "float"}
                ]
            }
        }
    }
    
    with nbxflow.step("fetch_weather_api", component_type="DataLoader"):
        print("Fetching weather data from API...")
        
        # Mark API as input
        nbxflow.mark_input(nbxflow.dataset_api("openmeteo", "/v1/forecast"))
        
        # Simulate API response
        weather_data = pd.DataFrame({
            'timestamp': pd.date_range('2024-01-01', periods=100, freq='H'),
            'temperature': np.random.normal(20, 5, 100),
            'humidity': np.random.normal(60, 15, 100)
        })
        
        # Mark SEMT table as output
        semt_dataset = nbxflow.dataset_from_semt_table(semt_table, base="http://localhost:3003")
        if semt_dataset:
            nbxflow.mark_output(semt_dataset)
        
        print(f"Fetched {len(weather_data)} weather records")

print("\n" + "=" * 60)
print("EXAMPLE 3: Advanced Contract Usage")
print("=" * 60)

# Example 3: Advanced contract usage
with nbxflow.flow("data_contracts_example"):
    
    with nbxflow.step("validate_with_contracts", component_type="QualityCheck"):
        print("Running advanced data validation...")
        
        # Create sample data with some quality issues
        sample_data = pd.DataFrame({
            'user_id': [1, 2, 3, 4, None],  # Missing value
            'email': ['a@b.com', 'invalid', 'c@d.com', 'e@f.com', 'g@h.com'],  # Invalid email
            'age': [25, 150, 30, -5, 40],  # Outlier values
            'status': ['active', 'active', 'inactive', 'pending', 'unknown']  # Unexpected status
        })
        
        nbxflow.mark_input(nbxflow.dataset_file("raw/user_data.csv"))
        
        # Infer contract if GE available
        if nbxflow.ge_infer_contract_from_dataframe:
            contract = nbxflow.ge_infer_contract_from_dataframe(
                sample_data.dropna(),  # Use clean sample for inference
                "user_data_quality_v1",
                mode="loose"
            )
            
            print(f"Inferred contract with {len(contract.get('expectations', []))} expectations")
            
            # Validate full dataset against contract
            if nbxflow.ge_validate_dataframe:
                validation = nbxflow.ge_validate_dataframe(sample_data, contract)
                print(f"Validation result: {validation.status}")
                
                if validation.failures:
                    print(f"Found {len(validation.failures)} quality issues:")
                    for issue in validation.failures[:3]:  # Show first 3
                        print(f"  - {issue}")
            
            # Add contract to step
            step = nbxflow.current_step()
            if step:
                step.add_contract(contract)
        
        nbxflow.mark_output(nbxflow.dataset_file("validated/user_data.parquet"))
        
        print("Data validation completed")

print("\n" + "=" * 60)
print("EXAMPLE 4: LLM-Assisted Operations")
print("=" * 60)

# Example 4: Using LLM helpers (if available)
if nbxflow.auto_classify_component:
    
    # Classify a function automatically
    def complex_data_processor(df, config):
        """
        Advanced data processing function that applies multiple transformations
        including outlier detection, feature engineering, and normalization.
        
        Uses external ML models for anomaly detection and calls REST APIs
        for data enrichment.
        """
        # Mock complex processing
        processed_df = df.copy()
        processed_df['anomaly_score'] = np.random.random(len(df))
        return processed_df
    
    classification = nbxflow.auto_classify_component(
        name="complex_data_processor",
        docstring=complex_data_processor.__doc__,
        code=inspect.getsource(complex_data_processor) if 'inspect' in globals() else "",
        hints="ML-based processing with external API calls"
    )
    
    print(f"🤖 Auto-classified as: {classification['component_type']}")
    print(f"   Confidence: {classification['confidence']:.2f}")
    print(f"   Rationale: {classification['rationale']}")
    
    # Use the classification in a step
    with nbxflow.flow("llm_assisted_pipeline"):
        with nbxflow.step("complex_processing", 
                         component_type=classification['component_type']):
            print(f"Running {classification['component_type']} step...")
            
            # Mock data
            df = pd.DataFrame({'values': np.random.random(100)})
            result = complex_data_processor(df, {})
            
            print(f"Processed {len(result)} records")

else:
    print("LLM helpers not available - install with: pip install 'nbxflow[llm]'")

print("\n" + "=" * 60)
print("EXAMPLE 5: Function Decorators")
print("=" * 60)

# Example 5: Using function decorators
@nbxflow.task(component_type="DataLoader")
def load_customer_data():
    """Load customer data from database."""
    print("Loading customer data...")
    
    # Mock database query
    customers = pd.DataFrame({
        'customer_id': range(1, 51),
        'name': [f'Customer_{i}' for i in range(1, 51)],
        'segment': np.random.choice(['Premium', 'Standard', 'Basic'], 50)
    })
    
    # Mark I/O within the function
    nbxflow.mark_input(nbxflow.dataset_api("customer_db", "/customers"))
    nbxflow.mark_output(nbxflow.dataset_file("staging/customers.parquet"))
    
    return customers

@nbxflow.quick_step("segment_analysis", component_type="Transformer")
def analyze_segments():
    """Analyze customer segments."""
    print("Analyzing customer segments...")
    
    # Mock analysis
    analysis = {
        'Premium': {'count': 15, 'avg_value': 1000},
        'Standard': {'count': 20, 'avg_value': 500},
        'Basic': {'count': 15, 'avg_value': 200}
    }
    
    return analysis

# Use the decorated functions
with nbxflow.simple_flow("decorator_example"):
    customer_data = load_customer_data()
    segment_analysis = analyze_segments()
    
    print(f"Loaded {len(customer_data)} customers")
    print(f"Analyzed {len(segment_analysis)} segments")

print("\n🎉 All notebook examples completed!")

# Show final summary
print(f"\n📊 Summary of generated flows:")
flow_files = list(Path.cwd().glob("*_flow_*.json"))
for i, flow_file in enumerate(flow_files[-4:], 1):  # Show last 4 files
    print(f"  {i}. {flow_file.name}")

if flow_files:
    print(f"\n💡 Try these commands:")
    latest_flow = flow_files[-1]
    print(f"  nbxflow lineage --flow-json {latest_flow.name} --format ascii")
    print(f"  nbxflow export --flow-json {latest_flow.name} --to airflow --out example_dag.py")