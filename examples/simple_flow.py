"""Simple flow example demonstrating basic nbxflow usage."""

import nbxflow
import pandas as pd
import time
from pathlib import Path

# Optional: Configure nbxflow (can also use environment variables)
# nbxflow.configure_openlineage("http://localhost:5000", namespace="examples")
# nbxflow.configure_otel(service_name="simple_example", enable_console=True)

def main():
    """Run a simple data pipeline with nbxflow instrumentation."""
    
    print("🚀 Starting simple nbxflow pipeline example")
    
    with nbxflow.flow("simple_data_pipeline") as flow_registry:
        
        # Step 1: Data Loading
        with nbxflow.step("load_sample_data", component_type="DataLoader") as s:
            print("📥 Loading sample data...")
            
            # Mark input dataset
            input_file = Path("examples/sample_data.csv")
            nbxflow.mark_input(nbxflow.dataset_file(str(input_file)))
            
            # Simulate data loading
            data = {
                'id': range(1, 101),
                'name': [f'Item_{i}' for i in range(1, 101)],
                'value': [i * 1.5 for i in range(1, 101)],
                'category': ['A' if i % 3 == 0 else 'B' if i % 3 == 1 else 'C' for i in range(1, 101)]
            }
            df = pd.DataFrame(data)
            
            # Mark output dataset  
            staged_file = Path("examples/staged_data.parquet")
            nbxflow.mark_output(nbxflow.dataset_file(str(staged_file)))
            
            print(f"✅ Loaded {len(df)} rows")
        
        # Step 2: Data Transformation
        with nbxflow.step("transform_data", component_type="Transformer") as s:
            print("⚙️ Transforming data...")
            
            # Mark input (output from previous step)
            nbxflow.mark_input(nbxflow.dataset_file(str(staged_file)))
            
            # Apply transformations
            df['value_doubled'] = df['value'] * 2
            df['category_code'] = df['category'].map({'A': 1, 'B': 2, 'C': 3})
            
            # Filter data
            transformed_df = df[df['value'] > 50]
            
            # Mark output
            transformed_file = Path("examples/transformed_data.parquet") 
            nbxflow.mark_output(nbxflow.dataset_file(str(transformed_file)))
            
            print(f"✅ Transformed data: {len(df)} -> {len(transformed_df)} rows")
        
        # Step 3: Data Quality Check
        with nbxflow.step("quality_check", component_type="QualityCheck") as s:
            print("✅ Running quality checks...")
            
            # Mark input
            nbxflow.mark_input(nbxflow.dataset_file(str(transformed_file)))
            
            # Simple quality checks
            assert len(transformed_df) > 0, "No data after transformation"
            assert not transformed_df.isnull().any().any(), "Null values found"
            assert transformed_df['value'].min() > 50, "Value filter failed"
            
            # Create a simple contract
            if nbxflow.ge_infer_contract_from_dataframe:
                contract = nbxflow.ge_infer_contract_from_dataframe(
                    transformed_df, 
                    "transformed_data_quality_v1",
                    mode="loose"
                )
                s.add_contract(contract)
                print(f"📋 Added contract with {len(contract.get('expectations', []))} expectations")
            
            print("✅ All quality checks passed")
        
        # Step 4: Data Export
        with nbxflow.step("export_results", component_type="Exporter") as s:
            print("📤 Exporting results...")
            
            # Mark input
            nbxflow.mark_input(nbxflow.dataset_file(str(transformed_file)))
            
            # Export to multiple formats
            export_paths = [
                Path("examples/output/results.csv"),
                Path("examples/output/results.json")
            ]
            
            # Ensure output directory exists
            export_paths[0].parent.mkdir(parents=True, exist_ok=True)
            
            # Save files
            transformed_df.to_csv(export_paths[0], index=False)
            transformed_df.to_json(export_paths[1], orient='records', indent=2)
            
            # Mark outputs
            for path in export_paths:
                nbxflow.mark_output(nbxflow.dataset_file(str(path)))
            
            print(f"✅ Exported to {len(export_paths)} formats")
    
    print("🎉 Pipeline completed successfully!")
    print(f"📊 Flow registry: {flow_registry.name} with {len(flow_registry.tasks)} tasks")
    
    # Show the generated FlowSpec file
    flow_files = list(Path.cwd().glob(f"{flow_registry.name}_flow_*.json"))
    if flow_files:
        print(f"📄 FlowSpec saved: {flow_files[0]}")
        
        # Show how to export to other formats
        print("\n💡 Next steps:")
        print(f"  Export to Airflow: nbxflow export --flow-json {flow_files[0]} --to airflow --out airflow_dag.py")
        print(f"  Export to Prefect: nbxflow export --flow-json {flow_files[0]} --to prefect --out prefect_flow.py")
        print(f"  View lineage: nbxflow lineage --flow-json {flow_files[0]} --format mermaid")

if __name__ == "__main__":
    main()