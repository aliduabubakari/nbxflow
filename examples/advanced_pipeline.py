"""Advanced pipeline example with metrics integration, contracts, and LLM helpers."""

import nbxflow
import pandas as pd
import time
import requests
from typing import Dict, Any
from pathlib import Path

# Mock Metrics class (users would have their own)
class Metrics:
    all_metrics = []
    
    def __init__(self, operation: str, **kwargs):
        self.operation = operation
        self.start_time = time.time()
        self.data = {'operation': operation, **kwargs}
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.data['wall_time_seconds'] = time.time() - self.start_time
        self.data['success'] = exc_type is None
        Metrics.all_metrics.append(self.data)
    
    def reliability(self, operation, retries=3, retry_delay=1.0):
        """Mock reliability wrapper."""
        attempts = 0
        for attempt in range(retries + 1):
            attempts += 1
            try:
                result = operation()
                self.data.update({
                    'reliability_attempts': attempts,
                    'reliability_retries': attempt,
                    'reliability_succeeded_after_retry': attempt > 0,
                    'reliability_success': True
                })
                return result
            except Exception as e:
                if attempt == retries:
                    self.data.update({
                        'reliability_attempts': attempts,
                        'reliability_retries': attempt,
                        'reliability_succeeded_after_retry': False,
                        'reliability_success': False,
                        'reliability_wasted_seconds': attempt * retry_delay
                    })
                    raise
                time.sleep(retry_delay)

def mock_api_call(endpoint: str, data: Dict[str, Any]) -> Dict[str, Any]:
    """Mock external API call that sometimes fails."""
    import random
    
    # Simulate API failure 20% of the time
    if random.random() < 0.2:
        raise requests.RequestException("Mock API failure")
    
    # Simulate processing time
    time.sleep(0.1)
    
    # Return mock enriched data
    return {
        **data,
        'enriched_field': f"enriched_{data.get('id', 'unknown')}",
        'api_timestamp': time.time(),
        'confidence_score': random.uniform(0.7, 1.0)
    }

def main():
    """Run an advanced pipeline with all nbxflow features."""
    
    print("🚀 Starting advanced nbxflow pipeline example")
    
    # Configure nbxflow
    nbxflow.configure_otel(service_name="advanced_pipeline", enable_console=True)
    
    with nbxflow.flow("advanced_data_pipeline") as flow_registry:
        
        # Step 1: Load data with metrics
        with nbxflow.step("load_large_dataset", component_type="DataLoader") as s:
            with Metrics("load_large_dataset", n_records=1000) as m:
                nbxflow.attach_metrics(m)
                
                print("📥 Loading large dataset...")
                
                # Create larger mock dataset
                data = {
                    'id': range(1, 1001),
                    'name': [f'Record_{i}' for i in range(1, 1001)],
                    'value': [i * 1.23 for i in range(1, 1001)],
                    'category': ['Type_' + str(i % 5) for i in range(1, 1001)],
                    'location': [f"City_{i % 50}" for i in range(1, 1001)]
                }
                df = pd.DataFrame(data)
                
                # Mark I/O
                input_source = nbxflow.dataset_api("data_warehouse", "/api/v1/records")
                nbxflow.mark_input(input_source)
                
                staged_data = nbxflow.dataset_file("staging/large_dataset.parquet")
                nbxflow.mark_output(staged_data)
                
                print(f"✅ Loaded {len(df)} records")
        
        # Step 2: Enrich with external API (with reliability handling)
        with nbxflow.step("enrich_with_external_api", component_type="Enricher") as s:
            with Metrics("enrich_with_external_api", api_calls=len(df), api_cost_per_call=0.001) as m:
                nbxflow.attach_metrics(m)
                
                print("🌐 Enriching with external API...")
                
                nbxflow.mark_input(staged_data)
                
                # Process in batches with reliability handling
                enriched_records = []
                batch_size = 50
                
                for i in range(0, len(df), batch_size):
                    batch = df.iloc[i:i+batch_size]
                    
                    def process_batch():
                        batch_results = []
                        for _, row in batch.iterrows():
                            result = mock_api_call("enrichment_service", row.to_dict())
                            batch_results.append(result)
                        return batch_results
                    
                    # Use reliability wrapper
                    batch_results = m.reliability(process_batch, retries=3, retry_delay=1.0)
                    enriched_records.extend(batch_results)
                
                enriched_df = pd.DataFrame(enriched_records)
                
                # Mark output
                enriched_data = nbxflow.dataset_file("staging/enriched_dataset.parquet")
                nbxflow.mark_output(enriched_data)
                
                # Update metrics
                m.data.update({
                    'input_records': len(df),
                    'output_records': len(enriched_df),
                    'llm_cost': len(df) * 0.001,
                    'throughput_records_per_sec': len(df) / m.data.get('wall_time_seconds', 1)
                })
                
                print(f"✅ Enriched {len(enriched_df)} records")
        
        # Step 3: Data reconciliation with auto-classification
        step_name = "reconcile_locations"
        step_doc = "Reconcile location names using external geocoding service"
        step_code = """
        def reconcile_locations(df):
            # Deduplicate and standardize location names
            unique_locations = df['location'].unique()
            location_mapping = {}
            
            for location in unique_locations:
                # Mock geocoding/reconciliation
                standardized = location.replace('_', ' ').title()
                location_mapping[location] = standardized
            
            df['location_standardized'] = df['location'].map(location_mapping)
            return df
        """
        
        # Auto-classify this step if LLM is available
        if nbxflow.auto_classify_component:
            classification = nbxflow.auto_classify_component(
                name=step_name,
                docstring=step_doc,
                code=step_code,
                hints="Uses external service for entity matching"
            )
            component_type = classification.get('component_type', 'Reconciliator')
            print(f"🤖 Auto-classified as: {component_type} (confidence: {classification.get('confidence', 0):.2f})")
        else:
            component_type = "Reconciliator"
        
        with nbxflow.step(step_name, component_type=component_type) as s:
            with Metrics("reconcile_locations") as m:
                nbxflow.attach_metrics(m)
                
                print("🔗 Reconciling locations...")
                
                nbxflow.mark_input(enriched_data)
                
                # Perform reconciliation
                unique_locations = enriched_df['location'].unique()
                location_mapping = {}
                
                for location in unique_locations:
                    standardized = location.replace('_', ' ').title()
                    location_mapping[location] = standardized
                
                enriched_df['location_standardized'] = enriched_df['location'].map(location_mapping)
                reconciled_df = enriched_df.copy()
                
                # Mark output
                reconciled_data = nbxflow.dataset_file("staging/reconciled_dataset.parquet")
                nbxflow.mark_output(reconciled_data)
                
                print(f"✅ Reconciled {len(unique_locations)} locations")
        
        # Step 4: Advanced quality checks with contract inference and LLM refinement
        with nbxflow.step("advanced_quality_check", component_type="QualityCheck") as s:
            with Metrics("advanced_quality_check") as m:
                nbxflow.attach_metrics(m)
                
                print("🔍 Running advanced quality checks...")
                
                nbxflow.mark_input(reconciled_data)
                
                # Infer initial contract
                if nbxflow.ge_infer_contract_from_dataframe:
                    initial_contract = nbxflow.ge_infer_contract_from_dataframe(
                        reconciled_df.sample(min(100, len(reconciled_df))),
                        "reconciled_data_quality_v1",
                        mode="strict"
                    )
                    
                    # Refine contract with LLM if available
                    if nbxflow.llm_refine_contract:
                        print("🤖 Refining contract with LLM...")
                        refinement = nbxflow.llm_refine_contract(
                            initial_contract,
                            reconciled_df.sample(10).to_dict('records'),
                            guidance="Allow for location name variations, maintain data quality"
                        )
                        
                        if refinement.get('updated_suite'):
                            refined_contract = refinement['updated_suite']
                            print(f"📋 Applied {len(refinement.get('suggestions', []))} refinements")
                            s.add_contract(refined_contract)
                        else:
                            s.add_contract(initial_contract)
                    else:
                        s.add_contract(initial_contract)
                    
                    # Validate data against contract
                    if nbxflow.ge_validate_dataframe:
                        validation_result = nbxflow.ge_validate_dataframe(reconciled_df, initial_contract)
                        print(f"📊 Validation: {validation_result.status}")
                        if validation_result.failures:
                            print(f"⚠️ {len(validation_result.failures)} validation issues found")
                
                print("✅ Quality checks completed")
        
        # Step 5: Export with multiple outputs
        with nbxflow.step("export_final_results", component_type="Exporter") as s:
            with Metrics("export_final_results") as m:
                nbxflow.attach_metrics(m)
                
                print("📤 Exporting final results...")
                
                nbxflow.mark_input(reconciled_data)
                
                # Export to multiple destinations
                output_dir = Path("examples/output/advanced")
                output_dir.mkdir(parents=True, exist_ok=True)
                
                # Export formats
                outputs = [
                    (output_dir / "final_results.csv", lambda df, path: df.to_csv(path, index=False)),
                    (output_dir / "final_results.parquet", lambda df, path: df.to_parquet(path)),
                    (output_dir / "final_results.json", lambda df, path: df.to_json(path, orient='records', indent=2))
                ]
                
                for path, export_func in outputs:
                    export_func(reconciled_df, path)
                    nbxflow.mark_output(nbxflow.dataset_file(str(path)))
                
                # Also export to mock API
                api_endpoint = nbxflow.dataset_api("results_api", "/v1/upload")
                nbxflow.mark_output(api_endpoint)
                
                print(f"✅ Exported to {len(outputs) + 1} destinations")
    
    print("🎉 Advanced pipeline completed successfully!")
    
    # Show metrics summary
    print(f"\n📊 Metrics Summary:")
    for metric in Metrics.all_metrics:
        operation = metric['operation']
        wall_time = metric.get('wall_time_seconds', 0)
        success = metric.get('success', False)
        print(f"  {operation}: {wall_time:.2f}s ({'✅' if success else '❌'})")
    
    # Show capabilities
    print(f"\n🔧 nbxflow Capabilities:")
    nbxflow.print_capabilities()
    
    # Show export options
    flow_files = list(Path.cwd().glob(f"{flow_registry.name}_flow_*.json"))
    if flow_files:
        print(f"\n📄 Generated FlowSpec: {flow_files[0]}")
        print(f"\n💡 Export Examples:")
        flow_file = flow_files[0]
        print(f"  nbxflow export --flow-json {flow_file} --to airflow --out dags/advanced_pipeline.py")
        print(f"  nbxflow export --flow-json {flow_file} --to prefect --out flows/advanced_pipeline.py") 
        print(f"  nbxflow export --flow-json {flow_file} --to dagster --out assets/advanced_pipeline.py")
        print(f"  nbxflow lineage --flow-json {flow_file} --format mermaid --output lineage.mmd")

if __name__ == "__main__":
    main()