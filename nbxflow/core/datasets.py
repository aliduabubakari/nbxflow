from dataclasses import dataclass, field
from typing import Dict, Any, Optional
import os
import urllib.parse

@dataclass
class DatasetRef:
    """Reference to a dataset for lineage tracking."""
    namespace: str
    name: str
    facets: Dict[str, Any] = field(default_factory=dict)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "namespace": self.namespace,
            "name": self.name,
            "facets": self.facets
        }
    
    def to_openlineage_dataset(self):
        """Convert to OpenLineage dataset format."""
        try:
            # Try to use OpenLineage client if available
            from openlineage.client.event_v2 import Dataset
            return Dataset(
                namespace=self.namespace,
                name=self.name,
                facets=self.facets if self.facets else None
            )
        except ImportError:
            # Fallback to dict format
            return {
                "namespace": self.namespace,
                "name": self.name,
                "facets": self.facets if self.facets else None
            }

def dataset_file(path: str, namespace: str = "file") -> DatasetRef:
    """Create a dataset reference for a file."""
    abs_path = os.path.abspath(path)
    return DatasetRef(namespace=namespace, name=abs_path)

def dataset_api(service: str, endpoint: str, namespace: str = "api") -> DatasetRef:
    """Create a dataset reference for an API endpoint."""
    name = f"{service}:{endpoint}"
    return DatasetRef(namespace=namespace, name=name)

def dataset_semt(dataset_id: str, table_id: str, base: Optional[str] = None) -> DatasetRef:
    """Create a dataset reference for a SEMT table."""
    if base:
        parsed = urllib.parse.urlparse(base)
        namespace = f"semt://{parsed.netloc}"
    else:
        namespace = "semt"
    
    name = f"dataset/{dataset_id}/table/{table_id}"
    return DatasetRef(namespace=namespace, name=name)

def dataset_from_semt_table(table_dict: Dict[str, Any], base: Optional[str] = None) -> Optional[DatasetRef]:
    """Create a dataset reference from a SEMT table dictionary."""
    try:
        table_info = table_dict.get("table", {})
        dataset_id = str(table_info.get("datasetId", "unknown"))
        table_id = str(table_info.get("id", "unknown"))
        return dataset_semt(dataset_id, table_id, base)
    except (KeyError, TypeError):
        return None

def add_schema_facet_from_dataframe(dataset: DatasetRef, df) -> DatasetRef:
    """Add schema facet from pandas DataFrame."""
    try:
        import pandas as pd
        if isinstance(df, pd.DataFrame):
            fields = []
            for col, dtype in zip(df.columns, df.dtypes):
                fields.append({
                    "name": str(col),
                    "type": str(dtype)
                })
            
            dataset.facets["schema"] = {
                "fields": fields
            }
            dataset.facets["stats"] = {
                "rowCount": len(df),
                "columnCount": len(df.columns)
            }
    except ImportError:
        pass  # pandas not available
    except Exception:
        pass  # ignore errors in schema inference
    
    return dataset