import uuid
from typing import Optional

def generate_run_id() -> str:
    """Generate a unique run ID."""
    return str(uuid.uuid4())

def generate_job_name(flow_name: str, step_name: Optional[str] = None) -> str:
    """Generate a job name for OpenLineage."""
    if step_name:
        return f"{flow_name}.{step_name}"
    return flow_name