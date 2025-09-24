from typing import Dict, Any, Optional, List
from dataclasses import dataclass, asdict

@dataclass
class BaseFacet:
    """Base class for OpenLineage facets."""
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert facet to dictionary."""
        return asdict(self)

@dataclass
class PerformanceFacet(BaseFacet):
    """Performance metrics facet."""
    wall_time_seconds: Optional[float] = None
    cpu_hours: Optional[float] = None
    memory_delta_mb: Optional[float] = None
    throughput_rps: Optional[float] = None
    input_tokens: Optional[int] = None
    output_tokens: Optional[int] = None
    llm_cost_usd: Optional[float] = None

@dataclass
class ReliabilityFacet(BaseFacet):
    """Reliability metrics facet."""
    attempts: int = 1
    retries: int = 0
    succeeded_after_retry: bool = False
    success: Optional[bool] = None
    mttr_seconds: Optional[float] = None
    wasted_seconds: Optional[float] = None

@dataclass
class ClassificationFacet(BaseFacet):
    """Component classification facet."""
    component_type: str
    method: str = "manual"
    rationale: str = ""

@dataclass
class ContractsFacet(BaseFacet):
    """Data contracts facet."""
    contracts: List[Dict[str, Any]]

@dataclass
class GEValidationFacet(BaseFacet):
    """Great Expectations validation facet."""
    suite_name: str
    status: str  # SUCCESS, FAILED, SKIPPED
    statistics: Dict[str, Any]
    failures: List[str]

def performance_from_metrics_row(row: Dict[str, Any]) -> Optional[PerformanceFacet]:
    """Create performance facet from metrics row."""
    if not row:
        return None
    
    return PerformanceFacet(
        wall_time_seconds=row.get("wall_time_seconds"),
        cpu_hours=row.get("cpu_hours"),
        memory_delta_mb=row.get("memory_delta_mb"),
        throughput_rps=row.get("throughput_records_per_sec"),
        input_tokens=row.get("input_tokens"),
        output_tokens=row.get("output_tokens"),
        llm_cost_usd=row.get("llm_cost")
    )

def reliability_from_metrics_row(row: Dict[str, Any]) -> Optional[ReliabilityFacet]:
    """Create reliability facet from metrics row."""
    if not row or "reliability_attempts" not in row:
        return None
    
    return ReliabilityFacet(
        attempts=row.get("reliability_attempts", 1),
        retries=row.get("reliability_retries", 0),
        succeeded_after_retry=row.get("reliability_succeeded_after_retry", False),
        success=row.get("reliability_success"),
        mttr_seconds=row.get("reliability_mttr_seconds"),
        wasted_seconds=row.get("reliability_wasted_seconds")
    )