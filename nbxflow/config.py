import os
from dataclasses import dataclass
from typing import Optional

@dataclass
class Settings:
    # OpenLineage
    ol_url: str = os.getenv("OPENLINEAGE_URL", "")
    ol_namespace: str = os.getenv("OPENLINEAGE_NAMESPACE", "notebook")
    ol_api_key: str = os.getenv("OPENLINEAGE_API_KEY", "")

    # OpenTelemetry
    otel_service_name: str = os.getenv("OTEL_SERVICE_NAME", "nbxflow")
    otel_enable_console: bool = os.getenv("OTEL_ENABLE_CONSOLE", "true").lower() == "true"
    otel_endpoint: str = os.getenv("OTEL_EXPORTER_OTLP_ENDPOINT", "")

    # Prometheus
    prometheus_port: int = int(os.getenv("NBX_PROM_PORT", "9108"))
    prometheus_enabled: bool = os.getenv("NBX_PROM_ENABLED", "false").lower() == "true"

    # Behavior
    warn_on_missing_io: bool = os.getenv("NBX_WARN_ON_MISSING_IO", "true").lower() == "true"
    
    # Contracts
    contracts_dir: str = os.getenv("NBX_CONTRACTS_DIR", ".nbxflow/contracts")

    # LLM
    llm_provider: str = os.getenv("NBX_LLM_PROVIDER", "openai")
    llm_model: str = os.getenv("NBX_LLM_MODEL", "gpt-4")
    llm_api_key: str = os.getenv("NBX_LLM_API_KEY", "")
    llm_endpoint: str = os.getenv("NBX_LLM_ENDPOINT", "")

settings = Settings()