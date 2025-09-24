from typing import Optional, Dict, Any
import time
import threading
from contextlib import contextmanager

from ..config import settings
from ..utils.logging import get_logger

logger = get_logger(__name__)

# Global state
_tracer = None
_meter = None
_prometheus_started = False
_prometheus_metrics = {}
_lock = threading.Lock()

def init_tracing(service_name: Optional[str] = None) -> None:
    """Initialize OpenTelemetry tracing."""
    global _tracer
    
    with _lock:
        if _tracer is not None:
            return
        
        try:
            from opentelemetry import trace
            from opentelemetry.sdk.trace import TracerProvider
            from opentelemetry.sdk.trace.export import BatchSpanProcessor
            
            # Set up tracer provider
            provider = TracerProvider()
            trace.set_tracer_provider(provider)
            
            # Add console exporter if enabled
            if settings.otel_enable_console:
                from opentelemetry.sdk.trace.export import ConsoleSpanExporter
                console_processor = BatchSpanProcessor(ConsoleSpanExporter())
                provider.add_span_processor(console_processor)
            
            # Add OTLP exporter if endpoint configured
            if settings.otel_endpoint:
                try:
                    from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
                    otlp_exporter = OTLPSpanExporter(endpoint=settings.otel_endpoint)
                    otlp_processor = BatchSpanProcessor(otlp_exporter)
                    provider.add_span_processor(otlp_processor)
                    logger.info(f"OTLP trace exporter configured for {settings.otel_endpoint}")
                except ImportError:
                    logger.warning("OTLP exporter not available. Install with: pip install 'nbxflow[otel]'")
                except Exception as e:
                    logger.error(f"Failed to configure OTLP exporter: {e}")
            
            # Get tracer
            service = service_name or settings.otel_service_name
            _tracer = trace.get_tracer(__name__, version="0.1.0")
            logger.info(f"OpenTelemetry tracing initialized for service: {service}")
            
        except ImportError:
            logger.warning("OpenTelemetry not available. Install with: pip install 'nbxflow[otel]'")
        except Exception as e:
            logger.error(f"Failed to initialize OpenTelemetry: {e}")

def get_tracer():
    """Get the OpenTelemetry tracer."""
    if _tracer is None:
        init_tracing()
    return _tracer

def init_metrics() -> None:
    """Initialize OpenTelemetry metrics."""
    global _meter
    
    with _lock:
        if _meter is not None:
            return
        
        try:
            from opentelemetry import metrics
            from opentelemetry.sdk.metrics import MeterProvider
            from opentelemetry.sdk.metrics.export import ConsoleMetricExporter, PeriodicExportingMetricReader
            
            # Set up meter provider
            readers = []
            
            # Add console exporter if enabled
            if settings.otel_enable_console:
                console_reader = PeriodicExportingMetricReader(
                    ConsoleMetricExporter(), 
                    export_interval_millis=30000
                )
                readers.append(console_reader)
            
            # Add OTLP metrics exporter if endpoint configured
            if settings.otel_endpoint:
                try:
                    from opentelemetry.exporter.otlp.proto.grpc.metric_exporter import OTLPMetricExporter
                    otlp_exporter = OTLPMetricExporter(endpoint=settings.otel_endpoint)
                    otlp_reader = PeriodicExportingMetricReader(
                        otlp_exporter,
                        export_interval_millis=30000
                    )
                    readers.append(otlp_reader)
                    logger.info(f"OTLP metrics exporter configured for {settings.otel_endpoint}")
                except ImportError:
                    logger.warning("OTLP metrics exporter not available")
                except Exception as e:
                    logger.error(f"Failed to configure OTLP metrics exporter: {e}")
            
            provider = MeterProvider(metric_readers=readers)
            metrics.set_meter_provider(provider)
            
            _meter = metrics.get_meter(__name__, version="0.1.0")
            logger.info("OpenTelemetry metrics initialized")
            
        except ImportError:
            logger.warning("OpenTelemetry metrics not available")
        except Exception as e:
            logger.error(f"Failed to initialize OpenTelemetry metrics: {e}")

def get_meter():
    """Get the OpenTelemetry meter."""
    if _meter is None:
        init_metrics()
    return _meter

def start_prometheus_server() -> None:
    """Start Prometheus metrics server."""
    global _prometheus_started, _prometheus_metrics
    
    with _lock:
        if _prometheus_started or not settings.prometheus_enabled:
            return
        
        try:
            from prometheus_client import start_http_server, Counter, Histogram, Gauge
            
            start_http_server(settings.prometheus_port)
            
            # Create common metrics
            _prometheus_metrics.update({
                'steps_total': Counter('nbxflow_steps_total', 'Total number of steps executed', ['flow', 'step', 'status']),
                'step_duration_seconds': Histogram('nbxflow_step_duration_seconds', 'Step execution time', ['flow', 'step']),
                'records_processed': Counter('nbxflow_records_processed_total', 'Total records processed', ['flow', 'step']),
                'active_flows': Gauge('nbxflow_active_flows', 'Number of active flows'),
                'active_steps': Gauge('nbxflow_active_steps', 'Number of active steps')
            })
            
            _prometheus_started = True
            logger.info(f"Prometheus metrics server started on port {settings.prometheus_port}")
            
        except ImportError:
            logger.warning("Prometheus client not available. Install with: pip install 'nbxflow[prometheus]'")
        except Exception as e:
            logger.error(f"Failed to start Prometheus server: {e}")

def get_prometheus_metrics() -> Dict[str, Any]:
    """Get Prometheus metrics."""
    if not _prometheus_started:
        start_prometheus_server()
    return _prometheus_metrics

@contextmanager
def trace_span(name: str, attributes: Optional[Dict[str, Any]] = None):
    """Context manager for creating OpenTelemetry spans."""
    tracer = get_tracer()
    if tracer is None:
        # No-op if tracing not available
        yield None
        return
    
    with tracer.start_as_current_span(name) as span:
        if attributes:
            for key, value in attributes.items():
                # OpenTelemetry attributes must be strings, ints, floats, or bools
                if isinstance(value, (str, int, float, bool)):
                    span.set_attribute(key, value)
                else:
                    span.set_attribute(key, str(value))
        yield span

class TracingContext:
    """Context for managing tracing state."""
    
    def __init__(self, span_name: str, attributes: Optional[Dict[str, Any]] = None):
        self.span_name = span_name
        self.attributes = attributes or {}
        self.span = None
        self.start_time = None
    
    def __enter__(self):
        self.start_time = time.perf_counter()
        self.span_context = trace_span(self.span_name, self.attributes)
        self.span = self.span_context.__enter__()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.span:
            # Add timing information
            duration = time.perf_counter() - self.start_time if self.start_time else 0
            self.span.set_attribute("duration_seconds", duration)
            
            # Add success/failure information
            if exc_type is None:
                self.span.set_attribute("success", True)
            else:
                self.span.set_attribute("success", False)
                self.span.set_attribute("error.type", exc_type.__name__)
                self.span.set_attribute("error.message", str(exc_val))
        
        if self.span_context:
            self.span_context.__exit__(exc_type, exc_val, exc_tb)
        
        return False  # Don't suppress exceptions