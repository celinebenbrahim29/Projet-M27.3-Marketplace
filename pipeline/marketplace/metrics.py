"""Métriques Prometheus exposées par le pipeline (supervision technique)."""
from prometheus_client import Counter, Gauge, Histogram, start_http_server

RUNS = Counter("marketplace_pipeline_runs_total", "Pipeline runs", ["status"])
DURATION = Histogram("marketplace_pipeline_duration_seconds", "Run duration")
LAST_SUCCESS = Gauge(
    "marketplace_pipeline_last_success_timestamp_seconds", "Last successful run"
)
ROWS = Gauge("marketplace_pipeline_last_rows", "Rows loaded")


def record_success(rows):
    ROWS.set(rows)
    LAST_SUCCESS.set_to_current_time()
    RUNS.labels("success").inc()


def record_failure():
    RUNS.labels("failed").inc()


def serve(port):
    """Démarre le serveur HTTP lu par Prometheus (endpoint /metrics)."""
    start_http_server(port)
