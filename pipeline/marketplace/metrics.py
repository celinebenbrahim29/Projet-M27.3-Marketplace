
"""Métriques Prometheus du pipeline Marketplace, lues depuis PostgreSQL."""
import logging
import threading
import time

import psycopg2
from prometheus_client import Counter, Gauge, Histogram, start_http_server

from marketplace import config

LOG = logging.getLogger(__name__)

RUNS = Counter("marketplace_pipeline_runs_total", "Pipeline runs", ["status"])
DURATION = Histogram("marketplace_pipeline_duration_seconds", "Run duration")
LAST_SUCCESS = Gauge(
    "marketplace_pipeline_last_success_timestamp_seconds",
    "Last successful run",
)
ROWS = Gauge("marketplace_pipeline_last_rows", "Rows loaded")

# Fonctions conservées pour les exécutions directes du pipeline.
def record_success(rows):
    ROWS.set(rows)
    LAST_SUCCESS.set_to_current_time()
    RUNS.labels("success").inc()


def record_failure():
    RUNS.labels("failed").inc()


def refresh_from_database():
    """Actualise les KPI à partir des exécutions enregistrées."""
    connection = psycopg2.connect(
        host=config.PG_HOST,
        port=config.PG_PORT,
        dbname=config.PG_DATABASE,
        user=config.PG_USER,
        password=config.PG_PASSWORD,
    )

    try:
        with connection.cursor() as cursor:
            cursor.execute("""
                SELECT
                    COUNT(*) FILTER (WHERE status = 'success'),
                    COUNT(*) FILTER (WHERE status = 'failed'),
                    COALESCE(
                        MAX(EXTRACT(EPOCH FROM finished_at))
                        FILTER (WHERE status = 'success'), 0
                    )
                FROM analytics.pipeline_runs
            """)
            successes, failures, last_success = cursor.fetchone()

            cursor.execute("""
                SELECT COALESCE(rows_loaded, 0)
                FROM analytics.pipeline_runs
                WHERE status = 'success'
                ORDER BY finished_at DESC NULLS LAST, id DESC
                LIMIT 1
            """)
            result = cursor.fetchone()
            rows = result[0] if result else 0

            cursor.execute("""
                SELECT
                    COALESCE(
                        AVG(EXTRACT(EPOCH FROM (finished_at - started_at))),
                        0
                    )
                FROM analytics.pipeline_runs
                WHERE status = 'success'
                  AND finished_at IS NOT NULL
                  AND started_at IS NOT NULL
            """)
            average_duration = float(cursor.fetchone()[0])

        # Le compteur Prometheus ne peut qu'augmenter.
        for status, total in (
            ("success", successes),
            ("failed", failures),
        ):
            counter = RUNS.labels(status)
            current = counter._value.get()
            if total > current:
                counter.inc(total - current)

        ROWS.set(rows)
        LAST_SUCCESS.set(float(last_success))

        # On expose aussi une jauge pour la durée moyenne historique.
        AVERAGE_DURATION.set(average_duration)

    finally:
        connection.close()


AVERAGE_DURATION = Gauge(
    "marketplace_pipeline_average_duration_seconds",
    "Average successful pipeline duration from PostgreSQL",
)


def serve(port):
    """Expose les métriques et les synchronise toutes les 30 secondes."""
    RUNS.labels("success")
    RUNS.labels("failed")

    def update_loop():
        while True:
            try:
                refresh_from_database()
            except Exception:
                LOG.exception("Impossible d'actualiser les métriques PostgreSQL")
            time.sleep(30)

    start_http_server(port)
    threading.Thread(target=update_loop, daemon=True).start()
