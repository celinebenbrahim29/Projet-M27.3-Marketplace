"""Tests du DAG (nécessitent Airflow : ignorés dans le conteneur `pipeline`).

Lancer dans le conteneur Airflow :
    docker compose exec airflow-scheduler python -m pytest /opt/airflow/tests/test_dag.py -v
"""
from pathlib import Path

import pytest

pytest.importorskip("airflow")

from airflow.models import DagBag  # noqa: E402

ROOT = Path(__file__).resolve().parents[1]
DAG_FOLDER = next(p for p in (ROOT / "airflow" / "dags", ROOT / "dags") if p.is_dir())


@pytest.fixture(scope="module")
def dagbag():
    return DagBag(dag_folder=str(DAG_FOLDER), include_examples=False)


def test_no_import_errors(dagbag):
    assert dagbag.import_errors == {}


def test_all_dags_catchup_false(dagbag):
    assert dagbag.dags
    assert all(dag.catchup is False for dag in dagbag.dags.values())


def test_pipeline_steps_run_in_order(dagbag):
    dag = dagbag.get_dag("marketplace_daily_pipeline")
    order = ["check_marketplace_api", "extract_to_raw", "load_staging",
             "transform_dwh", "aggregate_analytics", "record_run"]
    assert set(dag.task_ids) == set(order)
    for upstream, downstream in zip(order, order[1:]):
        assert downstream in dag.get_task(upstream).downstream_task_ids


def test_dag_runs_daily(dagbag):
    assert dagbag.get_dag("marketplace_daily_pipeline").schedule_interval == "@daily"
