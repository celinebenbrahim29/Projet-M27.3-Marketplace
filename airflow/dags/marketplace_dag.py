"""DAG quotidien : orchestre le pipeline Marketplace, une tâche par étape.

    check_api -> extract_to_raw -> load_staging -> transform_dwh -> aggregate_analytics
                                                                          \\-> record_run

Chaque tâche est idempotente : la rejouer ne crée pas de doublons. Les données passent
d'une tâche à l'autre par la couche raw (MinIO) et par PostgreSQL, jamais par XCom
(le catalogue pèse plusieurs dizaines de Mo). La logique métier est dans le package
`marketplace` (dossier pipeline/, monté dans le worker) ; Airflow planifie, relance,
journalise et affiche l'état de chaque étape.
"""
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from airflow import DAG
from airflow.exceptions import AirflowFailException
from airflow.operators.python import PythonOperator
from airflow.utils.state import TaskInstanceState
from airflow.utils.trigger_rule import TriggerRule

from marketplace_api_hook import MarketplaceAPIHook

PARIS = ZoneInfo("Europe/Paris")


def target_day(context):
    """Journée traitée : la veille de la fin de l'intervalle, en heure de Paris."""
    end = context["data_interval_end"].astimezone(PARIS).date()
    return (end - timedelta(days=1)).isoformat()


def check_marketplace_api():
    """Échoue si l'API source ne répond pas : inutile de continuer sinon."""
    print(MarketplaceAPIHook().check_health())


def extract_to_raw(**context):
    """Appelle l'API via le Hook, valide les données et les archive dans MinIO."""
    from marketplace import pipeline

    pipeline.extract_to_raw(target_day(context), fetch=MarketplaceAPIHook().fetch_dataset)


def load_staging(**context):
    """Charge dans staging les données archivées dans MinIO."""
    from marketplace import pipeline, warehouse

    dt = target_day(context)
    warehouse.load_staging(dt, pipeline.read_raw(dt))


def transform_dwh(**context):
    """Transforme staging en modèle en étoile (DELETE + INSERT par date)."""
    from marketplace import warehouse

    warehouse.transform_dwh(target_day(context))


def aggregate_analytics(**context):
    """Calcule les agrégats du jour et les anomalies de chiffre d'affaires."""
    from marketplace import warehouse

    rows = warehouse.aggregate_analytics(target_day(context))
    print(f"Pipeline termine : {rows} lignes chargees")


def record_run(**context):
    """Journalise le run dans analytics.pipeline_runs, puis le fait échouer si une étape a échoué.

    S'exécute toujours (trigger_rule ALL_DONE). Sans le `raise` final, cette tâche réussie
    ferait passer tout le run pour un succès malgré l'échec d'une étape précédente.
    """
    from marketplace import warehouse

    dt = target_day(context)
    this_task = context["task"].task_id
    failed = [
        ti.task_id
        for ti in context["dag_run"].get_task_instances()
        if ti.task_id != this_task and ti.state != TaskInstanceState.SUCCESS
    ]
    if failed:
        message = f"Étapes en échec : {', '.join(failed)}"
        warehouse.record_run(dt, "failed", error=message)
        raise AirflowFailException(message)
    warehouse.record_run(dt, "success", rows=warehouse.count_facts(dt))


with DAG(
    dag_id="marketplace_daily_pipeline",
    description="Orchestration Marketplace Analytics",
    start_date=datetime(2026, 11, 19, tzinfo=PARIS),
    schedule="@daily",
    catchup=False,  # l'historique se charge avec `runner.py backfill`, pas avec Airflow
    max_active_runs=1,
    default_args={
        "owner": "marketplace",
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["marketplace", "data-engineering"],
) as dag:

    check_api = PythonOperator(task_id="check_marketplace_api", python_callable=check_marketplace_api)
    extract = PythonOperator(task_id="extract_to_raw", python_callable=extract_to_raw)
    staging = PythonOperator(task_id="load_staging", python_callable=load_staging)
    dwh = PythonOperator(task_id="transform_dwh", python_callable=transform_dwh)
    analytics = PythonOperator(task_id="aggregate_analytics", python_callable=aggregate_analytics)
    record = PythonOperator(
        task_id="record_run",
        python_callable=record_run,
        trigger_rule=TriggerRule.ALL_DONE,
        retries=0,
    )

    check_api >> extract >> staging >> dwh >> analytics >> record
