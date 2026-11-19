
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from airflow import DAG
from airflow.operators.python import PythonOperator


def check_marketplace_api():
    import urllib.request

    url = "http://api-marketplace:5000/health"

    with urllib.request.urlopen(url, timeout=15) as response:
        if response.status != 200:
            raise RuntimeError("API Marketplace indisponible")

    print("API Marketplace disponible")


def execute_pipeline(**context):
    from runner import run

    paris = ZoneInfo("Europe/Paris")

    execution_date = (
        context["data_interval_end"]
        .astimezone(paris)
        .date()
        - timedelta(days=1)
    ).isoformat()

    print(f"Execution du pipeline : {execution_date}")

    rows = run(execution_date)

    print(f"Pipeline termine : {rows} lignes chargees")


with DAG(
    dag_id="marketplace_daily_pipeline",
    description="Orchestration Marketplace Analytics",
    start_date=datetime(
        2026, 11, 19,
        tzinfo=ZoneInfo("Europe/Paris")
    ),
    schedule="*/5 * * * *",
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "marketplace",
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["marketplace", "data-engineering"],
) as dag:

    check_api = PythonOperator(
        task_id="check_marketplace_api",
        python_callable=check_marketplace_api,
    )

    run_marketplace = PythonOperator(
        task_id="execute_marketplace_pipeline",
        python_callable=execute_pipeline,
    )

    check_api >> run_marketplace
