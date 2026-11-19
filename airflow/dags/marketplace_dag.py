
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator


def check_airflow():
    print("Airflow fonctionne correctement !")
    print("Projet : Marketplace Analytics")
    print("Le DAG est prêt.")


with DAG(
    dag_id="marketplace_daily_pipeline",
    description="Orchestration du projet Marketplace Analytics",
    start_date=datetime(2026, 11, 19),
    schedule_interval="0 8 * *",
    catchup=False,
    default_args={
        "owner": "marketplace",
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["marketplace", "data-engineering"],
) as dag:

    test_airflow = PythonOperator(
        task_id="check_airflow",
        python_callable=check_airflow,
    )
