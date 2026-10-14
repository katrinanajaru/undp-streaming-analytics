from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.sensors.python import PythonSensor

from undp_pipeline_helpers import ensure_cdc_ingestion, source_data_ready


with DAG(
    dag_id="undp_analytics_pipeline",
    description="Provision CDC ingestion, build analytics, and refresh impact scores.",
    start_date=datetime(2025, 1, 1),
    schedule=timedelta(minutes=15),
    catchup=False,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=1)},
    tags=["undp", "analytics", "cdc"],
    doc_md=(
        "Ensures ClickHouse CDC tables and the Debezium connector exist, waits for "
        "source rows, builds the dbt models, then refreshes `undp.impact_scores`. "
        "Requires the external PostgreSQL database to be reachable and configured "
        "through the local `.env` file."
    ),
) as dag:
    provision_cdc = PythonOperator(
        task_id="provision_cdc_ingestion",
        python_callable=ensure_cdc_ingestion,
    )

    wait_for_sources = PythonSensor(
        task_id="wait_for_source_data",
        python_callable=source_data_ready,
        mode="reschedule",
        poke_interval=30,
        timeout=3600,
    )

    build_analytics = BashOperator(
        task_id="build_analytics_models",
        bash_command=(
            "dbt run --project-dir /opt/airflow/dbt "
            "--profiles-dir /opt/airflow/dbt "
            "--target-path /tmp/dbt-target "
            "--log-path /tmp/dbt-logs"
        ),
    )

    score_projects = BashOperator(
        task_id="score_project_impact",
        bash_command="python /opt/airflow/scripts/ai_scoring_engine.py",
    )

    provision_cdc >> wait_for_sources >> build_analytics >> score_projects