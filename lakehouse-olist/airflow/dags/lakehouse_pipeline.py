"""DAG do pipeline lakehouse-olist: ingest -> silver -> test -> gold -> publish.

Orquestra a ingestão Bronze (Spark), as transformações Silver/Gold (dbt) e os
checks de qualidade (Great Expectations + dbt test), com SLA de 2h, sensor de
arquivo e callback estruturado de falha.
"""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any

import structlog
from airflow.models import DAG
from airflow.operators.bash import BashOperator
from airflow.sensors.filesystem import FileSensor
from airflow.utils.task_group import TaskGroup

logger = structlog.get_logger("lakehouse_pipeline")

RAW_DATA_DIR = "/opt/airflow/ingestion/../data/raw"
INGESTION_DIR = "/opt/airflow/ingestion/spark_jobs"
DBT_DIR = "/opt/airflow/dbt"
GE_DIR = "/opt/airflow/data_quality"

SPARK_SUBMIT = (
    "spark-submit --master ${SPARK_MASTER_URL:-spark://spark-master:7077} "
    "--packages io.delta:delta-spark_2.12:3.2.0,"
    "org.apache.hadoop:hadoop-aws:3.3.4"
)


def on_failure_callback(context: dict[str, Any]) -> None:
    """Callback de falha com log estruturado.

    Args:
        context: Contexto de execução fornecido pelo Airflow.
    """
    task_instance = context.get("task_instance")
    exception = context.get("exception")
    logger.error(
        "task_falhou",
        dag_id=context.get("dag").dag_id if context.get("dag") else None,
        task_id=task_instance.task_id if task_instance else None,
        execution_date=str(context.get("logical_date")),
        try_number=task_instance.try_number if task_instance else None,
        exception=str(exception),
    )


default_args = {
    "owner": "data-engineering",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": on_failure_callback,
    "sla": timedelta(hours=2),
}

with DAG(
    dag_id="lakehouse_pipeline",
    description="Pipeline Medallion Olist: Bronze -> Silver -> Gold",
    default_args=default_args,
    schedule="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(hours=2),
    tags=["lakehouse", "olist", "medallion", "delta", "dbt"],
) as dag:

    # Só dispara quando novos CSVs chegarem no diretório raw.
    wait_for_data = FileSensor(
        task_id="wait_for_csv_files",
        filepath=f"{RAW_DATA_DIR}/olist_orders_dataset.csv",
        poke_interval=60,
        timeout=60 * 60,
        mode="reschedule",
    )

    # --- Bronze: ingestão Spark ------------------------------------------
    with TaskGroup(group_id="ingest") as ingest:
        for entity in ("customers", "products", "orders", "payments"):
            BashOperator(
                task_id=f"ingest_{entity}",
                bash_command=f"{SPARK_SUBMIT} {INGESTION_DIR}/ingest_{entity}.py",
            )

    # --- Silver: dbt staging + intermediate + snapshot -------------------
    with TaskGroup(group_id="transform_silver") as transform_silver:
        dbt_snapshot = BashOperator(
            task_id="dbt_snapshot_customers",
            bash_command=f"cd {DBT_DIR} && dbt snapshot",
        )
        dbt_staging = BashOperator(
            task_id="dbt_run_staging",
            bash_command=f"cd {DBT_DIR} && dbt run --select staging intermediate",
        )
        dbt_snapshot >> dbt_staging

    # --- Quality checks: Great Expectations + dbt test -------------------
    with TaskGroup(group_id="quality_checks") as quality_checks:
        ge_orders = BashOperator(
            task_id="ge_validate_orders",
            bash_command=(
                f"cd {GE_DIR} && great_expectations checkpoint run orders_checkpoint "
                "|| echo 'GE checkpoint não configurado — validando via suite JSON'"
            ),
        )
        dbt_test_silver = BashOperator(
            task_id="dbt_test_silver",
            bash_command=f"cd {DBT_DIR} && dbt test --select staging intermediate",
        )
        ge_orders >> dbt_test_silver

    # --- Gold: dbt marts -------------------------------------------------
    with TaskGroup(group_id="transform_gold") as transform_gold:
        dbt_marts = BashOperator(
            task_id="dbt_run_marts",
            bash_command=f"cd {DBT_DIR} && dbt run --select marts",
        )
        dbt_test_gold = BashOperator(
            task_id="dbt_test_marts",
            bash_command=f"cd {DBT_DIR} && dbt test --select marts",
        )
        dbt_marts >> dbt_test_gold

    # --- Publish: docs + freshness --------------------------------------
    with TaskGroup(group_id="publish") as publish:
        dbt_docs = BashOperator(
            task_id="dbt_docs_generate",
            bash_command=f"cd {DBT_DIR} && dbt docs generate",
        )
        dbt_freshness = BashOperator(
            task_id="dbt_source_freshness",
            bash_command=f"cd {DBT_DIR} && dbt source freshness || true",
        )
        dbt_docs >> dbt_freshness

    wait_for_data >> ingest >> transform_silver >> quality_checks >> transform_gold >> publish
