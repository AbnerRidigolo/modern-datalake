"""Spark job de ingestão Bronze: olist_customers_dataset.csv -> Delta."""

from __future__ import annotations

import os
import sys

import structlog
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

sys.path.insert(0, "/opt/app")

from ingestion.utils.delta_utils import get_table_version, upsert_delta
from ingestion.utils.spark_session import get_spark_session

logger = structlog.get_logger(__name__)

SOURCE_FILE = "olist_customers_dataset.csv"
MERGE_KEYS = ["customer_id"]

SCHEMA = StructType(
    [
        StructField("customer_id", StringType(), nullable=False),
        StructField("customer_unique_id", StringType(), nullable=True),
        StructField("customer_zip_code_prefix", IntegerType(), nullable=True),
        StructField("customer_city", StringType(), nullable=True),
        StructField("customer_state", StringType(), nullable=True),
    ]
)


def read_csv(spark: SparkSession, raw_path: str) -> DataFrame:
    """Lê o CSV de clientes aplicando o schema explícito.

    Args:
        spark: Sessão Spark ativa.
        raw_path: Diretório que contém o CSV bruto.

    Returns:
        DataFrame com os clientes brutos.
    """
    full_path = f"{raw_path}/{SOURCE_FILE}"
    logger.info("lendo_csv", path=full_path)
    return spark.read.option("header", "true").schema(SCHEMA).csv(full_path)


def run(batch_date: str | None = None) -> None:
    """Executa a ingestão Bronze de clientes de forma idempotente.

    Args:
        batch_date: Data do lote (``YYYY-MM-DD``). Usa a data atual se ``None``.
    """
    raw_path = os.getenv("RAW_DATA_PATH", "/opt/app/data/raw")
    base = os.getenv("DELTA_BASE_PATH", "s3a://lakehouse-bronze")
    target = f"{base}/customers"

    spark = get_spark_session(app_name="ingest_customers")
    try:
        df = read_csv(spark, raw_path).filter(F.col("customer_id").isNotNull())
        upsert_delta(
            spark, df, target, merge_keys=MERGE_KEYS, source=SOURCE_FILE, batch_date=batch_date
        )
        logger.info("ingest_customers_ok", version=get_table_version(spark, target))
    finally:
        spark.stop()


if __name__ == "__main__":
    run(batch_date=sys.argv[1] if len(sys.argv) > 1 else None)
