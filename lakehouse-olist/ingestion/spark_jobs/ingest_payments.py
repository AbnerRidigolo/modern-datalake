"""Spark job de ingestão Bronze: olist_order_payments_dataset.csv -> Delta.

Também ingere olist_order_reviews_dataset.csv, pois ambos compartilham a chave
``order_id`` e são carregados na mesma etapa do pipeline.
"""

from __future__ import annotations

import os
import sys

import structlog
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

sys.path.insert(0, "/opt/app")

from ingestion.utils.delta_utils import get_table_version, upsert_delta
from ingestion.utils.spark_session import get_spark_session

logger = structlog.get_logger(__name__)

PAYMENTS_FILE = "olist_order_payments_dataset.csv"
REVIEWS_FILE = "olist_order_reviews_dataset.csv"

PAYMENTS_SCHEMA = StructType(
    [
        StructField("order_id", StringType(), nullable=False),
        StructField("payment_sequential", IntegerType(), nullable=False),
        StructField("payment_type", StringType(), nullable=True),
        StructField("payment_installments", IntegerType(), nullable=True),
        StructField("payment_value", DoubleType(), nullable=True),
    ]
)

REVIEWS_SCHEMA = StructType(
    [
        StructField("review_id", StringType(), nullable=False),
        StructField("order_id", StringType(), nullable=False),
        StructField("review_score", IntegerType(), nullable=True),
        StructField("review_comment_title", StringType(), nullable=True),
        StructField("review_comment_message", StringType(), nullable=True),
        StructField("review_creation_date", TimestampType(), nullable=True),
        StructField("review_answer_timestamp", TimestampType(), nullable=True),
    ]
)


def read_csv(
    spark: SparkSession, raw_path: str, file_name: str, schema: StructType
) -> DataFrame:
    """Lê um CSV aplicando o schema fornecido.

    Args:
        spark: Sessão Spark ativa.
        raw_path: Diretório que contém o CSV bruto.
        file_name: Nome do arquivo CSV.
        schema: Schema explícito a aplicar.

    Returns:
        DataFrame com os dados brutos.
    """
    full_path = f"{raw_path}/{file_name}"
    logger.info("lendo_csv", path=full_path)
    return (
        spark.read.option("header", "true")
        .option("timestampFormat", "yyyy-MM-dd HH:mm:ss")
        .option("multiLine", "true")
        .option("escape", '"')
        .schema(schema)
        .csv(full_path)
    )


def run(batch_date: str | None = None) -> None:
    """Executa a ingestão Bronze de pagamentos e reviews de forma idempotente.

    Args:
        batch_date: Data do lote (``YYYY-MM-DD``). Usa a data atual se ``None``.
    """
    raw_path = os.getenv("RAW_DATA_PATH", "/opt/app/data/raw")
    base = os.getenv("DELTA_BASE_PATH", "s3a://lakehouse-bronze")

    spark = get_spark_session(app_name="ingest_payments")
    try:
        payments = read_csv(spark, raw_path, PAYMENTS_FILE, PAYMENTS_SCHEMA).filter(
            F.col("order_id").isNotNull()
        )
        upsert_delta(
            spark,
            payments,
            f"{base}/payments",
            merge_keys=["order_id", "payment_sequential"],
            source=PAYMENTS_FILE,
            batch_date=batch_date,
        )
        logger.info("ingest_payments_ok", version=get_table_version(spark, f"{base}/payments"))

        reviews = read_csv(spark, raw_path, REVIEWS_FILE, REVIEWS_SCHEMA).filter(
            F.col("review_id").isNotNull()
        )
        upsert_delta(
            spark,
            reviews,
            f"{base}/order_reviews",
            merge_keys=["review_id"],
            source=REVIEWS_FILE,
            batch_date=batch_date,
        )
        logger.info("ingest_reviews_ok", version=get_table_version(spark, f"{base}/order_reviews"))
    finally:
        spark.stop()


if __name__ == "__main__":
    run(batch_date=sys.argv[1] if len(sys.argv) > 1 else None)
