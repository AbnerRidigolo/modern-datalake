"""Utilitários reutilizáveis de ingestão (Spark + Delta)."""

from ingestion.utils.delta_utils import (
    get_table_version,
    upsert_delta,
    write_bronze,
)
from ingestion.utils.spark_session import get_spark_session

__all__ = [
    "get_spark_session",
    "write_bronze",
    "upsert_delta",
    "get_table_version",
]
