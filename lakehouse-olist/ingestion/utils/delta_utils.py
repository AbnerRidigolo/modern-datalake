"""Funções reutilizáveis para escrita e manutenção de tabelas Delta (Bronze)."""

from __future__ import annotations

from datetime import date, datetime, timezone

import structlog
from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

logger = structlog.get_logger(__name__)

AUDIT_COLUMNS = ("_ingested_at", "_source", "_batch_date")


def _add_audit_columns(df: DataFrame, source: str, batch_date: str) -> DataFrame:
    """Adiciona as colunas de auditoria padrão da camada Bronze.

    Args:
        df: DataFrame de entrada.
        source: Identificador da origem dos dados (ex.: nome do CSV).
        batch_date: Data do lote no formato ``YYYY-MM-DD``.

    Returns:
        DataFrame com ``_ingested_at``, ``_source`` e ``_batch_date``.
    """
    return (
        df.withColumn("_ingested_at", F.lit(datetime.now(timezone.utc)).cast("timestamp"))
        .withColumn("_source", F.lit(source))
        .withColumn("_batch_date", F.lit(batch_date).cast("date"))
    )


def write_bronze(
    df: DataFrame,
    path: str,
    source: str,
    *,
    batch_date: str | None = None,
    mode: str = "append",
) -> str:
    """Escreve um DataFrame na camada Bronze como tabela Delta particionada.

    Adiciona colunas de auditoria, particiona por ``_batch_date`` e habilita
    retenção de log de 30 dias (time travel).

    Args:
        df: DataFrame a ser persistido.
        path: Caminho destino da tabela Delta (ex.: ``s3a://bucket/tabela``).
        source: Origem dos dados, gravada em ``_source``.
        batch_date: Data do lote (``YYYY-MM-DD``). Usa a data atual se ``None``.
        mode: Modo de escrita Spark (``append`` ou ``overwrite``).

    Returns:
        O ``path`` da tabela escrita.
    """
    batch_date = batch_date or date.today().isoformat()
    enriched = _add_audit_columns(df, source=source, batch_date=batch_date)

    (
        enriched.write.format("delta")
        .mode(mode)
        .partitionBy("_batch_date")
        .option("delta.logRetentionDuration", "interval 30 days")
        .option("delta.deletedFileRetentionDuration", "interval 30 days")
        .option("mergeSchema", "true")
        .save(path)
    )

    logger.info(
        "write_bronze_concluido",
        path=path,
        source=source,
        batch_date=batch_date,
        mode=mode,
        rows=enriched.count(),
    )
    return path


def upsert_delta(
    spark: SparkSession,
    df: DataFrame,
    path: str,
    merge_keys: list[str],
    source: str,
    *,
    batch_date: str | None = None,
) -> None:
    """Faz merge/upsert idempotente em uma tabela Delta.

    Se a tabela não existir, cria via :func:`write_bronze`. Caso exista, faz
    ``MERGE`` pelas ``merge_keys`` — atualizando linhas existentes e inserindo
    novas — de forma que re-executar o job não duplica dados.

    Args:
        spark: Sessão Spark ativa.
        df: DataFrame com os dados de origem.
        path: Caminho da tabela Delta.
        merge_keys: Colunas que identificam unicamente cada linha.
        source: Origem dos dados, gravada em ``_source``.
        batch_date: Data do lote (``YYYY-MM-DD``). Usa a data atual se ``None``.
    """
    batch_date = batch_date or date.today().isoformat()

    if not DeltaTable.isDeltaTable(spark, path):
        logger.info("upsert_delta_tabela_nova", path=path)
        write_bronze(df, path, source=source, batch_date=batch_date, mode="append")
        return

    enriched = _add_audit_columns(df, source=source, batch_date=batch_date)
    condition = " AND ".join(f"t.{k} = s.{k}" for k in merge_keys)
    delta_table = DeltaTable.forPath(spark, path)

    (
        delta_table.alias("t")
        .merge(enriched.alias("s"), condition)
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )

    logger.info(
        "upsert_delta_concluido",
        path=path,
        merge_keys=merge_keys,
        batch_date=batch_date,
    )


def get_table_version(spark: SparkSession, path: str) -> int:
    """Retorna a versão (snapshot) mais recente de uma tabela Delta.

    Útil para time travel e validação de idempotência em testes.

    Args:
        spark: Sessão Spark ativa.
        path: Caminho da tabela Delta.

    Returns:
        O número da versão atual; ``-1`` se o caminho não for uma tabela Delta.
    """
    if not DeltaTable.isDeltaTable(spark, path):
        logger.warning("get_table_version_nao_e_delta", path=path)
        return -1

    history = DeltaTable.forPath(spark, path).history(1)
    version = int(history.select("version").first()[0])
    logger.info("get_table_version", path=path, version=version)
    return version
