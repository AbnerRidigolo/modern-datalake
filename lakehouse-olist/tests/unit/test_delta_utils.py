"""Testes unitários para ingestion.utils.delta_utils."""

from __future__ import annotations

from pathlib import Path

import pytest
from pyspark.sql import SparkSession

from ingestion.utils.delta_utils import (
    AUDIT_COLUMNS,
    get_table_version,
    upsert_delta,
    write_bronze,
)


def _sample_df(spark: SparkSession):
    """Cria um DataFrame de pedidos de exemplo."""
    return spark.createDataFrame(
        [("o1", "c1", "delivered"), ("o2", "c2", "shipped")],
        ["order_id", "customer_id", "order_status"],
    )


def test_write_bronze_adiciona_colunas_auditoria(spark, tmp_path: Path) -> None:
    """write_bronze deve persistir a tabela com as 3 colunas de auditoria."""
    target = str(tmp_path / "orders")
    write_bronze(_sample_df(spark), target, source="orders.csv", batch_date="2024-01-01")

    result = spark.read.format("delta").load(target)
    for col in AUDIT_COLUMNS:
        assert col in result.columns
    assert result.count() == 2


def test_write_bronze_particiona_por_batch_date(spark, tmp_path: Path) -> None:
    """A tabela escrita deve estar particionada por _batch_date."""
    target = str(tmp_path / "orders_part")
    write_bronze(_sample_df(spark), target, source="orders.csv", batch_date="2024-02-02")

    partitions = [p.name for p in (tmp_path / "orders_part").iterdir()]
    assert any("_batch_date=2024-02-02" in p for p in partitions)


def test_upsert_delta_e_idempotente(spark, tmp_path: Path) -> None:
    """Reexecutar o upsert com os mesmos dados não deve duplicar linhas."""
    target = str(tmp_path / "orders_upsert")
    df = _sample_df(spark)

    upsert_delta(spark, df, target, merge_keys=["order_id"], source="orders.csv")
    upsert_delta(spark, df, target, merge_keys=["order_id"], source="orders.csv")

    result = spark.read.format("delta").load(target)
    assert result.count() == 2  # sem duplicação


def test_upsert_delta_atualiza_e_insere(spark, tmp_path: Path) -> None:
    """Upsert deve atualizar registros existentes e inserir novos."""
    target = str(tmp_path / "orders_merge")
    upsert_delta(spark, _sample_df(spark), target, merge_keys=["order_id"], source="orders.csv")

    updated = spark.createDataFrame(
        [("o1", "c1", "canceled"), ("o3", "c3", "created")],
        ["order_id", "customer_id", "order_status"],
    )
    upsert_delta(spark, updated, target, merge_keys=["order_id"], source="orders.csv")

    result = spark.read.format("delta").load(target)
    assert result.count() == 3  # o1, o2, o3
    status_o1 = result.filter("order_id = 'o1'").select("order_status").first()[0]
    assert status_o1 == "canceled"


def test_get_table_version_incrementa(spark, tmp_path: Path) -> None:
    """A versão da tabela deve aumentar a cada escrita."""
    target = str(tmp_path / "orders_version")
    write_bronze(_sample_df(spark), target, source="orders.csv", batch_date="2024-01-01")
    v0 = get_table_version(spark, target)

    upsert_delta(
        spark,
        spark.createDataFrame([("o9", "c9", "created")], ["order_id", "customer_id", "order_status"]),
        target,
        merge_keys=["order_id"],
        source="orders.csv",
    )
    v1 = get_table_version(spark, target)

    assert v0 == 0
    assert v1 > v0


def test_get_table_version_caminho_invalido(spark, tmp_path: Path) -> None:
    """Caminho que não é tabela Delta deve retornar -1."""
    assert get_table_version(spark, str(tmp_path / "nao_existe")) == -1
