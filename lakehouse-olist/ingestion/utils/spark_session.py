"""Factory de SparkSession configurada para Delta Lake + S3/MinIO."""

from __future__ import annotations

import os

import structlog
from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

logger = structlog.get_logger(__name__)

DELTA_PACKAGE_VERSION = "3.2.0"


def get_spark_session(
    app_name: str = "lakehouse-olist",
    *,
    log_retention: str | None = None,
    enable_s3: bool = True,
) -> SparkSession:
    """Cria (ou recupera) uma ``SparkSession`` pronta para Delta Lake.

    Configura a extensão Delta, o catálogo Delta e — quando ``enable_s3`` é
    verdadeiro — o acesso a um endpoint compatível com S3 (MinIO em dev, S3
    real em produção). As credenciais e endpoint são lidos das variáveis de
    ambiente ``AWS_ACCESS_KEY_ID``, ``AWS_SECRET_ACCESS_KEY`` e
    ``AWS_ENDPOINT_URL``.

    Args:
        app_name: Nome da aplicação exibido na Spark UI.
        log_retention: Período de retenção do log Delta (ex.: ``"interval 30
            days"``). Se ``None``, usa o valor da env ``DELTA_LOG_RETENTION`` ou
            o padrão de 30 dias.
        enable_s3: Quando ``True`` configura o conector ``s3a``.

    Returns:
        Uma ``SparkSession`` ativa e configurada.
    """
    retention = log_retention or os.getenv(
        "DELTA_LOG_RETENTION", "interval 30 days"
    )

    builder = (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        # Retenção de log para habilitar time travel (>= 30 dias)
        .config("spark.databricks.delta.properties.defaults.logRetentionDuration", retention)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "8")
    )

    if enable_s3:
        endpoint = os.getenv("AWS_ENDPOINT_URL", "http://minio:9000")
        builder = (
            builder.config("spark.hadoop.fs.s3a.endpoint", endpoint)
            .config(
                "spark.hadoop.fs.s3a.access.key",
                os.getenv("AWS_ACCESS_KEY_ID", "minioadmin"),
            )
            .config(
                "spark.hadoop.fs.s3a.secret.key",
                os.getenv("AWS_SECRET_ACCESS_KEY", "minioadmin"),
            )
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            .config(
                "spark.hadoop.fs.s3a.impl",
                "org.apache.hadoop.fs.s3a.S3AFileSystem",
            )
            .config(
                "spark.hadoop.fs.s3a.aws.credentials.provider",
                "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
            )
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        )

    spark = configure_spark_with_delta_pip(
        builder, extra_packages=["org.apache.hadoop:hadoop-aws:3.3.4"]
    ).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    logger.info(
        "spark_session_criada",
        app_name=app_name,
        log_retention=retention,
        s3_enabled=enable_s3,
    )
    return spark
