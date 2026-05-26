Crie um projeto completo de Modern Data Lakehouse chamado `lakehouse-olist` usando o dataset público Brazilian E-Commerce (Olist) do Kaggle.

## Stack
- Python 3.11 + PySpark + Delta Lake (camada Bronze)
- dbt-core + dbt-spark (camadas Silver e Gold)
- Apache Airflow 2.x (orquestração)
- Docker Compose (ambiente local completo)
- Terraform (infra AWS: S3, IAM, Glue Catalog)
- Great Expectations (data quality)
- Power BI / Tableau-ready (exposures no dbt)

## Estrutura de pastas
Crie exatamente esta estrutura:

lakehouse-olist/
├── infra/
│   ├── terraform/
│   │   ├── main.tf           # S3 buckets bronze/silver/gold, IAM roles
│   │   ├── variables.tf
│   │   └── outputs.tf
│   └── docker-compose.yml    # Spark, Airflow, Postgres (metastore), MinIO (S3 local)
├── ingestion/
│   ├── spark_jobs/
│   │   ├── ingest_orders.py
│   │   ├── ingest_customers.py
│   │   ├── ingest_products.py
│   │   └── ingest_payments.py
│   └── utils/
│       ├── delta_utils.py    # funções reutilizáveis de escrita Delta
│       └── spark_session.py  # factory de SparkSession configurada
├── dbt/
│   ├── models/
│   │   ├── staging/          # Silver: um model por entidade Olist
│   │   │   ├── stg_orders.sql
│   │   │   ├── stg_customers.sql
│   │   │   ├── stg_products.sql
│   │   │   ├── stg_payments.sql
│   │   │   └── stg_order_reviews.sql
│   │   ├── intermediate/     # joins e enriquecimentos
│   │   │   └── int_orders_enriched.sql
│   │   └── marts/            # Gold: modelos de negócio finais
│   │       ├── fct_orders.sql
│   │       ├── dim_customers.sql
│   │       ├── dim_products.sql
│   │       └── marketing_kpis.sql
│   ├── tests/
│   │   ├── assert_delivery_days_positive.sql
│   │   └── assert_payment_value_positive.sql
│   ├── macros/
│   │   └── generate_schema_name.sql
│   ├── sources.yml           # define bronze como source com freshness checks
│   ├── schema.yml            # descriptions + tests em todos os models
│   ├── exposures.yml         # exposures para Power BI e Tableau
│   └── dbt_project.yml
├── airflow/
│   └── dags/
│       └── lakehouse_pipeline.py   # DAG com TaskGroups: ingest → silver → test → gold → docs
├── data_quality/
│   └── expectations/
│       └── orders_suite.json       # Great Expectations suite para Bronze orders
├── tests/
│   └── unit/
│       └── test_delta_utils.py     # pytest para funções utilitárias
├── .env.example
├── Makefile                        # make setup, make run, make test, make docs
└── README.md                       # completo com arquitetura, setup e decisões técnicas

## Requisitos de implementação

### Ingestão (Bronze)
- Cada spark_job lê CSV e escreve Delta Lake particionado por `_batch_date`
- Adicionar colunas de auditoria: `_ingested_at`, `_source`, `_batch_date`
- Usar merge/upsert para idempotência (re-executar não duplica dados)
- Habilitar time travel com `delta.logRetentionDuration = "30 days"`
- delta_utils.py deve ter funções: `write_bronze()`, `upsert_delta()`, `get_table_version()`

### dbt Silver (staging models)
- Todos com `materialized='incremental'` e `unique_key` definido
- Tratar nulos, cast de tipos, deduplicação com ROW_NUMBER()
- Tests obrigatórios em todo model: not_null, unique, accepted_values onde aplicável
- Descriptions em português em todos os models e colunas no schema.yml

### dbt Gold (marts)
- `fct_orders`: grain = 1 linha por pedido, incluir métricas calculadas (delivery_days, delivery_tier, revenue_bucket)
- `dim_customers`: SCD Tipo 2 simulado com snapshot dbt
- `marketing_kpis`: model wide com cohort de clientes, LTV estimado, taxa de recompra por estado
- `materialized='table'` para todos os marts

### Airflow DAG
- Usar TaskGroups: `ingest`, `transform_silver`, `quality_checks`, `transform_gold`, `publish`
- Sensor de arquivo para só rodar quando novos CSVs chegarem
- Callbacks de on_failure_callback com log estruturado
- SLA de 2h para o pipeline completo

### Docker Compose
- Serviços: spark-master, spark-worker, airflow-webserver, airflow-scheduler, postgres, minio
- MinIO como S3 local (sem precisar de AWS real para desenvolvimento)
- Volumes persistentes para dados Delta
- Health checks em todos os serviços

### README.md
Deve conter:
1. Diagrama de arquitetura em ASCII art
2. Pré-requisitos e setup em 5 comandos
3. Como baixar o dataset Olist do Kaggle
4. Explicação das decisões técnicas (por que Delta Lake, por que dbt incremental, etc.)
5. Como rodar localmente vs AWS
6. Seção "LinkedIn Post" com texto sugerido para publicação

### Makefile

make setup     # cria .env, instala dependências, sobe Docker
make ingest    # roda todos os spark jobs
make transform # roda dbt run + dbt test
make docs      # gera dbt docs e abre no browser
make test      # roda pytest
make clean     # para containers e limpa volumes

## Qualidade de código
- Type hints em todo Python
- Docstrings em todas as funções
- f-strings, não .format()
- Logging estruturado com `structlog` (não print)
- `.env.example` com todas as variáveis necessárias documentadas

## Dataset
Use estas tabelas do Olist:
- olist_orders_dataset.csv
- olist_customers_dataset.csv
- olist_products_dataset.csv
- olist_order_payments_dataset.csv
- olist_order_reviews_dataset.csv

Comece pelo docker-compose.yml e Makefile para que o ambiente suba primeiro. Depois implemente na ordem: spark_session.py → delta_utils.py → spark_jobs → dbt models (staging → intermediate → marts) → DAG Airflow → testes → README.