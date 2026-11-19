# lakehouse-olist

Modern Data Lakehouse de ponta a ponta sobre o dataset público **Brazilian
E-Commerce (Olist)**, seguindo a arquitetura **Medallion** (Bronze → Silver →
Gold) com Delta Lake, dbt, Airflow e infraestrutura como código.

---

## 1. Arquitetura

```
                          ┌──────────────────────────────────────────────────┐
                          │                  ORQUESTRAÇÃO                      │
                          │              Apache Airflow (DAG)                  │
                          │  ingest → silver → quality → gold → publish        │
                          └───────────────┬──────────────────────────────────┘
                                          │
   CSV Olist (Kaggle)                     ▼
   ┌──────────────┐      ┌──────────────────────┐      ┌────────────────────┐      ┌───────────────────┐
   │  data/raw    │ ───► │   BRONZE (Delta)      │ ───► │  SILVER (Delta)    │ ───► │   GOLD (Delta)    │
   │  *.csv       │      │   PySpark + upsert    │      │  dbt staging +     │      │  dbt marts        │
   │              │      │   _ingested_at        │      │  intermediate      │      │  fct_orders       │
   │              │      │   _source             │      │  (incremental,     │      │  dim_customers    │
   │              │      │   _batch_date         │      │   merge, dedup)    │      │  dim_products     │
   └──────────────┘      └──────────┬───────────┘      └─────────┬──────────┘      │  marketing_kpis   │
                                    │                            │                 └─────────┬─────────┘
                                    ▼                            ▼                           │
                          ┌──────────────────┐         ┌──────────────────┐                 ▼
                          │ Great Expectations│        │    dbt tests      │       ┌───────────────────┐
                          │  orders_suite     │        │ not_null/unique/  │       │  Power BI / Tableau│
                          └──────────────────┘         │ accepted_values   │       │   (exposures)     │
                                                       └──────────────────┘        └───────────────────┘

   Storage:  MinIO (S3 local)  ──────────  em produção: AWS S3 + Glue Catalog (Terraform)
   Compute:  Spark master + worker        Metastore: Postgres
```

| Camada     | Tecnologia               | Responsabilidade                                            |
|------------|--------------------------|-------------------------------------------------------------|
| **Bronze** | PySpark + Delta Lake     | Ingestão bruta idempotente, auditoria, time travel          |
| **Silver** | dbt (staging/intermediate)| Limpeza, cast, deduplicação, joins, testes                 |
| **Gold**   | dbt (marts)              | Modelos de negócio: fatos, dimensões SCD2, KPIs             |
| **Infra**  | Docker Compose / Terraform| Ambiente local (MinIO) e AWS (S3 + IAM + Glue)             |

---

## 2. Executar localmente

```bash
git clone https://github.com/AbnerRidigolo/modern-datalake.git
cd modern-datalake/lakehouse-olist
make setup                       # cria .env, builda imagens e sobe tudo
# baixe os CSVs do Olist em data/raw/ (ver seção 3)
make ingest                      # Bronze: roda os spark jobs
make dbt-deps                    # instala as dependências do dbt
make transform                   # Silver + Gold: dbt snapshot + run + test
make docs                        # gera e serve a documentação dbt
```

Serviços após `make setup`:

| Serviço         | URL                     | Credenciais        |
|-----------------|-------------------------|--------------------|
| Airflow         | http://localhost:8081   | admin / admin      |
| MinIO Console   | http://localhost:9001   | minioadmin / minioadmin |
| Spark Master UI | http://localhost:8080   | —                  |
| dbt docs        | http://localhost:8085   | —                  |

**Pré-requisitos:** Docker + Docker Compose v2, ~6 GB de RAM livre, Make. Para
rodar os testes unitários localmente: Python 3.11 e `pip install -r
ingestion/requirements.txt`.

---

## 3. Baixar o dataset Olist do Kaggle

O dataset está em
[kaggle.com/datasets/olistbr/brazilian-ecommerce](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce).

**Via Kaggle CLI:**

```bash
pip install kaggle
# configure ~/.kaggle/kaggle.json com seu token de API do Kaggle
kaggle datasets download -d olistbr/brazilian-ecommerce -p data/raw --unzip
```

Garanta que estes arquivos fiquem em `data/raw/`:

- `olist_orders_dataset.csv`
- `olist_customers_dataset.csv`
- `olist_products_dataset.csv`
- `olist_order_payments_dataset.csv`
- `olist_order_reviews_dataset.csv`

---

## 4. Decisões técnicas

**Por que Delta Lake na Bronze?** Transações ACID sobre arquivos em object
storage, `MERGE` nativo (idempotência sem lógica manual de dedup) e *time
travel*. Configuramos `delta.logRetentionDuration = 30 days` para permitir
auditoria e reprocessamento de versões anteriores.

**Por que upsert/merge em vez de append puro?** Reprocessar um lote (ex.:
correção de CSV) não pode duplicar dados. O `MERGE` por chave de negócio
(`order_id`, etc.) torna cada job re-executável com segurança — validado nos
testes de `delta_utils`.

**Por que dbt incremental na Silver?** Os volumes do Olist crescem com o tempo;
materializar `incremental` + `incremental_strategy='merge'` reprocessa apenas os
lotes recentes (`lookback_days`), reduzindo custo. A deduplicação usa
`ROW_NUMBER()` pegando o registro mais recente por chave.

**Por que `table` na Gold?** Marts são consumidos por BI com leituras
frequentes e joins pesados; materializar como tabela Delta dá performance
previsível para Power BI/Tableau.

**SCD Tipo 2 em `dim_customers`:** usamos `dbt snapshot` (estratégia `check`
sobre cidade/estado) para versionar o histórico do cliente, e o model lê o
snapshot expondo `valid_from`, `valid_to` e `is_current`.

**Qualidade em duas frentes:** Great Expectations valida a Bronze (schema,
unicidade, domínios) *antes* de transformar; dbt tests validam Silver/Gold
(`not_null`, `unique`, `accepted_values` + testes singulares de regra de
negócio). Falhar cedo evita propagar lixo para o Gold.

**MinIO como S3 local:** desenvolvimento 100% offline com a mesma API (`s3a://`)
usada em produção — o mesmo código roda local e na AWS trocando apenas o
endpoint.

---

## 5. Rodar localmente vs. AWS

### Local (padrão)
Tudo via Docker Compose; storage no MinIO (`s3a://lakehouse-bronze`), metastore
no Postgres. Basta `make setup`.

### AWS
1. Provisione a infra:
   ```bash
   cd infra/terraform
   terraform init
   terraform apply -var="environment=prod"
   ```
   Cria buckets S3 `bronze/silver/gold`, IAM role de ETL e databases no Glue
   Catalog.
2. No `.env`, **remova `AWS_ENDPOINT_URL`** (para usar o S3 real), ajuste
   `AWS_REGION` e aponte `DELTA_BASE_PATH` para o bucket S3 de saída.
3. Use as credenciais da IAM role (`terraform output etl_role_arn`) no ambiente
   de execução do Spark/dbt (`target: prod` no `profiles.yml`).

---

## 6. Estrutura do projeto

```
lakehouse-olist/
├── infra/            # docker-compose, Dockerfiles, terraform (S3/IAM/Glue)
├── ingestion/        # spark_jobs + utils (spark_session, delta_utils)
├── dbt/              # models (staging/intermediate/marts), snapshots, tests, yml
├── airflow/dags/     # lakehouse_pipeline.py (TaskGroups + SLA + sensor)
├── data_quality/     # suites Great Expectations
├── tests/unit/       # pytest de delta_utils
├── Makefile          # setup, ingest, transform, docs, test, clean
└── README.md
```

---DataEngineering #Lakehouse #DeltaLake #dbt #ApacheAirflow #PySpark #AWS #DataQuality
