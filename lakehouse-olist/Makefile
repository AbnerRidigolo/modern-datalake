.DEFAULT_GOAL := help
SHELL := /bin/bash
COMPOSE := docker compose -f infra/docker-compose.yml
SPARK_SUBMIT := $(COMPOSE) exec -T spark-master spark-submit \
	--packages io.delta:delta-spark_2.12:3.2.0,org.apache.hadoop:hadoop-aws:3.3.4

.PHONY: help setup ingest transform docs test clean down logs dbt-deps lint

help: ## Lista os comandos disponíveis
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-12s\033[0m %s\n", $$1, $$2}'

setup: ## Cria .env, builda imagens e sobe todo o ambiente Docker
	@test -f .env || cp .env.example .env
	$(COMPOSE) build
	$(COMPOSE) up -d
	@echo "Ambiente no ar -> Airflow http://localhost:8081 | MinIO http://localhost:9001 | Spark http://localhost:8080"

ingest: ## Roda todos os spark jobs (camada Bronze)
	$(SPARK_SUBMIT) /opt/app/ingestion/spark_jobs/ingest_customers.py
	$(SPARK_SUBMIT) /opt/app/ingestion/spark_jobs/ingest_products.py
	$(SPARK_SUBMIT) /opt/app/ingestion/spark_jobs/ingest_orders.py
	$(SPARK_SUBMIT) /opt/app/ingestion/spark_jobs/ingest_payments.py

dbt-deps: ## Instala dependências dbt
	$(COMPOSE) exec -T airflow-scheduler bash -c "cd /opt/airflow/dbt && dbt deps"

transform: ## Roda dbt run + dbt test (camadas Silver e Gold)
	$(COMPOSE) exec -T airflow-scheduler bash -c "cd /opt/airflow/dbt && dbt snapshot && dbt run && dbt test"

docs: ## Gera a documentação dbt e serve no browser (porta 8085)
	$(COMPOSE) exec -T airflow-scheduler bash -c "cd /opt/airflow/dbt && dbt docs generate"
	$(COMPOSE) exec -d airflow-scheduler bash -c "cd /opt/airflow/dbt && dbt docs serve --port 8085 --host 0.0.0.0"
	@echo "dbt docs -> http://localhost:8085"

test: ## Roda os testes unitários pytest
	$(COMPOSE) exec -T spark-master bash -c "cd /opt/app && python -m pytest ingestion/../tests -v" || \
		python -m pytest tests -v

lint: ## Checa formatação/typing do código Python
	python -m ruff check ingestion tests || true
	python -m mypy ingestion || true

logs: ## Mostra os logs de todos os serviços
	$(COMPOSE) logs -f --tail=100

down: ## Para os containers sem apagar volumes
	$(COMPOSE) down

clean: ## Para containers e apaga volumes/dados persistentes
	$(COMPOSE) down -v --remove-orphans
	@echo "Containers e volumes removidos"
