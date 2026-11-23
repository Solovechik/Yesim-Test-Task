.env:
	cp .env.example .env

ifneq (,$(wildcard ./.env))
    include .env
    export
endif

COMPOSE = docker compose -f ./docker-compose.yml -f ./docker-compose.override.yml
LOGICAL_DATE = --logical-date "$$(date -u +%Y-%m-%dT%H:%M:%SZ)"

.PHONY: up next status down reset dbt-build dbt-docs

up: .env ## поднять MariaDB (состояние на 2026-10-31 00:00 UTC) и ваши сервисы из docker-compose.override.yml
	$(COMPOSE) up -d

next:      ## применить следующий пакет изменений в MariaDB (всего 6)
	./scripts/next_batch.sh

status:
	@echo "Применено пакетов: $$(cat state/BATCH) из 6"

down:
	$(COMPOSE) down -v

reset: .env ## удалить все данные и вернуться к исходному состоянию
	$(COMPOSE) down -v
	echo 0 > state/BATCH
	$(COMPOSE) up -d

dbt-build: ## запустить полную сборку и прогон всех тестов dbt внутри контейнера airflow
	$(COMPOSE) exec airflow dbt build --project-dir /opt/airflow/dbt --profiles-dir /opt/airflow/dbt --target bronze

dbt-docs:  ## сгенерировать документацию dbt и поднять сервер на http://localhost:8081
	$(COMPOSE) exec airflow dbt docs generate --project-dir /opt/airflow/dbt --profiles-dir /opt/airflow/dbt --target bronze
	python3 -m http.server 8081 --directory ./dbt/target

trigger-dags: ## запустить выгрузку из MariaDB в S3 в Airflow
	$(COMPOSE) exec airflow airflow dags trigger sync_mariadb_to_s3_dimensions $(LOGICAL_DATE) > /dev/null
	$(COMPOSE) exec airflow airflow dags trigger sync_mariadb_to_s3_events $(LOGICAL_DATE) > /dev/null
