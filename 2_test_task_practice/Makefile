.PHONY: up next status down reset

up:        ## поднять MariaDB (состояние на 2026-10-31 00:00 UTC) и ваши сервисы из docker-compose.override.yml
	docker compose -f ./docker-compose.yml -f ./docker-compose.override.yml up -d

next:      ## применить следующий пакет изменений в MariaDB (всего 6)
	./scripts/next_batch.sh

status:
	@echo "Применено пакетов: $$(cat state/BATCH) из 6"
	@docker compose ps

down:
	docker compose -f ./docker-compose.yml -f ./docker-compose.override.yml down -v

reset:     ## удалить все данные и вернуться к исходному состоянию
	docker compose -f ./docker-compose.yml -f ./docker-compose.override.yml down -v
	echo 0 > state/BATCH
	docker compose -f ./docker-compose.yml -f ./docker-compose.override.yml up -d
