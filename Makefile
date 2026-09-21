DB_URL ?= postgres://payments:payments@localhost:5433/payments?sslmode=disable
COMPOSE = docker compose -f docker/docker-compose.yml

.PHONY: up down psql migrate-up migrate-down migrate-redo test

up:
	$(COMPOSE) up -d --wait

down:
	$(COMPOSE) down

psql:
	$(COMPOSE) exec postgres psql -U payments -d payments

migrate-up:
	migrate -path db/migrations -database "$(DB_URL)" up

migrate-down:
	migrate -path db/migrations -database "$(DB_URL)" down 1

migrate-redo: migrate-down migrate-up

test:
	go test ./... -count=1
