DB_URL ?= postgres://payments:payments@localhost:5433/payments?sslmode=disable
COMPOSE = docker compose -f docker/docker-compose.yml

.PHONY: up down psql migrate-up migrate-down migrate-redo test apikey apikeys run

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

# -p 1 runs one package at a time. Packages share one database and each
# truncates every table, so running them in parallel has one package deleting
# another's rows mid-test.
test:
	go test ./... -count=1 -p 1

# Serve the API on :8080
run:
	@DATABASE_URL="$(DB_URL)" go run ./cmd/api

# Mint an API key and print it once. NAME is required: `make apikey NAME="local dev"`
apikey:
	@DATABASE_URL="$(DB_URL)" go run ./cmd/apikey -name "$(NAME)"

# List keys. Shows the prefix, never the secret.
apikeys:
	@DATABASE_URL="$(DB_URL)" go run ./cmd/apikey -list
