DB_URL ?= postgres://payments:payments@localhost:5433/payments?sslmode=disable
COMPOSE = docker compose -f docker/docker-compose.yml

.PHONY: up down psql migrate-up migrate-down migrate-redo test apikey apikeys run worker mock-bank demo

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

# No -p 1: each package migrates into its own schema (internal/testdb), so
# parallel packages cannot see each other's tables. `go test ./...` on its own
# works too, which matters because that is what anyone cloning this runs.
test:
	go test ./... -count=1

# Walk through everything phase 2 built, against a running API.
# Needs `make run` in another terminal.
demo:
	@./scripts/demo.sh

# Send accepted transfers to the provider. Run alongside `make run`.
worker:
	@DATABASE_URL="$(DB_URL)" PROVIDER_URL="http://localhost:8081" go run ./cmd/worker

# Serve MockBank on :8081. Run it alongside `make run`.
mock-bank:
	@WEBHOOK_URL="http://localhost:8080/v1/webhooks/mockbank" go run ./cmd/mock-bank

# Serve the API on :8080
run:
	@DATABASE_URL="$(DB_URL)" go run ./cmd/api

# Mint an API key and print it once. NAME is required: `make apikey NAME="local dev"`
apikey:
	@DATABASE_URL="$(DB_URL)" go run ./cmd/apikey -name "$(NAME)"

# List keys. Shows the prefix, never the secret.
apikeys:
	@DATABASE_URL="$(DB_URL)" go run ./cmd/apikey -list
