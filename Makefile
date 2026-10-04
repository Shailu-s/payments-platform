# Settings come from .env (copy .env.example). Every command requires them and
# stops at startup if one is missing, rather than falling back to a guess.
# `export` hands them to every command a target runs.
-include .env
export
COMPOSE = docker compose -f docker/docker-compose.yml

.PHONY: up down psql migrate-up migrate-down migrate-redo test apikey apikeys run worker mock-bank demo relay watch sender

up:
	$(COMPOSE) up -d --wait

down:
	$(COMPOSE) down

psql:
	$(COMPOSE) exec postgres psql -U payments -d payments

migrate-up:
	migrate -path db/migrations -database "$(DATABASE_URL)" up

migrate-down:
	migrate -path db/migrations -database "$(DATABASE_URL)" down 1

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
	@go run ./cmd/worker

# Publish outbox events to Kafka. Run alongside `make run`.
relay:
	@go run ./cmd/relay

# Print every event from transfers topic
watch:
	@go run ./cmd/watch

# Send transfers to MockBank as their events arrive. Run alongside `make run` and `make relay`.
sender:
	@go run ./cmd/sender

# Serve MockBank on :8081. Run it alongside `make run`.
mock-bank:
	@go run ./cmd/mock-bank

# Serve the API on :8080
run:
	@go run ./cmd/api

# Mint an API key and print it once. NAME is required: `make apikey NAME="local dev"`
apikey:
	@go run ./cmd/apikey -name "$(NAME)"

# List keys. Shows the prefix, never the secret.
apikeys:
	@go run ./cmd/apikey -list
