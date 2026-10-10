# Load .env and export settings to target commands.
-include .env
export
COMPOSE = docker compose -f docker/docker-compose.yml

.PHONY: up down psql migrate-up migrate-down migrate-redo test apikey apikeys run worker mock-bank demo relay watch sender webhook-sender chaos-kill-consumer reconcile bench-reconciliation bench

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

# Per-package schemas isolate parallel tests; no -p 1 workaround needed.
test:
	go test ./... -count=1

chaos-kill-consumer:
	@bash scripts/chaos-kill-consumer.sh

# Requires `make run` in another terminal; resets the local database.
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

webhook-sender:
	@go run ./cmd/webhook-sender

reconcile:
	@go run ./cmd/reconcile $(ARGS)

bench:
	@go test ./internal/api -run '^TestLockStrategyComparison$$' -v -count=3
	@go test ./internal/ratelimit -run '^$$' -bench 'Benchmark(Allow|BaselineRoundTrip)$$' -benchmem -benchtime=1s -count=3
	@$(MAKE) --no-print-directory bench-reconciliation

bench-reconciliation:
	@go test ./internal/reconciliation -run '^$$' -bench 'Benchmark(Compare|ReadReport)' -benchmem -benchtime=1s -count=3
	@go test ./internal/reconciliation -run '^$$' -bench BenchmarkReconcile -benchmem -benchtime=3x -count=3

# Run alongside `make run`; address comes from MOCKBANK_ADDR.
mock-bank:
	@go run ./cmd/mock-bank

# Address comes from API_ADDR.
run:
	@go run ./cmd/api

# Mint an API key and print it once. NAME is required: `make apikey NAME="local dev"`
apikey:
	@go run ./cmd/apikey -name "$(NAME)"

# List keys. Shows the prefix, never the secret.
apikeys:
	@go run ./cmd/apikey -list
