# Load .env and export settings to target commands.
-include .env
export
COMPOSE = docker compose -f docker/docker-compose.yml

.PHONY: up down psql migrate-up migrate-down migrate-redo test apikey apikeys run worker mock-bank mock-network demo relay watch sender webhook-sender chaos-kill-consumer reconcile hold-expirer clearing bench-reconciliation bench bench-metrics bench-http bench-balance bench-expiry bench-clearing demo-v1

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

demo-v1:
	@go test ./cmd/mock-bank -run '^TestV1PaymentSurvivesTimeoutAfterSuccess$$' -v -count=1

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

# Import the network's clearing file: make clearing ARGS="-download $(NETWORK_URL) -date 2026-10-18",
# ARGS="-file clearing-2026-10-18.csv -date 2026-10-18", ARGS="-list" or ARGS="-inspect clf_… -flag shortfall".
clearing:
	@go run ./cmd/clearing $(ARGS)

# Release card holds older than HOLD_EXPIRY_DAYS, every HOLD_EXPIRY_INTERVAL (0: once).
hold-expirer:
	@go run ./cmd/hold-expirer

bench:
	@go test ./internal/api -run '^TestLockStrategyComparison$$' -v -count=3
	@go test ./internal/ratelimit -run '^$$' -bench 'Benchmark(Allow|BaselineRoundTrip)$$' -benchmem -benchtime=1s -count=3
	@$(MAKE) --no-print-directory bench-reconciliation
	@$(MAKE) --no-print-directory bench-metrics

# Open-model HTTP sweep against a disposable payments_bench database. See BENCHMARKS.md.
bench-http:
	@bash scripts/bench-http.sh

bench-balance:
	@go test ./internal/ledger -run '^$$' -bench 'Benchmark(Stored)?BalanceByHistory' -benchtime=2s -count=1

bench-expiry:
	@go test ./internal/holds -run '^$$' -bench BenchmarkExpireBatch -benchtime=1x -count=3

bench-clearing:
	@go test ./internal/clearing -run '^$$' -bench BenchmarkImportClearing -benchtime=1x -count=3

bench-metrics:
	@go test ./internal/api -run '^$$' -bench 'Benchmark(HTTPMetricsObservation|MetricsScrape1000Records)' -benchmem -benchtime=1s -count=3

bench-reconciliation:
	@go test ./internal/reconciliation -run '^$$' -bench 'Benchmark(Compare|ReadReport)' -benchmem -benchtime=1s -count=3
	@go test ./internal/reconciliation -run '^$$' -bench BenchmarkReconcile -benchmem -benchtime=3x -count=3

# Run alongside `make run`; address comes from MOCKBANK_ADDR.
mock-bank:
	@go run ./cmd/mock-bank

# Card network simulator; settings come from NETWORK_* and ISSUER_URL. ARGS runs a
# scenario instead: make mock-network ARGS="scenario -name stand_in -card card_x -amount 5000"
mock-network:
	@go run ./cmd/mock-network $(ARGS)

# Address comes from API_ADDR.
run:
	@go run ./cmd/api

# Mint an API key and print it once. NAME is required: `make apikey NAME="local dev"`
apikey:
	@go run ./cmd/apikey -name "$(NAME)"

# List keys. Shows the prefix, never the secret.
apikeys:
	@go run ./cmd/apikey -list
