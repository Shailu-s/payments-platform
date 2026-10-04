#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."
make up
go test ./internal/consumer -run '^TestSenderSIGKILLRedeliversWithoutDuplicatePayment$' -count=1 -v -timeout=90s
