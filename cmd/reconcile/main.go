package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"io"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/Shailu-s/payments-platform/internal/config"
	"github.com/Shailu-s/payments-platform/internal/reconciliation"
	"github.com/jackc/pgx/v5/pgxpool"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, os.Args[1:], os.Stdout); err != nil && !errors.Is(err, context.Canceled) {
		slog.Error("reconciliation failed", "error", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, args []string, output io.Writer) error {
	flags := flag.NewFlagSet("reconcile", flag.ContinueOnError)
	reportPath := flags.String("report", "", "read a saved provider CSV instead of downloading it")
	id := flags.String("run-id", "", "stable ID for retries; generated when omitted")
	list := flags.Bool("list", false, "list saved runs")
	inspect := flags.String("inspect", "", "inspect a saved run")
	classification := flags.String("classification", "", "filter findings by classification")
	clientReference := flags.String("client-reference", "", "filter findings by transfer/client reference")
	limit := flags.Int("limit", 25, "page size (1–100)")
	offset := flags.Int("offset", 0, "page offset")
	interval := flags.Duration("interval", 0, "download a fresh report repeatedly at this interval; zero runs once")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 0 || *limit < 1 || *limit > 100 || *offset < 0 || *interval < 0 {
		return errors.New("invalid arguments or page bounds")
	}
	if (*list && *inspect != "") || ((*list || *inspect != "") && (*reportPath != "" || *id != "" || *interval != 0)) || (*interval > 0 && (*id != "" || *reportPath != "")) {
		return errors.New("choose one of run, list, inspect or scheduled download; schedules require fresh reports and IDs")
	}
	if *classification != "" && !validClassification(*classification) {
		return errors.New("unknown reconciliation classification")
	}
	var env config.Env
	dsn := env.Require("DATABASE_URL")
	providerURL := ""
	if !*list && *inspect == "" && *reportPath == "" {
		providerURL = env.Require("PROVIDER_URL")
	}
	if err := env.Err(); err != nil {
		return err
	}
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return errors.New("invalid DATABASE_URL")
	}
	defer pool.Close()
	if err := pool.Ping(ctx); err != nil {
		return errors.New("database unavailable; check DATABASE_URL and Postgres availability")
	}
	encoder := json.NewEncoder(output)
	encoder.SetIndent("", "  ")
	if *list {
		runs, err := reconciliation.ListRuns(ctx, pool, *limit, *offset)
		if err != nil {
			return err
		}
		return encoder.Encode(runs)
	}
	if *inspect != "" {
		saved, err := reconciliation.GetRun(ctx, pool, *inspect)
		if err != nil {
			return err
		}
		findings, err := reconciliation.Findings(ctx, pool, *inspect, *classification, *clientReference, *limit, *offset)
		if err != nil {
			return err
		}
		return encoder.Encode(struct {
			Run      reconciliation.Run       `json:"run"`
			Findings []reconciliation.Finding `json:"findings"`
		}{saved, findings})
	}
	client := &http.Client{Timeout: 30 * time.Second}
	attempt := func() error {
		started := time.Now()
		attemptCtx, cancel := context.WithTimeout(ctx, 2*time.Minute)
		defer cancel()
		runID := *id
		if runID == "" {
			var random [16]byte
			if _, err := rand.Read(random[:]); err != nil {
				return err
			}
			runID = "rcn_" + hex.EncodeToString(random[:])
		}
		var source io.Reader
		if *reportPath != "" {
			file, err := os.Open(*reportPath)
			if err != nil {
				return err
			}
			defer file.Close()
			source = file
		} else {
			report, err := reconciliation.FetchReport(attemptCtx, client, providerURL)
			if err != nil {
				return err
			}
			source = bytes.NewReader(report.Raw)
		}
		saved, err := reconciliation.Reconcile(attemptCtx, pool, runID, source)
		if err != nil {
			return err
		}
		return encoder.Encode(struct {
			reconciliation.Run
			ElapsedMS int64 `json:"elapsed_ms"`
		}{saved, time.Since(started).Milliseconds()})
	}
	for {
		if err := attempt(); err != nil {
			if *interval == 0 {
				return err
			}
			slog.Error("scheduled reconciliation attempt failed", "error", err)
		}
		if *interval == 0 {
			return nil
		}
		timer := time.NewTimer(*interval)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
}

func validClassification(value string) bool {
	switch value {
	case reconciliation.Matched, reconciliation.AmountMismatch, reconciliation.StatusMismatch, reconciliation.MissingExternal, reconciliation.MissingInternal, reconciliation.DuplicateExternal:
		return true
	default:
		return false
	}
}
