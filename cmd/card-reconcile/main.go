// Command card-reconcile reconciles one settlement date three ways (our
// records, the clearing file as posted, what we paid against the network's
// statement) and saves the run append-only; it lists and inspects saved runs.
// It reports; it never moves money.
package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/Shailu-s/payments-platform/internal/cardrecon"
	"github.com/Shailu-s/payments-platform/internal/config"
	"github.com/jackc/pgx/v5/pgxpool"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, os.Args[1:], os.Stdout); err != nil && !errors.Is(err, context.Canceled) {
		slog.Error("card reconciliation failed", "error", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, args []string, output io.Writer) error {
	flags := flag.NewFlagSet("card-reconcile", flag.ContinueOnError)
	date := flags.String("date", "", "the settlement date to reconcile, YYYY-MM-DD")
	path := flags.String("statement", "", "read the network's settlement statement saved on disk")
	networkURL := flags.String("download", "", "download the statement from this network base URL")
	id := flags.String("run-id", "", "stable ID for retries; generated when omitted")
	list := flags.Bool("list", false, "list saved runs")
	inspect := flags.String("inspect", "", "inspect a saved run")
	class := flags.String("class", "", "with -inspect, only findings of this class")
	limit := flags.Int("limit", 25, "page size (1–100)")
	offset := flags.Int("offset", 0, "page offset")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 0 || *limit < 1 || *limit > 100 || *offset < 0 {
		return errors.New("invalid arguments or page bounds")
	}
	modes := 0
	for _, set := range []bool{*path != "", *networkURL != "", *list, *inspect != ""} {
		if set {
			modes++
		}
	}
	running := *path != "" || *networkURL != ""
	switch {
	case modes != 1:
		return errors.New("choose one of -statement, -download, -list or -inspect")
	case running && *date == "":
		return errors.New("-date is required to reconcile")
	case !running && (*date != "" || *id != ""):
		return errors.New("-date and -run-id apply only to a run")
	case *class != "" && *inspect == "":
		return errors.New("-class applies only to -inspect")
	case *class != "" && !cardrecon.ValidClass(*class):
		return fmt.Errorf("unknown finding class %q", *class)
	}
	if running {
		if _, err := time.Parse(cardrecon.DateLayout, *date); err != nil {
			return errors.New("-date must be YYYY-MM-DD")
		}
	}

	var env config.Env
	dsn := env.Require("DATABASE_URL")
	rawDays := ""
	if running {
		rawDays = env.Require("HOLD_EXPIRY_DAYS")
	}
	if err := env.Err(); err != nil {
		return err
	}
	var cfg cardrecon.Config
	if running {
		days, err := strconv.Atoi(rawDays)
		if err != nil || days < 1 {
			return fmt.Errorf("HOLD_EXPIRY_DAYS must be a whole number of days, at least 1: %q", rawDays)
		}
		cfg.HoldExpiryDays = days
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

	switch {
	case *list:
		runs, err := cardrecon.ListRuns(ctx, pool, *limit, *offset)
		if err != nil {
			return err
		}
		return encoder.Encode(runs)
	case *inspect != "":
		saved, err := cardrecon.GetRun(ctx, pool, *inspect)
		if err != nil {
			return err
		}
		findings, err := cardrecon.Findings(ctx, pool, *inspect, *class, *limit, *offset)
		if err != nil {
			return err
		}
		return encoder.Encode(struct {
			Run      cardrecon.Run       `json:"run"`
			Findings []cardrecon.Finding `json:"findings"`
		}{saved, findings})
	}

	var raw []byte
	if *path != "" {
		file, err := os.Open(*path)
		if err != nil {
			return err
		}
		defer file.Close()
		if raw, err = io.ReadAll(io.LimitReader(file, cardrecon.MaxStatementBytes+1)); err != nil {
			return err
		}
	} else {
		downloadCtx, cancel := context.WithTimeout(ctx, time.Minute)
		defer cancel()
		if raw, err = cardrecon.FetchStatement(downloadCtx, &http.Client{Timeout: time.Minute}, *networkURL, *date); err != nil {
			return err
		}
	}
	stmt, err := cardrecon.ParseStatement(raw)
	if err != nil {
		return err
	}
	if stmt.Date != *date {
		return fmt.Errorf("the statement is for %s, not %s", stmt.Date, *date)
	}
	runID := *id
	if runID == "" {
		var random [16]byte
		if _, err := rand.Read(random[:]); err != nil {
			return err
		}
		runID = "crn_" + hex.EncodeToString(random[:])
	}
	started := time.Now()
	saved, err := cardrecon.Reconcile(ctx, pool, runID, bytes.NewReader(raw), cfg)
	if err != nil {
		return err
	}
	return encoder.Encode(struct {
		cardrecon.Run
		ElapsedMS int64 `json:"elapsed_ms"`
	}{saved, time.Since(started).Milliseconds()})
}
