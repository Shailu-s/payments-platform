// Command card-settle records what we pay the card network for one date: the
// net of the clearing lines posted for it. Running it again for the same date
// returns the recorded payment and moves no money.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/Shailu-s/payments-platform/internal/cardsettlement"
	"github.com/Shailu-s/payments-platform/internal/config"
	"github.com/jackc/pgx/v5/pgxpool"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, os.Args[1:], os.Stdout); err != nil && !errors.Is(err, context.Canceled) {
		slog.Error("settlement failed", "error", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, args []string, output io.Writer) error {
	flags := flag.NewFlagSet("card-settle", flag.ContinueOnError)
	date := flags.String("date", "", "the settlement date (the clearing file's UTC date), YYYY-MM-DD")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 0 {
		return errors.New("unexpected arguments")
	}
	day, err := time.Parse(cardsettlement.DateLayout, *date)
	if err != nil {
		return errors.New("-date is required as YYYY-MM-DD")
	}

	var env config.Env
	dsn := env.Require("DATABASE_URL")
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

	started := time.Now()
	s, created, err := cardsettlement.Settle(ctx, pool, day)
	if err != nil {
		return err
	}
	encoder := json.NewEncoder(output)
	encoder.SetIndent("", "  ")
	return encoder.Encode(struct {
		cardsettlement.Settlement
		Created   bool  `json:"created"`
		ElapsedMS int64 `json:"elapsed_ms"`
	}{s, created, time.Since(started).Milliseconds()})
}
