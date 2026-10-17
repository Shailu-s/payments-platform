// Command clearing imports the card network's daily clearing file, from disk
// or downloaded from the network, and lists or inspects imported files.
// Importing the same file again changes nothing; an import that stopped
// part-way is finished by running it again.
package main

import (
	"context"
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

	"github.com/Shailu-s/payments-platform/internal/clearing"
	"github.com/Shailu-s/payments-platform/internal/config"
	"github.com/jackc/pgx/v5/pgxpool"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, os.Args[1:], os.Stdout); err != nil && !errors.Is(err, context.Canceled) {
		slog.Error("clearing failed", "error", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, args []string, output io.Writer) error {
	flags := flag.NewFlagSet("clearing", flag.ContinueOnError)
	path := flags.String("file", "", "import a clearing file saved on disk")
	networkURL := flags.String("download", "", "import the file downloaded from this network base URL")
	date := flags.String("date", "", "the file's UTC date, YYYY-MM-DD")
	list := flags.Bool("list", false, "list imported files")
	inspect := flags.String("inspect", "", "inspect an imported file by id")
	onlyFlag := flags.String("flag", "", "with -inspect, only lines carrying this flag")
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
	importing := *path != "" || *networkURL != ""
	switch {
	case modes != 1:
		return errors.New("choose one of -file, -download, -list or -inspect")
	case importing && *date == "":
		return errors.New("-date is required to import")
	case !importing && *date != "":
		return errors.New("-date applies only to an import")
	case *onlyFlag != "" && *inspect == "":
		return errors.New("-flag applies only to -inspect")
	case *onlyFlag != "" && !clearing.ValidFlag(*onlyFlag):
		return fmt.Errorf("unknown clearing flag %q", *onlyFlag)
	}
	var day time.Time
	if importing {
		var err error
		if day, err = time.Parse(clearing.DateLayout, *date); err != nil {
			return errors.New("-date must be YYYY-MM-DD")
		}
	}

	var env config.Env
	dsn := env.Require("DATABASE_URL")
	tolerance := ""
	if importing {
		tolerance = env.Require("CLEARING_OVER_TOLERANCE_PCT")
	}
	if err := env.Err(); err != nil {
		return err
	}
	var cfg clearing.Config
	if importing {
		pct, err := strconv.ParseInt(tolerance, 10, 64)
		if err != nil || pct < 0 || pct > 1000 {
			return fmt.Errorf("CLEARING_OVER_TOLERANCE_PCT must be a whole percentage from 0 to 1000: %q", tolerance)
		}
		cfg.OverTolerancePct = pct
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
		files, err := clearing.ListFiles(ctx, pool, *limit, *offset)
		if err != nil {
			return err
		}
		return encoder.Encode(files)
	case *inspect != "":
		summary, err := clearing.Inspect(ctx, pool, *inspect)
		if err != nil {
			return err
		}
		records, err := clearing.Records(ctx, pool, *inspect, *onlyFlag, *limit, *offset)
		if err != nil {
			return err
		}
		return encoder.Encode(struct {
			Summary clearing.Summary  `json:"summary"`
			Records []clearing.Record `json:"records"`
		}{summary, records})
	}

	var raw []byte
	if *path != "" {
		file, err := os.Open(*path)
		if err != nil {
			return err
		}
		defer file.Close()
		if raw, err = io.ReadAll(io.LimitReader(file, clearing.MaxFileBytes+1)); err != nil {
			return err
		}
	} else {
		downloadCtx, cancel := context.WithTimeout(ctx, time.Minute)
		defer cancel()
		if raw, err = clearing.Download(downloadCtx, &http.Client{Timeout: time.Minute}, *networkURL, day); err != nil {
			return err
		}
	}
	started := time.Now()
	summary, err := clearing.Import(ctx, pool, raw, day, cfg)
	if err != nil {
		return err
	}
	return encoder.Encode(struct {
		clearing.Summary
		ElapsedMS int64 `json:"elapsed_ms"`
	}{summary, time.Since(started).Milliseconds()})
}
