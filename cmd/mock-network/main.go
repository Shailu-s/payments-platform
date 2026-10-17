// Command mock-network simulates the card network contract in docs/mock-network-api.md.
// It keeps its journal in memory and never touches the platform database.
//
//	mock-network                       serve the sandbox and clearing files on NETWORK_ADDR
//	mock-network scenario -name ...    run one scenario against ISSUER_URL and exit
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/Shailu-s/payments-platform/internal/cardnetwork"
	"github.com/Shailu-s/payments-platform/internal/config"
)

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo})))

	if len(os.Args) > 1 && os.Args[1] == "scenario" {
		if err := runScenario(os.Args[2:]); err != nil {
			slog.Error(err.Error())
			os.Exit(1)
		}
		return
	}
	if len(os.Args) > 1 {
		slog.Error("unknown command; use no arguments to serve, or `scenario`", "command", os.Args[1])
		os.Exit(2)
	}
	if err := serve(); err != nil {
		slog.Error(err.Error())
		os.Exit(1)
	}
}

func networkFromEnv(env *config.Env) (*cardnetwork.Network, error) {
	issuerURL := env.Require("ISSUER_URL")
	secret := env.Require("NETWORK_SECRET")
	rawDeadline := env.Require("NETWORK_AUTH_DEADLINE")
	rawLimit := env.Require("NETWORK_STANDIN_LIMIT")
	rawSeed := env.Require("NETWORK_SEED")
	if err := env.Err(); err != nil {
		return nil, err
	}
	deadline, err := time.ParseDuration(rawDeadline)
	if err != nil {
		return nil, fmt.Errorf("NETWORK_AUTH_DEADLINE: %q is not a duration such as 2s", rawDeadline)
	}
	limit, err := strconv.ParseInt(rawLimit, 10, 64)
	if err != nil {
		return nil, fmt.Errorf("NETWORK_STANDIN_LIMIT: %q is not an integer amount in minor units", rawLimit)
	}
	seed, err := strconv.ParseUint(rawSeed, 10, 64)
	if err != nil {
		return nil, fmt.Errorf("NETWORK_SEED: %q is not an unsigned integer", rawSeed)
	}
	n, err := cardnetwork.New(cardnetwork.Config{
		IssuerURL:    issuerURL,
		Secret:       []byte(secret),
		AuthDeadline: deadline,
		StandInLimit: limit,
		Seed:         seed,
	})
	if err != nil {
		return nil, fmt.Errorf("NETWORK_*: %w", err)
	}
	return n, nil
}

func serve() error {
	var env config.Env
	addr := env.Require("NETWORK_ADDR")
	network, err := networkFromEnv(&env)
	if err != nil {
		return err
	}

	srv := &http.Server{
		Addr:              addr,
		Handler:           NewServer(network).Handler(),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       15 * time.Second,
		// A scenario runs synchronously: two deadlines, then up to 31s of advice backoff.
		WriteTimeout: 2 * time.Minute,
		IdleTimeout:  60 * time.Second,
	}

	shutdown := make(chan os.Signal, 1)
	signal.Notify(shutdown, os.Interrupt, syscall.SIGTERM)
	failed := make(chan error, 1)
	go func() {
		slog.Info("mock-network listening", "addr", addr)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			failed <- err
		}
	}()

	select {
	case err := <-failed:
		return fmt.Errorf("server stopped: %w", err)
	case <-shutdown:
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := srv.Shutdown(ctx); err != nil {
		return fmt.Errorf("shutdown: %w", err)
	}
	slog.Info("stopped cleanly")
	return nil
}

// runScenario plays one scenario in-process, prints its result as JSON and, with
// -clearing-dir, writes the clearing file of every date the scenario touched.
func runScenario(args []string) error {
	flags := flag.NewFlagSet("scenario", flag.ContinueOnError)
	name := flags.String("name", "", "scenario name: "+fmt.Sprint(cardnetwork.Scenarios))
	card := flags.String("card", "", "issuer card_id")
	amount := flags.Int64("amount", 0, "amount in minor units")
	seed := flags.String("seed", "", "scenario seed (default: drawn from NETWORK_SEED)")
	dir := flags.String("clearing-dir", "", "directory to write clearing-YYYY-MM-DD.csv files into")
	if err := flags.Parse(args); err != nil {
		return err
	}

	var env config.Env
	network, err := networkFromEnv(&env)
	if err != nil {
		return err
	}
	req := cardnetwork.ScenarioRequest{Scenario: *name, CardID: *card, Amount: *amount}
	if *seed != "" {
		v, err := strconv.ParseUint(*seed, 10, 64)
		if err != nil {
			return fmt.Errorf("-seed: %q is not an unsigned integer", *seed)
		}
		req.Seed = &v
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	result, err := network.RunScenario(ctx, req)
	if err != nil {
		return err
	}
	if *dir != "" {
		paths, err := exportClearing(network, *dir, result.ClearingDates)
		if err != nil {
			return err
		}
		for _, p := range paths {
			slog.Info("clearing file written", "path", p)
		}
	}
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	return enc.Encode(result)
}
