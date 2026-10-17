// Command benchday drives one simulated card day against a disposable
// payments_bench* database and a running API, in three steps that
// scripts/bench-day.sh runs around a k6 load:
//
//	-prepare  the network simulator plays a day of scenarios on the fixture's
//	          cards (the holds a 10,000-line clearing file will clear) and
//	          places holds old enough to expire; it saves the file, the
//	          statement and what reconciliation must find
//	-batch    imports the clearing file while expiring the old holds, the two
//	          batch jobs of a card day, and records when they ran
//	-close    settles the day, reconciles it against the network's statement,
//	          checks the money, and cuts the k6 latencies into before, during
//	          and after the batch jobs
package main

import (
	"bytes"
	"context"
	"encoding/csv"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/Shailu-s/payments-platform/internal/cardnetwork"
	"github.com/Shailu-s/payments-platform/internal/cardrecon"
	"github.com/Shailu-s/payments-platform/internal/cardsettlement"
	"github.com/Shailu-s/payments-platform/internal/clearing"
	"github.com/Shailu-s/payments-platform/internal/config"
	"github.com/Shailu-s/payments-platform/internal/holds"
	"github.com/Shailu-s/payments-platform/internal/ledger"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	holdExpiryDays   = 7
	overTolerancePct = 20
	expiryBatch      = 500
)

// dayMix is the network's day: every run but the full reversal clears one
// line on the clearing date, so 10,000 lines; the offline purchases are the
// only lines without an authorization.
var dayMix = []struct {
	scenario string
	runs     int
}{
	{cardnetwork.ScenarioApproveAndClear, 6000},
	{cardnetwork.ScenarioClearWithTip, 1500},
	{cardnetwork.ScenarioPartialClear, 1500},
	{cardnetwork.ScenarioIncremental, 500},
	{cardnetwork.ScenarioDuplicateAuth, 200},
	{cardnetwork.ScenarioLateRefund, 200},
	{cardnetwork.ScenarioOfflinePurchase, 60},
	{cardnetwork.ScenarioTimeoutThenRetry, 20},
	{cardnetwork.ScenarioStandIn, 20},
	{cardnetwork.ScenarioReversalBeforeAuth, 100},
}

type fixture struct {
	Cards   []string `json:"cards"`
	HotCard string   `json:"hot_card"`
}

// expectation is what -prepare promises -close.
type expectation struct {
	Date          string         `json:"date"`
	Lines         int            `json:"lines"`
	ExpiryTargets int            `json:"expiry_targets"`
	Counts        map[string]int `json:"counts"`
	PrepareSecs   float64        `json:"prepare_seconds"`
}

type batchResult struct {
	StartUS    int64          `json:"start_unix_us"`
	EndUS      int64          `json:"end_unix_us"`
	ImportSecs float64        `json:"import_seconds"`
	Processed  int            `json:"processed"`
	Statuses   map[string]int `json:"statuses"`
	Flags      map[string]int `json:"flags"`
	ExpireSecs float64        `json:"expire_seconds"`
	Expired    int            `json:"expired"`
	Errors     []string       `json:"errors,omitempty"`
}

func main() {
	prepare := flag.Bool("prepare", false, "play the day's scenarios and place expirable holds")
	batch := flag.Bool("batch", false, "import the clearing file while expiring old holds")
	closeDay := flag.Bool("close", false, "settle, reconcile, check and report")
	dir := flag.String("dir", "bench/out/day", "where the day's files are kept")
	apiURL := flag.String("api", "http://127.0.0.1:18180", "the issuer the simulator calls (-prepare)")
	fixturePath := flag.String("fixture", "", "benchseed fixture with the cards (-prepare)")
	expiryTargets := flag.Int("expire", 2000, "holds placed old enough to expire (-prepare)")
	k6CSV := flag.String("k6", "", "k6 per-request CSV output (-close)")
	flag.Parse()

	var env config.Env
	dsn := env.Require("BENCH_DATABASE_URL")
	secret := ""
	if *prepare {
		secret = env.Require("NETWORK_SECRET")
	}
	if err := env.Err(); err != nil {
		slog.Error(err.Error())
		os.Exit(1)
	}
	ctx := context.Background()
	pool, err := connect(ctx, dsn)
	if err != nil {
		slog.Error("connect failed", "error", err)
		os.Exit(1)
	}
	defer pool.Close()

	switch {
	case *prepare:
		err = runPrepare(ctx, pool, *dir, *apiURL, *fixturePath, secret, *expiryTargets)
	case *batch:
		err = runBatch(ctx, pool, *dir)
	case *closeDay:
		err = runClose(ctx, pool, *dir, *k6CSV)
	default:
		err = errors.New("one of -prepare, -batch or -close is required")
	}
	if err != nil {
		slog.Error("bench day failed", "error", err)
		os.Exit(1)
	}
}

// connect refuses anything but a benchmark database: every step writes money.
func connect(ctx context.Context, dsn string) (*pgxpool.Pool, error) {
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, err
	}
	if !strings.HasPrefix(cfg.ConnConfig.Database, "payments_bench") {
		return nil, fmt.Errorf("database %q is not a payments_bench* database", cfg.ConnConfig.Database)
	}
	return pgxpool.NewWithConfig(ctx, cfg)
}

func runPrepare(ctx context.Context, pool *pgxpool.Pool, dir, apiURL, fixturePath, secret string, expiryTargets int) error {
	started := time.Now()
	raw, err := os.ReadFile(fixturePath)
	if err != nil {
		return err
	}
	var f fixture
	if err := json.Unmarshal(raw, &f); err != nil {
		return err
	}
	if len(f.Cards) == 0 {
		return errors.New("fixture has no cards: seed with benchseed -cards")
	}
	// The fake clock keeps lost answers and stand-ins from waiting in real
	// time; it starts now so every signature is inside the replay window.
	network, err := cardnetwork.New(cardnetwork.Config{
		IssuerURL: apiURL, Secret: []byte(secret), AuthDeadline: 2 * time.Second,
		StandInLimit: 1_000_000, Clock: cardnetwork.NewFakeClock(time.Now()),
	})
	if err != nil {
		return err
	}

	type job struct {
		scenario string
		seed     uint64
		card     string
	}
	jobs := make(chan job)
	var mu sync.Mutex
	dates := map[string]bool{}
	var firstErr error
	var wg sync.WaitGroup
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := range jobs {
				amount := int64(1000 + j.seed%9000)
				result, err := network.RunScenario(ctx, cardnetwork.ScenarioRequest{
					Scenario: j.scenario, CardID: j.card, Amount: amount, Seed: &j.seed,
				})
				mu.Lock()
				if err != nil && firstErr == nil {
					firstErr = fmt.Errorf("%s seed %d: %w", j.scenario, j.seed, err)
				}
				for _, d := range result.ClearingDates {
					dates[d] = true
				}
				mu.Unlock()
			}
		}()
	}
	seed := uint64(0)
	for _, m := range dayMix {
		for range m.runs {
			jobs <- job{scenario: m.scenario, seed: 160_000 + seed, card: f.Cards[int(seed)%len(f.Cards)]}
			seed++
		}
	}
	close(jobs)
	wg.Wait()
	if firstErr != nil {
		return firstErr
	}

	// Holds the network never clears, aged past the expiry window the way the
	// expirer's own tests age them.
	for i := range expiryTargets {
		r, err := network.Authorize(ctx, cardnetwork.Authorization{
			AuthID: fmt.Sprintf("auth_expire_%05d", i), CardID: f.Cards[i%len(f.Cards)], Amount: 500,
			Currency: cardnetwork.Currency, Kind: cardnetwork.KindInitial,
			Merchant: cardnetwork.Merchant{ID: "mer_hotel", Name: "Harbour Hotel", MCC: "7011"},
		})
		if err != nil {
			return err
		}
		if r.Decision != cardnetwork.DecisionApproved || r.StandIn {
			return fmt.Errorf("expiry target %s: %+v, want approved by the issuer", r.AuthID, r)
		}
	}
	if _, err := pool.Exec(ctx, `UPDATE card_authorizations
		SET created_at = created_at - make_interval(days => $1)
		WHERE auth_id LIKE 'auth\_expire\_%'`, holdExpiryDays+1); err != nil {
		return fmt.Errorf("age expiry targets: %w", err)
	}

	// Late refunds clear weeks later: only the purchases' date is today's file.
	day, lines, err := clearingDay(network, dates)
	if err != nil {
		return err
	}
	file, err := network.ClearingFile(day)
	if err != nil {
		return err
	}
	statement, err := network.SettlementStatement(day)
	if err != nil {
		return err
	}
	exp := expectation{Date: day, Lines: len(lines), ExpiryTargets: expiryTargets, Counts: map[string]int{}}
	for _, class := range cardrecon.Classes {
		exp.Counts[class] = 0
	}
	for _, l := range lines {
		if l.AuthID == "" {
			exp.Counts[cardrecon.ClearedNotAuthorized]++
		} else {
			exp.Counts[cardrecon.Matched]++
		}
	}
	exp.PrepareSecs = time.Since(started).Seconds()
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return err
	}
	for name, data := range map[string][]byte{"clearing.csv": file, "statement.csv": statement} {
		if err := os.WriteFile(filepath.Join(dir, name), data, 0o600); err != nil {
			return err
		}
	}
	if err := writeJSON(filepath.Join(dir, "expect.json"), exp); err != nil {
		return err
	}
	slog.Info("prepared", "date", day, "lines", len(lines), "expiry_targets", expiryTargets,
		"seconds", exp.PrepareSecs)
	return nil
}

// clearingDay is the one date carrying the day's purchases. Two such dates
// mean the run straddled UTC midnight, which would split the file.
func clearingDay(network *cardnetwork.Network, dates map[string]bool) (string, []cardnetwork.ClearingLine, error) {
	var day string
	var lines []cardnetwork.ClearingLine
	for _, d := range slices.Sorted(maps.Keys(dates)) {
		t, _ := time.Parse(cardnetwork.DateLayout, d)
		l := network.ClearingLines(t)
		purchases := slices.ContainsFunc(l, func(c cardnetwork.ClearingLine) bool { return c.Kind == cardnetwork.ClearingPurchase })
		if !purchases {
			continue
		}
		if day != "" {
			return "", nil, fmt.Errorf("purchases clear on %s and %s: rerun away from UTC midnight", day, d)
		}
		day, lines = d, l
	}
	if day == "" {
		return "", nil, errors.New("no purchase cleared")
	}
	return day, lines, nil
}

func runBatch(ctx context.Context, pool *pgxpool.Pool, dir string) error {
	var exp expectation
	if err := readJSON(filepath.Join(dir, "expect.json"), &exp); err != nil {
		return err
	}
	raw, err := os.ReadFile(filepath.Join(dir, "clearing.csv"))
	if err != nil {
		return err
	}
	day, err := time.Parse(cardnetwork.DateLayout, exp.Date)
	if err != nil {
		return err
	}

	var res batchResult
	var mu sync.Mutex
	fail := func(err error) {
		mu.Lock()
		res.Errors = append(res.Errors, err.Error())
		mu.Unlock()
	}
	start := time.Now()
	res.StartUS = start.UnixMicro()
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		summary, err := clearing.Import(ctx, pool, raw, day, clearing.Config{OverTolerancePct: overTolerancePct})
		res.ImportSecs = time.Since(start).Seconds()
		if err != nil {
			fail(err)
			return
		}
		res.Processed, res.Statuses, res.Flags = summary.Processed, summary.Statuses, summary.Flags
	}()
	go func() {
		defer wg.Done()
		for {
			n, err := holds.ExpireBatch(ctx, pool, time.Now(), holdExpiryDays*24*time.Hour, expiryBatch)
			res.Expired += n
			if err != nil {
				fail(err)
				break
			}
			if n < expiryBatch {
				break
			}
		}
		res.ExpireSecs = time.Since(start).Seconds()
	}()
	wg.Wait()
	res.EndUS = time.Now().UnixMicro()
	if err := writeJSON(filepath.Join(dir, "batch.json"), res); err != nil {
		return err
	}
	slog.Info("batch done", "import_seconds", res.ImportSecs, "processed", res.Processed,
		"expire_seconds", res.ExpireSecs, "expired", res.Expired, "errors", len(res.Errors))
	if len(res.Errors) > 0 {
		return errors.New(res.Errors[0])
	}
	return nil
}

func runClose(ctx context.Context, pool *pgxpool.Pool, dir, k6CSV string) error {
	var exp expectation
	if err := readJSON(filepath.Join(dir, "expect.json"), &exp); err != nil {
		return err
	}
	var batch batchResult
	if err := readJSON(filepath.Join(dir, "batch.json"), &batch); err != nil {
		return err
	}
	statement, err := os.ReadFile(filepath.Join(dir, "statement.csv"))
	if err != nil {
		return err
	}
	day, err := time.Parse(cardnetwork.DateLayout, exp.Date)
	if err != nil {
		return err
	}

	var failures []string
	check := func(ok bool, format string, args ...any) {
		line := fmt.Sprintf(format, args...)
		if !ok {
			failures = append(failures, line)
			line = "FAIL " + line
		} else {
			line = "ok   " + line
		}
		fmt.Println(line)
	}

	settled, _, err := cardsettlement.Settle(ctx, pool, day)
	if err != nil {
		return fmt.Errorf("settle %s: %w", exp.Date, err)
	}
	run, err := cardrecon.Reconcile(ctx, pool, "crn_bench_day_"+exp.Date, bytes.NewReader(statement),
		cardrecon.Config{HoldExpiryDays: holdExpiryDays})
	if err != nil {
		return fmt.Errorf("reconcile %s: %w", exp.Date, err)
	}
	fmt.Printf("\nday %s: %d clearing lines, settled net %d over %d records\n\n",
		exp.Date, exp.Lines, settled.NetAmount, settled.RecordCount)
	check(batch.Processed == exp.Lines, "clearing posted %d of %d lines in %.2f s", batch.Processed, exp.Lines, batch.ImportSecs)
	check(batch.Expired == exp.ExpiryTargets, "expired %d of %d aged holds in %.2f s", batch.Expired, exp.ExpiryTargets, batch.ExpireSecs)
	for _, class := range cardrecon.Classes {
		check(run.Counts[class] == exp.Counts[class], "%-24s %6d (want %d)", class, run.Counts[class], exp.Counts[class])
	}
	drifts, err := ledger.VerifyBalances(ctx, pool)
	if err != nil {
		return err
	}
	check(len(drifts) == 0, "stored balances drifted from the ledger: %d", len(drifts))
	for _, c := range []struct{ what, q string }{
		{"unbalanced ledger transactions", `
			SELECT count(*) FROM (
				SELECT txn_id FROM ledger_entries GROUP BY txn_id
				HAVING SUM(CASE WHEN direction = 'credit' THEN amount ELSE -amount END) <> 0) bad`},
		{"held accounts disagreeing with their authorizations", `
			SELECT count(*) FROM card_accounts ca
			JOIN account_balances b ON b.account_id = ca.held_account_id
			WHERE b.balance <> (
				SELECT COALESCE(sum(a.held_amount), 0) FROM card_authorizations a
				JOIN cards c ON c.id = a.card_id WHERE c.account_id = ca.account_id)`},
		{"approved authorizations holding without a ledger hold", `
			SELECT count(*) FROM card_authorizations a
			LEFT JOIN ledger_transactions t ON t.id = a.ledger_txn_id
			WHERE a.decision = 'approved' AND a.held_amount > 0 AND t.id IS NULL`},
		{"reversed day authorizations still holding", `
			SELECT count(*) FROM card_authorizations
			WHERE auth_id LIKE 'auth\_day\_%\_rev\_%' AND held_amount > 0`},
		{"unapplied pending reversals", `SELECT count(*) FROM pending_reversals WHERE applied_at IS NULL`},
	} {
		var n int
		if err := pool.QueryRow(ctx, c.q).Scan(&n); err != nil {
			return fmt.Errorf("%s: %w", c.what, err)
		}
		check(n == 0, "%s: %d", c.what, n)
	}

	if k6CSV != "" {
		report, err := latencyReport(k6CSV, batch)
		if err != nil {
			return err
		}
		fmt.Print("\n" + report)
	}
	if len(failures) > 0 {
		return fmt.Errorf("%d checks failed, first: %s", len(failures), failures[0])
	}
	return nil
}

type sample struct {
	at     int64
	ms     float64
	status string
}

// latencyReport reads k6's per-request CSV and reports each message type
// before, during and after the batch jobs. The first five seconds are left
// out as the open model's ramp.
func latencyReport(path string, batch batchResult) (string, error) {
	file, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer file.Close()
	r := csv.NewReader(file)
	r.ReuseRecord = true
	header, err := r.Read()
	if err != nil {
		return "", err
	}
	col := map[string]int{}
	for i, name := range header {
		col[name] = i
	}
	samples := map[string][]sample{}
	dropped := map[string]float64{}
	var first int64
	for {
		rec, err := r.Read()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return "", err
		}
		at, _ := strconv.ParseInt(rec[col["timestamp"]], 10, 64)
		if first == 0 || at < first {
			first = at
		}
		value, _ := strconv.ParseFloat(rec[col["metric_value"]], 64)
		switch rec[col["metric_name"]] {
		case "dropped_iterations":
			dropped[rec[col["scenario"]]] += value
		case "http_req_duration":
			key := rec[col["scenario"]] + " " + tagValue(rec[col["extra_tags"]], "msg")
			samples[key] = append(samples[key], sample{at: at, ms: value, status: rec[col["status"]]})
		}
	}

	windows := []struct {
		name     string
		from, to int64
	}{
		{"before batch", first + 5_000_000, batch.StartUS},
		{"during batch", batch.StartUS, batch.EndUS},
		{"after batch", batch.EndUS, 1 << 62},
	}
	var b strings.Builder
	fmt.Fprintf(&b, "batch jobs ran %.1f s (import %.1f s, expiry %.1f s)\n\n",
		float64(batch.EndUS-batch.StartUS)/1e6, batch.ImportSecs, batch.ExpireSecs)
	b.WriteString("| traffic | window | requests | per s | p50 ms | p95 ms | p99 ms | max ms | non-200 |\n")
	b.WriteString("|---|---|---:|---:|---:|---:|---:|---:|---|\n")
	for _, key := range slices.Sorted(maps.Keys(samples)) {
		for _, w := range windows {
			var ms []float64
			statuses := map[string]int{}
			last := w.from
			for _, s := range samples[key] {
				if s.at < w.from || s.at >= w.to {
					continue
				}
				ms = append(ms, s.ms)
				last = max(last, s.at)
				if s.status != "200" {
					statuses[s.status]++
				}
			}
			if len(ms) == 0 {
				continue
			}
			slices.Sort(ms)
			span := float64(min(w.to, last)-w.from) / 1e6
			fmt.Fprintf(&b, "| %s | %s | %d | %.0f | %.2f | %.2f | %.2f | %.2f | %s |\n", key, w.name, len(ms),
				float64(len(ms))/max(span, 1e-3), percentile(ms, 0.50), percentile(ms, 0.95), percentile(ms, 0.99),
				ms[len(ms)-1], statusText(statuses))
		}
	}
	fmt.Fprintf(&b, "\ndropped iterations: %v\n", dropped)
	return b.String(), nil
}

func tagValue(extra, name string) string {
	for _, kv := range strings.Split(extra, "&") {
		if k, v, ok := strings.Cut(kv, "="); ok && k == name {
			return v
		}
	}
	return ""
}

// percentile interpolates between closest ranks, as k6 does.
func percentile(sorted []float64, p float64) float64 {
	pos := p * float64(len(sorted)-1)
	lo := int(pos)
	if lo+1 >= len(sorted) {
		return sorted[lo]
	}
	return sorted[lo] + (sorted[lo+1]-sorted[lo])*(pos-float64(lo))
}

func statusText(statuses map[string]int) string {
	if len(statuses) == 0 {
		return "0"
	}
	var parts []string
	for _, s := range slices.Sorted(maps.Keys(statuses)) {
		parts = append(parts, fmt.Sprintf("%d×%s", statuses[s], s))
	}
	return strings.Join(parts, ", ")
}

func writeJSON(path string, v any) error {
	data, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, data, 0o600)
}

func readJSON(path string, v any) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	return json.Unmarshal(data, v)
}
