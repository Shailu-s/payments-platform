package relay

import (
      "context"
      "fmt"
      "os"
      "testing"
      "time"

      "github.com/Shailu-s/payments-platform/internal/testdb"
      "github.com/jackc/pgx/v5/pgxpool"
      "github.com/twmb/franz-go/pkg/kgo"
)

const (
      testSchema = "test_relay"
      brokers    = "localhost:9092"
)

var (
      testPool *pgxpool.Pool
      kafka    *kgo.Client
)

func TestMain(m *testing.M) {
      ctx := context.Background()

      pool, err := testdb.Connect(ctx, testSchema)
      if err != nil {
              fmt.Fprint(os.Stderr, testdb.ConnectionHint(err))
              if os.Getenv("LEDGER_TESTS") == "skip" {
                      os.Exit(0)
              }
              os.Exit(1)
      }
      testPool = pool

      // NewClient is lazy; Ping detects a missing broker before running tests.
      kafka, err = kgo.NewClient(kgo.SeedBrokers(brokers))
      if err != nil {
              fmt.Fprintf(os.Stderr, "kafka client: %v\n", err)
              os.Exit(1)
      }
      pingCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
      err = kafka.Ping(pingCtx)
      cancel()
      if err != nil {
              fmt.Fprintf(os.Stderr, "\nno usable Kafka at %s: %v\nrun `make up` first\n\n", brokers, err)
              os.Exit(1)
      }

      code := m.Run()

      // Closed by hand, not deferred: os.Exit does not run deferred calls.
      kafka.Close()
      pool.Close()
      os.Exit(code)
}

func resetDB(t testing.TB) {
      t.Helper()
      if err := testdb.TruncateAll(context.Background(), testPool, testSchema); err != nil {
              t.Fatalf("reset: %v", err)
      }
}