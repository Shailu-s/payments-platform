package api

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// A request that cannot get a connection this quickly is refused: waiting
// longer only moves it further toward its deadline, where the work is wasted.
const poolAcquireWait = 20 * time.Millisecond

// boundedPool is a DB whose calls wait at most wait for a pool connection and
// fail with ErrOverloaded after that. Only the acquire is bounded: the query
// or transaction then runs under the caller's own deadline.
type boundedPool struct {
	pool *pgxpool.Pool
	wait time.Duration
}

func newBoundedPool(pool *pgxpool.Pool, wait time.Duration) *boundedPool {
	return &boundedPool{pool: pool, wait: wait}
}

func (b *boundedPool) acquire(ctx context.Context) (*pgxpool.Conn, error) {
	acquireCtx, cancel := context.WithTimeout(ctx, b.wait)
	defer cancel()
	conn, err := b.pool.Acquire(acquireCtx)
	if err != nil {
		if ctx.Err() == nil && acquireCtx.Err() != nil {
			return nil, fmt.Errorf("no connection within %v: %w", b.wait, ErrOverloaded)
		}
		return nil, err
	}
	return conn, nil
}

func (b *boundedPool) QueryRow(ctx context.Context, sql string, args ...any) pgx.Row {
	conn, err := b.acquire(ctx)
	if err != nil {
		return errRow{err}
	}
	return &releasingRow{row: conn.QueryRow(ctx, sql, args...), conn: conn}
}

func (b *boundedPool) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	conn, err := b.acquire(ctx)
	if err != nil {
		return nil, err
	}
	rows, err := conn.Query(ctx, sql, args...)
	if err != nil {
		conn.Release()
		return nil, err
	}
	return &releasingRows{Rows: rows, conn: conn}, nil
}

func (b *boundedPool) Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	conn, err := b.acquire(ctx)
	if err != nil {
		return pgconn.CommandTag{}, err
	}
	defer conn.Release()
	return conn.Exec(ctx, sql, args...)
}

func (b *boundedPool) Begin(ctx context.Context) (pgx.Tx, error) {
	conn, err := b.acquire(ctx)
	if err != nil {
		return nil, err
	}
	tx, err := conn.Begin(ctx)
	if err != nil {
		conn.Release()
		return nil, err
	}
	return &releasingTx{Tx: tx, conn: conn}, nil
}

// The wrappers below return the connection to the pool when the caller is
// done with it, as pgxpool's own Row, Rows and Tx do.

type errRow struct{ err error }

func (r errRow) Scan(...any) error { return r.err }

type releasingRow struct {
	row  pgx.Row
	conn *pgxpool.Conn
}

func (r *releasingRow) Scan(dest ...any) error {
	err := r.row.Scan(dest...)
	if r.conn != nil {
		r.conn.Release()
		r.conn = nil
	}
	return err
}

type releasingRows struct {
	pgx.Rows
	conn *pgxpool.Conn
}

func (r *releasingRows) Next() bool {
	if r.Rows.Next() {
		return true
	}
	r.Close()
	return false
}

func (r *releasingRows) Close() {
	r.Rows.Close()
	if r.conn != nil {
		r.conn.Release()
		r.conn = nil
	}
}

type releasingTx struct {
	pgx.Tx
	conn *pgxpool.Conn
}

func (tx *releasingTx) Commit(ctx context.Context) error {
	err := tx.Tx.Commit(ctx)
	tx.release()
	return err
}

func (tx *releasingTx) Rollback(ctx context.Context) error {
	err := tx.Tx.Rollback(ctx)
	tx.release()
	return err
}

func (tx *releasingTx) release() {
	if tx.conn != nil {
		tx.conn.Release()
		tx.conn = nil
	}
}
