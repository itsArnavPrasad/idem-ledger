package store

import (
	"context"
	"os"
	"strconv"

	"github.com/jackc/pgx/v5/pgxpool"
)

// defaultPoolMaxConns is a reasonable default for both the API server and the
// loadtest. pgxpool's built-in default is max(4, runtime.NumCPU()), which on
// Apple Silicon resolves to 8. That starves 100-worker stress runs — each
// goroutine holds a connection for ~5ms; 8 connections caps throughput at
// ~1,600 TPS before queueing dominates. 25 lets the pool breathe.
//
// Override with DATABASE_POOL_MAX_CONNS env var for fine-tuning.
const defaultPoolMaxConns = 25

func NewPool(ctx context.Context, databaseURL string) (*pgxpool.Pool, error) {
	cfg, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		return nil, err
	}
	maxConns := int32(defaultPoolMaxConns)
	if s := os.Getenv("DATABASE_POOL_MAX_CONNS"); s != "" {
		if n, err := strconv.ParseInt(s, 10, 32); err == nil && n > 0 {
			maxConns = int32(n)
		}
	}
	cfg.MaxConns = maxConns
	return pgxpool.NewWithConfig(ctx, cfg)
}
