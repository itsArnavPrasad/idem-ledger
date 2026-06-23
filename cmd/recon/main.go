// cmd/recon: standalone reconciliation job.
// Runs conservation, balance integrity, and negative-balance checks against the
// live DB. Exits 0 on clean, 1 on any drift.
package main

import (
	"context"
	"log"
	"os"

	"github.com/arnavprasad/idem-ledger/internal/recon"
	"github.com/arnavprasad/idem-ledger/internal/store"
)

func main() {
	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		dbURL = "postgres://idem:idem@localhost:5432/idemledger?sslmode=disable"
	}

	ctx := context.Background()
	pool, err := store.NewPool(ctx, dbURL)
	if err != nil {
		log.Fatalf("connect: %v", err)
	}
	defer pool.Close()

	result, err := recon.Run(ctx, pool)
	if err != nil {
		log.Fatalf("recon error: %v", err)
	}

	recon.Report(os.Stdout, result)

	if !result.ConservationOK || len(result.DriftAccounts) > 0 || result.NegativeCount > 0 {
		os.Exit(1)
	}
}
