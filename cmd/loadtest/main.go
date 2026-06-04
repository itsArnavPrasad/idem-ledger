// cmd/loadtest: concurrent transfer stress harness + invariant checker.
// Usage: go run ./cmd/loadtest [-accounts N] [-transfers M] [-workers W] [-strategy S]
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	hdrhistogram "github.com/HdrHistogram/hdrhistogram-go"
	"github.com/arnavprasad/idem-ledger/internal/ledger"
	"github.com/arnavprasad/idem-ledger/internal/store"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	dbURL        = "postgres://idem:idem@localhost:5432/idemledger?sslmode=disable"
	seedBalance  = int64(10_000_000) // 10M minor units per account — plenty of headroom
)

func main() {
	nAccounts  := flag.Int("accounts",  500,                    "number of accounts to seed")
	nTransfers := flag.Int("transfers", 50_000,                 "number of transfers to run")
	nWorkers   := flag.Int("workers",   20,                     "concurrent goroutines")
	strategy   := flag.String("strategy", "conditional_update", "conditional_update | select_for_update | optimistic")
	flag.Parse()

	ctx := context.Background()
	pool, err := store.NewPool(ctx, dbURL)
	if err != nil {
		log.Fatalf("connect: %v", err)
	}
	defer pool.Close()

	// Seed accounts
	fmt.Printf("Seeding %d accounts...\n", *nAccounts)
	accountIDs := seedAccounts(ctx, pool, *nAccounts)

	// Run transfers
	fmt.Printf("Running %d transfers (%d workers, strategy=%s)...\n", *nTransfers, *nWorkers, *strategy)
	start := time.Now()
	violations, hist := runTransfers(ctx, pool, accountIDs, *nTransfers, *nWorkers, *strategy)
	elapsed := time.Since(start)

	tps := float64(*nTransfers) / elapsed.Seconds()
	// hdrhistogram stores values in microseconds; convert back to Duration for display.
	p50 := time.Duration(hist.ValueAtQuantile(50)) * time.Microsecond
	p99 := time.Duration(hist.ValueAtQuantile(99)) * time.Microsecond

	fmt.Printf("\n=== Results ===\n")
	fmt.Printf("Strategy:            %s\n", *strategy)
	fmt.Printf("Transfers:           %d\n", *nTransfers)
	fmt.Printf("Workers:             %d\n", *nWorkers)
	fmt.Printf("Accounts:            %d\n", *nAccounts)
	fmt.Printf("Wall time:           %v\n", elapsed.Round(time.Millisecond))
	fmt.Printf("TPS:                 %.0f\n", tps)
	fmt.Printf("Latency p50:         %v\n", p50)
	fmt.Printf("Latency p99:         %v\n", p99)
	fmt.Printf("Transfer errors:     %d\n", violations)

	// Post-run invariant assertions — the correctness proof
	fmt.Printf("\n=== Invariant Check ===\n")
	checkInvariants(ctx, pool, accountIDs, *nAccounts)
}

func seedAccounts(ctx context.Context, pool *pgxpool.Pool, n int) []int64 {
	ids := make([]int64, 0, n)
	for i := 0; i < n; i++ {
		name := fmt.Sprintf("load-acct-%d", i)
		acc, err := store.CreateAccount(ctx, pool, name, "INR")
		if err != nil {
			log.Fatalf("create account: %v", err)
		}
		// Fund the account
		if _, err := pool.Exec(ctx,
			`UPDATE accounts SET balance = $1 WHERE id = $2`,
			seedBalance, acc.ID,
		); err != nil {
			log.Fatalf("fund account: %v", err)
		}
		ids = append(ids, acc.ID)
	}
	return ids
}

// runTransfers fires total transfers across workers goroutines using the given strategy.
// Returns the error count and an HdrHistogram of per-transfer latencies in microseconds.
// HdrHistogram range: 1µs to 30s (covers everything from fast commits to lock timeouts).
func runTransfers(ctx context.Context, pool *pgxpool.Pool, ids []int64, total, workers int, strategy string) (int64, *hdrhistogram.Histogram) {
	jobs := make(chan struct{}, total)
	for i := 0; i < total; i++ {
		jobs <- struct{}{}
	}
	close(jobs)

	// One histogram per worker; merge at the end to avoid per-record mutex contention.
	workerHists := make([]*hdrhistogram.Histogram, workers)
	for i := range workerHists {
		workerHists[i] = hdrhistogram.New(1, 30_000_000, 3) // 1µs – 30s, 3 sig figs
	}

	var (
		errors atomic.Int64
		wg     sync.WaitGroup
	)

	for w := 0; w < workers; w++ {
		w := w
		wg.Add(1)
		go func() {
			defer wg.Done()
			rng := rand.New(rand.NewSource(rand.Int63()))
			hist := workerHists[w]
			for range jobs {
				from := ids[rng.Intn(len(ids))]
				to := ids[rng.Intn(len(ids))]
				for to == from {
					to = ids[rng.Intn(len(ids))]
				}
				amount := int64(rng.Intn(500) + 1)
				req := ledger.TransferRequest{
					FromAccount: from,
					ToAccount:   to,
					Amount:      amount,
					Currency:    "INR",
				}

				t0 := time.Now()
				var err error
				switch strategy {
				case "select_for_update":
					_, _, err = ledger.ExecuteWithForUpdate(ctx, pool, req)
				case "optimistic":
					_, _, err = ledger.ExecuteOptimistic(ctx, pool, req)
				default:
					_, _, err = ledger.Execute(ctx, pool, req)
				}
				micros := time.Since(t0).Microseconds()

				if err != nil && err != ledger.ErrInsufficientFunds {
					errors.Add(1)
					log.Printf("transfer error: %v", err)
				}
				hist.RecordValue(micros)
			}
		}()
	}
	wg.Wait()

	// Merge all per-worker histograms into one.
	merged := hdrhistogram.New(1, 30_000_000, 3)
	for _, h := range workerHists {
		merged.Merge(h)
	}
	return errors.Load(), merged
}

func checkInvariants(ctx context.Context, pool *pgxpool.Pool, ids []int64, n int) {
	// 1. Conservation: SUM(all postings) == 0
	var sumPostings int64
	if err := pool.QueryRow(ctx, `SELECT COALESCE(SUM(amount),0) FROM postings`).Scan(&sumPostings); err != nil {
		log.Fatalf("conservation check: %v", err)
	}

	// 2. Balance integrity: account.balance == seedBalance + SUM(postings) for each seeded account.
	// We seed accounts with a direct UPDATE (not through postings), so the journal only records
	// transfers. The expected balance is seedBalance + net_postings. Drift means the balance
	// column diverged from what the journal says happened on top of the seed.
	rows, err := pool.Query(ctx,
		`SELECT a.id, a.balance, COALESCE(SUM(p.amount),0) AS journal_balance
		 FROM accounts a
		 LEFT JOIN postings p ON p.account_id = a.id
		 WHERE a.id = ANY($1)
		 GROUP BY a.id, a.balance
		 HAVING a.balance - COALESCE(SUM(p.amount),0) <> $2`,
		ids, seedBalance,
	)
	if err != nil {
		log.Fatalf("balance integrity check: %v", err)
	}
	defer rows.Close()

	driftCount := 0
	for rows.Next() {
		var id, bal, journalBal int64
		rows.Scan(&id, &bal, &journalBal)
		fmt.Printf("  DRIFT account %d: balance=%d, journal_sum=%d\n", id, bal, journalBal)
		driftCount++
	}

	// 3. No negative balances
	var negCount int64
	if err := pool.QueryRow(ctx,
		`SELECT COUNT(*) FROM accounts WHERE id = ANY($1) AND balance < 0`, ids,
	).Scan(&negCount); err != nil {
		log.Fatalf("negative balance check: %v", err)
	}

	fmt.Printf("Conservation (SUM postings):   %d  (want 0)\n", sumPostings)
	fmt.Printf("Balance drift accounts:         %d  (want 0)\n", driftCount)
	fmt.Printf("Negative balances:              %d  (want 0)\n", negCount)

	if sumPostings != 0 || driftCount != 0 || negCount != 0 {
		fmt.Printf("\n*** INVARIANT VIOLATIONS DETECTED ***\n")
	} else {
		fmt.Printf("\nInvariant violations: 0 ✓\n")
	}
}

