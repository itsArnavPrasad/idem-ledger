// cmd/chaostest: proves the outbox pattern delivers every event even when the
// poller is killed mid-run and restarted, simulating a process crash.
//
// Test flow:
//  1. Seed N events directly into outbox (status=pending).
//  2. Start a poller goroutine against a local httptest server (our "merchant").
//  3. After ~100ms (mid-delivery), cancel the poller context (simulated crash).
//  4. Wait 500ms (simulated restart delay).
//  5. Start a fresh poller goroutine.
//  6. Wait until 0 rows remain in pending or in_flight state.
//  7. Assert: total delivered == N, zero lost, zero X-Event-ID duplicates.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"time"

	"github.com/arnavprasad/idem-ledger/internal/outbox"
	"github.com/arnavprasad/idem-ledger/internal/store"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	dbURL     = "postgres://idem:idem@localhost:5432/idemledger?sslmode=disable"
	numEvents = 500
)

func main() {
	ctx := context.Background()
	pool, err := store.NewPool(ctx, dbURL)
	if err != nil {
		log.Fatalf("connect: %v", err)
	}
	defer pool.Close()

	// Track every X-Event-ID the merchant server receives.
	var (
		mu          sync.Mutex
		received    = map[string]int{}
		deliveredN  atomic.Int64
	)

	// Merchant test server — records delivery, always returns 200.
	merchantServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id := r.Header.Get("X-Event-ID")
		mu.Lock()
		received[id]++
		mu.Unlock()
		deliveredN.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer merchantServer.Close()

	// Clean up any leftover outbox rows from previous test runs.
	if _, err := pool.Exec(ctx, `DELETE FROM outbox`); err != nil {
		log.Fatalf("cleanup: %v", err)
	}

	// Seed N outbox events, all pointing at the merchant test server.
	fmt.Printf("Seeding %d outbox events...\n", numEvents)
	if err := seedEvents(ctx, pool, merchantServer.URL, numEvents); err != nil {
		log.Fatalf("seed: %v", err)
	}

	// Phase 1: Start poller, let it run for ~100ms, then kill it.
	ctx1, cancel1 := context.WithCancel(ctx)
	p1 := outbox.New(pool)
	go p1.Run(ctx1)

	time.Sleep(150 * time.Millisecond) // let it deliver some events
	cancel1()                           // simulated crash
	beforeCrash := deliveredN.Load()
	fmt.Printf("Delivered before crash:    %d\n", beforeCrash)

	// Phase 2: Simulate restart delay, then start fresh poller.
	time.Sleep(500 * time.Millisecond)
	ctx2, cancel2 := context.WithCancel(ctx)
	defer cancel2()
	p2 := outbox.New(pool)
	go p2.Run(ctx2)

	// Wait until no pending/in_flight rows remain (all delivered or dead-lettered).
	if err := waitUntilDrained(ctx, pool, 30*time.Second); err != nil {
		log.Fatalf("drain timeout: %v", err)
	}
	cancel2()

	// Final counts from the DB.
	counts := queryStatusCounts(ctx, pool)
	totalDelivered := counts["delivered"]
	deadLettered := counts["dead_letter"]

	// Duplicate check.
	mu.Lock()
	duplicates := 0
	for _, n := range received {
		if n > 1 {
			duplicates += n - 1
		}
	}
	uniqueIDs := len(received)
	mu.Unlock()

	lost := numEvents - totalDelivered - deadLettered

	fmt.Printf("\n=== Chaos Test Results ===\n")
	fmt.Printf("Events queued:             %d\n", numEvents)
	fmt.Printf("Delivered before crash:    %d\n", beforeCrash)
	fmt.Printf("Remaining after restart:   %d\n", numEvents-int(beforeCrash))
	fmt.Printf("Total delivered (DB):      %d\n", totalDelivered)
	fmt.Printf("Dead-lettered:             %d\n", deadLettered)
	fmt.Printf("Unique X-Event-IDs seen:   %d\n", uniqueIDs)
	fmt.Printf("HTTP duplicate deliveries: %d\n", duplicates)
	fmt.Printf("Lost (not delivered/DL):   %d\n", lost)

	if lost != 0 || duplicates != 0 {
		fmt.Printf("\n*** CHAOS TEST FAILED: lost=%d duplicates=%d ***\n", lost, duplicates)
	} else {
		fmt.Printf("\nChaos test: PASS — Lost: 0, Duplicates: 0 ✓\n")
	}
}

func seedEvents(ctx context.Context, pool *pgxpool.Pool, targetURL string, n int) error {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	payload, _ := json.Marshal(map[string]string{"type": "chaos.test"})
	for i := 0; i < n; i++ {
		if err := store.InsertOutboxEventInTx(ctx, tx, "chaos.test", payload, &targetURL); err != nil {
			return err
		}
	}
	return tx.Commit(ctx)
}

func waitUntilDrained(ctx context.Context, pool *pgxpool.Pool, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		var count int
		err := pool.QueryRow(ctx,
			`SELECT COUNT(*) FROM outbox WHERE status IN ('pending','in_flight')`,
		).Scan(&count)
		if err != nil {
			return err
		}
		if count == 0 {
			return nil
		}
		time.Sleep(100 * time.Millisecond)
	}
	return fmt.Errorf("timed out waiting for outbox to drain")
}

func queryStatusCounts(ctx context.Context, pool *pgxpool.Pool) map[string]int {
	rows, err := pool.Query(ctx, `SELECT status, COUNT(*) FROM outbox GROUP BY status`)
	if err != nil {
		log.Printf("status counts: %v", err)
		return nil
	}
	defer rows.Close()
	m := map[string]int{}
	for rows.Next() {
		var status string
		var n int
		rows.Scan(&status, &n)
		m[status] = n
	}
	return m
}
