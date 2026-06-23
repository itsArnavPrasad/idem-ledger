// Package recon runs integrity checks against the ledger.
// These are the same invariants the loadtest asserts — here they run as a
// standalone job that can be scheduled periodically (cron, CI, incident response).
package recon

import (
	"context"
	"fmt"
	"io"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Result summarises a reconciliation run.
type Result struct {
	ConservationOK bool
	ConservationSum int64          // want 0
	DriftAccounts  []DriftAccount // want empty
	NegativeCount  int64          // want 0
}

// DriftAccount is one account where balance != SUM(postings).
type DriftAccount struct {
	ID          int64
	Balance     int64
	PostingsSum int64
}

// Run executes all invariant checks and returns the result.
func Run(ctx context.Context, db *pgxpool.Pool) (Result, error) {
	var r Result

	// 1. Global conservation: SUM(all postings) == 0.
	if err := db.QueryRow(ctx,
		`SELECT COALESCE(SUM(amount), 0) FROM postings`,
	).Scan(&r.ConservationSum); err != nil {
		return r, fmt.Errorf("conservation check: %w", err)
	}
	r.ConservationOK = (r.ConservationSum == 0)

	// 2. Per-account balance integrity: balance == SUM(postings) for each account.
	// Accounts seeded outside the journal (direct UPDATE) are excluded by
	// requiring balance == SUM(postings), not balance - seed == SUM(postings).
	// This is the production check: in production every credit/debit goes through
	// the ledger, so balance == SUM(postings) exactly.
	rows, err := db.Query(ctx,
		`SELECT a.id, a.balance, COALESCE(SUM(p.amount), 0) AS postings_sum
		 FROM accounts a
		 LEFT JOIN postings p ON p.account_id = a.id
		 GROUP BY a.id, a.balance
		 HAVING a.balance <> COALESCE(SUM(p.amount), 0)`,
	)
	if err != nil {
		return r, fmt.Errorf("balance integrity check: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var d DriftAccount
		if err := rows.Scan(&d.ID, &d.Balance, &d.PostingsSum); err != nil {
			return r, err
		}
		r.DriftAccounts = append(r.DriftAccounts, d)
	}
	if err := rows.Err(); err != nil {
		return r, err
	}

	// 3. No negative balances.
	if err := db.QueryRow(ctx,
		`SELECT COUNT(*) FROM accounts WHERE balance < 0`,
	).Scan(&r.NegativeCount); err != nil {
		return r, fmt.Errorf("negative balance check: %w", err)
	}

	return r, nil
}

// Report writes a human-readable summary to w.
func Report(w io.Writer, r Result) {
	fmt.Fprintf(w, "=== Reconciliation Report ===\n")

	if r.ConservationOK {
		fmt.Fprintf(w, "Conservation (SUM postings): 0 ✓\n")
	} else {
		fmt.Fprintf(w, "Conservation (SUM postings): %d ✗  DRIFT DETECTED\n", r.ConservationSum)
	}

	if len(r.DriftAccounts) == 0 {
		fmt.Fprintf(w, "Balance integrity:           all accounts clean ✓\n")
	} else {
		fmt.Fprintf(w, "Balance integrity:           %d account(s) drifted ✗\n", len(r.DriftAccounts))
		for _, d := range r.DriftAccounts {
			fmt.Fprintf(w, "  account %d: balance=%d postings_sum=%d delta=%d\n",
				d.ID, d.Balance, d.PostingsSum, d.Balance-d.PostingsSum)
		}
	}

	if r.NegativeCount == 0 {
		fmt.Fprintf(w, "Negative balances:           0 ✓\n")
	} else {
		fmt.Fprintf(w, "Negative balances:           %d ✗  INTEGRITY VIOLATION\n", r.NegativeCount)
	}

	if r.ConservationOK && len(r.DriftAccounts) == 0 && r.NegativeCount == 0 {
		fmt.Fprintf(w, "\nDrift: none ✓\n")
	} else {
		fmt.Fprintf(w, "\nDrift: DETECTED — investigate immediately\n")
	}
}
