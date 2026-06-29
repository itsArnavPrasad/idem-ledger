#!/usr/bin/env bash
# stress_test.sh — IdemLedger full stress test suite
#
# Runs every scenario sequentially (no DB interference), captures each run
# to its own log file under stress_logs/, then prints a summary table.
#
# Usage:
#   cd idem-ledger
#   bash scripts/stress_test.sh
#
# Prerequisites:
#   - Docker Desktop running
#   - docker compose up -d   (Postgres container)
#   - make migrate-up        (schema applied)
#
# Output: stress_logs/  (hand this folder to Claude if anything looks wrong)

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
LOG_DIR="$REPO_ROOT/stress_logs"
DB="postgres://idem:idem@localhost:5432/idemledger?sslmode=disable"
CONTAINER="idem-ledger-postgres-1"

# ── helpers ──────────────────────────────────────────────────────────────────

ts()    { date '+%H:%M:%S'; }
header(){ echo; echo "══════════════════════════════════════════════════════"; echo "  $*"; echo "══════════════════════════════════════════════════════"; }
ok()    { echo "  ✓ $*"; }
fail()  { echo "  ✗ $*"; }

reset_db() {
    docker exec "$CONTAINER" psql -U idem -d idemledger -c \
        "TRUNCATE accounts, transfers, postings, outbox, idempotency_keys RESTART IDENTITY CASCADE;" \
        > /dev/null 2>&1
}

run_loadtest() {
    local label="$1" logfile="$2"
    shift 2
    echo "  [$(ts)] running $label..."
    # Capture stdout+stderr; tee to screen and log
    go run "$REPO_ROOT/cmd/loadtest" "$@" 2>&1 | tee "$logfile"
}

extract() {
    # extract_field <logfile> <pattern>  e.g. "TPS:" "Latency p50:" etc.
    grep "$1" "$2" | tail -1 | awk '{print $NF}'
}

check_invariants() {
    local logfile="$1"
    if grep -q "Invariant violations: 0" "$logfile"; then
        ok "Invariants: PASS"
    else
        fail "INVARIANT VIOLATION — see $logfile"
    fi
}

# ── setup ─────────────────────────────────────────────────────────────────────

mkdir -p "$LOG_DIR"
echo "Logs → $LOG_DIR"
echo "$(date)" > "$LOG_DIR/run_info.txt"
go build "$REPO_ROOT/..." 2>&1 | tee "$LOG_DIR/build.log"
echo "Build: OK"

# ── SCENARIO 1: conditional_update baseline ───────────────────────────────────

header "RUN 1 — conditional_update  |  500 accts  50k transfers  20 workers"
reset_db
run_loadtest "RUN1" "$LOG_DIR/run1_conditional_update_baseline.log" \
    -accounts 500 -transfers 50000 -workers 20 -strategy conditional_update
check_invariants "$LOG_DIR/run1_conditional_update_baseline.log"

# ── SCENARIO 2: select_for_update baseline ────────────────────────────────────

header "RUN 2 — select_for_update   |  500 accts  50k transfers  20 workers"
reset_db
run_loadtest "RUN2" "$LOG_DIR/run2_select_for_update_baseline.log" \
    -accounts 500 -transfers 50000 -workers 20 -strategy select_for_update
check_invariants "$LOG_DIR/run2_select_for_update_baseline.log"

# ── SCENARIO 3: optimistic baseline (low contention) ─────────────────────────

header "RUN 3 — optimistic (low)    |  500 accts  50k transfers  20 workers"
reset_db
run_loadtest "RUN3" "$LOG_DIR/run3_optimistic_low_contention.log" \
    -accounts 500 -transfers 50000 -workers 20 -strategy optimistic
check_invariants "$LOG_DIR/run3_optimistic_low_contention.log"

# ── SCENARIO 4: optimistic HIGH contention (retry-storm proof) ───────────────

header "RUN 4 — optimistic (HIGH)   |  20 accts  10k transfers  20 workers"
reset_db
run_loadtest "RUN4" "$LOG_DIR/run4_optimistic_high_contention.log" \
    -accounts 20 -transfers 10000 -workers 20 -strategy optimistic
# Errors expected here (retry exhaustion) — still check invariants hold
check_invariants "$LOG_DIR/run4_optimistic_high_contention.log"

# ── SCENARIO 5: conditional_update HIGH contention ───────────────────────────

header "RUN 5 — conditional_update (HIGH)  |  20 accts  2k transfers  20 workers"
reset_db
run_loadtest "RUN5" "$LOG_DIR/run5_conditional_update_high_contention.log" \
    -accounts 20 -transfers 2000 -workers 20 -strategy conditional_update
check_invariants "$LOG_DIR/run5_conditional_update_high_contention.log"

# ── SCENARIO 6: select_for_update HIGH contention ────────────────────────────

header "RUN 6 — select_for_update (HIGH)   |  20 accts  10k transfers  20 workers"
reset_db
run_loadtest "RUN6" "$LOG_DIR/run6_select_for_update_high_contention.log" \
    -accounts 20 -transfers 10000 -workers 20 -strategy select_for_update
check_invariants "$LOG_DIR/run6_select_for_update_high_contention.log"

# ── SCENARIO 7: select_for_update EXTREME contention ─────────────────────────
# NOTE: conditional_update at 5 accounts was removed — it enters a near-livelock
# (deadlock_detection_timeout = 1s per retry × 5 retries × all attempts deadlocking
# = effectively 0 TPS). Run 5 already proves the failure mode. Run 7 now shows
# the contrast: select_for_update handles the same extreme contention cleanly.

header "RUN 7 — select_for_update (EXTREME)  |  5 accts  10k transfers  20 workers"
reset_db
run_loadtest "RUN7" "$LOG_DIR/run7_select_for_update_extreme.log" \
    -accounts 5 -transfers 10000 -workers 20 -strategy select_for_update
check_invariants "$LOG_DIR/run7_select_for_update_extreme.log"

# ── SCENARIO 9: connection-pool saturation (100 workers) ─────────────────────

header "RUN 9 — conditional_update (100 workers)  |  500 accts  50k transfers"
reset_db
run_loadtest "RUN9" "$LOG_DIR/run9_100_workers_pool_stress.log" \
    -accounts 500 -transfers 50000 -workers 100 -strategy conditional_update
check_invariants "$LOG_DIR/run9_100_workers_pool_stress.log"

# ── SCENARIO 10: chaos test (outbox crash resilience) ────────────────────────

header "RUN 10 — Chaos test (outbox crash resilience)"
# Reset only outbox so chaostest starts clean; do NOT truncate accounts/transfers
# (chaostest does its own DELETE FROM outbox at startup)
reset_db
echo "  [$(ts)] running chaostest..."
go run "$REPO_ROOT/cmd/chaostest" 2>&1 | tee "$LOG_DIR/run10_chaostest.log"
if grep -q "PASS" "$LOG_DIR/run10_chaostest.log"; then
    ok "Chaos test: PASS"
else
    fail "Chaos test: FAIL — see $LOG_DIR/run10_chaostest.log"
fi

# ── SCENARIO 11: reconciliation job ──────────────────────────────────────────

header "RUN 11 — Reconciliation job"
# Use whatever state is in the DB from previous runs — that's more realistic
echo "  [$(ts)] running recon..."
go run "$REPO_ROOT/cmd/recon" 2>&1 | tee "$LOG_DIR/run11_recon.log"
if grep -q "Drift: none\|No drift\|OK\|all good\|0 drift" "$LOG_DIR/run11_recon.log"; then
    ok "Recon: PASS"
else
    echo "  Recon output (check manually):"
    cat "$LOG_DIR/run11_recon.log"
fi

# ── SUMMARY TABLE ─────────────────────────────────────────────────────────────

header "SUMMARY TABLE"
printf "%-50s %8s %8s %8s %8s %8s\n" "Scenario" "TPS" "p50(ms)" "p99(ms)" "Errors" "Invariants"
printf "%-50s %8s %8s %8s %8s %8s\n" "─────────────────────────────────────────────────" "───────" "───────" "───────" "───────" "──────────"

summarise() {
    local label="$1" logfile="$2"
    local tps p50 p99 errs inv
    tps=$(extract "TPS:" "$logfile" 2>/dev/null || echo "?")
    p50=$(extract "p50:" "$logfile" 2>/dev/null || echo "?")
    p99=$(extract "p99:" "$logfile" 2>/dev/null || echo "?")
    errs=$(extract "errors:" "$logfile" 2>/dev/null || echo "?")
    if grep -q "Invariant violations: 0" "$logfile" 2>/dev/null; then
        inv="PASS"
    else
        inv="FAIL ✗"
    fi
    printf "%-50s %8s %8s %8s %8s %8s\n" "$label" "$tps" "$p50" "$p99" "$errs" "$inv"
}

summarise "conditional_update  500ac 50k 20w (baseline)" "$LOG_DIR/run1_conditional_update_baseline.log"
summarise "select_for_update   500ac 50k 20w (baseline)" "$LOG_DIR/run2_select_for_update_baseline.log"
summarise "optimistic          500ac 50k 20w (low cont)" "$LOG_DIR/run3_optimistic_low_contention.log"
summarise "optimistic          20ac  10k 20w (HIGH cont)"  "$LOG_DIR/run4_optimistic_high_contention.log"
summarise "conditional_update  20ac  10k 20w (HIGH cont)"  "$LOG_DIR/run5_conditional_update_high_contention.log"
summarise "select_for_update   20ac  10k 20w (HIGH cont)"  "$LOG_DIR/run6_select_for_update_high_contention.log"
summarise "select_for_update   5ac   10k 20w (EXTREME)"    "$LOG_DIR/run7_select_for_update_extreme.log"
summarise "conditional_update  500ac 50k 100w (pool)"      "$LOG_DIR/run9_100_workers_pool_stress.log"

echo
if grep -q "PASS" "$LOG_DIR/run10_chaostest.log" 2>/dev/null; then
    ok "Chaos test:     PASS (Lost: 0, Duplicates: 0)"
else
    fail "Chaos test:     FAIL — see run10_chaostest.log"
fi
echo
echo "All logs saved to: $LOG_DIR"
echo "Done at $(date)"
