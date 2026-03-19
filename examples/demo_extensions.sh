#!/usr/bin/env bash
set -euo pipefail

# =============================================================================
# Tuber Extension Features Demo
#
# Demonstrates grp: (job groups), aft: (after-group dependencies),
# and con: (concurrency keys) against a live tuber server.
#
# Usage: ./demo_extensions.sh [--addr HOST:PORT]
# =============================================================================

# ---------------------------------------------------------------------------
# Colors & output helpers
# ---------------------------------------------------------------------------
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

section() {
    echo -e "\n${BLUE}═══════════════════════════════════════${NC}"
    echo -e "${BLUE}  $1${NC}"
    echo -e "${BLUE}═══════════════════════════════════════${NC}"
}

info() { echo -e "${GREEN}▸ $1${NC}"; }
result() { echo -e "${YELLOW}  -> $1${NC}"; }

# ---------------------------------------------------------------------------
# Argument parsing
# ---------------------------------------------------------------------------
ADDR="localhost:11300"

while [[ $# -gt 0 ]]; do
    case "$1" in
        --addr)
            ADDR="$2"
            shift 2
            ;;
        *)
            echo "Usage: $0 [--addr HOST:PORT]"
            exit 1
            ;;
    esac
done

HOST="${ADDR%%:*}"
PORT="${ADDR##*:}"

info "Connecting to tuber at ${HOST}:${PORT}"

# ---------------------------------------------------------------------------
# TCP helpers
# ---------------------------------------------------------------------------

# Send a single command on its own connection and print the response.
send_cmd() {
    local cmd="$1"
    printf '%s\r\n' "$cmd" | nc -w 2 "$HOST" "$PORT"
}

# Send multiple commands on a single connection (required for use+put,
# watch+reserve+delete flows, etc.). Each argument is one command.
send_session() {
    {
        for cmd in "$@"; do
            printf '%s\r\n' "$cmd"
            sleep 0.1
        done
    } | nc -w 2 "$HOST" "$PORT"
}

# Put a job on a specific tube with optional extension tags.
#   put_job <tube> <body> [tags...]
# e.g. put_job demo "hello world" "grp:batch1" "con:api"
put_job() {
    local tube="$1"
    local body="$2"
    shift 2
    local tags="$*"
    local len=${#body}
    local put_line="put 0 0 60 ${len}"
    if [[ -n "$tags" ]]; then
        put_line="${put_line} ${tags}"
    fi
    send_session "use ${tube}" "${put_line}" "${body}"
}

# Reserve a job from a tube with a short timeout.
# Returns the full reserve response (RESERVED id bytes\r\n<data> or TIMED_OUT).
reserve_from() {
    local tube="$1"
    local timeout="${2:-1}"
    send_session "watch ${tube}" "ignore default" "reserve-with-timeout ${timeout}"
}

# Reserve a job, then delete it. Note: the reserve connection closes between
# steps, which releases the job back to Ready. The delete then removes the
# now-Ready job. This is fine for demo cleanup purposes.
reserve_and_delete() {
    local tube="$1"
    local timeout="${2:-1}"
    local resp
    resp=$(send_session "watch ${tube}" "ignore default" "reserve-with-timeout ${timeout}")
    echo "$resp"
    local job_id
    job_id=$(echo "$resp" | grep -o 'RESERVED [0-9]*' | awk '{print $2}')
    if [[ -n "$job_id" ]]; then
        send_cmd "delete ${job_id}" > /dev/null
    fi
}

# Flush a tube (tuber extension).
flush() {
    local tube="$1"
    send_cmd "flush-tube ${tube}"
}

# ---------------------------------------------------------------------------
# Cleanup helper
# ---------------------------------------------------------------------------
TUBES_USED=()

register_tube() {
    TUBES_USED+=("$1")
}

cleanup_all() {
    info "Cleaning up all demo tubes..."
    for tube in "${TUBES_USED[@]}"; do
        flush "$tube" 2>/dev/null || true
    done
}

trap cleanup_all EXIT

# ═══════════════════════════════════════════════════════════════════════════
# Scenario 1: Job Groups (grp / aft)
# ═══════════════════════════════════════════════════════════════════════════
section "Scenario 1: Job Groups (grp/aft)"
TUBE="demo-groups"
register_tube "$TUBE"

info "Put 3 child jobs with grp:demo-batch"
for i in 1 2 3; do
    resp=$(put_job "$TUBE" "child-${i}" "grp:demo-batch")
    result "Child ${i}: $(echo "$resp" | grep INSERTED || echo "$resp")"
done

info "Put 1 after-job with aft:demo-batch (held until children complete)"
resp=$(put_job "$TUBE" "after-job-payload" "aft:demo-batch")
result "After-job: $(echo "$resp" | grep INSERTED || echo "$resp")"

info "Try to reserve the after-job (should get a child, not the after-job)"
resp=$(reserve_from "$TUBE" 1)
result "$(echo "$resp" | grep -E 'RESERVED|TIMED_OUT' | head -1)"

info "Reserve and delete all 3 children"
for i in 1 2 3; do
    resp=$(reserve_and_delete "$TUBE" 1)
    result "Child ${i}: $(echo "$resp" | grep -E 'RESERVED|TIMED_OUT' | head -1)"
done

info "All children deleted -- after-job should now be ready"
resp=$(reserve_and_delete "$TUBE" 2)
result "After-job: $(echo "$resp" | grep -E 'RESERVED|TIMED_OUT' | head -1)"

flush "$TUBE" > /dev/null 2>&1 || true

# ═══════════════════════════════════════════════════════════════════════════
# Scenario 2: Chained Groups (pipeline)
# ═══════════════════════════════════════════════════════════════════════════
section "Scenario 2: Chained Groups (pipeline)"
TUBE="demo-pipeline"
register_tube "$TUBE"

info "Stage A -> Stage B (aft:stageA, grp:stageB) -> Final (aft:stageB)"

info "Put 2 stage-A jobs with grp:stageA"
for i in 1 2; do
    resp=$(put_job "$TUBE" "stageA-${i}" "grp:stageA")
    result "Stage A-${i}: $(echo "$resp" | grep INSERTED || echo "$resp")"
done

info "Put stage-B job with aft:stageA grp:stageB"
resp=$(put_job "$TUBE" "stageB-work" "aft:stageA grp:stageB")
result "Stage B: $(echo "$resp" | grep INSERTED || echo "$resp")"

info "Put final job with aft:stageB"
resp=$(put_job "$TUBE" "final-work" "aft:stageB")
result "Final: $(echo "$resp" | grep INSERTED || echo "$resp")"

info "Process stage A (2 jobs)"
for i in 1 2; do
    resp=$(reserve_and_delete "$TUBE" 1)
    result "Stage A-${i}: $(echo "$resp" | grep -E 'RESERVED|TIMED_OUT' | head -1)"
done

info "Stage A done -- stage B should be ready"
resp=$(reserve_and_delete "$TUBE" 2)
result "Stage B: $(echo "$resp" | grep -E 'RESERVED|TIMED_OUT' | head -1)"

info "Stage B done -- final job should be ready"
resp=$(reserve_and_delete "$TUBE" 2)
result "Final: $(echo "$resp" | grep -E 'RESERVED|TIMED_OUT' | head -1)"

flush "$TUBE" > /dev/null 2>&1 || true

# ═══════════════════════════════════════════════════════════════════════════
# Scenario 3: Concurrency Keys (con:)
# ═══════════════════════════════════════════════════════════════════════════
section "Scenario 3: Concurrency Keys (con:)"
TUBE="demo-concurrency"
register_tube "$TUBE"

info "Put 3 jobs with con:api-rate-limit (default limit: 1)"
for i in 1 2 3; do
    resp=$(put_job "$TUBE" "api-call-${i}" "con:api-rate-limit")
    result "Job ${i}: $(echo "$resp" | grep INSERTED || echo "$resp")"
done

info "Reserve first job (should succeed)"
resp1=$(reserve_from "$TUBE" 1)
result "First: $(echo "$resp1" | grep -E 'RESERVED|TIMED_OUT' | head -1)"
JOB1_ID=$(echo "$resp1" | grep -o 'RESERVED [0-9]*' | awk '{print $2}')

info "Try to reserve second job (should TIMED_OUT -- concurrency limit 1)"
resp2=$(reserve_from "$TUBE" 1)
result "Second: $(echo "$resp2" | grep -E 'RESERVED|TIMED_OUT' | head -1)"

info "Only 1 job reserved at a time with con:api-rate-limit"

info "Delete first job, then reserve second (should succeed now)"
send_cmd "delete ${JOB1_ID}" > /dev/null
resp3=$(reserve_and_delete "$TUBE" 1)
result "Second (retry): $(echo "$resp3" | grep -E 'RESERVED|TIMED_OUT' | head -1)"

flush "$TUBE" > /dev/null 2>&1 || true

# ═══════════════════════════════════════════════════════════════════════════
# Scenario 4: Concurrency with limit > 1 (con:key:N)
# ═══════════════════════════════════════════════════════════════════════════
section "Scenario 4: Concurrency limit > 1 (con:key:N)"
TUBE="demo-concurrency-multi"
register_tube "$TUBE"

info "Put 5 jobs with con:api:3 (limit 3)"
for i in 1 2 3 4 5; do
    resp=$(put_job "$TUBE" "batch-${i}" "con:api:3")
    result "Job ${i}: $(echo "$resp" | grep INSERTED || echo "$resp")"
done

info "Reserve 3 jobs (all should succeed)"
RESERVED_IDS=()
for i in 1 2 3; do
    resp=$(reserve_from "$TUBE" 1)
    result "Reserve ${i}: $(echo "$resp" | grep -E 'RESERVED|TIMED_OUT' | head -1)"
    jid=$(echo "$resp" | grep -o 'RESERVED [0-9]*' | awk '{print $2}')
    if [[ -n "$jid" ]]; then
        RESERVED_IDS+=("$jid")
    fi
done

info "Try 4th reservation (should TIMED_OUT -- limit is 3)"
resp=$(reserve_from "$TUBE" 1)
result "Reserve 4: $(echo "$resp" | grep -E 'RESERVED|TIMED_OUT' | head -1)"

info "3 concurrent reservations, 4th blocked"

info "Delete reserved jobs to clean up"
for jid in "${RESERVED_IDS[@]}"; do
    send_cmd "delete ${jid}" > /dev/null
done

flush "$TUBE" > /dev/null 2>&1 || true

# ═══════════════════════════════════════════════════════════════════════════
# Scenario 5: Combined -- Group + Concurrency
# ═══════════════════════════════════════════════════════════════════════════
section "Scenario 5: Group + Concurrency combined"
TUBE="demo-combined"
register_tube "$TUBE"

info "Put 3 jobs with grp:import con:db-conn (concurrency limit 1)"
for i in 1 2 3; do
    resp=$(put_job "$TUBE" "import-${i}" "grp:import con:db-conn")
    result "Job ${i}: $(echo "$resp" | grep INSERTED || echo "$resp")"
done

info "Put after-job with aft:import (runs after all import jobs finish)"
resp=$(put_job "$TUBE" "post-import-cleanup" "aft:import")
result "After-job: $(echo "$resp" | grep INSERTED || echo "$resp")"

info "Reserve first (succeeds, concurrency slot taken)"
resp1=$(reserve_from "$TUBE" 1)
result "First: $(echo "$resp1" | grep -E 'RESERVED|TIMED_OUT' | head -1)"
JOB1_ID=$(echo "$resp1" | grep -o 'RESERVED [0-9]*' | awk '{print $2}')

info "Try second (should TIMED_OUT -- concurrency limit)"
resp2=$(reserve_from "$TUBE" 1)
result "Second: $(echo "$resp2" | grep -E 'RESERVED|TIMED_OUT' | head -1)"

info "Delete first, then reserve+delete remaining children one at a time"
send_cmd "delete ${JOB1_ID}" > /dev/null

for i in 2 3; do
    resp=$(reserve_and_delete "$TUBE" 1)
    result "Child ${i}: $(echo "$resp" | grep -E 'RESERVED|TIMED_OUT' | head -1)"
done

info "All children done -- after-job should now be ready"
resp=$(reserve_and_delete "$TUBE" 2)
result "After-job: $(echo "$resp" | grep -E 'RESERVED|TIMED_OUT' | head -1)"

flush "$TUBE" > /dev/null 2>&1 || true

# ═══════════════════════════════════════════════════════════════════════════
section "All scenarios complete!"
echo -e "${GREEN}Demo finished successfully.${NC}"
