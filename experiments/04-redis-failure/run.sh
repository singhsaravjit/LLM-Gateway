#!/usr/bin/env bash
# Experiment 4 (stretch) — Redis failure: fail open vs fail closed.
#
#   REDIS_ADDR=localhost:6379 ./experiments/04-redis-failure/run.sh
#
# LOCAL ONLY: this script stops and restarts the Redis at REDIS_ADDR (it must be
# a throwaway redis-server this machine can restart, never a shared one). On AWS,
# block the ElastiCache security group instead.
#
# One API key limited to 10 rps, load 50 rps for 40 s across 2 nodes; only the
# limiter uses Redis (cache off, queue off, budgets unlimited). Faults:
#   down: Redis process stopped at t=10 s, restarted at t=25 s (connection refused)
#   hang: Redis paused with CLIENT PAUSE for 15 s at t=10 s (accepts, never answers)
# Per-second timelines go to results/raw/*.csv; summary.csv has the totals.
source "$(dirname "$0")/../tools/common.sh"
RESULTS="$ROOT/experiments/04-redis-failure/results"
mkdir -p "$RESULTS/logs" "$RESULTS/raw"
build
command -v redis-server >/dev/null || { echo "needs a local redis-server"; exit 1; }

fault() { # kind — runs in background, timed from now
  sleep 10
  case $1 in
    down) rcli shutdown nosave >/dev/null 2>&1 || true
          sleep 15
          redis-server --port "$REDIS_PORT" --save '' --daemonize yes >/dev/null ;;
    hang) rcli client pause 15000 all >/dev/null ;;
  esac
}

SUMMARY="$RESULTS/summary.csv"
echo "fault,fail_mode,sent,admitted,allowed_by_limit,let_through_during_fault,rejected_503,rejected_429,p99_ms,max_latency_ms" >"$SUMMARY"
start_mock LATENCY_MS_MEAN=20 LATENCY_MS_JITTER=5 RATE_LIMIT_RPS=0
for kind in down hang; do
  for mode in open closed; do
    rcli ping >/dev/null 2>&1 || redis-server --port "$REDIS_PORT" --save '' --daemonize yes >/dev/null
    sleep 0.5; rcli flushall >/dev/null
    start_gateways 2 RATELIMIT_BACKEND=redis RATELIMIT_FAIL_MODE=$mode RATE_LIMIT_PER_MIN=600 RATE_LIMIT_BURST=10 \
      QUEUE_ENABLED=false CACHE_ENABLED=false BUDGET_BACKEND=memory
    fault $kind & fpid=$!
    name="$kind-$mode"
    out=$("$BIN/loadgen" -nodes "$NODES" -shape "50:40s" -keys 1 -out "$RESULTS/raw/$name")
    wait $fpid || true
    python3 - "$out" "$RESULTS/raw/$name.csv" "$kind" "$mode" <<'PY' | tee -a "$SUMMARY"
import csv, json, sys
s = json.loads(sys.argv[1]); rows = list(csv.DictReader(open(sys.argv[2]))); kind, mode = sys.argv[3:]
admitted = sum(r["status"] == "200" for r in rows)
during = sum(r["status"] == "200" and 10000 <= int(r["start_ms"]) < 25000 for r in rows)
lat = max(float(r["latency_ms"]) for r in rows)
c = s["codes"]
print(",".join(map(str, [kind, mode, s["sent"], admitted, 10 + 10 * 40, during, c.get("503", 0), c.get("429", 0),
      round(s["p99_ms"]), round(lat)])))
PY
    for k in 1 2; do kill "${PIDS[-1]}" 2>/dev/null; wait "${PIDS[-1]}" 2>/dev/null || true; unset 'PIDS[-1]'; done
  done
done
rcli ping >/dev/null 2>&1 || redis-server --port "$REDIS_PORT" --save '' --daemonize yes >/dev/null
echo "done: $SUMMARY"
