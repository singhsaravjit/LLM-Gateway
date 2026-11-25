#!/usr/bin/env bash
# Experiment 3 — rate limiter accuracy across nodes (see PLAN.md).
#
#   REDIS_ADDR=localhost:6379 ./experiments/03-ratelimit/run.sh
#
# One API key limited to 10 rps (600/min, burst 10). Load: 100 rps, open loop,
# round-robin across N gateway nodes. Provider never limits, cache and queue
# off, so the limiter is the only thing deciding.
# Env overrides: DUR (default 30s), REPS (3), NODE_COUNTS ("1 2 4 8"), RPS (100).
source "$(dirname "$0")/../tools/common.sh"
RESULTS="$ROOT/experiments/03-ratelimit/results"
mkdir -p "$RESULTS/logs" "$RESULTS/raw"
DUR="${DUR:-30s}"; REPS="${REPS:-3}"; NODE_COUNTS="${NODE_COUNTS:-1 2 4 8}"; RPS="${RPS:-100}"
LIMIT_PER_MIN=600; BURST=10
build

SUMMARY="$RESULTS/summary.csv"
echo "backend,nodes,rep,offered_rps,duration_s,sent,admitted,allowed,over_admission_pct,limiter_check_us,p50_ms,p99_ms" >"$SUMMARY"
dur_s=$(python3 -c "import sys; d=sys.argv[1]; print(float(d[:-1])*(60 if d.endswith('m') else 1))" "$DUR")
allowed=$(python3 -c "print(round($BURST + $LIMIT_PER_MIN/60*$dur_s))")

start_mock LATENCY_MS_MEAN=20 LATENCY_MS_JITTER=5 RATE_LIMIT_RPS=0
for rep in $(seq 1 "$REPS"); do
  for backend in local redis; do
    for n in $NODE_COUNTS; do
      rcli flushall >/dev/null
      start_gateways "$n" RATELIMIT_BACKEND=$backend RATE_LIMIT_PER_MIN=$LIMIT_PER_MIN RATE_LIMIT_BURST=$BURST \
        QUEUE_ENABLED=false CACHE_ENABLED=false
      out=$("$BIN/loadgen" -nodes "$NODES" -shape "$RPS:$DUR" -keys 1 -out "$RESULTS/raw/${backend}-n${n}-r${rep}")
      admitted=$(json_get "$out" codes 200 2>/dev/null || echo 0)
      sum=$(metric ratelimit_check_seconds_sum); cnt=$(metric ratelimit_check_seconds_count)
      row=$(python3 -c "
a=$admitted; al=$allowed; s=$sum; c=$cnt
print(f'{(a-al)/al*100:.1f},{(s/c*1e6 if c else 0):.1f}')")
      echo "$backend,$n,$rep,$RPS,$dur_s,$(json_get "$out" sent),$admitted,$allowed,${row%,*},${row#*,},$(json_get "$out" p50_ms),$(json_get "$out" p99_ms)" | tee -a "$SUMMARY"
      # stop gateways only (keep the mock): they are the last n PIDs
      for ((k = 0; k < n; k++)); do kill "${PIDS[-1]}" 2>/dev/null; wait "${PIDS[-1]}" 2>/dev/null || true; unset 'PIDS[-1]'; done
    done
  done
done
echo "done: $SUMMARY"
