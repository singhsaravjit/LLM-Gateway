#!/usr/bin/env bash
# Experiment 5 — queueing under provider rate limits (see PLAN.md).
#
#   REDIS_ADDR=localhost:6379 ./experiments/05-queueing/run.sh
#
# Mock provider limited to 50 rps with 800 ± 300 ms latency. 2 gateway nodes,
# cache off, 50 API keys (so per-key limits never interfere). Provider pacer
# at 45 rps, shared through Redis. Queue wait timeout 30 s, client timeout 60 s.
# Env overrides: DUR (40s), LOADS ("25 50 75 100 150"), CONFIGS ("off on1000 on100").
source "$(dirname "$0")/../tools/common.sh"
RESULTS="$ROOT/experiments/05-queueing/results"
mkdir -p "$RESULTS/logs" "$RESULTS/raw" "$RESULTS/timeline"
DUR="${DUR:-40s}"; LOADS="${LOADS:-25 50 75 100 150}"; CONFIGS="${CONFIGS:-off on1000 on100}"
build

config_env() {
  case $1 in
    off)    echo "QUEUE_ENABLED=false" ;;
    on1000) echo "QUEUE_ENABLED=true QUEUE_MAX=1000" ;;
    on100)  echo "QUEUE_ENABLED=true QUEUE_MAX=100" ;;
    *) echo "unknown config $1" >&2; exit 1 ;;
  esac
}

# sample_depth FILE — record queue depth (sum over nodes' view; Redis XLEN is global) every second
sample_depth() {
  local f=$1 t=0
  echo "t_s,queue_depth" >"$f"
  while true; do echo "$t,$(rcli xlen queue:provider 2>/dev/null || echo 0)" >>"$f"; sleep 1; t=$((t + 1)); done
}

run_one() { # name shape config
  local name=$1 shape=$2 cfg=$3
  rcli flushall >/dev/null
  # shellcheck disable=SC2046
  start_gateways 2 $(config_env "$cfg") QUEUE_WAIT_TIMEOUT_MS=30000 PROVIDER_RPS=45 \
    RATE_LIMIT_PER_MIN=600000 RATE_LIMIT_BURST=100000 CACHE_ENABLED=false
  sample_depth "$RESULTS/timeline/$name-depth.csv" & local sampler=$!
  local out; out=$("$BIN/loadgen" -nodes "$NODES" -shape "$shape" -keys 50 -timeout 60s -out "$RESULTS/raw/$name")
  kill $sampler 2>/dev/null; wait $sampler 2>/dev/null || true
  local up429 full timeout maxdepth
  up429=$(metric upstream_429_total); full=$(metric 'queue_rejected_total{reason="full"}')
  timeout=$(metric 'queue_rejected_total{reason="timeout"}')
  maxdepth=$(tail -n +2 "$RESULTS/timeline/$name-depth.csv" | cut -d, -f2 | sort -n | tail -1)
  python3 - "$out" "$name" "$cfg" "$shape" "$up429" "$full" "$timeout" "${maxdepth:-0}" <<'PY' | tee -a "$SUMMARY"
import json, sys
s = json.loads(sys.argv[1]); name, cfg, shape, up429, full, tmo, depth = sys.argv[2:]
c = s["codes"]
print(",".join(map(str, [name, cfg, shape, s["sent"], f'{s["success_pct"]:.1f}', c.get("429", 0), c.get("503", 0),
      c.get("504", 0), c.get("502", 0) + c.get("0", 0), round(s["p50_ms"]), round(s["p95_ms"]), round(s["p99_ms"]),
      int(float(up429)), int(float(depth))])))
PY
  for k in 1 2; do kill "${PIDS[-1]}" 2>/dev/null; wait "${PIDS[-1]}" 2>/dev/null || true; unset 'PIDS[-1]'; done
}

SUMMARY="$RESULTS/summary.csv"
echo "run,config,shape,sent,success_pct,client_429,client_503_full,client_504_timeout,other_errors,p50_ms,p95_ms,p99_ms,upstream_429,max_queue_depth" >"$SUMMARY"
start_mock LATENCY_MS_MEAN=800 LATENCY_MS_JITTER=300 RATE_LIMIT_RPS=50
for cfg in $CONFIGS; do
  for rps in $LOADS; do run_one "$cfg-${rps}rps" "$rps:$DUR" "$cfg"; done
done
# Burst shape: 20 s at 3x the provider limit, then 40 s at 0.5x.
for cfg in off on1000; do run_one "$cfg-burst" "150:20s,25:40s" "$cfg"; done
echo "done: $SUMMARY"
