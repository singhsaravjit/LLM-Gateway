#!/usr/bin/env bash
# Experiment 2: bursts of identical requests with coalescing off / local / redis,
# on 1 and 2 gateway nodes. Needs Redis on localhost:6379 (docker compose up -d redis).
#
#   experiments/02-coalescing/run.sh [bin dir with gateway and mockllm]
#
# Rate limits are set very high so every request is a cache result, not a 429.
#
# Deployed (AWS): set TARGET to one URL (the load balancer) or a comma
# separated list of gateway URLs, and MODE to the CACHE_COALESCE the
# deployment runs with. Nothing is started locally:
#   TARGET=http://<alb> MODE=redis NODES=4 experiments/02-coalescing/run.sh
set -euo pipefail
cd "$(dirname "$0")/../.."
OUT=experiments/02-coalescing/results
if [[ -n "${TARGET:-}" ]]; then
  mode=${MODE:?set MODE to the deployed CACHE_COALESCE}
  mkdir -p "$OUT"
  for n in 10 50 200; do
    f="$OUT/aws_${mode}_nodes${NODES:-x}_n${n}.json"
    python3 experiments/02-coalescing/burst.py --n "$n" --host "$TARGET" --out "$f" >/dev/null
    python3 -c "import json;d=json.load(open('$f'));print('$mode n=$n provider calls', d['provider_calls'], d['outcomes'], d['latency_ms'])"
  done
  exit 0
fi
BIN=${1:-experiments/.bin}
[ -x "$BIN/gateway" ] || go build -o "$BIN/" ./cmd/gateway ./cmd/mockllm
LOGS=$(mktemp -d)
mkdir -p "$OUT"

PORT=19000 RATE_LIMIT_RPS=100000 "$BIN/mockllm" >"$LOGS/mock.log" 2>&1 &
MOCK=$!
trap 'kill $MOCK 2>/dev/null || true' EXIT

run() { # mode nodes
  local mode=$1 nodes=$2 pids=() hosts=()
  for i in $(seq "$nodes"); do
    local port=$((18080 + i))
    PORT=$port PROVIDER_URL=http://localhost:19000 REDIS_ADDR=localhost:6379 \
      RATE_LIMIT_PER_MIN=1000000 RATE_LIMIT_BURST=1000000 CACHE_COALESCE=$mode \
      "$BIN/gateway" >"$LOGS/gw-$mode-$nodes-$i.log" 2>&1 &
    pids+=($!)
    hosts+=("http://localhost:$port")
  done
  sleep 1
  grep -h '"msg":"coalescing"' "$LOGS"/gw-$mode-$nodes-*.log | head -1
  local joined
  joined=$(IFS=,; echo "${hosts[*]}")
  for n in 10 50 200; do
    python3 experiments/02-coalescing/burst.py --n "$n" --host "$joined" \
      --out "$OUT/${mode}_nodes${nodes}_n${n}.json" >/dev/null
    python3 -c "import json;d=json.load(open('$OUT/${mode}_nodes${nodes}_n${n}.json'));print('$mode nodes=$nodes n=$n provider calls', d['provider_calls'], d['outcomes'], d['latency_ms'])"
  done
  kill "${pids[@]}"
  wait "${pids[@]}" 2>/dev/null || true
}

for nodes in 1 2; do
  for mode in off local redis; do run "$mode" "$nodes"; done
done
