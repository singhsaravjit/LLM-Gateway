# Shared helpers for the experiment run scripts. Source it, don't run it.
#
# Starts real processes (mock LLM, N gateway nodes) against a Redis you provide:
#   REDIS_ADDR   host:port of Redis (default localhost:6379)
#   RESULTS      output directory (set by each run.sh)
# Local only for now; on AWS the same loadgen commands run against the ALB.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
BIN="$ROOT/experiments/.bin"
REDIS_ADDR="${REDIS_ADDR:-localhost:6379}"
REDIS_HOST="${REDIS_ADDR%:*}"
REDIS_PORT="${REDIS_ADDR##*:}"
MOCK_PORT=9100
GW_BASE_PORT=8100
PIDS=()

build() {
  mkdir -p "$BIN"
  (cd "$ROOT" && go build -o "$BIN/" ./cmd/gateway ./cmd/mockllm ./experiments/tools/loadgen)
}

rcli() { redis-cli -h "$REDIS_HOST" -p "$REDIS_PORT" "$@"; }

wait_http() { curl -s --retry 50 --retry-connrefused --retry-delay 0 --retry-max-time 15 -o /dev/null "$1"; }

# start_mock KEY=VALUE...   (env for cmd/mockllm)
start_mock() {
  env PORT=$MOCK_PORT "$@" "$BIN/mockllm" >"$RESULTS/logs/mock.log" 2>&1 &
  PIDS+=($!)
  wait_http "http://localhost:$MOCK_PORT/healthz"
}

# start_gateways N KEY=VALUE...   (env for cmd/gateway); sets NODES (comma list)
start_gateways() {
  local n=$1; shift
  NODES=""
  for ((i = 1; i <= n; i++)); do
    local port=$((GW_BASE_PORT + i))
    env PORT=$port NODE_ID="node$i" REDIS_ADDR="$REDIS_ADDR" PROVIDER_URL="http://localhost:$MOCK_PORT" "$@" \
      "$BIN/gateway" >"$RESULTS/logs/gateway$i.log" 2>&1 &
    PIDS+=($!)
    NODES+="${NODES:+,}http://localhost:$port"
  done
  for ((i = 1; i <= n; i++)); do wait_http "http://localhost:$((GW_BASE_PORT + i))/healthz"; done
}

stop_all() {
  for p in "${PIDS[@]:-}"; do [ -n "$p" ] && kill "$p" 2>/dev/null || true; done
  for p in "${PIDS[@]:-}"; do [ -n "$p" ] && wait "$p" 2>/dev/null || true; done
  PIDS=()
}
trap stop_all EXIT

# metric NAME — sum of one metric across all gateway nodes in $NODES (0 if absent)
metric() {
  local total=0 v
  for u in ${NODES//,/ }; do
    v=$(curl -s "$u/metrics" | awk -v k="llmgw_$1" '$1==k {print $2}')
    total=$(awk -v a="$total" -v b="${v:-0}" 'BEGIN{printf "%.9g", a+b}')
  done
  echo "$total"
}

# jq-free JSON field read: json_get '{"a":1}' a
json_get() { python3 -c 'import json,sys; v=json.loads(sys.argv[1]); print(eval("v"+"".join("[%r]"%k for k in sys.argv[2:])))' "$@"; }
