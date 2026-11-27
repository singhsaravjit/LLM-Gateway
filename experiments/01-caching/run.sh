#!/usr/bin/env bash
# Experiment 1 (caching), one local run.
#
#   experiments/01-caching/run.sh <name> [KEY=VALUE ...]
#
# KEY=VALUE pairs are passed to the gateway and to Locust, e.g.
#   run.sh cache_off NO_CACHE=1
#   run.sh cache_on_ttl300 CACHE_TTL_SECONDS=300
#   run.sh worst_unique UNIQUE_PROMPTS=1
#
# Local (default): each run starts from an empty Redis and a fresh gateway
# (metrics at zero). Needs Docker (for Redis), Go, Locust. Ports 6379, 8080,
# 9000 must be free.
#
# Deployed (AWS): set TARGET to the load balancer URL. Nothing is started
# locally and KEY=VALUE pairs only reach Locust; gateway settings must be set
# in the deployment (they are recorded in config.txt as written). Empty Redis
# and restart the gateways before each run so counters start at zero. Set
# METRICS_URLS to every gateway's /metrics (comma separated), since the load
# balancer sends each scrape to one node:
#   TARGET=http://<alb> METRICS_URLS=http://10.0.1.5:8080/metrics,... run.sh ttl300
#
# Override USERS, SPAWN, DURATION, GATEWAY_BIN, MOCK_BIN from the environment.
set -euo pipefail

name=${1:?usage: run.sh <name> [KEY=VALUE ...]}
shift
here=$(cd "$(dirname "$0")" && pwd)
repo=$(cd "$here/../.." && pwd)
out="$here/results/$name"
mkdir -p "$out"

USERS=${USERS:-20}
SPAWN=${SPAWN:-5}
DURATION=${DURATION:-2m}

if [[ -n "${TARGET:-}" ]]; then
  echo "== $name: $USERS users, $DURATION against $TARGET, Locust settings: ${*:-none}"
  printf '%s\n' "name=$name" "target=$TARGET" "users=$USERS" "spawn=$SPAWN" "duration=$DURATION" "$@" >"$out/config.txt"
  env "$@" locust -f "$repo/loadtest/locustfile.py" --host "$TARGET" \
    --headless -u "$USERS" -r "$SPAWN" -t "$DURATION" --only-summary \
    --csv "$out/locust" >"$out/locust.log" 2>&1 || true
  i=0
  IFS=, read -ra urls <<<"${METRICS_URLS:-$TARGET/metrics}"
  for u in "${urls[@]}"; do curl -s "$u" >"$out/metrics-$i.txt"; i=$((i + 1)); done
  python3 "$here/analyze.py" "$out"
  exit 0
fi

# Fixed for every run: realistic provider latency, no rate limits, no queue
# pacing, so the only thing that changes between runs is the cache setting.
fixed=(
  LATENCY_MS_MEAN=800 LATENCY_MS_JITTER=300 RATE_LIMIT_RPS=0
  RATE_LIMIT_PER_MIN=1000000 RATE_LIMIT_BURST=100000 QUEUE_ENABLED=false
  REDIS_ADDR=localhost:6379 PROVIDER_URL=http://localhost:9000
)

bin="$repo/experiments/.bin"
mkdir -p "$bin"
if [[ -z "${GATEWAY_BIN:-}" ]]; then (cd "$repo" && go build -o "$bin/gateway" ./cmd/gateway); GATEWAY_BIN="$bin/gateway"; fi
if [[ -z "${MOCK_BIN:-}" ]]; then (cd "$repo" && go build -o "$bin/mockllm" ./cmd/mockllm); MOCK_BIN="$bin/mockllm"; fi

redis=llmgw-exp1-redis
cleanup() {
  [[ -n "${gw:-}" ]] && kill "$gw" 2>/dev/null || true
  [[ -n "${mock:-}" ]] && kill "$mock" 2>/dev/null || true
  wait 2>/dev/null || true
  docker rm -f "$redis" >/dev/null 2>&1 || true
}
trap cleanup EXIT

docker rm -f "$redis" >/dev/null 2>&1 || true
docker run -d --rm --name "$redis" -p 6379:6379 redis:7-alpine >/dev/null
until docker exec "$redis" redis-cli ping >/dev/null 2>&1; do sleep 0.2; done

env "${fixed[@]}" "$@" PORT=9000 "$MOCK_BIN" >"$out/mock.log" 2>&1 &
mock=$!
env "${fixed[@]}" "$@" PORT=8080 "$GATEWAY_BIN" >"$out/gateway.log" 2>&1 &
gw=$!
until curl -sf localhost:8080/healthz >/dev/null && curl -sf localhost:9000/healthz >/dev/null; do sleep 0.2; done

echo "== $name: $USERS users, $DURATION, settings: ${*:-none}"
printf '%s\n' "name=$name" "users=$USERS" "spawn=$SPAWN" "duration=$DURATION" "${fixed[@]}" "$@" >"$out/config.txt"

env "$@" locust -f "$repo/loadtest/locustfile.py" --host http://localhost:8080 \
  --headless -u "$USERS" -r "$SPAWN" -t "$DURATION" --only-summary \
  --csv "$out/locust" >"$out/locust.log" 2>&1 || true

curl -s localhost:8080/metrics >"$out/metrics.txt"
docker exec "$redis" redis-cli info memory | grep -E '^used_memory_human' >"$out/redis_memory.txt" || true
docker exec "$redis" redis-cli dbsize >>"$out/redis_memory.txt" || true

python3 "$here/analyze.py" "$out"
