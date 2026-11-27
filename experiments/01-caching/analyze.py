"""Summarise one Experiment 1 run.

    python3 experiments/01-caching/analyze.py experiments/01-caching/results/<run>

Reads the gateway's per-request log (one JSON line per request, schema in
docs/api.md), the Locust CSV and /metrics. Writes requests.csv (one row per
request) and summary.json, and prints a short summary.

For a deployed run (no gateway.log, see run.sh TARGET), hit, miss and cost
come from the summed /metrics of every node (metrics-*.txt) instead, and
per-request latency comes from Locust only.

Compare runs on cost per 1,000 requests, not total cost: Locust users send
their next request as soon as the last one returns, so a run with fast cache
hits serves more requests in the same time.
"""
import csv
import json
import statistics
import sys
from pathlib import Path


def pct(values, p):
    if not values:
        return None
    s = sorted(values)
    k = max(0, min(len(s) - 1, round(p / 100 * len(s) + 0.5) - 1))
    return s[k]


def read_metrics(run):
    """Sum every llmgw_ series over metrics.txt and metrics-*.txt."""
    total = {}
    for f in sorted(run.glob("metrics*.txt")):
        for line in f.read_text().splitlines():
            if line.startswith("llmgw_"):
                name, _, v = line.rpartition(" ")
                total[name] = total.get(name, 0.0) + float(v)
    return total


def sum_prefix(m, prefix):
    return sum(v for k, v in m.items() if k.startswith(prefix))


def from_metrics(run):
    m = read_metrics(run)
    hits = sum_prefix(m, 'llmgw_requests_total{result="ok",cache="hit"')
    misses = sum_prefix(m, 'llmgw_requests_total{result="ok",cache="miss"')
    bypass = sum_prefix(m, 'llmgw_requests_total{result="ok",cache="bypass"')
    coalesced = sum_prefix(m, 'llmgw_requests_total{result="ok",cache="coalesced"')
    served = hits + misses + bypass + coalesced
    paid = sum_prefix(m, "llmgw_cost_usd_total")
    lookups = hits + misses + coalesced
    return {
        "source": "metrics (deployed run)",
        "requests_served": served,
        "hits": hits, "misses": misses, "bypass": bypass, "coalesced": coalesced,
        "hit_rate": round(hits / lookups, 4) if lookups else None,
        "provider_calls": misses + bypass,
        "cache_errors": sum_prefix(m, "llmgw_cache_errors_total"),
        "cost_paid_usd": round(paid, 6),
        "cost_saved_usd": round(sum_prefix(m, "llmgw_cost_saved_usd_total"), 6),
        "cost_per_1k_requests_usd": round(paid / served * 1000, 4) if served else None,
    }


def main(run_dir):
    run = Path(run_dir)
    if not (run / "gateway.log").exists():
        summary = from_metrics(run)
        summary["locust"] = locust_stats(run)
        (run / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")
        print(json.dumps(summary, indent=2))
        return
    rows = []
    for line in (run / "gateway.log").read_text().splitlines():
        try:
            e = json.loads(line)
        except json.JSONDecodeError:
            continue
        if e.get("msg") == "request":
            rows.append(e)

    with open(run / "requests.csv", "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["cache", "model", "latency_ms", "tokens", "cost_usd"])
        for e in rows:
            w.writerow([e["cache"], e["model"], e["latency_ms"], e["tokens"], e["cost_usd"]])

    by = {}
    for e in rows:
        by.setdefault(e["cache"], []).append(e)

    def lat(es):
        v = [e["latency_ms"] for e in es]
        return {"p50": pct(v, 50), "p95": pct(v, 95), "p99": pct(v, 99),
                "mean": round(statistics.mean(v), 1) if v else None}

    hits, misses, bypass = (len(by.get(k, [])) for k in ("HIT", "MISS", "BYPASS"))
    paid = [e["cost_usd"] for e in rows if e["cache"] in ("MISS", "BYPASS")]
    cost_paid = sum(paid)
    lookups = hits + misses

    loc = locust_stats(run)
    errors = sum_prefix(read_metrics(run), "llmgw_cache_errors_total")

    summary = {
        "requests_logged": len(rows),
        "hits": hits,
        "misses": misses,
        "bypass": bypass,
        "hit_rate": round(hits / lookups, 4) if lookups else None,
        "provider_calls": misses + bypass,
        "cache_errors": errors,
        "cost_paid_usd": round(cost_paid, 6),
        "cost_per_1k_requests_usd": round(cost_paid / len(rows) * 1000, 4) if rows else None,
        "latency_ms": {k: lat(v) for k, v in by.items()},
        "latency_ms_all": lat(rows),
        "locust": loc,
    }
    (run / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")

    print(f"  requests {len(rows)}  hits {hits}  misses {misses}  bypass {bypass}  "
          f"hit rate {summary['hit_rate']}  provider calls {summary['provider_calls']}  "
          f"cache errors {errors:g}")
    print(f"  cost paid ${cost_paid:.4f}  per 1k requests ${summary['cost_per_1k_requests_usd']}")
    for k, v in summary["latency_ms"].items():
        print(f"  {k:6} latency p50 {v['p50']} ms  p95 {v['p95']} ms  p99 {v['p99']} ms")
    if loc:
        print(f"  locust: {loc['requests']} req, {loc['failures']} failures, {loc['rps']} req/s, "
              f"p50 {loc['p50_ms']:.0f} ms, p95 {loc['p95_ms']:.0f} ms")


def locust_stats(run):
    stats = run / "locust_stats.csv"
    if not stats.exists():
        return {}
    for r in csv.DictReader(stats.open()):
        if r["Name"] == "Aggregated":
            return {
                "requests": int(r["Request Count"]),
                "failures": int(r["Failure Count"]),
                "rps": round(float(r["Requests/s"]), 1),
                "p50_ms": float(r["50%"]),
                "p95_ms": float(r["95%"]),
                "p99_ms": float(r["99%"]),
                "avg_ms": round(float(r["Average Response Time"]), 1),
            }
    return {}


if __name__ == "__main__":
    main(sys.argv[1])
