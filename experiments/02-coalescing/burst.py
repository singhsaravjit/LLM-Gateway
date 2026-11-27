"""Experiment 2 (request coalescing): fire N identical requests at the same moment.

    python3 experiments/02-coalescing/burst.py [--n 50] [--host http://localhost:8080]
    python3 experiments/02-coalescing/burst.py --n 50 --host http://localhost:18081,http://localhost:18082

Uses a fresh prompt each run, so the key is cold (nothing cached yet). All N
threads wait on a barrier and send together, each with its own API key (many
users asking the same thing), so per-key rate limits do not reject the burst. Prints how many requests were
HIT vs MISS and their latency. Every MISS is one call to the provider, so
"MISS count" = provider calls for this burst (ideal with coalescing: 1).
With several comma-separated hosts, requests are spread round robin.
Standard library only.
"""
import argparse
import json
import statistics
import threading
import time
import urllib.request
import uuid


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--n", type=int, default=50)
    ap.add_argument("--host", default="http://localhost:8080", help="one URL, or several comma-separated")
    ap.add_argument("--out", help="optional JSON file for the results")
    args = ap.parse_args()

    prompt = f"burst {uuid.uuid4()}: explain request coalescing in one paragraph."
    body = json.dumps({"messages": [{"role": "user", "content": prompt}]}).encode()
    barrier = threading.Barrier(args.n)
    results = [None] * args.n

    hosts = args.host.split(",")

    def send(i):
        req = urllib.request.Request(
            hosts[i % len(hosts)] + "/v1/chat/completions", data=body, method="POST",
            headers={"Authorization": f"Bearer burst-user-{i}", "Content-Type": "application/json"})
        barrier.wait()
        start = time.perf_counter()
        try:
            with urllib.request.urlopen(req, timeout=30) as r:
                results[i] = (r.status, r.headers.get("X-Cache"), (time.perf_counter() - start) * 1000)
        except urllib.error.HTTPError as e:
            results[i] = (e.code, None, (time.perf_counter() - start) * 1000)
        except Exception as e:  # connection errors, timeouts
            results[i] = (0, type(e).__name__, (time.perf_counter() - start) * 1000)

    threads = [threading.Thread(target=send, args=(i,)) for i in range(args.n)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    counts = {}
    for status, cache, _ in results:
        counts[f"{status} {cache}"] = counts.get(f"{status} {cache}", 0) + 1
    lat = sorted(r[2] for r in results)
    misses = sum(1 for r in results if r[1] == "MISS")
    summary = {
        "n": args.n,
        "hosts": len(hosts),
        "outcomes": counts,
        "provider_calls": misses,
        "latency_ms": {"min": round(lat[0]), "p50": round(statistics.median(lat)), "max": round(lat[-1])},
    }
    print(json.dumps(summary, indent=2))
    if args.out:
        with open(args.out, "w") as f:
            json.dump(summary, f, indent=2)
            f.write("\n")


if __name__ == "__main__":
    main()
