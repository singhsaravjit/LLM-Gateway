#!/usr/bin/env python3
"""Charts for Experiments 3, 4 and 5 from the run scripts' CSVs.

    pip install matplotlib
    python3 experiments/tools/charts.py            # all experiments with results
    python3 experiments/tools/charts.py 3 5        # only some

Static PNGs for the report. Colors: validated categorical slots 1-3
(blue, orange, aqua); every series is also direct-labeled and uses its own
marker/line style, so identity never depends on color alone. Numbers behind
each chart are in the experiment's summary.csv and RESULTS.md tables.
"""
import csv
import os
import statistics
import sys
from collections import defaultdict

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
BLUE, ORANGE, AQUA = "#257de0", "#f47230", "#00b894"
TEXT, TEXT2, GRID = "#191919", "#666666", "#e7e7e7"

plt.rcParams.update({
    "figure.dpi": 150, "font.size": 9, "axes.edgecolor": GRID, "axes.labelcolor": TEXT2,
    "xtick.color": TEXT2, "ytick.color": TEXT2, "axes.grid": True, "grid.color": GRID,
    "grid.linewidth": 0.6, "axes.spines.top": False, "axes.spines.right": False,
    "axes.titlesize": 10, "axes.titleweight": "bold", "axes.titlecolor": TEXT, "lines.linewidth": 2,
})


def read(path):
    with open(path) as f:
        return list(csv.DictReader(f))


def label_end(ax, x, y, text, color_dummy=None, dy=0):
    """Direct label at a line's last point, in text ink (never the series color)."""
    ax.annotate(text, (x, y), xytext=(6, dy), textcoords="offset points", va="center", color=TEXT, fontsize=8)


def save(fig, path):
    fig.tight_layout()
    fig.savefig(path, facecolor="white")
    plt.close(fig)
    print("wrote", os.path.relpath(path, ROOT))


def exp3():
    d = os.path.join(ROOT, "experiments/03-ratelimit/results")
    rows = read(os.path.join(d, "summary.csv"))
    by = defaultdict(lambda: defaultdict(list))
    for r in rows:
        by[r["backend"]][int(r["nodes"])].append(r)

    fig, ax = plt.subplots(figsize=(6, 3.4))
    styles = {"local": (ORANGE, "o", "-", "Per-node counters"), "redis": (BLUE, "s", "-", "Shared Redis counter")}
    for backend, (color, marker, ls, name) in styles.items():
        ns = sorted(by[backend])
        means = [statistics.mean(float(r["over_admission_pct"]) for r in by[backend][n]) for n in ns]
        lo = [min(float(r["over_admission_pct"]) for r in by[backend][n]) for n in ns]
        hi = [max(float(r["over_admission_pct"]) for r in by[backend][n]) for n in ns]
        ax.errorbar(ns, means, yerr=[[m - l for m, l in zip(means, lo)], [h - m for m, h in zip(means, hi)]],
                    color=color, marker=marker, markersize=6, linestyle=ls, capsize=3, elinewidth=1)
        label_end(ax, ns[-1], means[-1], f"{name}  {means[-1]:+.0f}%")
    ax.axhline(0, color=TEXT2, linewidth=0.8)
    ax.set_xscale("log", base=2)
    ax.set_xticks([1, 2, 4, 8], ["1", "2", "4", "8"])
    ax.set_xlabel("Gateway nodes")
    ax.set_ylabel("Requests admitted above the limit (%)")
    ax.set_title("Exp 3 · Over-admission vs node count (1 key, 10 rps limit, 100 rps offered)", loc="left")
    ax.set_xlim(0.8, 22)
    save(fig, os.path.join(d, "over_admission.png"))

    fig, ax = plt.subplots(figsize=(6, 3.0))
    for backend, (color, marker, ls, name) in styles.items():
        ns = sorted(by[backend])
        us = [statistics.mean(float(r["limiter_check_us"]) for r in by[backend][n]) for n in ns]
        ax.plot(ns, us, color=color, marker=marker, markersize=6, linestyle=ls)
        label_end(ax, ns[-1], us[-1], f"{name}  {us[-1]:.0f} µs")
    ax.set_xscale("log", base=2)
    ax.set_xticks([1, 2, 4, 8], ["1", "2", "4", "8"])
    ax.set_xlim(0.8, 22)
    ax.set_ylim(bottom=0)
    ax.set_xlabel("Gateway nodes")
    ax.set_ylabel("Mean limiter check (µs)")
    ax.set_title("Exp 3 · Cost of the check: one Redis round trip per request", loc="left")
    save(fig, os.path.join(d, "limiter_latency.png"))


def per_second(rows, length_s):
    ok, r429, r503, r504, other = ([0] * length_s for _ in range(5))
    for r in rows:
        t = int(r["start_ms"]) // 1000
        if t >= length_s:
            continue
        s = r["status"]
        {"200": ok, "429": r429, "503": r503, "504": r504}.get(s, other)[t] += 1
    return ok, r429, r503, r504, other


def exp4():
    d = os.path.join(ROOT, "experiments/04-redis-failure/results")
    if not os.path.exists(os.path.join(d, "summary.csv")):
        return
    runs = [("down", "open"), ("down", "closed"), ("hang", "open"), ("hang", "closed")]
    fig, axes = plt.subplots(2, 2, figsize=(9, 5.2), sharex=True, sharey=True)
    for ax, (kind, mode) in zip(axes.flat, runs):
        path = os.path.join(d, "raw", f"{kind}-{mode}.csv")
        if not os.path.exists(path):
            ax.set_visible(False)
            continue
        ok, r429, r503, _, other = per_second(read(path), 40)
        t = list(range(40))
        ax.axvspan(10, 25, color=GRID, alpha=0.7, linewidth=0)
        ax.text(17.5, 0.97, "Redis down" if kind == "down" else "Redis hung", transform=ax.get_xaxis_transform(),
                ha="center", va="top", color=TEXT2, fontsize=8)
        ax.plot(t, ok, color=BLUE, label="Admitted (200)")
        ax.plot(t, r429, color=ORANGE, linestyle="--", label="Rate limited (429)")
        ax.plot(t, r503, color=AQUA, linestyle=":", label="Limiter unavailable (503)")
        ax.axhline(10, color=TEXT2, linewidth=0.8, linestyle=(0, (1, 2)))
        ax.set_title(f"{'Down' if kind == 'down' else 'Hang'} · fail {mode}", loc="left")
    for ax in axes[1]:
        ax.set_xlabel("Seconds")
    for ax in axes[:, 0]:
        ax.set_ylabel("Requests per second")
    axes[0][0].annotate("limit = 10 rps", (39, 10), xytext=(0, 4), textcoords="offset points", ha="right",
                        color=TEXT2, fontsize=7)
    handles, labels = axes[0][0].get_legend_handles_labels()
    fig.legend(handles, labels, loc="lower center", ncol=3, frameon=False, fontsize=8, bbox_to_anchor=(0.5, -0.01))
    fig.suptitle("Exp 4 · Redis failure: fail open vs fail closed (50 rps offered, 2 nodes)", x=0.01, ha="left",
                 fontweight="bold", fontsize=10, color=TEXT)
    fig.tight_layout(rect=(0, 0.05, 1, 1))
    fig.savefig(os.path.join(d, "timeline.png"), facecolor="white")
    plt.close(fig)
    print("wrote", os.path.relpath(os.path.join(d, "timeline.png"), ROOT))


def exp5():
    d = os.path.join(ROOT, "experiments/05-queueing/results")
    rows = read(os.path.join(d, "summary.csv"))
    steady = [r for r in rows if not r["run"].endswith("burst")]
    styles = {"off": (ORANGE, "o", "--", "Queue off"), "on1000": (BLUE, "s", "-", "Queue on (max 1000)"),
              "on100": (AQUA, "^", ":", "Queue on (max 100)")}

    def offered(r):
        return float(r["shape"].split(":")[0]) / 50  # x provider limit

    for metric, ylabel, fname, title, fmt in [
        ("success_pct", "Successful requests (%)", "success_rate.png", "Exp 5 · Success rate vs offered load",
         "{:.0f}%"),
        ("p99_ms", "p99 latency of successful requests (s)", "p99.png", "Exp 5 · Tail latency vs offered load",
         "{:.1f} s"),
    ]:
        fig, ax = plt.subplots(figsize=(6, 3.4))
        for cfg, (color, marker, ls, name) in styles.items():
            pts = sorted((offered(r), float(r[metric])) for r in steady if r["config"] == cfg)
            if not pts:
                continue
            xs = [p[0] for p in pts]
            ys = [p[1] / (1000 if metric == "p99_ms" else 1) for p in pts]
            ax.plot(xs, ys, color=color, marker=marker, markersize=6, linestyle=ls)
            label_end(ax, xs[-1], ys[-1], f"{name}  " + fmt.format(ys[-1]),
                      dy={"off": -8, "on1000": 8, "on100": 0}[cfg] if metric == "success_pct" else 0)
        ax.axvline(1, color=TEXT2, linewidth=0.8, linestyle=(0, (1, 2)))
        ax.text(1.02, 0.03, "provider limit", transform=ax.get_xaxis_transform(), color=TEXT2, fontsize=7)
        ax.set_xlabel("Offered load (× provider limit of 50 rps)")
        ax.set_ylabel(ylabel)
        ax.set_xlim(0.4, 4.6)
        ax.set_ylim(bottom=0)
        ax.set_title(title + " (provider: 50 rps, 800 ms)", loc="left")
        save(fig, os.path.join(d, fname))

    # Burst: per-second outcomes, queue off vs on, plus queue depth.
    fig, axes = plt.subplots(3, 1, figsize=(7, 6.2), sharex=True)
    for ax, cfg, name in [(axes[0], "off", "Queue off"), (axes[1], "on1000", "Queue on (max 1000)")]:
        path = os.path.join(d, "raw", f"{cfg}-burst.csv")
        if not os.path.exists(path):
            continue
        ok, r429, r503, r504, other = per_second(read(path), 60)
        t = list(range(60))
        ax.axvspan(0, 20, color=GRID, alpha=0.7, linewidth=0)
        ax.plot(t, ok, color=BLUE, label="Succeeded")
        ax.plot(t, [a + b + c for a, b, c in zip(r429, r503, r504)], color=ORANGE, linestyle="--",
                label="Failed (429/503/504)")
        ax.set_title(f"{name}: requests by send time", loc="left")
        ax.set_ylabel("Requests / s")
    axes[0].text(10, 0.97, "burst: 150 rps (3×)", transform=axes[0].get_xaxis_transform(), ha="center", va="top",
                 color=TEXT2, fontsize=8)
    axes[0].legend(frameon=False, fontsize=8, loc="center right")
    dp = os.path.join(d, "timeline", "on1000-burst-depth.csv")
    if os.path.exists(dp):
        dep = read(dp)
        xs = [int(r["t_s"]) for r in dep]
        ys = [int(r["queue_depth"]) for r in dep]
        axes[2].axvspan(0, 20, color=GRID, alpha=0.7, linewidth=0)
        axes[2].plot(xs, ys, color=BLUE)
        peak = max(range(len(ys)), key=lambda i: ys[i])
        axes[2].annotate(f"peak {ys[peak]}", (xs[peak], ys[peak]), xytext=(6, 0), textcoords="offset points",
                         color=TEXT, fontsize=8, va="center")
        axes[2].set_title("Queue on: queue depth (all nodes)", loc="left")
        axes[2].set_ylabel("Jobs waiting")
    axes[2].set_xlabel("Seconds since start")
    fig.suptitle("Exp 5 · 20 s burst at 3× the provider limit, then 0.5×", x=0.01, ha="left", fontweight="bold",
                 fontsize=10, color=TEXT)
    fig.tight_layout()
    fig.savefig(os.path.join(d, "burst.png"), facecolor="white")
    plt.close(fig)
    print("wrote", os.path.relpath(os.path.join(d, "burst.png"), ROOT))


if __name__ == "__main__":
    which = sys.argv[1:] or ["3", "4", "5"]
    for w in which:
        p = {"3": "03-ratelimit", "4": "04-redis-failure", "5": "05-queueing"}[w]
        if os.path.exists(os.path.join(ROOT, "experiments", p, "results", "summary.csv")):
            {"3": exp3, "4": exp4, "5": exp5}[w]()
