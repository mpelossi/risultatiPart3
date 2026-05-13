"""Generate the three bar plots for Part 3, Question 1(a).

For each run we plot:
  - top panel: memcached 95th percentile latency (ms) per mcperf sample
    drawn as bars whose width matches the [ts_start, ts_end] window
  - bottom panel: per-core occupancy on the two heterogeneous nodes,
    coloured by the PARSEC job running on that core
  - x = 0 corresponds to the start time of the first batch container

Run from this directory:
    python make_part3_plots.py
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

import matplotlib.patches as mpatches
import matplotlib.pyplot as plt

RUNS_DIR = Path(
    r"c:/Users/User/Desktop/ETH/MSc/1 Semester/CCA/risultatiPart3_windows/Matte/automation/runs/part3-PartA"
)
OUT_DIR = Path(__file__).parent

RUN_IDS = [
    "2026-05-10-02h28m11s",  # run 1
    "2026-05-10-02h34m13s",  # run 2
    "2026-05-10-02h40m25s",  # run 3
]

# matches \definecolor in main.tex (HTML triplets)
JOB_COLOR = {
    "barnes": "#AACCCA",
    "blackscholes": "#CCA000",
    "canneal": "#CCCCAA",
    "freqmine": "#0CCA00",
    "radix": "#00CCA0",
    "streamcluster": "#CCACCA",
    "vips": "#CC0A00",
}
MEMCACHED_COLOR = "#888888"

# from aFFinalscheduleParta bis.yaml
JOB_CORES = {
    "streamcluster": ("node-a-8core", range(0, 8)),
    "freqmine":      ("node-a-8core", range(0, 8)),
    "vips":          ("node-a-8core", range(0, 6)),
    "radix":         ("node-a-8core", range(6, 8)),
    "blackscholes":  ("node-b-4core", range(1, 4)),
    "canneal":       ("node-b-4core", range(1, 4)),
    "barnes":        ("node-b-4core", range(1, 4)),
}
NODE_CORES = {
    "node-a-8core": list(range(0, 8)),
    "node-b-4core": list(range(0, 4)),
}


def parse_iso(ts: str) -> float:
    if ts.endswith("Z"):
        ts = ts[:-1] + "+00:00"
    return datetime.fromisoformat(ts).replace(tzinfo=timezone.utc).timestamp()


def load_summary(run_id: str):
    with (RUNS_DIR / run_id / "summary.json").open() as f:
        return json.load(f)


def load_mcperf(run_id: str):
    rows = []
    path = RUNS_DIR / run_id / "mcperf.txt"
    with path.open() as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            tok = line.split()
            # columns: type avg std min p5 p10 p50 p67 p75 p80 p85 p90 p95 p99 p999 p9999 QPS target ts_start ts_end
            p95_us = float(tok[12])
            ts_start_ms = int(tok[18])
            ts_end_ms = int(tok[19])
            rows.append((p95_us, ts_start_ms / 1000.0, ts_end_ms / 1000.0))
    return rows


def plot_run(run_id: str, idx: int) -> None:
    summary = load_summary(run_id)
    jobs = summary["jobs"]
    samples = load_mcperf(run_id)

    # Reference time: start of the first batch container.
    t0 = min(parse_iso(j["started_at"]) for j in jobs.values())
    t_last_end = max(parse_iso(j["finished_at"]) for j in jobs.values())
    makespan = t_last_end - t0

    fig, (ax_top, ax_bot) = plt.subplots(
        2, 1, figsize=(10, 5.5), sharex=True,
        gridspec_kw={"height_ratios": [2.0, 1.4]},
    )

    # ---- top panel: p95 latency bars ----
    for p95_us, s, e in samples:
        x0 = s - t0
        w = e - s
        # p95 in ms (mcperf reports microseconds)
        ax_top.bar(x0, p95_us / 1000.0, width=w, align="edge",
                   color="#3a6ea5", edgecolor="black", linewidth=0.3)
    ax_top.axhline(1.0, color="red", linestyle="--", linewidth=1.0,
                   label="SLO: 1 ms")
    ax_top.set_ylabel("p95 latency [ms]")
    ax_top.set_ylim(0, max(1.1, max(p / 1000.0 for p, _, _ in samples) * 1.25))
    ax_top.set_title(f"Run {idx}: makespan = {makespan:.0f} s, "
                     f"max p95 = {summary['max_observed_p95_us']:.1f} us, "
                     f"SLO violations = {summary['slo_violations']}/{summary['sample_count']}")
    ax_top.legend(loc="upper right", fontsize=8)
    ax_top.grid(True, axis="y", alpha=0.3)

    # ---- bottom panel: per-core timeline ----
    # Row layout: node-a cores 0..7 then node-b cores 0..3 (top to bottom).
    rows = []
    for c in NODE_CORES["node-a-8core"]:
        rows.append(("node-a-8core", c, f"A-c{c}"))
    for c in NODE_CORES["node-b-4core"]:
        rows.append(("node-b-4core", c, f"B-c{c}"))

    def row_y(node: str, core: int) -> int:
        return next(i for i, (n, c, _) in enumerate(rows) if n == node and c == core)

    # memcached occupies core 0 of node-b for the whole window.
    ax_bot.barh(row_y("node-b-4core", 0), makespan, left=0, height=0.8,
                color=MEMCACHED_COLOR, edgecolor="black", linewidth=0.3,
                label="memcached")

    for name, info in jobs.items():
        s = parse_iso(info["started_at"]) - t0
        e = parse_iso(info["finished_at"]) - t0
        node, cores = JOB_CORES[name]
        for c in cores:
            ax_bot.barh(row_y(node, c), e - s, left=s, height=0.8,
                        color=JOB_COLOR[name], edgecolor="black", linewidth=0.3)

    ax_bot.set_yticks(range(len(rows)))
    ytick_labels = []
    for node, core, _ in rows:
        prefix = "node-a" if node == "node-a-8core" else "node-b"
        ytick_labels.append(f"{prefix}  core {core}")
    ax_bot.set_yticklabels(ytick_labels, fontsize=8)
    ax_bot.invert_yaxis()
    ax_bot.set_xlabel("time since first container start [s]")
    ax_bot.set_xlim(0, makespan * 1.02)
    ax_bot.grid(True, axis="x", alpha=0.3)
    # Visual separator between the two nodes.
    ax_bot.axhline(7.5, color="black", linewidth=0.6)

    handles = [mpatches.Patch(color=JOB_COLOR[j], label=j) for j in JOB_COLOR]
    handles.append(mpatches.Patch(color=MEMCACHED_COLOR, label="memcached"))
    ax_bot.legend(handles=handles, loc="upper right", ncol=4, fontsize=7,
                  framealpha=0.9)

    fig.tight_layout()
    out_path = OUT_DIR / f"part3_q1a_run{idx}.png"
    fig.savefig(out_path, dpi=160)
    plt.close(fig)
    print(f"wrote {out_path}")


if __name__ == "__main__":
    for i, run_id in enumerate(RUN_IDS, start=1):
        plot_run(run_id, i)
