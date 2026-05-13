"""Generate Part 3 Question 1(a) plots.

Three-panel layout per run:
  - top: memcached p95 latency per mcperf sample (bars whose [left, width]
    come straight from the augmented mcperf ``ts_start``/``ts_end`` columns,
    in milliseconds). The 1 ms SLO line and the in-window mean p95 are
    drawn for reference.
  - middle: per-core occupancy on ``node-a-8core`` (8 cores).
  - bottom: per-core occupancy on ``node-b-4core`` (4 cores); core 0 hosts
    memcached for the entire window.

Vertical reference lines at every batch-job start/end are drawn across all
three panels so the latency samples can be aligned with scheduling events.
``x = 0`` is the start time of the first batch container.

Job timings are read from ``results.json`` (the canonical kubectl output that
the grader receives), not from the locally-derived ``summary.json``.

Outputs ``cl_part3_q1a_run{1,2,3}.png`` next to this script.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

import matplotlib.patches as mpatches
import matplotlib.pyplot as plt


RUNS_DIR = Path(
    r"C:/Users/User/Desktop/ETH/MSc/1 Semester/CCA/risultatiPart3_windows/Matte/automation/runs/part3-PartA"
)
OUT_DIR = Path(__file__).resolve().parent

RUN_IDS = [
    "2026-05-10-02h28m11s",
    "2026-05-10-02h34m13s",
    "2026-05-10-02h40m25s",
]

JOB_ORDER = [
    "barnes",
    "blackscholes",
    "canneal",
    "freqmine",
    "radix",
    "streamcluster",
    "vips",
]

# Matches \definecolor entries in main.tex.
JOB_COLOR = {
    "barnes": "#AACCCA",
    "blackscholes": "#CCA000",
    "canneal": "#CCCCAA",
    "freqmine": "#0CCA00",
    "radix": "#00CCA0",
    "streamcluster": "#CCACCA",
    "vips": "#CC0A00",
}
MEMCACHED_COLOR = "#8f8f8f"

# From aFFinalscheduleParta bis.yaml.
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


def parse_iso_seconds(value: str) -> float:
    if value.endswith("Z"):
        value = value[:-1] + "+00:00"
    return datetime.fromisoformat(value).replace(tzinfo=timezone.utc).timestamp()


def load_jobs_from_results(run_id: str) -> dict[str, dict[str, object]]:
    """Return per-job container start/finish timestamps from results.json."""
    path = RUNS_DIR / run_id / "results.json"
    payload = json.loads(path.read_text(encoding="utf-8"))
    jobs: dict[str, dict[str, object]] = {}
    for item in payload["items"]:
        labels = item.get("metadata", {}).get("labels", {})
        job = labels.get("cca-project-job-id")
        if job not in JOB_COLOR:
            continue
        status = item.get("status", {})
        container = (status.get("containerStatuses") or [{}])[0]
        terminated = container.get("state", {}).get("terminated", {})
        jobs[job] = {
            "started_at": terminated["startedAt"],
            "finished_at": terminated["finishedAt"],
            "node_name": item.get("spec", {}).get("nodeName", ""),
        }
    missing = sorted(set(JOB_COLOR) - set(jobs))
    if missing:
        raise ValueError(f"{run_id}: missing jobs in results.json: {missing}")
    return jobs


def load_mcperf_samples(run_id: str) -> list[dict[str, float]]:
    path = RUNS_DIR / run_id / "mcperf.txt"
    lines = path.read_text(encoding="utf-8").splitlines()
    header = lines[0].split()
    index_of = {name: i for i, name in enumerate(header)}
    samples = []
    for line in lines[1:]:
        columns = line.split()
        if not columns:
            continue
        samples.append(
            {
                "p95_ms": float(columns[index_of["p95"]]) / 1000.0,
                "ts_start_s": float(columns[index_of["ts_start"]]) / 1000.0,
                "ts_end_s": float(columns[index_of["ts_end"]]) / 1000.0,
            }
        )
    return samples


def text_color_for(background: str) -> str:
    r = int(background[1:3], 16) / 255.0
    g = int(background[3:5], 16) / 255.0
    b = int(background[5:7], 16) / 255.0
    luminance = 0.2126 * r + 0.7152 * g + 0.0722 * b
    return "black" if luminance > 0.52 else "white"


def configure_time_axis(ax, makespan: float) -> None:
    ticks = [0, 60, 120, 180]
    if makespan > 220:
        ticks.append(round(makespan))
    ax.set_xlim(0, makespan)
    ax.set_xticks(ticks)
    ax.grid(True, axis="x", color="#d0d0d0", linewidth=0.7)


def draw_latency_axis(
    ax, samples: list[dict[str, float]], t0: float, t1: float,
) -> tuple[int, int, float]:
    violations = 0
    in_window = 0
    p95_sum = 0.0
    for sample in samples:
        start = max(sample["ts_start_s"], t0)
        end = min(sample["ts_end_s"], t1)
        if end <= start:
            continue
        in_window += 1
        p95_sum += sample["p95_ms"]
        if sample["p95_ms"] > 1.0:
            violations += 1
        ax.bar(
            start - t0,
            sample["p95_ms"],
            width=end - start,
            align="edge",
            color="#cfcfcf",
            edgecolor="#969696",
            linewidth=0.8,
        )
    mean_p95 = p95_sum / in_window if in_window else 0.0
    ax.axhline(1.0, color="#d62728", linestyle=(0, (5, 5)), linewidth=1.2)
    ax.text(t1 - t0, 1.02, "1 ms", ha="right", va="bottom", color="#d62728")
    ax.axhline(mean_p95, color="#1f6b3a", linestyle=(0, (2, 3)), linewidth=1.0)
    ax.text(
        t1 - t0,
        mean_p95 + 0.015,
        f"mean p95 = {mean_p95 * 1000:.0f} µs",
        ha="right", va="bottom", color="#1f6b3a", fontsize=8,
    )
    ax.set_ylim(0, 1.12)
    ax.set_yticks([0.0, 0.25, 0.5, 0.75, 1.0])
    ax.set_ylabel("p95 [ms]")
    ax.grid(True, axis="y", color="#e4e4e4", linewidth=0.7)
    return violations, in_window, mean_p95


def draw_node_axis(
    ax,
    *,
    node: str,
    jobs: dict[str, dict[str, object]],
    t0: float,
    t1: float,
) -> None:
    cores_desc = sorted(NODE_CORES[node], reverse=True)
    y_by_core = {core: index for index, core in enumerate(cores_desc)}
    bar_height = 0.78

    if node == "node-b-4core":
        y = y_by_core[0]
        ax.barh(
            y, t1 - t0, left=0, height=bar_height,
            color=MEMCACHED_COLOR, edgecolor="black", linewidth=0.7,
        )
        ax.text(
            (t1 - t0) / 2, y, "memcached",
            ha="center", va="center",
            color=text_color_for(MEMCACHED_COLOR),
            fontsize=9, fontweight="bold",
        )

    for job in JOB_ORDER:
        node_for_job, cores = JOB_CORES[job]
        if node_for_job != node:
            continue
        info = jobs[job]
        start = parse_iso_seconds(str(info["started_at"])) - t0
        finish = parse_iso_seconds(str(info["finished_at"])) - t0
        for core in cores:
            ax.barh(
                y_by_core[core], finish - start, left=start, height=bar_height,
                color=JOB_COLOR[job], edgecolor="black", linewidth=0.7,
            )
        core_list = list(cores)
        y_center = (y_by_core[core_list[0]] + y_by_core[core_list[-1]]) / 2.0
        x_center = (start + finish) / 2.0
        width = finish - start
        rotation = 90 if width < 28 else 0
        label_size = 8 if width < 28 else 9
        ax.text(
            x_center, y_center, job,
            ha="center", va="center", rotation=rotation,
            color=text_color_for(JOB_COLOR[job]),
            fontsize=label_size, fontweight="bold",
        )

    prefix = "a" if node == "node-a-8core" else "b"
    ax.set_yticks(range(len(cores_desc)))
    ax.set_yticklabels([f"{prefix}{core}" for core in cores_desc])
    ax.invert_yaxis()
    ax.set_title(node, loc="left", pad=4, fontweight="bold")
    ax.set_ylim(len(cores_desc) - 0.45, -0.55)
    ax.grid(True, axis="x", color="#d0d0d0", linewidth=0.7)


def plot_run(run_id: str, index: int) -> None:
    jobs = load_jobs_from_results(run_id)
    samples = load_mcperf_samples(run_id)

    start_times = [parse_iso_seconds(str(info["started_at"])) for info in jobs.values()]
    finish_times = [parse_iso_seconds(str(info["finished_at"])) for info in jobs.values()]
    t0 = min(start_times)
    t1 = max(finish_times)
    makespan = t1 - t0

    plt.rcParams.update(
        {
            "font.size": 10,
            "axes.labelsize": 10,
            "axes.titlesize": 11,
            "xtick.labelsize": 9,
            "ytick.labelsize": 9,
        }
    )
    fig = plt.figure(figsize=(11.2, 7.2))
    grid = fig.add_gridspec(3, 1, height_ratios=[1.35, 2.15, 1.25], hspace=0.18)
    latency_ax = fig.add_subplot(grid[0])
    node_a_ax = fig.add_subplot(grid[1], sharex=latency_ax)
    node_b_ax = fig.add_subplot(grid[2], sharex=latency_ax)

    violations, in_window, _mean_p95 = draw_latency_axis(latency_ax, samples, t0, t1)
    draw_node_axis(node_a_ax, node="node-a-8core", jobs=jobs, t0=t0, t1=t1)
    draw_node_axis(node_b_ax, node="node-b-4core", jobs=jobs, t0=t0, t1=t1)

    transitions = set()
    for info in jobs.values():
        transitions.add(parse_iso_seconds(str(info["started_at"])) - t0)
        transitions.add(parse_iso_seconds(str(info["finished_at"])) - t0)
    for axis in (latency_ax, node_a_ax, node_b_ax):
        for t in transitions:
            axis.axvline(t, color="#3a3a3a", linewidth=0.4, alpha=0.25, zorder=0)

    for axis in (latency_ax, node_a_ax, node_b_ax):
        configure_time_axis(axis, makespan)
    plt.setp(latency_ax.get_xticklabels(), visible=False)
    plt.setp(node_a_ax.get_xticklabels(), visible=False)
    node_b_ax.set_xlabel("time since first batch container start [s]")

    max_p95 = max(sample["p95_ms"] for sample in samples)
    latency_ax.set_title(
        f"Run {index}: {run_id}, makespan {makespan:.0f} s, "
        f"max p95 {max_p95:.3f} ms, SLO violations {violations}/{in_window}"
    )

    legend_handles = [mpatches.Patch(color=JOB_COLOR[job], label=job) for job in JOB_ORDER]
    legend_handles.append(mpatches.Patch(color=MEMCACHED_COLOR, label="memcached"))
    fig.legend(
        handles=legend_handles,
        loc="lower center",
        ncol=4,
        frameon=False,
        bbox_to_anchor=(0.5, 0.01),
        fontsize=9,
    )
    fig.subplots_adjust(left=0.08, right=0.985, top=0.92, bottom=0.13)

    output = OUT_DIR / f"cl_part3_q1a_run{index}.png"
    fig.savefig(output, dpi=220)
    plt.close(fig)
    print(f"wrote {output}")


def main() -> None:
    for index, run_id in enumerate(RUN_IDS, start=1):
        plot_run(run_id, index)


if __name__ == "__main__":
    main()
