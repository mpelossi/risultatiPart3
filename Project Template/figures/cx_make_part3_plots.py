"""Generate Part 3 Question 1(a) plots with matplotlib.

Each mcperf row is one latency sample interval. The augmented mcperf columns
ts_start and ts_end are Unix timestamps in milliseconds for the beginning and
end of that sample interval. They define the left edge and width of each p95
latency bar after subtracting the first batch container start time.
Colored vertical markers show batch job starts and finishes using the same job
colors as the placement bars.
"""

from __future__ import annotations

import json
import os
import statistics
import tempfile
from datetime import datetime, timezone
from pathlib import Path

os.environ.setdefault("MPLBACKEND", "Agg")
os.environ.setdefault("MPLCONFIGDIR", str(Path(tempfile.gettempdir()) / "cca-matplotlib"))

import matplotlib.patches as mpatches
import matplotlib.pyplot as plt


RUNS_ROOT = Path(
    r"C:/Users/User/Desktop/ETH/MSc/1 Semester/CCA/risultatiPart3 WIN/Matte/automation/runs"
)
FINAL_RUNS_DIR = RUNS_ROOT / "part3-PartA"
AI_RUNS_DIR = RUNS_ROOT / "part3-PartB"
HANDCRAFTED_RUNS_DIR = RUNS_ROOT / "part3-handcrafted"
OUT_DIR = Path(__file__).resolve().parent / "part3"
OPENEVOLVE_RUN_DIR = Path(
    r"C:/Users/User/Desktop/ETH/MSc/1 Semester/CCA/risultatiPart3 WIN/Matte/part3_openEvolve/runs/run_20260505_154001"
)
OPENEVOLVE_CHECKPOINTS_DIR = OPENEVOLVE_RUN_DIR / "checkpoints"
OPENEVOLVE_EVAL_RUNS_DIR = RUNS_ROOT / "part3-OPENEVOLVE"

FINAL_RUN_IDS = [
    "2026-05-10-02h28m11s",
    "2026-05-10-02h34m13s",
    "2026-05-10-02h40m25s",
]
AGGRESSIVE_RUN_ID = "2026-04-27-07h02m33s"
AI_RUN_IDS = [
    "2026-05-09-15h15m46s",
    "2026-05-09-15h21m20s",
    "2026-05-09-15h26m52s",
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

FINAL_JOB_CORES = {
    "streamcluster": ("node-a-8core", range(0, 8)),
    "freqmine": ("node-a-8core", range(0, 8)),
    "vips": ("node-a-8core", range(0, 6)),
    "radix": ("node-a-8core", range(6, 8)),
    "blackscholes": ("node-b-4core", range(1, 4)),
    "canneal": ("node-b-4core", range(1, 4)),
    "barnes": ("node-b-4core", range(1, 4)),
}

AGGRESSIVE_JOB_CORES = {
    "streamcluster": ("node-a-8core", range(0, 4)),
    "freqmine": ("node-a-8core", range(4, 8)),
    "barnes": ("node-a-8core", range(4, 8)),
    "radix": ("node-a-8core", range(4, 8)),
    "canneal": ("node-b-4core", range(1, 4)),
    "blackscholes": ("node-b-4core", range(1, 4)),
    "vips": ("node-b-4core", range(1, 4)),
}

AI_JOB_CORES = {
    "barnes": ("node-a-8core", range(0, 4)),
    "freqmine": ("node-a-8core", range(0, 4)),
    "streamcluster": ("node-a-8core", range(4, 8)),
    "radix": ("node-a-8core", range(4, 8)),
    "vips": ("node-a-8core", range(0, 8)),
    "canneal": ("node-b-4core", range(1, 4)),
    "blackscholes": ("node-b-4core", range(1, 4)),
}

NODE_CORES = {
    "node-a-8core": list(range(0, 8)),
    "node-b-4core": list(range(0, 4)),
}


def parse_iso_seconds(value: str) -> float:
    if value.endswith("Z"):
        value = value[:-1] + "+00:00"
    return datetime.fromisoformat(value).replace(tzinfo=timezone.utc).timestamp()


def load_jobs_from_results(runs_dir: Path, run_id: str) -> dict[str, dict[str, object]]:
    path = runs_dir / run_id / "results.json"
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


def load_mcperf_samples_from_run_dir(run_dir: Path) -> list[dict[str, float]]:
    path = run_dir / "mcperf.txt"
    lines = path.read_text(encoding="utf-8").splitlines()
    header = lines[0].split()
    indexes = {name: i for i, name in enumerate(header)}
    samples = []
    for line in lines[1:]:
        columns = line.split()
        if not columns:
            continue
        samples.append(
            {
                "p95_ms": float(columns[indexes["p95"]]) / 1000.0,
                "ts_start_s": float(columns[indexes["ts_start"]]) / 1000.0,
                "ts_end_s": float(columns[indexes["ts_end"]]) / 1000.0,
            }
        )
    return samples


def load_mcperf_samples(runs_dir: Path, run_id: str) -> list[dict[str, float]]:
    return load_mcperf_samples_from_run_dir(runs_dir / run_id)


def _metric_value(payload: dict[str, object], key: str) -> float:
    metrics = payload.get("metrics", {})
    if not isinstance(metrics, dict) or key not in metrics:
        raise ValueError(f"missing metric {key}")
    return float(metrics[key])


def _iteration_value(payload: dict[str, object]) -> int:
    value = payload.get("iteration_found", payload.get("iteration"))
    if value is None:
        raise ValueError("missing iteration")
    return int(value)


def _run_id_from_artifacts(payload: dict[str, object]) -> str:
    raw_artifacts = payload.get("artifacts_json")
    if not isinstance(raw_artifacts, str) or not raw_artifacts:
        return ""
    try:
        artifacts = json.loads(raw_artifacts)
        summary = json.loads(artifacts.get("summary", "{}"))
    except (TypeError, ValueError, json.JSONDecodeError):
        return ""
    run_id = summary.get("run_id", "")
    return str(run_id) if run_id else ""


def load_run_summary(run_dir: Path) -> dict[str, object]:
    return json.loads((run_dir / "summary.json").read_text(encoding="utf-8"))


def summary_matches_point(summary: dict[str, object], point: dict[str, object]) -> bool:
    return (
        abs(float(summary.get("makespan_s", -1.0)) - float(point["makespan_s"])) < 0.01
        and abs(float(summary.get("max_observed_p95_us", -1.0)) - float(point["max_p95_us"])) < 0.01
        and float(summary.get("slo_violations", -1.0)) == float(point["slo_violations"])
    )


def find_openevolve_eval_run_dir(point: dict[str, object]) -> Path:
    run_id = str(point.get("run_id") or "")
    if run_id:
        candidate = OPENEVOLVE_EVAL_RUNS_DIR / run_id
        if (candidate / "summary.json").exists() and (candidate / "mcperf.txt").exists():
            return candidate

    for candidate in OPENEVOLVE_EVAL_RUNS_DIR.iterdir():
        if not candidate.is_dir():
            continue
        summary_path = candidate / "summary.json"
        if not summary_path.exists() or not (candidate / "mcperf.txt").exists():
            continue
        summary = json.loads(summary_path.read_text(encoding="utf-8"))
        if run_id and summary.get("run_id") == run_id:
            return candidate
        if not run_id and summary_matches_point(summary, point):
            return candidate

    raise ValueError(f"could not find raw OpenEvolve run for iteration {point['iteration']}")


def add_mcperf_p95_stats(point: dict[str, object]) -> None:
    run_dir = find_openevolve_eval_run_dir(point)
    summary = load_run_summary(run_dir)
    jobs = summary.get("jobs", {})
    if not isinstance(jobs, dict):
        raise ValueError(f"{run_dir.name}: missing jobs in summary.json")

    start_times = [parse_iso_seconds(str(info["started_at"])) for info in jobs.values()]
    finish_times = [parse_iso_seconds(str(info["finished_at"])) for info in jobs.values()]
    t0 = min(start_times)
    t1 = max(finish_times)
    p95_values_us = []
    for sample in load_mcperf_samples_from_run_dir(run_dir):
        start = max(sample["ts_start_s"], t0)
        end = min(sample["ts_end_s"], t1)
        if end <= start:
            continue
        p95_values_us.append(sample["p95_ms"] * 1000.0)

    if not p95_values_us:
        raise ValueError(f"{run_dir.name}: no mcperf samples overlap the batch window")
    point["run_dir"] = run_dir.name
    point["p95_mean_us"] = sum(p95_values_us) / len(p95_values_us)
    point["p95_std_us"] = statistics.pstdev(p95_values_us)
    point["p95_sample_count"] = len(p95_values_us)


def load_openevolve_iterations() -> list[dict[str, object]]:
    programs: dict[str, dict[str, object]] = {}
    for path in OPENEVOLVE_CHECKPOINTS_DIR.glob("checkpoint_*/programs/*.json"):
        payload = json.loads(path.read_text(encoding="utf-8"))
        program_id = str(payload["id"])
        iteration = _iteration_value(payload)
        current = programs.get(program_id)
        if current is not None and iteration >= int(current["iteration"]):
            continue
        programs[program_id] = {
            "id": program_id,
            "iteration": iteration,
            "combined_score": _metric_value(payload, "combined_score"),
            "makespan_s": _metric_value(payload, "makespan_s"),
            "max_p95_us": _metric_value(payload, "max_p95_us"),
            "slo_violations": _metric_value(payload, "slo_violations"),
            "run_id": _run_id_from_artifacts(payload),
        }

    if not programs:
        raise ValueError(f"no OpenEvolve program metadata found in {OPENEVOLVE_CHECKPOINTS_DIR}")
    points = sorted(programs.values(), key=lambda item: int(item["iteration"]))
    for point in points:
        add_mcperf_p95_stats(point)
    return points


def configure_iteration_axis(ax, points: list[dict[str, object]]) -> None:
    last_iteration = max(int(point["iteration"]) for point in points)
    ticks = list(range(0, last_iteration + 1, 2))
    if ticks[-1] != last_iteration:
        ticks.append(last_iteration)
    ax.set_xlim(-0.5, last_iteration + 0.5)
    ax.set_xticks(ticks)
    ax.set_xlabel("OpenEvolve iteration")
    ax.grid(True, color="#e5e5e5", linewidth=0.7)


def plot_openevolve_latency_and_makespan(points: list[dict[str, object]]) -> None:
    iterations = [int(point["iteration"]) for point in points]
    p95_values = [float(point["p95_mean_us"]) for point in points]
    p95_std_values = [float(point["p95_std_us"]) for point in points]
    makespan_values = [float(point["makespan_s"]) for point in points]

    plt.rcParams.update(
        {
            "font.size": 10,
            "axes.labelsize": 10,
            "axes.titlesize": 11,
            "xtick.labelsize": 9,
            "ytick.labelsize": 9,
        }
    )
    fig, latency_ax = plt.subplots(figsize=(8.8, 4.8))
    makespan_ax = latency_ax.twinx()

    p95_line = latency_ax.errorbar(
        iterations,
        p95_values,
        yerr=p95_std_values,
        color="#2f5f8f",
        ecolor="#8ca8c2",
        elinewidth=1.0,
        capsize=3,
        capthick=1.0,
        fmt="-o",
        markersize=4.5,
        linewidth=1.8,
        label="mean p95 latency +/- std dev",
    )
    makespan_line, = makespan_ax.plot(
        iterations,
        makespan_values,
        color="#3d7b46",
        marker="o",
        markersize=4.5,
        linewidth=1.8,
        label="makespan",
    )
    slo_threshold = latency_ax.axhline(
        1000.0,
        color="#d62728",
        linestyle=(0, (5, 5)),
        linewidth=1.2,
        label="1 ms SLO",
    )

    configure_iteration_axis(latency_ax, points)
    latency_ax.set_ylabel("p95 latency mean +/- std dev [us]", color="#2f5f8f")
    latency_ax.tick_params(axis="y", labelcolor="#2f5f8f")
    latency_ax.set_ylim(0, max(1050.0, max(value + std for value, std in zip(p95_values, p95_std_values)) * 1.18))
    makespan_ax.set_ylabel("makespan [s]", color="#3d7b46")
    makespan_ax.tick_params(axis="y", labelcolor="#3d7b46")
    latency_ax.set_title("OpenEvolve policy evaluations: latency variability and makespan")
    latency_ax.legend(handles=[p95_line, makespan_line, slo_threshold], loc="upper right", bbox_to_anchor=(1.0, 0.95),frameon=False)
    fig.tight_layout()

    output = OUT_DIR / "cx_part3_q2a_openevolve_p95_makespan.png"
    fig.savefig(output, dpi=220)
    plt.close(fig)
    print(f"wrote {output}")


def plot_openevolve_slo_and_score(points: list[dict[str, object]]) -> None:
    iterations = [int(point["iteration"]) for point in points]
    slo_values = [float(point["slo_violations"]) for point in points]
    score_values = [float(point["combined_score"]) for point in points]

    plt.rcParams.update(
        {
            "font.size": 10,
            "axes.labelsize": 10,
            "axes.titlesize": 11,
            "xtick.labelsize": 9,
            "ytick.labelsize": 9,
        }
    )
    fig, slo_ax = plt.subplots(figsize=(8.8, 4.8))
    score_ax = slo_ax.twinx()

    slo_line, = slo_ax.plot(
        iterations,
        slo_values,
        color="#c23b22",
        marker="x",
        markersize=5.0,
        linewidth=1.8,
        label="SLO violations",
    )
    score_line, = score_ax.plot(
        iterations,
        score_values,
        color="#5d4a8d",
        marker="s",
        markersize=4.0,
        linewidth=1.8,
        label="combined score",
    )

    configure_iteration_axis(slo_ax, points)
    slo_ax.set_ylabel("SLO violations [samples]", color="#c23b22")
    slo_ax.tick_params(axis="y", labelcolor="#c23b22")
    slo_ax.set_ylim(0, max(1.0, max(slo_values) * 1.18))
    slo_ax.set_yticks(range(0, int(slo_ax.get_ylim()[1]) + 1))
    score_ax.set_ylabel("combined score", color="#5d4a8d")
    score_ax.tick_params(axis="y", labelcolor="#5d4a8d")
    slo_ax.set_title("OpenEvolve policy evaluations: SLO violations and score")
    slo_ax.legend(handles=[slo_line, score_line], loc="upper right", frameon=False)
    fig.tight_layout()

    output = OUT_DIR / "cx_part3_q2a_openevolve_slo_score.png"
    fig.savefig(output, dpi=220)
    plt.close(fig)
    print(f"wrote {output}")


def plot_openevolve_iterations() -> None:
    points = load_openevolve_iterations()
    plot_openevolve_latency_and_makespan(points)
    plot_openevolve_slo_and_score(points)


def text_color(background: str) -> str:
    red = int(background[1:3], 16) / 255.0
    green = int(background[3:5], 16) / 255.0
    blue = int(background[5:7], 16) / 255.0
    luminance = 0.2126 * red + 0.7152 * green + 0.0722 * blue
    return "black" if luminance > 0.00 else "white"


def configure_time_axis(ax, makespan: float) -> None:
    ticks = [0, 60, 120, 180]
    if makespan > 220:
        ticks.append(round(makespan))
    ax.set_xlim(0, makespan)
    ax.set_xticks(ticks)
    ax.grid(False, axis="x")


def draw_latency_axis(ax, samples: list[dict[str, float]], t0: float, t1: float) -> tuple[int, int, float]:
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
    ax.text(t1 - t0 + 9, 1.02, "1 ms", ha="right", va="bottom", color="#d62728")
    ax.axhline(mean_p95, color="#1f6b3a", linestyle=(0, (2, 3)), linewidth=1.0)
    ax.text(
        t1 - t0,
        mean_p95 + 0.015,
        f"mean p95 = {mean_p95 * 1000:.0f} μs",
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
    job_cores: dict[str, tuple[str, range]],
    t0: float,
    t1: float,
    annotate_durations: bool = False,
) -> None:
    cores_desc = sorted(NODE_CORES[node], reverse=True)
    y_by_core = {core: index for index, core in enumerate(cores_desc)}
    height = 0.78

    if node == "node-b-4core":
        y = y_by_core[0]
        ax.barh(
            y,
            t1 - t0,
            left=0,
            height=height,
            color=MEMCACHED_COLOR,
            edgecolor="black",
            linewidth=0.7,
        )
        ax.text(
            (t1 - t0) / 2,
            y,
            "memcached",
            ha="center",
            va="center",
            color=text_color(MEMCACHED_COLOR),
            fontsize=9,
            fontweight="bold",
        )

    for job in JOB_ORDER:
        node_for_job, cores = job_cores[job]
        if node_for_job != node:
            continue
        info = jobs[job]
        start = parse_iso_seconds(str(info["started_at"])) - t0
        finish = parse_iso_seconds(str(info["finished_at"])) - t0
        for core in cores:
            ax.barh(
                y_by_core[core],
                finish - start,
                left=start,
                height=height,
                color=JOB_COLOR[job],
                edgecolor="black",
                linewidth=0.7,
            )
        core_list = list(cores)
        y_offset = 0.0
        
        if (len(cores))%2 == 0:
            # print("even cores, applying offset" , max(cores))
            y_offset = 0.5
            
        y_center = (y_by_core[core_list[0]] + y_by_core[core_list[-1]]) / 2.0 + (- y_offset) 
        x_center = (start + finish) / 2.0
        width = finish - start
        rotation = 90 if width < 10 else 0
        label_size = 10 if width < 28 else 12
        label = f"{job} ({width:.0f}s)" if annotate_durations else job
        ax.text(
            x_center,
            y_center ,
            label,
            ha="center",
            va="center",
            rotation=rotation,
            color=text_color(JOB_COLOR[job]),
            fontsize=label_size,
            fontweight="bold",
            zorder=4
        )

    prefix = "a" if node == "node-a-8core" else "b"
    ax.set_yticks(range(len(cores_desc)))
    ax.set_yticklabels([f"{prefix}{core}" for core in cores_desc])
    ax.invert_yaxis()
    ax.set_title(node, loc="left", pad=4, fontweight="bold")
    ax.set_ylim(len(cores_desc) - 0.45, -0.55)
    ax.grid(False, axis="x")


def draw_time_labels(
    ax,
    jobs: dict[str, dict[str, object]],
    t0: float,
) -> None:
    events: list[tuple[float, str, str]] = []
    for job in JOB_ORDER:
        info = jobs[job]
        start = parse_iso_seconds(str(info["started_at"])) - t0
        finish = parse_iso_seconds(str(info["finished_at"])) - t0
        events.append((start, f"{start:.0f}s", JOB_COLOR[job]))
        events.append((finish, f"{finish:.0f}s", JOB_COLOR[job]))

    previous_x: float | None = None
    level = 0
    for x, label, color in sorted(events):
        if previous_x is not None and x - previous_x < 5:
            level = (level + 1) % 4
        else:
            level = 0
        previous_x = x
        ax.text(
            x - 2,
            1.105 - level * 0.055,
            label,
            ha="center",
            va="top",
            rotation=90,
            color=color,
            fontsize=10,
            fontweight="bold",
            zorder=4,
        )


def plot_run(
    *,
    runs_dir: Path,
    run_id: str,
    title: str,
    output_name: str,
    job_cores: dict[str, tuple[str, range]],
    annotate_details: bool = False,
) -> None:
    jobs = load_jobs_from_results(runs_dir, run_id)
    samples = load_mcperf_samples(runs_dir, run_id)

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
    draw_node_axis(
        node_a_ax,
        node="node-a-8core",
        jobs=jobs,
        job_cores=job_cores,
        t0=t0,
        t1=t1,
        annotate_durations=annotate_details,
    )
    draw_node_axis(
        node_b_ax,
        node="node-b-4core",
        jobs=jobs,
        job_cores=job_cores,
        t0=t0,
        t1=t1,
        annotate_durations=annotate_details,
    )

    for job in JOB_ORDER:
        info = jobs[job]
        start = parse_iso_seconds(str(info["started_at"])) - t0
        finish = parse_iso_seconds(str(info["finished_at"])) - t0
        color = JOB_COLOR[job]
        for axis in (latency_ax, node_a_ax, node_b_ax):
            axis.axvline(start, color=color, linewidth=1.1, alpha=0.85, zorder=3)
            axis.axvline(
                finish,
                color=color,
                linewidth=1.1,
                alpha=0.75,
                linestyle="dotted",
                zorder=3,
            )

    if annotate_details:
        draw_time_labels(latency_ax, jobs, t0)

    for axis in (latency_ax, node_a_ax, node_b_ax):
        configure_time_axis(axis, makespan)
    plt.setp(latency_ax.get_xticklabels(), visible=False)
    plt.setp(node_a_ax.get_xticklabels(), visible=False)
    node_b_ax.set_xlabel("time since first batch container start [s]")

    max_p95 = max(sample["p95_ms"] for sample in samples)
    latency_ax.set_title(
        f"{title}: makespan {makespan:.0f} s, "
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
    fig.subplots_adjust(left=0.08, right=0.96, top=0.92, bottom=0.13)

    output = OUT_DIR / output_name
    fig.savefig(output, dpi=220)
    plt.close(fig)
    print(f"wrote {output}")


def main() -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    for index, run_id in enumerate(FINAL_RUN_IDS, start=1):
        plot_run(
            runs_dir=FINAL_RUNS_DIR,
            run_id=run_id,
            title=f"Run {index}",
            output_name=f"cx_part3_q1a_run{index}.png",
            job_cores=FINAL_JOB_CORES,
             annotate_details=True,
        )

    plot_run(
        runs_dir=HANDCRAFTED_RUNS_DIR,
        run_id=AGGRESSIVE_RUN_ID,
        title="Aggressive split-on-node-A candidate",
        output_name="cx_part3_q1b_aggressive_candidate.png",
        job_cores=AGGRESSIVE_JOB_CORES,
         annotate_details=True,
    )

    for index, run_id in enumerate(AI_RUN_IDS, start=1):
        plot_run(
            runs_dir=AI_RUNS_DIR,
            run_id=run_id,
            title=f"Run {index}",
            output_name=f"cx_part3_q2a_ai_run{index}.png",
            job_cores=AI_JOB_CORES,
            annotate_details=True,
        )

    plot_openevolve_iterations()


if __name__ == "__main__":
    main()
