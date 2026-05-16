from __future__ import annotations

import json
from pathlib import Path

from .metrics import parse_mcperf_output
from .utils import format_run_id_label


def resolve_experiment_root(results_root: Path, experiment_id: str) -> Path:
    experiment_root = results_root / experiment_id
    if not experiment_root.exists():
        raise FileNotFoundError(
            f"Experiment directory not found: {experiment_root}. "
            "Pass --results-root if your runs directory lives somewhere else."
        )
    return experiment_root


def load_run_summaries(results_root: Path, experiment_id: str) -> list[dict[str, object]]:
    experiment_root = resolve_experiment_root(results_root, experiment_id)
    summaries: list[dict[str, object]] = []
    for run_dir in sorted(path for path in experiment_root.iterdir() if path.is_dir()):
        summary_path = run_dir / "summary.json"
        if not summary_path.exists():
            continue
        summary = json.loads(summary_path.read_text(encoding="utf-8"))
        _refresh_measurement_summary(summary, run_dir / "mcperf.txt")
        summary["run_dir"] = str(run_dir)
        summary["run_label"] = format_run_id_label(str(summary.get("run_id", run_dir.name)))
        summaries.append(summary)
    return summaries


def _refresh_measurement_summary(summary: dict[str, object], mcperf_path: Path) -> None:
    try:
        mcperf_summary = parse_mcperf_output(mcperf_path if mcperf_path.exists() else None)
    except ValueError:
        mcperf_summary = {
            "samples": [],
            "max_p95_us": None,
            "slo_violations": None,
            "measurement_status": "parse_error",
        }
    if mcperf_summary["measurement_status"] == "ok":
        summary["max_observed_p95_us"] = mcperf_summary["max_p95_us"]
        summary["slo_violations"] = mcperf_summary["slo_violations"]
        summary["sample_count"] = len(mcperf_summary["samples"])
        return
    summary["measurement_status"] = mcperf_summary["measurement_status"]
    summary["max_observed_p95_us"] = mcperf_summary["max_p95_us"]
    summary["slo_violations"] = mcperf_summary["slo_violations"]
    summary["sample_count"] = len(mcperf_summary["samples"])
    if summary.get("overall_status") == "pass":
        summary["overall_status"] = "infra_fail"


def sort_best_runs(summaries: list[dict[str, object]]) -> list[dict[str, object]]:
    def is_valid_best_candidate(summary: dict[str, object]) -> bool:
        sample_count = summary.get("sample_count")
        try:
            parsed_sample_count = int(sample_count or 0)
        except (TypeError, ValueError):
            parsed_sample_count = 0
        return (
            summary.get("overall_status") == "pass"
            and summary.get("measurement_status") == "ok"
            and summary.get("makespan_s") is not None
            and summary.get("max_observed_p95_us") is not None
            and summary.get("timing_complete") is not False
            and parsed_sample_count > 0
        )

    def sort_key(summary: dict[str, object]) -> tuple[int, float, float]:
        is_pass = 0 if is_valid_best_candidate(summary) else 1
        makespan = float(summary.get("makespan_s") or 1e18)
        p95 = float(summary.get("max_observed_p95_us") or 1e18)
        return (is_pass, makespan, p95)

    return sorted(summaries, key=sort_key)
