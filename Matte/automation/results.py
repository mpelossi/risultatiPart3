from __future__ import annotations

import json
from pathlib import Path

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
        summary["run_dir"] = str(run_dir)
        summary["run_label"] = format_run_id_label(str(summary.get("run_id", run_dir.name)))
        summaries.append(summary)
    return summaries


def sort_best_runs(summaries: list[dict[str, object]]) -> list[dict[str, object]]:
    def sort_key(summary: dict[str, object]) -> tuple[int, float, float]:
        is_pass = 0 if summary.get("overall_status") == "pass" else 1
        makespan = float(summary.get("makespan_s") or 1e18)
        p95 = float(summary.get("max_observed_p95_us") or 1e18)
        return (is_pass, makespan, p95)

    return sorted(summaries, key=sort_key)
