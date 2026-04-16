from __future__ import annotations

import json
import shutil
from pathlib import Path


def _load_summary(path: Path) -> dict[str, object]:
    return json.loads(path.read_text(encoding="utf-8"))


def export_submission(
    *,
    results_root: Path,
    experiment_id: str,
    group: str,
    task: str,
    output_root: Path,
    selected_run_ids: list[str] | None = None,
) -> Path:
    if task != "3_1":
        raise ValueError("Only task 3_1 export is implemented")
    experiment_root = results_root / experiment_id
    if not experiment_root.exists():
        raise FileNotFoundError(f"Experiment directory not found: {experiment_root}")
    run_dirs = [path for path in experiment_root.iterdir() if path.is_dir()]
    summaries: list[tuple[Path, dict[str, object]]] = []
    for run_dir in sorted(run_dirs):
        summary_path = run_dir / "summary.json"
        if not summary_path.exists():
            continue
        summary = _load_summary(summary_path)
        if summary.get("overall_status") != "pass":
            continue
        summaries.append((run_dir, summary))
    if selected_run_ids:
        selected = [item for item in summaries if item[1].get("run_id") in selected_run_ids]
    else:
        selected = summaries[-3:]
    if len(selected) != 3:
        raise ValueError("Submission export requires exactly three successful runs")
    target_dir = output_root / f"part_3_1_results_group_{str(group).zfill(3)}"
    target_dir.mkdir(parents=True, exist_ok=True)
    for index, (run_dir, _) in enumerate(selected, start=1):
        shutil.copyfile(run_dir / "pods.json", target_dir / f"pods_{index}.json")
        shutil.copyfile(run_dir / "mcperf.txt", target_dir / f"mcperf_{index}.txt")
    return target_dir

