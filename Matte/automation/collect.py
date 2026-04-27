from __future__ import annotations

from pathlib import Path

from .cluster import ClusterController
from .metrics import build_summary
from .utils import resolve_existing_run_results_path, run_results_path, write_json


def collect_live_pods(cluster: ClusterController, run_dir: Path) -> Path:
    results_path = run_results_path(run_dir)
    cluster.capture_pods_json(results_path)
    return results_path


def summarize_run(
    run_dir: Path,
    *,
    experiment_id: str,
    policy_name: str,
    run_id: str,
    expected_jobs: set[str],
    node_platforms: dict[str, object] | None = None,
) -> dict[str, object]:
    pods_path = resolve_existing_run_results_path(run_dir)
    mcperf_path = run_dir / "mcperf.txt"
    summary = build_summary(
        pods_path,
        mcperf_path if mcperf_path.exists() else None,
        expected_jobs,
        run_id=run_id,
        experiment_id=experiment_id,
        policy_name=policy_name,
        node_platforms=node_platforms,
    )
    write_json(run_dir / "summary.json", summary)
    return summary


def collect_describes(
    cluster: ClusterController,
    run_dir: Path,
    *,
    job_name_map: dict[str, str],
    summary: dict[str, object],
) -> None:
    describe_dir = run_dir / "describe"
    describe_dir.mkdir(exist_ok=True)
    for job_id, job_summary in summary["jobs"].items():
        if job_summary.get("status") != "completed":
            cluster.describe_job(job_name_map[job_id], describe_dir / f"{job_id}.txt")
