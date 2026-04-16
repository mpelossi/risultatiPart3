from __future__ import annotations

from pathlib import Path

from .cluster import ClusterController
from .metrics import build_summary
from .utils import write_json


def collect_live_pods(cluster: ClusterController, run_dir: Path) -> Path:
    pods_path = run_dir / "pods.json"
    cluster.capture_pods_json(pods_path)
    return pods_path


def summarize_run(
    run_dir: Path,
    *,
    experiment_id: str,
    policy_name: str,
    run_id: str,
    expected_jobs: set[str],
) -> dict[str, object]:
    pods_path = run_dir / "pods.json"
    mcperf_path = run_dir / "mcperf.txt"
    summary = build_summary(
        pods_path,
        mcperf_path if mcperf_path.exists() else None,
        expected_jobs,
        run_id=run_id,
        experiment_id=experiment_id,
        policy_name=policy_name,
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

