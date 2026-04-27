from __future__ import annotations

from datetime import datetime
from pathlib import Path

from .timing import collect_completed_job_timings, compute_makespan_s, load_pod_payload


TIME_FORMAT = "%Y-%m-%dT%H:%M:%SZ"
SLO_P95_US = 1000.0
MCPERF_SYNC_ERROR_MARKERS = (
    "sync_agent",
    "ERROR during synchronization",
)


def _parse_time(value: str | None) -> datetime | None:
    if not value:
        return None
    return datetime.strptime(value, TIME_FORMAT)


def parse_mcperf_output(path: Path | None) -> dict[str, object]:
    if path is None or not path.exists():
        return {
            "samples": [],
            "max_p95_us": None,
            "slo_violations": None,
            "measurement_status": "missing",
        }
    lines = path.read_text(encoding="utf-8").splitlines()
    if not lines:
        return {
            "samples": [],
            "max_p95_us": None,
            "slo_violations": None,
            "measurement_status": "empty",
        }
    sync_error_lines = [
        line for line in lines if any(marker in line for marker in MCPERF_SYNC_ERROR_MARKERS)
    ]
    if sync_error_lines:
        return {
            "samples": [],
            "max_p95_us": None,
            "slo_violations": None,
            "measurement_status": "parse_error",
        }
    header = lines[0].split()
    if "p95" not in header:
        raise ValueError(f"mcperf output is missing p95 column: {path}")
    p95_index = header.index("p95")
    samples: list[dict[str, object]] = []
    p95_values: list[float] = []
    for line in lines[1:]:
        columns = line.split()
        if len(columns) <= p95_index:
            continue
        sample_type = columns[0]
        p95_value = float(columns[p95_index])
        p95_values.append(p95_value)
        samples.append({"type": sample_type, "p95_us": p95_value, "raw": line})
    if not samples:
        return {
            "samples": [],
            "max_p95_us": None,
            "slo_violations": None,
            "measurement_status": "no_samples",
        }
    return {
        "samples": samples,
        "max_p95_us": max(p95_values),
        "slo_violations": sum(1 for value in p95_values if value > SLO_P95_US),
        "measurement_status": "ok",
    }


def summarize_pods(path: Path, expected_jobs: set[str]) -> dict[str, object]:
    payload = load_pod_payload(path)
    completed_timings = collect_completed_job_timings(payload, expected_jobs=expected_jobs)
    job_summaries: dict[str, dict[str, object]] = {
        job_id: {"job_id": job_id, "status": "missing"} for job_id in expected_jobs
    }
    memcached_summary: dict[str, object] | None = None

    for item in payload.get("items", []):
        metadata = item.get("metadata", {})
        labels = metadata.get("labels", {})
        spec = item.get("spec", {})
        status = item.get("status", {})
        phase = status.get("phase")
        container_status = (status.get("containerStatuses") or [{}])[0]
        container_name = container_status.get("name")
        state = container_status.get("state", {})
        terminated = state.get("terminated", {})
        running = state.get("running", {})
        summary = {
            "pod_name": metadata.get("name"),
            "node_name": spec.get("nodeName"),
            "pod_ip": status.get("podIP"),
            "phase": phase,
        }
        if labels.get("cca-project-role") == "memcached" or container_name == "memcached":
            memcached_summary = summary
            continue
        job_id = labels.get("cca-project-job-id")
        if not job_id and isinstance(container_name, str) and container_name.startswith("parsec-"):
            job_id = container_name.removeprefix("parsec-")
        if not job_id and isinstance(container_name, str) and container_name in expected_jobs:
            job_id = container_name
        if not job_id or job_id not in expected_jobs:
            continue
        timing = completed_timings.get(job_id)
        started_at = timing.started_at if timing is not None else terminated.get("startedAt")
        finished_at = timing.finished_at if timing is not None else terminated.get("finishedAt")
        runtime_s = timing.runtime_s if timing is not None else None
        if terminated:
            exit_code = terminated.get("exitCode")
            job_status = "completed" if exit_code == 0 else "failed"
        elif running:
            job_status = "running"
        else:
            job_status = str(phase or "unknown").lower()
        summary.update(
            {
                "started_at": started_at,
                "finished_at": finished_at,
                "runtime_s": runtime_s,
                "status": job_status,
            }
        )
        job_summaries[job_id] = summary

    timing_complete = len(completed_timings) == len(expected_jobs) and all(
        job_summaries[job_id].get("status") == "completed" for job_id in expected_jobs
    )
    makespan_s = compute_makespan_s(completed_timings) if timing_complete else None
    return {
        "jobs": job_summaries,
        "memcached": memcached_summary,
        "makespan_s": makespan_s,
        "completed_job_count": len(completed_timings),
        "timing_complete": timing_complete,
    }


def build_summary(
    pods_path: Path,
    mcperf_path: Path | None,
    expected_jobs: set[str],
    *,
    run_id: str,
    experiment_id: str,
    policy_name: str,
    node_platforms: dict[str, object] | None = None,
) -> dict[str, object]:
    pod_summary = summarize_pods(pods_path, expected_jobs)
    mcperf_summary = parse_mcperf_output(mcperf_path)
    jobs = pod_summary["jobs"]
    all_jobs_completed = all(job.get("status") == "completed" for job in jobs.values())
    measurement_status = mcperf_summary["measurement_status"]
    if pod_summary["memcached"] is None or measurement_status != "ok":
        overall_status = "infra_fail"
    elif not all_jobs_completed:
        overall_status = "job_fail"
    elif (mcperf_summary["slo_violations"] or 0) > 0:
        overall_status = "slo_fail"
    else:
        overall_status = "pass"
    summary = {
        "experiment_id": experiment_id,
        "run_id": run_id,
        "policy_name": policy_name,
        "overall_status": overall_status,
        "memcached": pod_summary["memcached"],
        "jobs": jobs,
        "makespan_s": pod_summary["makespan_s"],
        "completed_job_count": pod_summary["completed_job_count"],
        "expected_job_count": len(expected_jobs),
        "timing_complete": pod_summary["timing_complete"],
        "max_observed_p95_us": mcperf_summary["max_p95_us"],
        "slo_violations": mcperf_summary["slo_violations"],
        "measurement_status": measurement_status,
        "sample_count": len(mcperf_summary["samples"]),
    }
    if node_platforms is not None:
        summary["node_platforms"] = node_platforms
    return summary
