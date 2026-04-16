from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path


TIME_FORMAT = "%Y-%m-%dT%H:%M:%SZ"
SLO_P95_US = 1000.0


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
    return {
        "samples": samples,
        "max_p95_us": max(p95_values) if p95_values else None,
        "slo_violations": sum(1 for value in p95_values if value > SLO_P95_US),
        "measurement_status": "ok",
    }


def summarize_pods(path: Path, expected_jobs: set[str]) -> dict[str, object]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    job_summaries: dict[str, dict[str, object]] = {
        job_id: {"job_id": job_id, "status": "missing"} for job_id in expected_jobs
    }
    memcached_summary: dict[str, object] | None = None
    start_times = []
    finish_times = []

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
        started_at = terminated.get("startedAt")
        finished_at = terminated.get("finishedAt")
        parsed_start = _parse_time(started_at)
        parsed_finish = _parse_time(finished_at)
        if parsed_start:
            start_times.append(parsed_start)
        if parsed_finish:
            finish_times.append(parsed_finish)
        runtime_s = None
        if parsed_start and parsed_finish:
            runtime_s = (parsed_finish - parsed_start).total_seconds()
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

    makespan_s = None
    if start_times and finish_times:
        makespan_s = (max(finish_times) - min(start_times)).total_seconds()
    return {
        "jobs": job_summaries,
        "memcached": memcached_summary,
        "makespan_s": makespan_s,
    }


def build_summary(
    pods_path: Path,
    mcperf_path: Path | None,
    expected_jobs: set[str],
    *,
    run_id: str,
    experiment_id: str,
    policy_name: str,
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
    return {
        "experiment_id": experiment_id,
        "run_id": run_id,
        "policy_name": policy_name,
        "overall_status": overall_status,
        "memcached": pod_summary["memcached"],
        "jobs": jobs,
        "makespan_s": pod_summary["makespan_s"],
        "max_observed_p95_us": mcperf_summary["max_p95_us"],
        "slo_violations": mcperf_summary["slo_violations"],
        "measurement_status": measurement_status,
        "sample_count": len(mcperf_summary["samples"]),
    }
