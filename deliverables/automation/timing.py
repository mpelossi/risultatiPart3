from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path


TIME_FORMAT = "%Y-%m-%dT%H:%M:%SZ"


@dataclass(frozen=True)
class JobTimingWindow:
    job_id: str
    container_name: str
    started_at: str
    finished_at: str
    runtime_s: float


@dataclass(frozen=True)
class GetTimeReport:
    job_names: tuple[str, ...]
    completed_jobs: tuple[JobTimingWindow, ...]
    missing_completion_for: str | None
    total_runtime: timedelta | None
    expected_job_count: int

    @property
    def completed_job_count(self) -> int:
        return len(self.completed_jobs)

    @property
    def is_complete(self) -> bool:
        return self.missing_completion_for is None and self.completed_job_count == self.expected_job_count


def _parse_time(value: str | None) -> datetime | None:
    if not value:
        return None
    return datetime.strptime(value, TIME_FORMAT)


def load_pod_payload(path: Path) -> dict[str, object]:
    return json.loads(path.read_text(encoding="utf-8"))


def _infer_job_id(
    item: dict[str, object],
    *,
    container_name: str,
    expected_jobs: set[str] | None,
) -> str | None:
    metadata = item.get("metadata", {})
    labels = metadata.get("labels", {})
    labeled_job_id = labels.get("cca-project-job-id")
    if isinstance(labeled_job_id, str) and (expected_jobs is None or labeled_job_id in expected_jobs):
        return labeled_job_id
    candidate = container_name.removeprefix("parsec-")
    if expected_jobs is None or candidate in expected_jobs:
        return candidate
    return None


def collect_completed_job_timings(
    payload: dict[str, object],
    *,
    expected_jobs: set[str] | None = None,
) -> dict[str, JobTimingWindow]:
    timings: dict[str, JobTimingWindow] = {}
    for item in payload.get("items", []):
        status = item.get("status", {})
        container_status = (status.get("containerStatuses") or [{}])[0]
        container_name = container_status.get("name")
        if not isinstance(container_name, str) or container_name == "memcached":
            continue
        job_id = _infer_job_id(item, container_name=container_name, expected_jobs=expected_jobs)
        if job_id is None:
            continue
        terminated = container_status.get("state", {}).get("terminated", {})
        started_at = terminated.get("startedAt")
        finished_at = terminated.get("finishedAt")
        parsed_start = _parse_time(started_at)
        parsed_finish = _parse_time(finished_at)
        if parsed_start is None or parsed_finish is None:
            continue
        timings[job_id] = JobTimingWindow(
            job_id=job_id,
            container_name=container_name,
            started_at=started_at,
            finished_at=finished_at,
            runtime_s=(parsed_finish - parsed_start).total_seconds(),
        )
    return timings


def compute_makespan_s(job_timings: dict[str, JobTimingWindow]) -> float | None:
    if not job_timings:
        return None
    start_times = [_parse_time(window.started_at) for window in job_timings.values()]
    finish_times = [_parse_time(window.finished_at) for window in job_timings.values()]
    valid_start_times = [value for value in start_times if value is not None]
    valid_finish_times = [value for value in finish_times if value is not None]
    if not valid_start_times or not valid_finish_times:
        return None
    return (max(valid_finish_times) - min(valid_start_times)).total_seconds()


def build_get_time_report(
    path: Path,
    *,
    expected_job_count: int = 7,
    expected_jobs: set[str] | None = None,
) -> GetTimeReport:
    payload = load_pod_payload(path)
    observed_job_names: list[str] = []
    missing_completion_for: str | None = None
    completed_timings = collect_completed_job_timings(payload, expected_jobs=expected_jobs)

    for item in payload.get("items", []):
        status = item.get("status", {})
        container_status = (status.get("containerStatuses") or [{}])[0]
        container_name = container_status.get("name")
        if not isinstance(container_name, str) or container_name == "memcached":
            continue
        observed_job_names.append(container_name)
        job_id = _infer_job_id(item, container_name=container_name, expected_jobs=expected_jobs)
        if job_id is None:
            continue
        terminated = container_status.get("state", {}).get("terminated", {})
        if not terminated.get("startedAt") or not terminated.get("finishedAt"):
            missing_completion_for = container_name
            break

    completed_jobs = tuple(
        window for _job_id, window in sorted(completed_timings.items(), key=lambda entry: entry[1].container_name)
    )
    total_runtime = None
    if missing_completion_for is None and len(completed_jobs) == expected_job_count:
        makespan_s = compute_makespan_s(completed_timings)
        if makespan_s is not None:
            total_runtime = timedelta(seconds=makespan_s)

    return GetTimeReport(
        job_names=tuple(observed_job_names),
        completed_jobs=completed_jobs,
        missing_completion_for=missing_completion_for,
        total_runtime=total_runtime,
        expected_job_count=expected_job_count,
    )
