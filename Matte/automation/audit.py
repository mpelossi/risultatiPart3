from __future__ import annotations

import csv
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .catalog import JOB_CATALOG, NODE_A, NODE_B, count_cores
from .config import (
    _load_structured_file,
    _require_int,
    _require_list,
    _require_mapping,
    _require_str,
    expand_path,
)


EPSILON = 1e-9


@dataclass(frozen=True)
class AuditMemcached:
    node: str
    cores: str
    threads: int


@dataclass(frozen=True)
class AuditJob:
    job_id: str
    node: str
    cores: str
    threads: int
    dependencies: tuple[str, ...]
    delay_s: int
    order: int
    phase_id: str | None = None


@dataclass(frozen=True)
class ScheduleModel:
    policy_name: str
    config_path: Path | None
    memcached: AuditMemcached
    jobs: dict[str, AuditJob]
    parse_errors: tuple[str, ...] = ()


@dataclass(frozen=True)
class RuntimeTable:
    source_path: Path
    runtimes: dict[str, dict[int, float]]


@dataclass(frozen=True)
class AuditIssue:
    level: str
    message: str
    node: str | None = None
    jobs: tuple[str, ...] = ()


@dataclass(frozen=True)
class ScheduledWindow:
    job_id: str
    label: str
    kind: str
    node: str
    cores: str
    core_ids: tuple[int, ...]
    threads: int
    start_s: float
    end_s: float
    duration_s: float
    dependencies: tuple[str, ...]


@dataclass(frozen=True)
class AuditReport:
    model: ScheduleModel
    jobs: dict[str, ScheduledWindow]
    windows_by_node: dict[str, list[ScheduledWindow]]
    errors: list[AuditIssue]
    warnings: list[AuditIssue]
    makespan_s: float | None

    @property
    def status(self) -> str:
        if self.errors:
            return "error"
        if self.warnings:
            return "warning"
        return "ok"


def expand_core_spec(core_spec: str) -> tuple[int, ...]:
    core_ids: list[int] = []
    for part in core_spec.split(","):
        token = part.strip()
        if not token:
            continue
        if "-" in token:
            start_text, end_text = token.split("-", 1)
            start = int(start_text)
            end = int(end_text)
            if end < start:
                raise ValueError(f"Invalid core range: {core_spec}")
            core_ids.extend(range(start, end + 1))
        else:
            core_ids.append(int(token))
    if not core_ids:
        raise ValueError(f"Invalid core set: {core_spec}")
    return tuple(sorted(set(core_ids)))


def dependency_text(dependencies: tuple[str, ...]) -> str:
    if not dependencies:
        return "start"
    return ",".join(dependencies)


def parse_dependency_text(raw: str) -> tuple[str, ...]:
    value = raw.strip()
    if not value or value == "start":
        return ()
    dependencies = [token.strip() for token in value.split(",") if token.strip()]
    if not dependencies:
        return ()
    return tuple(dependencies)


def load_runtime_table(path_str: str) -> RuntimeTable:
    path = expand_path(path_str)
    runtimes: dict[str, dict[int, float]] = {}
    with path.open(encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        required_fields = {"job", "threads", "real_time_seconds"}
        if reader.fieldnames is None or not required_fields.issubset(reader.fieldnames):
            raise ValueError(f"{path} must contain columns: job, threads, real_time_seconds")
        for row in reader:
            job_id = str(row["job"]).strip()
            threads = int(str(row["threads"]).strip())
            duration = float(str(row["real_time_seconds"]).strip())
            runtimes.setdefault(job_id, {})[threads] = duration
    return RuntimeTable(source_path=path, runtimes=runtimes)


def estimate_runtime(job_id: str, threads: int, runtime_table: RuntimeTable) -> float | None:
    samples = runtime_table.runtimes.get(job_id)
    if not samples:
        return None
    if threads in samples:
        return samples[threads]
    ordered = sorted(samples.items())
    lower: tuple[int, float] | None = None
    upper: tuple[int, float] | None = None
    for sample_threads, duration in ordered:
        if sample_threads < threads:
            lower = (sample_threads, duration)
        elif sample_threads > threads and upper is None:
            upper = (sample_threads, duration)
            break
    if lower and upper:
        lower_threads, lower_duration = lower
        upper_threads, upper_duration = upper
        span = upper_threads - lower_threads
        ratio = (threads - lower_threads) / span
        return lower_duration + ((upper_duration - lower_duration) * ratio)
    return None


def _optional_str(raw: Any, default: str) -> str:
    if raw is None:
        return default
    return _require_str(raw, "value")


def _optional_int(raw: Any, default: int) -> int:
    if raw is None:
        return default
    return _require_int(raw, "value")


def _parse_memcached(raw: dict[str, Any]) -> AuditMemcached:
    memcached_raw = _require_mapping(raw.get("memcached", {}), "memcached")
    return AuditMemcached(
        node=_require_str(memcached_raw.get("node"), "memcached.node"),
        cores=_require_str(memcached_raw.get("cores"), "memcached.cores"),
        threads=_require_int(memcached_raw.get("threads", 1), "memcached.threads"),
    )


def _load_simple_jobs(raw: dict[str, Any]) -> tuple[dict[str, AuditJob], tuple[str, ...]]:
    parse_errors: list[str] = []
    jobs_raw = _require_mapping(raw.get("jobs", {}), "jobs")
    jobs: dict[str, AuditJob] = {}
    for index, (job_id, job_raw) in enumerate(jobs_raw.items()):
        if job_id not in JOB_CATALOG:
            parse_errors.append(f"Unknown job in jobs mapping: {job_id}")
            continue
        catalog_entry = JOB_CATALOG[job_id]
        schedule_map = _require_mapping(job_raw, f"jobs.{job_id}")
        after_raw = schedule_map.get("after", "start")
        if isinstance(after_raw, list):
            dependencies = tuple(
                _require_str(item, f"jobs.{job_id}.after dependency")
                for item in _require_list(after_raw, f"jobs.{job_id}.after")
            )
        else:
            after_value = _require_str(after_raw, f"jobs.{job_id}.after")
            dependencies = () if after_value == "start" else (after_value,)
        jobs[job_id] = AuditJob(
            job_id=job_id,
            node=_optional_str(schedule_map.get("node"), catalog_entry.default_node),
            cores=_optional_str(schedule_map.get("cores"), catalog_entry.default_cores),
            threads=_optional_int(schedule_map.get("threads"), catalog_entry.default_threads),
            dependencies=dependencies,
            delay_s=_optional_int(schedule_map.get("delay_s"), 0),
            order=index,
        )
    return jobs, tuple(parse_errors)


def _load_phase_jobs(raw: dict[str, Any]) -> tuple[dict[str, AuditJob], tuple[str, ...]]:
    parse_errors: list[str] = []
    jobs: dict[str, AuditJob] = {}
    overrides_raw = _require_mapping(raw.get("job_overrides", {}), "job_overrides")
    phases_raw = _require_list(raw.get("phases", []), "phases")
    phase_launches: dict[str, tuple[str, ...]] = {}
    for phase_index, phase_raw in enumerate(phases_raw):
        phase_map = _require_mapping(phase_raw, f"phases[{phase_index}]")
        phase_id = _require_str(phase_map.get("id"), f"phases[{phase_index}].id")
        after = _require_str(phase_map.get("after", "start"), f"phases[{phase_index}].after")
        jobs_complete = tuple(
            _require_str(item, f"phases[{phase_index}].jobs_complete item")
            for item in _require_list(phase_map.get("jobs_complete", []), f"phases[{phase_index}].jobs_complete")
        )
        launch = tuple(
            _require_str(item, f"phases[{phase_index}].launch item")
            for item in _require_list(phase_map.get("launch", []), f"phases[{phase_index}].launch")
        )
        delay_s = _optional_int(phase_map.get("delay_s"), 0)
        if after == "start":
            dependencies = ()
        elif after == "jobs_complete":
            dependencies = jobs_complete
        elif after.startswith("phase:"):
            referenced_phase = after.split(":", 1)[1]
            if referenced_phase not in phase_launches:
                parse_errors.append(f"Phase {phase_id} depends on unknown earlier phase: {after}")
                dependencies = ()
            else:
                dependencies = phase_launches[referenced_phase]
        else:
            parse_errors.append(f"Unsupported phase dependency: {after}")
            dependencies = ()
        for launch_index, job_id in enumerate(launch):
            if job_id not in JOB_CATALOG:
                parse_errors.append(f"Unknown job in phase {phase_id}: {job_id}")
                continue
            if job_id in jobs:
                parse_errors.append(f"Job {job_id} is launched more than once")
                continue
            catalog_entry = JOB_CATALOG[job_id]
            override_map = _require_mapping(overrides_raw.get(job_id, {}), f"job_overrides.{job_id}")
            jobs[job_id] = AuditJob(
                job_id=job_id,
                node=_optional_str(override_map.get("node"), catalog_entry.default_node),
                cores=_optional_str(override_map.get("cores"), catalog_entry.default_cores),
                threads=_optional_int(override_map.get("threads"), catalog_entry.default_threads),
                dependencies=dependencies,
                delay_s=delay_s,
                order=(phase_index * 100) + launch_index,
                phase_id=phase_id,
            )
        phase_launches[phase_id] = launch
    return jobs, tuple(parse_errors)


def load_schedule_model(path_str: str) -> ScheduleModel:
    path = expand_path(path_str)
    raw = _load_structured_file(path)
    memcached = _parse_memcached(raw)
    if "jobs" in raw and "phases" not in raw:
        jobs, parse_errors = _load_simple_jobs(raw)
    else:
        jobs, parse_errors = _load_phase_jobs(raw)
    return ScheduleModel(
        policy_name=_require_str(raw.get("policy_name", path.stem), "policy_name"),
        config_path=path,
        memcached=memcached,
        jobs=jobs,
        parse_errors=parse_errors,
    )


def build_schedule_model(
    *,
    policy_name: str,
    memcached: AuditMemcached,
    jobs: dict[str, AuditJob],
    config_path: Path | None = None,
    parse_errors: tuple[str, ...] = (),
) -> ScheduleModel:
    return ScheduleModel(
        policy_name=policy_name,
        config_path=config_path,
        memcached=memcached,
        jobs=jobs,
        parse_errors=parse_errors,
    )


def _validate_core_assignment(
    *,
    label: str,
    node: str,
    cores: str,
    threads: int,
    allowed_cores: tuple[str, ...],
    errors: list[AuditIssue],
) -> tuple[int, ...] | None:
    if node not in (NODE_A, NODE_B):
        errors.append(AuditIssue(level="error", message=f"{label} uses unsupported node: {node}", jobs=(label,)))
        return None
    if cores not in allowed_cores:
        errors.append(
            AuditIssue(
                level="error",
                message=f"{label} uses unsupported core set {cores} on {node}",
                node=node,
                jobs=(label,),
            )
        )
        return None
    try:
        core_ids = expand_core_spec(cores)
    except ValueError as exc:
        errors.append(AuditIssue(level="error", message=str(exc), node=node, jobs=(label,)))
        return None
    if threads <= 0:
        errors.append(AuditIssue(level="error", message=f"{label} must use at least one thread", node=node, jobs=(label,)))
        return None
    if threads > count_cores(cores):
        errors.append(
            AuditIssue(
                level="error",
                message=f"{label} threads ({threads}) exceed pinned cores ({cores})",
                node=node,
                jobs=(label,),
            )
        )
        return None
    return core_ids


def _topological_job_order(jobs: dict[str, AuditJob], errors: list[AuditIssue]) -> list[str]:
    indegree = {job_id: 0 for job_id in jobs}
    graph: dict[str, list[str]] = {job_id: [] for job_id in jobs}
    for job_id, job in jobs.items():
        for dependency in job.dependencies:
            if dependency not in jobs:
                continue
            indegree[job_id] += 1
            graph[dependency].append(job_id)
    ready = sorted((job_id for job_id, degree in indegree.items() if degree == 0), key=lambda item: jobs[item].order)
    ordered: list[str] = []
    while ready:
        job_id = ready.pop(0)
        ordered.append(job_id)
        for dependent in sorted(graph[job_id], key=lambda item: jobs[item].order):
            indegree[dependent] -= 1
            if indegree[dependent] == 0:
                ready.append(dependent)
                ready.sort(key=lambda item: jobs[item].order)
    if len(ordered) != len(jobs):
        cycle_jobs = sorted(job_id for job_id, degree in indegree.items() if degree > 0)
        errors.append(
            AuditIssue(
                level="error",
                message="Dependency cycle detected: " + ", ".join(cycle_jobs),
                jobs=tuple(cycle_jobs),
            )
        )
        return []
    return ordered


def _overlap_interval(a: ScheduledWindow, b: ScheduledWindow) -> tuple[float, float] | None:
    start = max(a.start_s, b.start_s)
    end = min(a.end_s, b.end_s)
    if end - start <= EPSILON:
        return None
    return start, end


def _format_seconds(value: float | None) -> str:
    if value is None:
        return "n/a"
    return f"{value:.2f}s"


def audit_schedule(model: ScheduleModel, runtime_table: RuntimeTable) -> AuditReport:
    errors: list[AuditIssue] = [AuditIssue(level="error", message=message) for message in model.parse_errors]
    warnings: list[AuditIssue] = []

    if set(model.jobs) != set(JOB_CATALOG):
        missing = sorted(set(JOB_CATALOG) - set(model.jobs))
        extra = sorted(set(model.jobs) - set(JOB_CATALOG))
        if missing:
            errors.append(AuditIssue(level="error", message="Missing jobs: " + ", ".join(missing), jobs=tuple(missing)))
        if extra:
            errors.append(AuditIssue(level="error", message="Unknown jobs: " + ", ".join(extra), jobs=tuple(extra)))

    job_core_ids: dict[str, tuple[int, ...]] = {}
    for job_id, job in model.jobs.items():
        catalog_entry = JOB_CATALOG.get(job_id)
        if catalog_entry is None:
            continue
        core_ids = _validate_core_assignment(
            label=job_id,
            node=job.node,
            cores=job.cores,
            threads=job.threads,
            allowed_cores=catalog_entry.allowed_cores_by_node.get(job.node, ()),
            errors=errors,
        )
        if core_ids is not None:
            job_core_ids[job_id] = core_ids
        if job.delay_s < 0:
            errors.append(AuditIssue(level="error", message=f"{job_id} delay_s must be non-negative", jobs=(job_id,)))
        if job_id in job.dependencies:
            errors.append(AuditIssue(level="error", message=f"{job_id} depends on itself", jobs=(job_id,)))
        for dependency in job.dependencies:
            if dependency not in model.jobs:
                errors.append(
                    AuditIssue(
                        level="error",
                        message=f"{job_id} depends on unknown job {dependency}",
                        jobs=(job_id, dependency),
                    )
                )

    memcached_allowed = (NODE_A, NODE_B)
    if model.memcached.node not in memcached_allowed:
        errors.append(
            AuditIssue(level="error", message=f"memcached uses unsupported node: {model.memcached.node}", jobs=("memcached",))
        )
        memcached_cores: tuple[int, ...] | None = None
    else:
        memcached_allowed_cores = JOB_CATALOG["barnes"].allowed_cores_by_node[model.memcached.node]
        memcached_cores = _validate_core_assignment(
            label="memcached",
            node=model.memcached.node,
            cores=model.memcached.cores,
            threads=model.memcached.threads,
            allowed_cores=memcached_allowed_cores,
            errors=errors,
        )

    ordered_jobs = _topological_job_order(model.jobs, errors)
    if errors:
        return AuditReport(
            model=model,
            jobs={},
            windows_by_node={NODE_A: [], NODE_B: []},
            errors=errors,
            warnings=warnings,
            makespan_s=None,
        )

    scheduled_jobs: dict[str, ScheduledWindow] = {}
    for job_id in ordered_jobs:
        job = model.jobs[job_id]
        missing_dependencies = [dependency for dependency in job.dependencies if dependency not in scheduled_jobs]
        if missing_dependencies:
            errors.append(
                AuditIssue(
                    level="error",
                    message=(
                        f"Cannot schedule {job_id} because dependency estimates are unavailable for "
                        + ", ".join(missing_dependencies)
                    ),
                    jobs=(job_id, *missing_dependencies),
                )
            )
            continue
        duration = estimate_runtime(job_id, job.threads, runtime_table)
        if duration is None:
            errors.append(
                AuditIssue(
                    level="error",
                    message=f"Missing runtime estimate for {job_id} with {job.threads} thread(s)",
                    jobs=(job_id,),
                )
            )
            continue
        start_s = 0.0
        if job.dependencies:
            start_s = max(scheduled_jobs[dependency].end_s for dependency in job.dependencies)
        start_s += float(job.delay_s)
        scheduled_jobs[job_id] = ScheduledWindow(
            job_id=job_id,
            label=job_id,
            kind="job",
            node=job.node,
            cores=job.cores,
            core_ids=job_core_ids[job_id],
            threads=job.threads,
            start_s=start_s,
            end_s=start_s + duration,
            duration_s=duration,
            dependencies=job.dependencies,
        )

    if errors:
        return AuditReport(
            model=model,
            jobs=scheduled_jobs,
            windows_by_node={NODE_A: [], NODE_B: []},
            errors=errors,
            warnings=warnings,
            makespan_s=None,
        )

    makespan_s = max((window.end_s for window in scheduled_jobs.values()), default=0.0)
    windows_by_node: dict[str, list[ScheduledWindow]] = {
        NODE_A: [window for window in scheduled_jobs.values() if window.node == NODE_A],
        NODE_B: [window for window in scheduled_jobs.values() if window.node == NODE_B],
    }
    if memcached_cores is not None:
        memcached_window = ScheduledWindow(
            job_id="memcached",
            label="memcached",
            kind="memcached",
            node=model.memcached.node,
            cores=model.memcached.cores,
            core_ids=memcached_cores,
            threads=model.memcached.threads,
            start_s=0.0,
            end_s=makespan_s,
            duration_s=makespan_s,
            dependencies=(),
        )
        windows_by_node.setdefault(model.memcached.node, []).append(memcached_window)

    for node, windows in windows_by_node.items():
        ordered_windows = sorted(windows, key=lambda item: (item.start_s, item.end_s, item.label))
        for index, left in enumerate(ordered_windows):
            for right in ordered_windows[index + 1 :]:
                overlap = _overlap_interval(left, right)
                if overlap is None:
                    continue
                if set(left.core_ids) & set(right.core_ids):
                    start_s, end_s = overlap
                    errors.append(
                        AuditIssue(
                            level="error",
                            node=node,
                            jobs=(left.job_id, right.job_id),
                            message=(
                                f"Core overlap on {node}: {left.label} {left.cores} and {right.label} {right.cores} "
                                f"overlap from {start_s:.2f}s to {end_s:.2f}s"
                            ),
                        )
                    )
        batch_windows = [window for window in ordered_windows if window.kind == "job"]
        for left, right in zip(batch_windows, batch_windows[1:]):
            gap_s = right.start_s - left.end_s
            if gap_s > EPSILON:
                warnings.append(
                    AuditIssue(
                        level="warning",
                        node=node,
                        jobs=(left.job_id, right.job_id),
                        message=(
                            f"Idle gap on {node}: {gap_s:.2f}s between {left.label} ending at "
                            f"{left.end_s:.2f}s and {right.label} starting at {right.start_s:.2f}s"
                        ),
                    )
                )
        windows_by_node[node] = ordered_windows

    return AuditReport(
        model=model,
        jobs=scheduled_jobs,
        windows_by_node=windows_by_node,
        errors=errors,
        warnings=warnings,
        makespan_s=makespan_s,
    )


def build_explicit_phases(model: ScheduleModel) -> list[dict[str, object]]:
    ordered_jobs = sorted(model.jobs.values(), key=lambda item: (item.order, item.job_id))
    topo_errors: list[AuditIssue] = []
    topo_order = _topological_job_order(model.jobs, topo_errors)
    topo_rank = {job_id: index for index, job_id in enumerate(topo_order)}
    grouped: dict[tuple[tuple[str, ...], int], list[str]] = {}
    for job in ordered_jobs:
        grouped.setdefault((job.dependencies, job.delay_s), []).append(job.job_id)
    phase_items = sorted(
        grouped.items(),
        key=lambda item: (
            min(topo_rank.get(job_id, 10**9) for job_id in item[1]),
            min(model.jobs[job_id].order for job_id in item[1]),
            item[1],
        ),
    )
    phases: list[dict[str, object]] = []
    for index, ((dependencies, delay_s), job_ids) in enumerate(phase_items, start=1):
        phase: dict[str, object] = {
            "id": f"phase-{index:02d}",
            "after": "start" if not dependencies else "jobs_complete",
            "delay_s": delay_s,
            "launch": list(job_ids),
        }
        if dependencies:
            phase["jobs_complete"] = list(dependencies)
        phases.append(phase)
    return phases


def build_policy_document(model: ScheduleModel) -> dict[str, object]:
    ordered_jobs = sorted(model.jobs.values(), key=lambda item: (item.order, item.job_id))
    return {
        "policy_name": model.policy_name,
        "memcached": {
            "node": model.memcached.node,
            "cores": model.memcached.cores,
            "threads": model.memcached.threads,
        },
        "job_overrides": {
            job.job_id: {
                "node": job.node,
                "cores": job.cores,
                "threads": job.threads,
            }
            for job in ordered_jobs
        },
        "phases": build_explicit_phases(model),
    }


def serialize_policy_document(model: ScheduleModel) -> str:
    return json.dumps(build_policy_document(model), indent=2) + "\n"


def write_policy_document(model: ScheduleModel, destination: Path) -> None:
    destination.write_text(serialize_policy_document(model), encoding="utf-8")


def render_audit_report(report: AuditReport) -> str:
    lines = [
        f"Policy: {report.model.policy_name}",
        f"Status: {report.status}",
        f"Estimated makespan: {_format_seconds(report.makespan_s)}",
        "",
        "Jobs:",
    ]
    for window in sorted(report.jobs.values(), key=lambda item: (item.start_s, item.end_s, item.label)):
        lines.append(
            "  - "
            + f"{window.label}: node={window.node} cores={window.cores} threads={window.threads} "
            + f"deps={dependency_text(window.dependencies)} start={window.start_s:.2f}s "
            + f"end={window.end_s:.2f}s duration={window.duration_s:.2f}s"
        )
    if not report.jobs:
        lines.append("  - no schedulable jobs")
    for node in (NODE_A, NODE_B):
        lines.append("")
        lines.append(f"{node}:")
        for window in report.windows_by_node.get(node, []):
            lines.append(
                "  - "
                + f"{window.label} [{window.cores}] {window.start_s:.2f}s -> {window.end_s:.2f}s"
            )
        if not report.windows_by_node.get(node):
            lines.append("  - no windows")
    if report.errors:
        lines.append("")
        lines.append("Errors:")
        for issue in report.errors:
            lines.append(f"  - {issue.message}")
    if report.warnings:
        lines.append("")
        lines.append("Warnings:")
        for issue in report.warnings:
            lines.append(f"  - {issue.message}")
    return "\n".join(lines)
