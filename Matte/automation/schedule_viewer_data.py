from __future__ import annotations

import json
from collections import defaultdict
from pathlib import Path
from typing import Any

from .audit import (
    AuditJob,
    AuditMemcached,
    AuditReport,
    ScheduleModel,
    audit_schedule,
    build_schedule_model,
    dependency_text,
    estimate_runtime_detail,
    estimate_runtime,
    load_runtime_table,
    load_schedule_model,
    parse_dependency_text,
)
from .catalog import JOB_CATALOG, NODE_A, NODE_B, NODE_CORE_COUNTS, suggested_core_sets
from .config import load_policy_config, load_run_queue_config
from .runtime_stats import RuntimeStatsIndex, load_runtime_stats


NODE_META = (
    {"lane_id": NODE_A, "label": "Node A", "short_label": "A"},
    {"lane_id": NODE_B, "label": "Node B", "short_label": "B"},
)


class _HybridRuntimeSource:
    def __init__(
        self,
        *,
        runtime_stats: RuntimeStatsIndex | None,
        runtime_stats_path: Path | None,
        csv_table,
    ) -> None:
        self.runtime_stats = runtime_stats
        self.runtime_stats_path = runtime_stats_path
        self.csv_table = csv_table
        self.csv_is_fallback = runtime_stats_path is not None
        if runtime_stats is not None:
            self.source_path = runtime_stats.source_path
            self.source_label = str(runtime_stats.source_path)
        elif csv_table is not None:
            self.source_path = csv_table.source_path
            self.source_label = str(csv_table.source_path)
        else:
            self.source_path = runtime_stats_path or Path("runtime_stats.json")
            self.source_label = str(self.source_path)

    def estimate(
        self,
        *,
        job_id: str,
        node: str | None,
        threads: int,
        memcached_node: str | None,
    ):
        if self.runtime_stats is not None and node is not None and memcached_node is not None:
            estimate = self.runtime_stats.estimate(
                job_id=job_id,
                node=node,
                threads=threads,
                memcached_node=memcached_node,
            )
            if estimate is not None:
                return estimate
        if self.csv_table is None:
            return None
        duration = estimate_runtime(job_id, threads, self.csv_table)
        if duration is None:
            return None
        match_type = "csv" if self.csv_is_fallback else "exact"
        message = None
        if self.csv_is_fallback:
            message = (
                f"Using CSV fallback for {job_id} with {threads} thread(s); "
                "no run-derived runtime sample matched."
            )
        return {
            "duration_s": duration,
            "source": str(self.csv_table.source_path),
            "match_type": match_type,
            "message": message,
        }


def list_schedule_view(
    *,
    schedules_dir: Path,
    schedule_queue_path: Path | None,
    times_csv_path: Path,
    runtime_stats_path: Path | None = None,
) -> dict[str, object]:
    schedule_paths = _discover_schedule_paths(schedules_dir)
    queue_payload, queue_paths, queue_error = _load_queue_listing(schedule_queue_path)
    for path in queue_paths:
        schedule_paths.setdefault(_schedule_id_for_path(path, schedules_dir, schedule_queue_path), path)

    queued_by_id: dict[str, list[dict[str, object]]] = defaultdict(list)
    if queue_payload is not None:
        for entry in queue_payload["entries"]:
            schedule_id = _schedule_id_for_path(Path(str(entry["policy_path"])), schedules_dir, schedule_queue_path)
            queued_by_id[schedule_id].append(
                {
                    "queue_index": entry["queue_index"],
                    "runs": entry["runs"],
                }
            )

    schedules = []
    for schedule_id, path in sorted(schedule_paths.items(), key=lambda item: item[0]):
        schedules.append(_schedule_listing_entry(schedule_id, path, queued_by_id.get(schedule_id, [])))

    queue_entries = []
    if queue_payload is not None:
        for entry in queue_payload["entries"]:
            path = Path(str(entry["policy_path"]))
            schedule_id = _schedule_id_for_path(path, schedules_dir, schedule_queue_path)
            queue_entries.append(
                {
                    "queue_index": entry["queue_index"],
                    "schedule_id": schedule_id,
                    "policy_path": str(path),
                    "label": path.name,
                    "runs": entry["runs"],
                }
            )

    default_schedule_id = queue_entries[0]["schedule_id"] if queue_entries else (schedules[0]["schedule_id"] if schedules else None)
    return {
        "schedules": schedules,
        "queue": {
            "queue_name": queue_payload["queue_name"] if queue_payload is not None else None,
            "path": str(schedule_queue_path) if schedule_queue_path is not None else None,
            "entries": queue_entries,
            "error": queue_error,
        },
        "default_schedule_id": default_schedule_id,
        "metrics_source": _runtime_source_label(runtime_stats_path, times_csv_path),
        "catalog": _catalog_view(),
    }


def load_schedule_view(
    *,
    schedules_dir: Path,
    schedule_queue_path: Path | None,
    times_csv_path: Path,
    runtime_stats_path: Path | None = None,
    schedule_id: str,
) -> dict[str, object]:
    schedule_path = _resolve_schedule_id(
        schedule_id=schedule_id,
        schedules_dir=schedules_dir,
        schedule_queue_path=schedule_queue_path,
    )
    model = load_schedule_model(str(schedule_path))
    return _build_schedule_payload(
        model=model,
        times_csv_path=times_csv_path,
        runtime_stats_path=runtime_stats_path,
        schedule_id=schedule_id,
        schedule_path=schedule_path,
    )


def preview_schedule_view(
    *,
    times_csv_path: Path,
    runtime_stats_path: Path | None = None,
    payload: dict[str, Any],
) -> dict[str, object]:
    model = _model_from_editor_payload(payload)
    schedule_id = str(payload.get("schedule_id") or "preview")
    return _build_schedule_payload(
        model=model,
        times_csv_path=times_csv_path,
        runtime_stats_path=runtime_stats_path,
        schedule_id=schedule_id,
        schedule_path=None,
    )


def _build_schedule_payload(
    *,
    model: ScheduleModel,
    times_csv_path: Path,
    runtime_stats_path: Path | None,
    schedule_id: str,
    schedule_path: Path | None,
) -> dict[str, object]:
    runtime_source = _load_runtime_source(runtime_stats_path, times_csv_path)
    report = audit_schedule(model, runtime_source)
    return {
        "schedule_id": schedule_id,
        "path": str(schedule_path) if schedule_path is not None else None,
        "policy_name": model.policy_name,
        "editor": _editor_view(model, runtime_source),
        "prediction": _prediction_view(report),
        "yaml": serialize_simple_schedule(model),
        "metrics_source": runtime_source.source_label,
        "catalog": _catalog_view(),
    }


def _load_runtime_source(runtime_stats_path: Path | None, times_csv_path: Path) -> _HybridRuntimeSource:
    runtime_stats = None
    if runtime_stats_path is not None and runtime_stats_path.exists():
        runtime_stats = load_runtime_stats(runtime_stats_path)
    csv_table = load_runtime_table(str(times_csv_path)) if times_csv_path.exists() else None
    return _HybridRuntimeSource(
        runtime_stats=runtime_stats,
        runtime_stats_path=runtime_stats_path,
        csv_table=csv_table,
    )


def _runtime_source_label(runtime_stats_path: Path | None, times_csv_path: Path) -> str:
    if runtime_stats_path is not None and runtime_stats_path.exists():
        return str(runtime_stats_path)
    return str(times_csv_path)


def serialize_simple_schedule(model: ScheduleModel) -> str:
    lines = [
        f"policy_name: {_yaml_string(model.policy_name)}",
        "memcached:",
        f"  node: {_yaml_string(model.memcached.node)}",
        f"  cores: {_yaml_string(model.memcached.cores)}",
        f"  threads: {model.memcached.threads}",
        "jobs:",
    ]
    for job in sorted(model.jobs.values(), key=lambda item: (item.order, item.job_id)):
        lines.extend(
            [
                f"  {job.job_id}:",
                f"    node: {_yaml_string(job.node)}",
                f"    cores: {_yaml_string(job.cores)}",
                f"    threads: {job.threads}",
                f"    after: {_yaml_after(job.dependencies)}",
            ]
        )
        if job.delay_s:
            lines.append(f"    delay_s: {job.delay_s}")
    return "\n".join(lines) + "\n"


def _discover_schedule_paths(schedules_dir: Path) -> dict[str, Path]:
    if not schedules_dir.exists():
        return {}
    paths: dict[str, Path] = {}
    for path in schedules_dir.iterdir():
        if not path.is_file() or path.name.startswith(".") or path.suffix.lower() not in {".yaml", ".yml"}:
            continue
        paths[_schedule_id_for_path(path, schedules_dir, None)] = path.resolve()
    return paths


def _load_queue_listing(schedule_queue_path: Path | None) -> tuple[dict[str, object] | None, list[Path], str | None]:
    if schedule_queue_path is None or not schedule_queue_path.exists():
        return None, [], None
    try:
        queue = load_run_queue_config(str(schedule_queue_path))
    except Exception as exc:
        return None, [], str(exc)
    entries = [
        {
            "queue_index": index,
            "policy_path": str(entry.policy_path),
            "runs": entry.runs,
        }
        for index, entry in enumerate(queue.entries)
    ]
    return {"queue_name": queue.queue_name, "entries": entries}, [entry.policy_path for entry in queue.entries], None


def _schedule_listing_entry(
    schedule_id: str,
    path: Path,
    queue_entries: list[dict[str, object]],
) -> dict[str, object]:
    policy_name = None
    error = None
    try:
        policy = load_policy_config(str(path))
    except Exception as exc:
        error = str(exc)
    else:
        policy_name = policy.policy_name
    return {
        "schedule_id": schedule_id,
        "label": path.name,
        "path": str(path),
        "policy_name": policy_name,
        "in_queue": bool(queue_entries),
        "queued_runs": sum(int(entry["runs"]) for entry in queue_entries),
        "queue_entries": queue_entries,
        "error": error,
    }


def _resolve_schedule_id(
    *,
    schedule_id: str,
    schedules_dir: Path,
    schedule_queue_path: Path | None,
) -> Path:
    candidates = _discover_schedule_paths(schedules_dir)
    _queue_payload, queue_paths, _queue_error = _load_queue_listing(schedule_queue_path)
    for path in queue_paths:
        candidates.setdefault(_schedule_id_for_path(path, schedules_dir, schedule_queue_path), path)
    path = candidates.get(schedule_id)
    if path is None:
        raise FileNotFoundError(f"Schedule not found: {schedule_id}")
    return path.resolve()


def _schedule_id_for_path(path: Path, schedules_dir: Path, schedule_queue_path: Path | None) -> str:
    resolved = path.resolve()
    try:
        return resolved.relative_to(schedules_dir.resolve()).as_posix()
    except ValueError:
        pass
    if schedule_queue_path is not None:
        try:
            return resolved.relative_to(schedule_queue_path.resolve().parent).as_posix()
        except ValueError:
            pass
    return resolved.name


def _catalog_view() -> dict[str, object]:
    return {
        "nodes": [
            {
                "node_id": node_id,
                "label": "Node A" if node_id == NODE_A else "Node B",
                "core_count": NODE_CORE_COUNTS[node_id],
                "core_suggestions": list(suggested_core_sets(node_id)),
            }
            for node_id in (NODE_A, NODE_B)
        ],
        "jobs": [
            {
                "job_id": entry.job_id,
                "suite": entry.suite,
                "program": entry.program,
                "default_node": entry.default_node,
                "default_cores": entry.default_cores,
                "default_threads": entry.default_threads,
                "core_suggestions": {
                    NODE_A: list(entry.suggested_cores_by_node[NODE_A]),
                    NODE_B: list(entry.suggested_cores_by_node[NODE_B]),
                },
            }
            for entry in JOB_CATALOG.values()
        ],
    }


def _editor_view(model: ScheduleModel, runtime_source) -> dict[str, object]:
    return {
        "policy_name": model.policy_name,
        "memcached": {
            "node": model.memcached.node,
            "cores": model.memcached.cores,
            "threads": model.memcached.threads,
        },
        "jobs": [
            {
                "job_id": job.job_id,
                "order": index + 1,
                "node": job.node,
                "cores": job.cores,
                "threads": job.threads,
                "after": dependency_text(job.dependencies),
                "delay_s": job.delay_s,
                "runtime_s": _editor_runtime_s(model, job, runtime_source),
            }
            for index, job in enumerate(sorted(model.jobs.values(), key=lambda item: (item.order, item.job_id)))
        ],
    }


def _editor_runtime_s(model: ScheduleModel, job: AuditJob, runtime_source) -> float | None:
    estimate = estimate_runtime_detail(
        job.job_id,
        job.threads,
        runtime_source,
        node=job.node,
        memcached_node=model.memcached.node,
    )
    return estimate.duration_s if estimate is not None else None


def _prediction_view(report: AuditReport) -> dict[str, object]:
    return {
        "status": report.status,
        "makespan_s": report.makespan_s,
        "errors": [_issue_view(issue) for issue in report.errors],
        "warnings": [_issue_view(issue) for issue in report.warnings],
        "timeline": _timeline_view(report),
    }


def _timeline_view(report: AuditReport) -> dict[str, object]:
    lanes = {
        meta["lane_id"]: {
            "lane_id": meta["lane_id"],
            "label": meta["label"],
            "short_label": meta["short_label"],
            "segments": [],
            "node_names": [str(meta["lane_id"])],
        }
        for meta in NODE_META
    }
    max_end_s: float | None = None
    for node_id in (NODE_A, NODE_B):
        for window in report.windows_by_node.get(node_id, []):
            segment = _window_segment(window)
            lanes[node_id]["segments"].append(segment)
            max_end_s = segment["end_s"] if max_end_s is None else max(max_end_s, segment["end_s"])
    for lane in lanes.values():
        lane["segments"].sort(key=lambda item: (float(item["start_s"]), str(item["job_id"])))
    return {
        "has_data": any(lane["segments"] for lane in lanes.values()),
        "anchor_started_at": None,
        "max_end_s": max_end_s,
        "lanes": [lanes[NODE_A], lanes[NODE_B]],
    }


def _window_segment(window) -> dict[str, object]:
    return {
        "job_id": window.job_id,
        "label": window.label,
        "kind": window.kind,
        "status": "planned" if window.kind == "job" else "running",
        "start_s": window.start_s,
        "end_s": window.end_s,
        "duration_s": window.duration_s,
        "planned_node": window.node,
        "cores": window.cores,
        "core_ids": list(window.core_ids),
        "threads": window.threads,
        "raw_node_name": window.node,
        "started_at": None,
        "finished_at": None,
    }


def _issue_view(issue) -> dict[str, object]:
    return {
        "level": issue.level,
        "message": issue.message,
        "node": issue.node,
        "jobs": list(issue.jobs),
    }


def _model_from_editor_payload(payload: dict[str, Any]) -> ScheduleModel:
    parse_errors: list[str] = []
    editor = payload.get("editor", payload)
    if not isinstance(editor, dict):
        raise ValueError("Preview payload must contain an editor object")

    policy_name = str(editor.get("policy_name") or "planner-policy").strip() or "planner-policy"
    memcached_raw = editor.get("memcached", {})
    if not isinstance(memcached_raw, dict):
        parse_errors.append("memcached must be an object")
        memcached_raw = {}
    memcached = AuditMemcached(
        node=str(memcached_raw.get("node") or NODE_B).strip(),
        cores=str(memcached_raw.get("cores") or "0").strip(),
        threads=_coerce_int(memcached_raw.get("threads", 1), "memcached.threads", parse_errors, 1),
    )

    raw_jobs = editor.get("jobs", [])
    if isinstance(raw_jobs, dict):
        job_items = list(raw_jobs.values())
    elif isinstance(raw_jobs, list):
        job_items = raw_jobs
    else:
        parse_errors.append("jobs must be a list")
        job_items = []

    jobs: dict[str, AuditJob] = {}
    for index, raw_job in enumerate(job_items):
        if not isinstance(raw_job, dict):
            parse_errors.append(f"jobs[{index}] must be an object")
            continue
        job_id = str(raw_job.get("job_id") or "").strip()
        if not job_id:
            parse_errors.append(f"jobs[{index}].job_id is required")
            continue
        catalog_entry = JOB_CATALOG.get(job_id)
        if catalog_entry is None:
            parse_errors.append(f"Unknown job: {job_id}")
            continue
        dependencies = _dependencies_from_preview(raw_job.get("after", "start"), f"jobs.{job_id}.after", parse_errors)
        jobs[job_id] = AuditJob(
            job_id=job_id,
            node=str(raw_job.get("node") or catalog_entry.default_node).strip(),
            cores=str(raw_job.get("cores") or catalog_entry.default_cores).strip(),
            threads=_coerce_int(raw_job.get("threads", catalog_entry.default_threads), f"jobs.{job_id}.threads", parse_errors, catalog_entry.default_threads),
            dependencies=dependencies,
            delay_s=_coerce_int(raw_job.get("delay_s", 0), f"jobs.{job_id}.delay_s", parse_errors, 0),
            order=_coerce_int(raw_job.get("order", index + 1), f"jobs.{job_id}.order", parse_errors, index + 1),
        )

    return build_schedule_model(
        policy_name=policy_name,
        memcached=memcached,
        jobs=jobs,
        parse_errors=tuple(parse_errors),
    )


def _dependencies_from_preview(raw: Any, field_name: str, parse_errors: list[str]) -> tuple[str, ...]:
    if isinstance(raw, list):
        dependencies: list[str] = []
        for index, item in enumerate(raw):
            if not isinstance(item, str) or not item.strip():
                parse_errors.append(f"{field_name}[{index}] must be a non-empty string")
                continue
            dependencies.append(item.strip())
        return tuple(dependencies)
    if raw is None:
        return ()
    return parse_dependency_text(str(raw))


def _coerce_int(raw: Any, field_name: str, parse_errors: list[str], fallback: int) -> int:
    if isinstance(raw, bool):
        parse_errors.append(f"{field_name} must be an integer")
        return fallback
    try:
        return int(str(raw).strip())
    except (TypeError, ValueError):
        parse_errors.append(f"{field_name} must be an integer")
        return fallback


def _yaml_string(value: str) -> str:
    return json.dumps(str(value))


def _yaml_after(dependencies: tuple[str, ...]) -> str:
    if not dependencies:
        return _yaml_string("start")
    if len(dependencies) == 1:
        return _yaml_string(dependencies[0])
    return "[" + ", ".join(_yaml_string(dependency) for dependency in dependencies) + "]"
