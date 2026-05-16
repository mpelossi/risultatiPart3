from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Any

from .catalog import JOB_CATALOG, NODE_A, NODE_B, validate_node_core_spec
from .config import PolicyConfig, load_policy_config
from .metrics import MCPERF_SYNC_ERROR_MARKERS, SLO_P95_US, summarize_pods
from .results import resolve_experiment_root, sort_best_runs
from .timing import load_pod_payload
from .utils import format_run_id_label, parse_run_id_timestamp, resolve_existing_run_results_path


TIME_FORMAT = "%Y-%m-%dT%H:%M:%SZ"

LANE_META = (
    {"lane_id": NODE_A, "label": "Node A", "short_label": "A"},
    {"lane_id": NODE_B, "label": "Node B", "short_label": "B"},
)


def list_run_experiments(results_root: Path) -> list[dict[str, object]]:
    if not results_root.exists():
        return []
    experiments: list[dict[str, object]] = []
    for experiment_root in sorted(path for path in results_root.iterdir() if path.is_dir() and not path.name.startswith("__")):
        run_count = sum(1 for path in experiment_root.iterdir() if path.is_dir())
        experiments.append(
            {
                "experiment_id": experiment_root.name,
                "run_count": run_count,
            }
        )
    return experiments


def load_experiment_view(results_root: Path, experiment_id: str) -> dict[str, object]:
    experiment_root = resolve_experiment_root(results_root, experiment_id)
    run_dirs = [path for path in experiment_root.iterdir() if path.is_dir()]
    runs = [_build_run_view(run_dir, experiment_id=experiment_id) for run_dir in run_dirs]
    runs.sort(key=_history_sort_key, reverse=True)

    eligible_runs = [run for run in runs if run.get("eligible_for_best")]
    best_run_id = None
    if eligible_runs:
        best_run_id = str(sort_best_runs(eligible_runs)[0].get("run_id"))

    return {
        "experiment_id": experiment_id,
        "runs": runs,
        "best_run_id": best_run_id,
        "run_count": len(runs),
    }


def load_run_view(results_root: Path, experiment_id: str, run_id: str) -> dict[str, object]:
    experiment_root = resolve_experiment_root(results_root, experiment_id)
    run_dir = experiment_root / run_id
    if not run_dir.exists() or not run_dir.is_dir():
        raise FileNotFoundError(f"Run not found: {run_dir}")
    return _build_run_view(run_dir, experiment_id=experiment_id)


def load_run_policy_view(
    results_root: Path,
    schedules_dir: Path,
    experiment_id: str,
    run_id: str,
) -> dict[str, object]:
    experiment_root = resolve_experiment_root(results_root, experiment_id)
    run_dir = experiment_root / run_id
    if not run_dir.exists() or not run_dir.is_dir():
        raise FileNotFoundError(f"Run not found: {run_dir}")

    policy_path = run_dir / "policy.yaml"
    if not policy_path.exists():
        return {
            "experiment_id": experiment_id,
            "run_id": run_id,
            "policy_yaml": "",
            "matches": [],
            "match_status": "missing_policy",
            "errors": [f"policy.yaml is missing for run {run_id}."],
        }

    try:
        policy_yaml = policy_path.read_text(encoding="utf-8")
    except OSError as exc:
        return {
            "experiment_id": experiment_id,
            "run_id": run_id,
            "policy_yaml": "",
            "matches": [],
            "match_status": "parse_error",
            "errors": [f"policy.yaml could not be read: {exc}"],
        }

    run_policy, error = _parse_policy_mapping(policy_yaml, policy_path)
    if error is not None:
        return {
            "experiment_id": experiment_id,
            "run_id": run_id,
            "policy_yaml": policy_yaml,
            "matches": [],
            "match_status": "parse_error",
            "errors": [error],
        }

    assert run_policy is not None
    run_fingerprint = _policy_fingerprint(run_policy)
    matches: list[dict[str, object]] = []
    errors: list[str] = []
    for schedule_path in _iter_schedule_policy_paths(schedules_dir):
        try:
            schedule_yaml = schedule_path.read_text(encoding="utf-8")
        except OSError as exc:
            errors.append(f"{schedule_path.name} could not be read: {exc}")
            continue
        schedule_policy, schedule_error = _parse_policy_mapping(schedule_yaml, schedule_path)
        if schedule_error is not None:
            errors.append(schedule_error)
            continue
        assert schedule_policy is not None
        if _policy_fingerprint(schedule_policy) == run_fingerprint:
            matches.append(
                {
                    "schedule_id": _schedule_id(schedule_path, schedules_dir),
                    "label": schedule_path.name,
                    "path": str(schedule_path.resolve()),
                }
            )

    return {
        "experiment_id": experiment_id,
        "run_id": run_id,
        "policy_yaml": policy_yaml,
        "matches": matches,
        "match_status": "matched" if matches else "unmatched",
        "errors": errors,
    }


def _build_run_view(run_dir: Path, *, experiment_id: str) -> dict[str, object]:
    run_id = run_dir.name
    run_label = format_run_id_label(run_id)
    parsed_run_timestamp = parse_run_id_timestamp(run_id)
    timestamp_iso = parsed_run_timestamp.isoformat() if parsed_run_timestamp is not None else None

    summary_path = run_dir / "summary.json"
    results_path = run_dir / "results.json"
    pods_path = run_dir / "pods.json"
    policy_path = run_dir / "policy.yaml"
    mcperf_path = run_dir / "mcperf.txt"
    node_platforms_path = run_dir / "node_platforms.json"
    snapshot_path = _existing_snapshot_path(run_dir)

    artifact_flags: dict[str, bool] = {
        "summary": summary_path.exists(),
        "results": results_path.exists(),
        "pods": pods_path.exists(),
        "policy": policy_path.exists(),
        "mcperf": mcperf_path.exists(),
        "node_platforms": node_platforms_path.exists(),
        "snapshot": snapshot_path is not None,
    }
    issues: list[str] = []

    policy = None
    policy_name = None
    expected_jobs = set(JOB_CATALOG)
    planned_job_nodes = {job_id: entry.default_node for job_id, entry in JOB_CATALOG.items()}
    planned_job_cores = {job_id: entry.default_cores for job_id, entry in JOB_CATALOG.items()}
    planned_job_threads = {job_id: entry.default_threads for job_id, entry in JOB_CATALOG.items()}
    planned_memcached_node: str | None = None
    planned_memcached_cores: str | None = None
    planned_memcached_threads: int | None = None

    if policy_path.exists():
        try:
            policy = load_policy_config(str(policy_path))
        except Exception as exc:
            issues.append(f"policy.yaml could not be parsed: {exc}")
        else:
            policy_name = policy.policy_name
            expected_jobs = set(policy.job_overrides) or set(JOB_CATALOG)
            planned_job_nodes = _planned_job_nodes(policy)
            planned_job_cores = _planned_job_cores(policy)
            planned_job_threads = _planned_job_threads(policy)
            planned_memcached_node = policy.memcached.node
            planned_memcached_cores = policy.memcached.cores
            planned_memcached_threads = policy.memcached.threads

    summary_payload = None
    is_reconstructed = False
    if summary_path.exists():
        try:
            loaded_summary = json.loads(summary_path.read_text(encoding="utf-8"))
        except json.JSONDecodeError as exc:
            issues.append(f"summary.json could not be parsed: {exc}")
        else:
            if isinstance(loaded_summary, dict):
                summary_payload = loaded_summary
            else:
                issues.append("summary.json does not contain an object.")

    if summary_payload is None and snapshot_path is not None:
        summary_payload, summary_issues = _build_reconstructed_summary(
            snapshot_path,
            mcperf_path if mcperf_path.exists() else None,
            expected_jobs=expected_jobs,
            experiment_id=experiment_id,
            run_id=run_id,
            policy_name=policy_name or "unknown",
        )
        issues.extend(summary_issues)
        is_reconstructed = True
    elif summary_payload is None:
        measurement_summary = _parse_mcperf_output_tolerant(mcperf_path if mcperf_path.exists() else None)
        issues.append("No results.json or pods.json snapshot found.")
        issues.extend(measurement_summary["issues"])
        summary_payload = _build_artifact_only_summary(
            experiment_id=experiment_id,
            run_id=run_id,
            policy_name=policy_name or "unknown",
            expected_jobs=expected_jobs,
            measurement_summary=measurement_summary,
        )
    else:
        measurement_summary = _parse_mcperf_output_tolerant(mcperf_path if mcperf_path.exists() else None)
        if measurement_summary["measurement_status"] != "ok":
            issues.extend(measurement_summary["issues"])
            summary_payload["measurement_status"] = measurement_summary["measurement_status"]
            summary_payload["max_observed_p95_us"] = measurement_summary["max_p95_us"]
            summary_payload["slo_violations"] = measurement_summary["slo_violations"]
            summary_payload["sample_count"] = len(measurement_summary["samples"])
            if summary_payload.get("overall_status") == "pass":
                summary_payload["overall_status"] = "infra_fail"

    payload = load_pod_payload(snapshot_path) if snapshot_path is not None else None
    memcached_timing = _extract_memcached_timing(payload) if payload is not None else None
    node_platforms = None
    raw_node_platforms = summary_payload.get("node_platforms")
    if isinstance(raw_node_platforms, dict):
        node_platforms = raw_node_platforms
    elif raw_node_platforms is not None:
        issues.append("summary.json node_platforms does not contain an object.")
    if node_platforms is None and node_platforms_path.exists():
        try:
            loaded_node_platforms = json.loads(node_platforms_path.read_text(encoding="utf-8"))
        except json.JSONDecodeError as exc:
            issues.append(f"node_platforms.json could not be parsed: {exc}")
        else:
            if isinstance(loaded_node_platforms, dict):
                node_platforms = loaded_node_platforms
            else:
                issues.append("node_platforms.json does not contain an object.")

    jobs = _build_jobs_view(
        summary_payload.get("jobs"),
        expected_jobs=expected_jobs,
        planned_job_nodes=planned_job_nodes,
        planned_job_cores=planned_job_cores,
        planned_job_threads=planned_job_threads,
    )
    timeline = _build_timeline(
        jobs,
        planned_memcached_node=planned_memcached_node,
        planned_memcached_cores=planned_memcached_cores,
        planned_memcached_threads=planned_memcached_threads,
        memcached_summary=_ensure_mapping(summary_payload.get("memcached")),
        memcached_timing=memcached_timing,
    )

    measurement_status = str(summary_payload.get("measurement_status") or "missing")
    makespan_s = _safe_float(summary_payload.get("makespan_s"))
    max_p95 = _safe_float(summary_payload.get("max_observed_p95_us"))
    timing_complete = bool(summary_payload.get("timing_complete"))
    overall_status = str(summary_payload.get("overall_status") or "unknown")

    if not timing_complete and artifact_flags["snapshot"]:
        issues.append("Pod timing data is incomplete.")

    run_view = {
        "experiment_id": str(summary_payload.get("experiment_id") or experiment_id),
        "run_id": str(summary_payload.get("run_id") or run_id),
        "run_label": run_label,
        "timestamp_iso": timestamp_iso,
        "policy_name": str(summary_payload.get("policy_name") or policy_name or "unknown"),
        "overall_status": overall_status,
        "measurement_status": measurement_status,
        "sample_count": _safe_int(summary_payload.get("sample_count")),
        "makespan_s": makespan_s,
        "max_observed_p95_us": max_p95,
        "timing_complete": timing_complete,
        "completed_job_count": _safe_int(summary_payload.get("completed_job_count")),
        "expected_job_count": _safe_int(summary_payload.get("expected_job_count")) or len(expected_jobs),
        "slo_violations": _safe_int(summary_payload.get("slo_violations")),
        "is_reconstructed": is_reconstructed,
        "eligible_for_best": _is_best_run_candidate(
            overall_status=overall_status,
            measurement_status=measurement_status,
            timing_complete=timing_complete,
            makespan_s=makespan_s,
            max_p95_us=max_p95,
            sample_count=_safe_int(summary_payload.get("sample_count")),
        ),
        "artifact_flags": artifact_flags,
        "issues": _dedupe_preserve_order(issues),
        "jobs": jobs,
        "timeline": timeline,
        "node_platforms": node_platforms,
        "run_dir": str(run_dir),
    }
    return run_view


def _history_sort_key(run: dict[str, object]) -> tuple[float, str]:
    run_id = str(run.get("run_id") or "")
    parsed = parse_run_id_timestamp(run_id)
    timestamp = parsed.timestamp() if parsed is not None else float("-inf")
    return (timestamp, run_id)


def _parse_policy_mapping(raw: str, path: Path) -> tuple[dict[str, Any] | None, str | None]:
    try:
        import yaml  # type: ignore

        loaded = yaml.safe_load(raw)
    except ModuleNotFoundError:
        try:
            loaded = json.loads(raw)
        except json.JSONDecodeError as exc:
            return None, (
                f"{path.name} is not valid JSON-compatible YAML. "
                "Install PyYAML or keep configs in JSON syntax."
            )
    except Exception as exc:
        return None, f"{path.name} could not be parsed: {exc}"
    if not isinstance(loaded, dict):
        return None, f"{path.name} must contain a top-level mapping."
    return loaded, None


def _policy_fingerprint(policy: dict[str, Any]) -> str:
    return json.dumps(policy, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def _iter_schedule_policy_paths(schedules_dir: Path):
    if not schedules_dir.exists():
        return
    for path in sorted(schedules_dir.iterdir()):
        if not path.is_file() or path.name.startswith(".") or path.suffix.lower() not in {".yaml", ".yml"}:
            continue
        yield path


def _schedule_id(path: Path, schedules_dir: Path) -> str:
    try:
        return path.resolve().relative_to(schedules_dir.resolve()).as_posix()
    except ValueError:
        return path.name


def _existing_snapshot_path(run_dir: Path) -> Path | None:
    path = resolve_existing_run_results_path(run_dir)
    if path.exists():
        return path
    return None


def _build_reconstructed_summary(
    snapshot_path: Path,
    mcperf_path: Path | None,
    *,
    expected_jobs: set[str],
    experiment_id: str,
    run_id: str,
    policy_name: str,
) -> tuple[dict[str, object], list[str]]:
    issues: list[str] = []
    pod_summary = summarize_pods(snapshot_path, expected_jobs)
    measurement_summary = _parse_mcperf_output_tolerant(mcperf_path)
    issues.extend(measurement_summary["issues"])

    jobs = pod_summary["jobs"]
    all_jobs_completed = all(
        _ensure_mapping(job_summary).get("status") == "completed" for job_summary in _ensure_mapping(jobs).values()
    )
    measurement_status = measurement_summary["measurement_status"]

    if pod_summary["memcached"] is None or measurement_status != "ok":
        overall_status = "infra_fail"
    elif not all_jobs_completed:
        overall_status = "job_fail"
    elif (measurement_summary["slo_violations"] or 0) > 0:
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
        "max_observed_p95_us": measurement_summary["max_p95_us"],
        "slo_violations": measurement_summary["slo_violations"],
        "measurement_status": measurement_status,
        "sample_count": len(measurement_summary["samples"]),
    }
    return summary, issues


def _build_artifact_only_summary(
    *,
    experiment_id: str,
    run_id: str,
    policy_name: str,
    expected_jobs: set[str],
    measurement_summary: dict[str, object],
) -> dict[str, object]:
    jobs = {job_id: {"job_id": job_id, "status": "missing"} for job_id in sorted(expected_jobs)}
    return {
        "experiment_id": experiment_id,
        "run_id": run_id,
        "policy_name": policy_name,
        "overall_status": "unknown",
        "memcached": None,
        "jobs": jobs,
        "makespan_s": None,
        "completed_job_count": 0,
        "expected_job_count": len(expected_jobs),
        "timing_complete": False,
        "max_observed_p95_us": measurement_summary["max_p95_us"],
        "slo_violations": measurement_summary["slo_violations"],
        "measurement_status": measurement_summary["measurement_status"],
        "sample_count": len(measurement_summary["samples"]),
    }


def _parse_mcperf_output_tolerant(path: Path | None) -> dict[str, object]:
    if path is None or not path.exists():
        return {
            "samples": [],
            "max_p95_us": None,
            "slo_violations": None,
            "measurement_status": "missing",
            "issues": ["mcperf.txt is missing."],
        }

    lines = path.read_text(encoding="utf-8").splitlines()
    if not lines:
        return {
            "samples": [],
            "max_p95_us": None,
            "slo_violations": None,
            "measurement_status": "empty",
            "issues": ["mcperf.txt is empty."],
        }
    if any(any(marker in line for marker in MCPERF_SYNC_ERROR_MARKERS) for line in lines):
        return {
            "samples": [],
            "max_p95_us": None,
            "slo_violations": None,
            "measurement_status": "parse_error",
            "issues": ["mcperf.txt contains mcperf agent synchronization errors."],
        }

    header = lines[0].split()
    if "p95" not in header:
        return {
            "samples": [],
            "max_p95_us": None,
            "slo_violations": None,
            "measurement_status": "parse_error",
            "issues": ["mcperf.txt is missing the p95 column."],
        }

    p95_index = header.index("p95")
    samples: list[dict[str, object]] = []
    p95_values: list[float] = []
    for line_number, line in enumerate(lines[1:], start=2):
        if not line.strip():
            continue
        columns = line.split()
        if len(columns) <= p95_index:
            continue
        try:
            p95_value = float(columns[p95_index])
        except ValueError:
            return {
                "samples": [],
                "max_p95_us": None,
                "slo_violations": None,
                "measurement_status": "parse_error",
                "issues": [f"mcperf.txt contains malformed latency data on line {line_number}."],
            }
        samples.append({"type": columns[0], "p95_us": p95_value, "raw": line})
        p95_values.append(p95_value)

    if not samples:
        return {
            "samples": [],
            "max_p95_us": None,
            "slo_violations": None,
            "measurement_status": "no_samples",
            "issues": ["mcperf.txt contains no usable samples."],
        }

    return {
        "samples": samples,
        "max_p95_us": max(p95_values),
        "slo_violations": sum(1 for value in p95_values if value > SLO_P95_US),
        "measurement_status": "ok",
        "issues": [],
    }


def _build_jobs_view(
    raw_jobs: Any,
    *,
    expected_jobs: set[str],
    planned_job_nodes: dict[str, str],
    planned_job_cores: dict[str, str],
    planned_job_threads: dict[str, int],
) -> dict[str, dict[str, object]]:
    jobs_map = _ensure_mapping(raw_jobs)
    all_job_ids = sorted(set(expected_jobs) | set(jobs_map))
    jobs: dict[str, dict[str, object]] = {}
    for job_id in all_job_ids:
        raw_job = _ensure_mapping(jobs_map.get(job_id))
        actual_node = _string_or_none(raw_job.get("node_name"))
        canonical_node = _canonical_node_name(actual_node) or planned_job_nodes.get(job_id)
        planned_node = planned_job_nodes.get(job_id)
        planned_cores = planned_job_cores.get(job_id)
        jobs[job_id] = {
            "job_id": job_id,
            "status": str(raw_job.get("status") or "missing"),
            "phase": _string_or_none(raw_job.get("phase")),
            "node_name": actual_node,
            "canonical_node": canonical_node,
            "planned_node": planned_node,
            "planned_cores": planned_cores,
            "planned_core_ids": list(_parse_core_ids(planned_cores, planned_node)),
            "planned_threads": planned_job_threads.get(job_id),
            "pod_name": _string_or_none(raw_job.get("pod_name")),
            "pod_ip": _string_or_none(raw_job.get("pod_ip")),
            "started_at": _string_or_none(raw_job.get("started_at")),
            "finished_at": _string_or_none(raw_job.get("finished_at")),
            "runtime_s": _safe_float(raw_job.get("runtime_s")),
        }
    return jobs


def _build_timeline(
    jobs: dict[str, dict[str, object]],
    *,
    planned_memcached_node: str | None,
    planned_memcached_cores: str | None,
    planned_memcached_threads: int | None,
    memcached_summary: dict[str, object],
    memcached_timing: dict[str, object] | None,
) -> dict[str, object]:
    job_segments: list[dict[str, object]] = []
    anchor_time: datetime | None = None

    for job_id, job in sorted(jobs.items()):
        started_at = _parse_time(job.get("started_at"))
        finished_at = _parse_time(job.get("finished_at"))
        if started_at is None or finished_at is None:
            continue
        if anchor_time is None or started_at < anchor_time:
            anchor_time = started_at
        job_segments.append(
            {
                "job_id": job_id,
                "label": job_id,
                "kind": "job",
                "status": job.get("status"),
                "lane_id": job.get("canonical_node"),
                "planned_node": job.get("planned_node"),
                "cores": job.get("planned_cores"),
                "core_ids": job.get("planned_core_ids"),
                "threads": job.get("planned_threads"),
                "raw_node_name": job.get("node_name"),
                "started_at": job.get("started_at"),
                "finished_at": job.get("finished_at"),
                "_start": started_at,
                "_end": finished_at,
            }
        )

    if anchor_time is None and memcached_timing is not None:
        memcached_start = _parse_time(memcached_timing.get("started_at"))
        if memcached_start is not None:
            anchor_time = memcached_start

    max_end_s: float | None = None
    lanes = {
        lane_meta["lane_id"]: {
            "lane_id": lane_meta["lane_id"],
            "label": lane_meta["label"],
            "short_label": lane_meta["short_label"],
            "segments": [],
        }
        for lane_meta in LANE_META
    }

    for segment in job_segments:
        if anchor_time is None:
            continue
        start_s = max(0.0, (segment["_start"] - anchor_time).total_seconds())
        end_s = max(start_s, (segment["_end"] - anchor_time).total_seconds())
        timeline_segment = {
            "job_id": segment["job_id"],
            "label": segment["label"],
            "kind": segment["kind"],
            "status": segment["status"],
            "start_s": start_s,
            "end_s": end_s,
            "duration_s": end_s - start_s,
            "planned_node": segment["planned_node"],
            "cores": segment["cores"],
            "core_ids": segment["core_ids"],
            "threads": segment["threads"],
            "raw_node_name": segment["raw_node_name"],
            "started_at": segment["started_at"],
            "finished_at": segment["finished_at"],
        }
        lane_id = segment["lane_id"]
        if lane_id in lanes:
            lanes[lane_id]["segments"].append(timeline_segment)
            max_end_s = end_s if max_end_s is None else max(max_end_s, end_s)

    memcached_lane_id = _canonical_node_name(_string_or_none(memcached_summary.get("node_name"))) or planned_memcached_node
    memcached_start = _parse_time(memcached_timing.get("started_at")) if memcached_timing is not None else None
    memcached_end = _parse_time(memcached_timing.get("finished_at")) if memcached_timing is not None else None
    if memcached_lane_id in lanes and anchor_time is not None:
        start_s = 0.0
        if memcached_start is not None and memcached_start >= anchor_time:
            start_s = (memcached_start - anchor_time).total_seconds()
        if memcached_end is not None:
            end_s = max(start_s, (memcached_end - anchor_time).total_seconds())
        elif max_end_s is not None:
            end_s = max_end_s
        else:
            end_s = start_s
        lanes[memcached_lane_id]["segments"].append(
            {
                "job_id": "memcached",
                "label": "memcached",
                "kind": "memcached",
                "status": _string_or_none(memcached_summary.get("phase")) or "running",
                "start_s": start_s,
                "end_s": end_s,
                "duration_s": max(0.0, end_s - start_s),
                "planned_node": planned_memcached_node,
                "cores": planned_memcached_cores,
                "core_ids": list(_parse_core_ids(planned_memcached_cores, planned_memcached_node)),
                "threads": planned_memcached_threads,
                "raw_node_name": _string_or_none(memcached_summary.get("node_name")),
                "started_at": memcached_timing.get("started_at") if memcached_timing is not None else None,
                "finished_at": memcached_timing.get("finished_at") if memcached_timing is not None else None,
            }
        )
        max_end_s = end_s if max_end_s is None else max(max_end_s, end_s)

    for lane in lanes.values():
        lane["segments"].sort(key=lambda item: (float(item["start_s"]), str(item["job_id"])))
        lane["node_names"] = sorted(
            {
                str(segment["raw_node_name"])
                for segment in lane["segments"]
                if segment.get("raw_node_name")
            }
        )

    return {
        "has_data": any(lane["segments"] for lane in lanes.values()),
        "anchor_started_at": anchor_time.strftime(TIME_FORMAT) if anchor_time is not None else None,
        "max_end_s": max_end_s,
        "lanes": [lanes[NODE_A], lanes[NODE_B]],
    }


def _extract_memcached_timing(payload: dict[str, object]) -> dict[str, object] | None:
    for item in payload.get("items", []):
        metadata = _ensure_mapping(item.get("metadata"))
        labels = _ensure_mapping(metadata.get("labels"))
        status = _ensure_mapping(item.get("status"))
        container_status = (_ensure_list(status.get("containerStatuses")) or [{}])[0]
        container_name = _ensure_mapping(container_status).get("name")
        if labels.get("cca-project-role") != "memcached" and container_name != "memcached":
            continue
        state = _ensure_mapping(_ensure_mapping(container_status).get("state"))
        running = _ensure_mapping(state.get("running"))
        terminated = _ensure_mapping(state.get("terminated"))
        return {
            "started_at": _string_or_none(running.get("startedAt")) or _string_or_none(terminated.get("startedAt")),
            "finished_at": _string_or_none(terminated.get("finishedAt")),
        }
    return None


def _planned_job_nodes(policy: PolicyConfig) -> dict[str, str]:
    planned: dict[str, str] = {}
    for job_id, catalog_entry in JOB_CATALOG.items():
        override = policy.job_overrides.get(job_id)
        planned[job_id] = override.node if override is not None and override.node is not None else catalog_entry.default_node
    return planned


def _planned_job_cores(policy: PolicyConfig) -> dict[str, str]:
    planned: dict[str, str] = {}
    for job_id, catalog_entry in JOB_CATALOG.items():
        override = policy.job_overrides.get(job_id)
        planned[job_id] = override.cores if override is not None and override.cores is not None else catalog_entry.default_cores
    return planned


def _planned_job_threads(policy: PolicyConfig) -> dict[str, int]:
    planned: dict[str, int] = {}
    for job_id, catalog_entry in JOB_CATALOG.items():
        override = policy.job_overrides.get(job_id)
        planned[job_id] = override.threads if override is not None and override.threads is not None else catalog_entry.default_threads
    return planned


def _parse_core_ids(core_spec: str | None, node: str | None) -> tuple[int, ...]:
    if core_spec is None or node is None:
        return ()
    try:
        return validate_node_core_spec(core_spec, node)
    except ValueError:
        return ()


def _canonical_node_name(value: str | None) -> str | None:
    if value is None:
        return None
    lowered = value.lower()
    if lowered == NODE_A or lowered.startswith(f"{NODE_A}-"):
        return NODE_A
    if lowered == NODE_B or lowered.startswith(f"{NODE_B}-"):
        return NODE_B
    return None


def _is_best_run_candidate(
    *,
    overall_status: str,
    measurement_status: str,
    timing_complete: bool,
    makespan_s: float | None,
    max_p95_us: float | None,
    sample_count: int | None,
) -> bool:
    return (
        overall_status == "pass"
        and measurement_status == "ok"
        and timing_complete
        and makespan_s is not None
        and max_p95_us is not None
        and (sample_count or 0) > 0
    )


def _parse_time(value: Any) -> datetime | None:
    if not isinstance(value, str) or not value:
        return None
    try:
        return datetime.strptime(value, TIME_FORMAT)
    except ValueError:
        return None


def _safe_float(value: Any) -> float | None:
    if value is None or value == "":
        return None
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    try:
        return float(str(value))
    except ValueError:
        return None


def _safe_int(value: Any) -> int | None:
    if value is None or value == "":
        return None
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    try:
        return int(str(value))
    except ValueError:
        return None


def _string_or_none(value: Any) -> str | None:
    if isinstance(value, str) and value:
        return value
    return None


def _ensure_mapping(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _ensure_list(value: Any) -> list[Any]:
    return value if isinstance(value, list) else []


def _dedupe_preserve_order(items: list[str]) -> list[str]:
    seen: set[str] = set()
    deduped: list[str] = []
    for item in items:
        if item in seen:
            continue
        seen.add(item)
        deduped.append(item)
    return deduped
