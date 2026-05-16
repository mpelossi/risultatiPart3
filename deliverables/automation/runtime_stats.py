from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from statistics import mean, median
from typing import Any

from .catalog import JOB_CATALOG
from .config import JobOverride, PolicyConfig, load_policy_config
from .metrics import parse_mcperf_output, summarize_pods
from .utils import resolve_existing_run_results_path, write_json


RUNTIME_STATS_FILENAME = "runtime_stats.json"
RUNTIME_STATS_SCHEMA_VERSION = 1


@dataclass(frozen=True)
class RuntimeStatsEstimate:
    duration_s: float
    source: str
    match_type: str
    sample_count: int
    message: str | None = None


class RuntimeStatsIndex:
    def __init__(self, *, source_path: Path, payload: dict[str, object]):
        self.source_path = source_path
        self.payload = payload
        aggregates = _ensure_mapping(payload.get("aggregates"))
        self._exact = _index_aggregates(_ensure_list(aggregates.get("exact")))
        self._same_node = _index_aggregates(_ensure_list(aggregates.get("same_node")))
        self._node = _index_aggregates(_ensure_list(aggregates.get("node")))

    def estimate(
        self,
        *,
        job_id: str,
        node: str,
        threads: int,
        memcached_node: str,
    ) -> RuntimeStatsEstimate | None:
        exact_key = _aggregate_key(job_id=job_id, node=node, threads=threads, memcached_node=memcached_node)
        exact = self._exact.get(exact_key)
        if exact is not None:
            return _estimate_from_aggregate(exact, self.source_path, "exact")

        same_node_key = _aggregate_key(
            job_id=job_id,
            node=node,
            threads=threads,
            memcached_same_node=(node == memcached_node),
        )
        same_node = self._same_node.get(same_node_key)
        if same_node is not None:
            estimate = _estimate_from_aggregate(same_node, self.source_path, "same_node")
            return RuntimeStatsEstimate(
                duration_s=estimate.duration_s,
                source=estimate.source,
                match_type=estimate.match_type,
                sample_count=estimate.sample_count,
                message=(
                    f"Using same-node runtime fallback for {job_id} on {node} with {threads} thread(s); "
                    f"no exact samples for memcached on {memcached_node}."
                ),
            )

        node_key = _aggregate_key(job_id=job_id, node=node, threads=threads)
        node_aggregate = self._node.get(node_key)
        if node_aggregate is not None:
            estimate = _estimate_from_aggregate(node_aggregate, self.source_path, "node")
            return RuntimeStatsEstimate(
                duration_s=estimate.duration_s,
                source=estimate.source,
                match_type=estimate.match_type,
                sample_count=estimate.sample_count,
                message=(
                    f"Using node/thread runtime fallback for {job_id} on {node} with {threads} thread(s); "
                    f"no memcached-placement samples matched."
                ),
            )

        return None


def runtime_stats_path(results_root: Path) -> Path:
    return results_root / RUNTIME_STATS_FILENAME


def load_runtime_stats(path: Path) -> RuntimeStatsIndex:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"{path} must contain a JSON object")
    return RuntimeStatsIndex(source_path=path, payload=payload)


def rebuild_runtime_stats_file(results_root: Path, *, output_path: Path | None = None) -> dict[str, object]:
    payload = build_runtime_stats(results_root)
    destination = output_path or runtime_stats_path(results_root)
    write_json(destination, payload)
    payload["output_path"] = str(destination)
    return payload


def build_runtime_stats(results_root: Path) -> dict[str, object]:
    samples: list[dict[str, object]] = []
    skipped_runs: list[dict[str, object]] = []
    run_dirs = _discover_run_dirs(results_root)

    for run_dir in run_dirs:
        run_samples, skip_reason = _samples_from_run(run_dir)
        if skip_reason is not None:
            skipped_runs.append(
                {
                    "experiment_id": run_dir.parent.name,
                    "run_id": run_dir.name,
                    "run_dir": str(run_dir),
                    "reason": skip_reason,
                }
            )
            continue
        samples.extend(run_samples)

    return {
        "schema_version": RUNTIME_STATS_SCHEMA_VERSION,
        "generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "results_root": str(results_root),
        "run_count": len(run_dirs),
        "eligible_run_count": len({(sample["experiment_id"], sample["run_id"]) for sample in samples}),
        "sample_count": len(samples),
        "samples": samples,
        "aggregates": {
            "exact": _build_aggregates(
                samples,
                key_fields=("job", "job_node", "threads", "memcached_node"),
                key_names=("job", "node", "threads", "memcached_node"),
            ),
            "same_node": _build_aggregates(
                samples,
                key_fields=("job", "job_node", "threads", "memcached_same_node"),
                key_names=("job", "node", "threads", "memcached_same_node"),
            ),
            "node": _build_aggregates(
                samples,
                key_fields=("job", "job_node", "threads"),
                key_names=("job", "node", "threads"),
            ),
        },
        "skipped_runs": skipped_runs,
    }


def _discover_run_dirs(results_root: Path) -> list[Path]:
    if not results_root.exists():
        return []
    run_dirs: list[Path] = []
    for experiment_root in sorted(path for path in results_root.iterdir() if path.is_dir() and not path.name.startswith("__")):
        run_dirs.extend(sorted(path for path in experiment_root.iterdir() if path.is_dir()))
    return run_dirs


def _samples_from_run(run_dir: Path) -> tuple[list[dict[str, object]], str | None]:
    policy_path = run_dir / "policy.yaml"
    if not policy_path.exists():
        return [], "missing policy.yaml"
    try:
        policy = load_policy_config(str(policy_path))
    except Exception as exc:
        return [], f"policy.yaml could not be parsed: {exc}"

    summary, summary_error = _load_or_reconstruct_summary(run_dir, policy=policy)
    if summary_error is not None:
        return [], summary_error
    if summary.get("measurement_status") != "ok":
        return [], f"measurement_status={summary.get('measurement_status')}"
    if summary.get("timing_complete") is not True:
        return [], "timing is incomplete"
    if not isinstance(summary.get("memcached"), dict):
        return [], "memcached summary is missing"

    jobs = _ensure_mapping(summary.get("jobs"))
    missing_or_incomplete = [
        job_id
        for job_id in sorted(JOB_CATALOG)
        if _ensure_mapping(jobs.get(job_id)).get("status") != "completed"
        or _safe_float(_ensure_mapping(jobs.get(job_id)).get("runtime_s")) is None
    ]
    if missing_or_incomplete:
        return [], "incomplete jobs: " + ", ".join(missing_or_incomplete)

    node_platforms = _node_platforms(summary, run_dir)
    memcached_node = policy.memcached.node
    memcached_summary = _ensure_mapping(summary.get("memcached"))
    samples = []
    for job_id in sorted(JOB_CATALOG):
        job = _ensure_mapping(jobs.get(job_id))
        runtime_s = _safe_float(job.get("runtime_s"))
        if runtime_s is None:
            continue
        job_config = _job_config(policy, job_id)
        sample = {
            "experiment_id": str(summary.get("experiment_id") or run_dir.parent.name),
            "run_id": str(summary.get("run_id") or run_dir.name),
            "run_dir": str(run_dir),
            "policy_name": str(summary.get("policy_name") or policy.policy_name),
            "job": job_id,
            "runtime_s": runtime_s,
            "job_node": job_config["node"],
            "cores": job_config["cores"],
            "threads": job_config["threads"],
            "memcached_node": memcached_node,
            "memcached_cores": policy.memcached.cores,
            "memcached_threads": policy.memcached.threads,
            "memcached_same_node": job_config["node"] == memcached_node,
            "started_at": _string_or_none(job.get("started_at")),
            "finished_at": _string_or_none(job.get("finished_at")),
            "pod_name": _string_or_none(job.get("pod_name")),
            "node_name": _string_or_none(job.get("node_name")),
            "memcached_pod_name": _string_or_none(memcached_summary.get("pod_name")),
            "memcached_node_name": _string_or_none(memcached_summary.get("node_name")),
        }
        sample.update(_platform_sample_fields(node_platforms, str(job_config["node"]), memcached_node))
        samples.append(sample)
    return samples, None


def _load_or_reconstruct_summary(run_dir: Path, *, policy: PolicyConfig) -> tuple[dict[str, object], str | None]:
    summary_path = run_dir / "summary.json"
    if summary_path.exists():
        try:
            loaded = json.loads(summary_path.read_text(encoding="utf-8"))
        except json.JSONDecodeError as exc:
            return {}, f"summary.json could not be parsed: {exc}"
        if not isinstance(loaded, dict):
            return {}, "summary.json does not contain an object"
        return loaded, None

    snapshot_path = resolve_existing_run_results_path(run_dir)
    if not snapshot_path.exists():
        return {}, "missing summary.json and pod snapshot"
    try:
        pod_summary = summarize_pods(snapshot_path, set(JOB_CATALOG))
        mcperf_summary = parse_mcperf_output(run_dir / "mcperf.txt")
    except Exception as exc:
        return {}, f"summary reconstruction failed: {exc}"
    return {
        "experiment_id": run_dir.parent.name,
        "run_id": run_dir.name,
        "policy_name": policy.policy_name,
        "memcached": pod_summary["memcached"],
        "jobs": pod_summary["jobs"],
        "makespan_s": pod_summary["makespan_s"],
        "completed_job_count": pod_summary["completed_job_count"],
        "expected_job_count": len(JOB_CATALOG),
        "timing_complete": pod_summary["timing_complete"],
        "max_observed_p95_us": mcperf_summary["max_p95_us"],
        "slo_violations": mcperf_summary["slo_violations"],
        "measurement_status": mcperf_summary["measurement_status"],
        "sample_count": len(mcperf_summary["samples"]),
    }, None


def _job_config(policy: PolicyConfig, job_id: str) -> dict[str, object]:
    catalog_entry = JOB_CATALOG[job_id]
    override = policy.job_overrides.get(job_id, JobOverride())
    return {
        "node": override.node or catalog_entry.default_node,
        "cores": override.cores or catalog_entry.default_cores,
        "threads": override.threads or catalog_entry.default_threads,
    }


def _node_platforms(summary: dict[str, object], run_dir: Path) -> dict[str, object]:
    raw_node_platforms = summary.get("node_platforms")
    if isinstance(raw_node_platforms, dict):
        return raw_node_platforms
    node_platforms_path = run_dir / "node_platforms.json"
    if not node_platforms_path.exists():
        return {}
    try:
        loaded = json.loads(node_platforms_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {}
    return loaded if isinstance(loaded, dict) else {}


def _platform_sample_fields(
    node_platforms: dict[str, object],
    job_node: str,
    memcached_node: str,
) -> dict[str, object]:
    nodes = _ensure_mapping(node_platforms.get("nodes"))
    job_platform = _ensure_mapping(nodes.get(job_node))
    memcached_platform = _ensure_mapping(nodes.get(memcached_node))
    return {
        "job_cpu_platform": _string_or_none(job_platform.get("cpu_platform")),
        "job_machine_type": _string_or_none(job_platform.get("machine_type")),
        "memcached_cpu_platform": _string_or_none(memcached_platform.get("cpu_platform")),
        "memcached_machine_type": _string_or_none(memcached_platform.get("machine_type")),
    }


def _build_aggregates(
    samples: list[dict[str, object]],
    *,
    key_fields: tuple[str, ...],
    key_names: tuple[str, ...],
) -> list[dict[str, object]]:
    grouped: dict[tuple[object, ...], list[dict[str, object]]] = {}
    for sample in samples:
        key = tuple(sample[field] for field in key_fields)
        grouped.setdefault(key, []).append(sample)

    aggregates: list[dict[str, object]] = []
    for key, group in sorted(grouped.items(), key=lambda item: tuple(str(part) for part in item[0])):
        runtimes = sorted(float(sample["runtime_s"]) for sample in group)
        aggregates.append(
            {
                "key": {key_name: key_value for key_name, key_value in zip(key_names, key)},
                "sample_count": len(runtimes),
                "median_s": median(runtimes),
                "mean_s": mean(runtimes),
                "min_s": min(runtimes),
                "max_s": max(runtimes),
                "source_runs": sorted(
                    {
                        f"{sample['experiment_id']}/{sample['run_id']}"
                        for sample in group
                    }
                ),
            }
        )
    return aggregates


def _index_aggregates(aggregates: list[object]) -> dict[tuple[object, ...], dict[str, object]]:
    index: dict[tuple[object, ...], dict[str, object]] = {}
    for aggregate in aggregates:
        aggregate_map = _ensure_mapping(aggregate)
        key_map = _ensure_mapping(aggregate_map.get("key"))
        key = _aggregate_key(
            job_id=_string_or_none(key_map.get("job")) or "",
            node=_string_or_none(key_map.get("node")) or "",
            threads=int(key_map.get("threads") or 0),
            memcached_node=_string_or_none(key_map.get("memcached_node")),
            memcached_same_node=(
                bool(key_map.get("memcached_same_node"))
                if "memcached_same_node" in key_map
                else None
            ),
        )
        index[key] = aggregate_map
    return index


def _aggregate_key(
    *,
    job_id: str,
    node: str,
    threads: int,
    memcached_node: str | None = None,
    memcached_same_node: bool | None = None,
) -> tuple[object, ...]:
    key: tuple[object, ...] = (job_id, node, threads)
    if memcached_node is not None:
        key += (memcached_node,)
    if memcached_same_node is not None:
        key += (memcached_same_node,)
    return key


def _estimate_from_aggregate(
    aggregate: dict[str, object],
    source_path: Path,
    match_type: str,
) -> RuntimeStatsEstimate:
    return RuntimeStatsEstimate(
        duration_s=float(aggregate["median_s"]),
        source=str(source_path),
        match_type=match_type,
        sample_count=int(aggregate.get("sample_count") or 0),
    )


def _ensure_mapping(raw: Any) -> dict[str, Any]:
    return raw if isinstance(raw, dict) else {}


def _ensure_list(raw: Any) -> list[Any]:
    return raw if isinstance(raw, list) else []


def _safe_float(value: Any) -> float | None:
    if value is None or value == "" or isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    try:
        return float(str(value))
    except ValueError:
        return None


def _string_or_none(value: Any) -> str | None:
    if isinstance(value, str) and value:
        return value
    return None
