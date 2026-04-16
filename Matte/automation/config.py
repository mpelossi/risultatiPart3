from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .catalog import JOB_CATALOG, NODE_A, NODE_B, count_cores
from .utils import expand_path


def _load_structured_file(path: Path) -> dict[str, Any]:
    text = path.read_text(encoding="utf-8")
    try:
        import yaml  # type: ignore

        loaded = yaml.safe_load(text)
    except ModuleNotFoundError:
        try:
            loaded = json.loads(text)
        except json.JSONDecodeError as exc:
            raise ValueError(
                f"{path} is not valid JSON-compatible YAML. "
                "Install PyYAML or keep configs in JSON syntax."
            ) from exc
    if not isinstance(loaded, dict):
        raise ValueError(f"{path} must contain a top-level mapping")
    return loaded


def _require_mapping(raw: Any, field_name: str) -> dict[str, Any]:
    if not isinstance(raw, dict):
        raise ValueError(f"{field_name} must be a mapping")
    return raw


def _require_list(raw: Any, field_name: str) -> list[Any]:
    if not isinstance(raw, list):
        raise ValueError(f"{field_name} must be a list")
    return raw


def _require_str(raw: Any, field_name: str) -> str:
    if not isinstance(raw, str) or not raw.strip():
        raise ValueError(f"{field_name} must be a non-empty string")
    return raw


def _require_int(raw: Any, field_name: str) -> int:
    if isinstance(raw, bool) or not isinstance(raw, int):
        raise ValueError(f"{field_name} must be an integer")
    return raw


def _optional_str(raw: Any, field_name: str) -> str | None:
    if raw is None:
        return None
    return _require_str(raw, field_name)


@dataclass(frozen=True)
class MeasurementConfig:
    agent_a_threads: int
    agent_b_threads: int
    measure_threads: int
    connections: int
    depth: int
    qps_interval: int
    scan_start: int
    scan_stop: int
    scan_step: int
    max_start_wait_s: int
    completion_timeout_s: int


@dataclass(frozen=True)
class ExperimentConfig:
    config_path: Path
    experiment_id: str
    cluster_name: str
    zone: str
    kops_state_store: str
    ssh_key_path: Path
    ssh_user: str
    cluster_config_path: Path
    results_root: Path
    submission_group: str
    memcached_name: str
    remote_repo_dir: str
    measurement: MeasurementConfig


@dataclass(frozen=True)
class JobOverride:
    node: str | None = None
    cores: str | None = None
    threads: int | None = None
    cpu_request: str | None = None
    memory_request: str | None = None
    memory_limit: str | None = None


@dataclass(frozen=True)
class MemcachedConfig:
    node: str
    cores: str
    threads: int


@dataclass(frozen=True)
class Phase:
    phase_id: str
    after: str
    jobs_complete: tuple[str, ...]
    delay_s: int
    launch: tuple[str, ...]


@dataclass(frozen=True)
class PolicyConfig:
    config_path: Path
    policy_name: str
    memcached: MemcachedConfig
    job_overrides: dict[str, JobOverride]
    phases: list[Phase]


def load_experiment_config(path_str: str) -> ExperimentConfig:
    path = expand_path(path_str)
    raw = _load_structured_file(path)
    base_dir = path.parent

    measurement_raw = _require_mapping(raw.get("mcperf_measurement", {}), "mcperf_measurement")
    measurement = MeasurementConfig(
        agent_a_threads=_require_int(measurement_raw.get("agent_a_threads", 2), "mcperf_measurement.agent_a_threads"),
        agent_b_threads=_require_int(measurement_raw.get("agent_b_threads", 4), "mcperf_measurement.agent_b_threads"),
        measure_threads=_require_int(measurement_raw.get("measure_threads", 6), "mcperf_measurement.measure_threads"),
        connections=_require_int(measurement_raw.get("connections", 4), "mcperf_measurement.connections"),
        depth=_require_int(measurement_raw.get("depth", 4), "mcperf_measurement.depth"),
        qps_interval=_require_int(measurement_raw.get("qps_interval", 1000), "mcperf_measurement.qps_interval"),
        scan_start=_require_int(measurement_raw.get("scan_start", 30000), "mcperf_measurement.scan_start"),
        scan_stop=_require_int(measurement_raw.get("scan_stop", 30500), "mcperf_measurement.scan_stop"),
        scan_step=_require_int(measurement_raw.get("scan_step", 5), "mcperf_measurement.scan_step"),
        max_start_wait_s=_require_int(measurement_raw.get("max_start_wait_s", 180), "mcperf_measurement.max_start_wait_s"),
        completion_timeout_s=_require_int(
            measurement_raw.get("completion_timeout_s", 3600),
            "mcperf_measurement.completion_timeout_s",
        ),
    )

    submission_group = str(raw.get("submission_group", "000")).zfill(3)
    return ExperimentConfig(
        config_path=path,
        experiment_id=_require_str(raw.get("experiment_id", "part3-handcrafted"), "experiment_id"),
        cluster_name=_require_str(raw.get("cluster_name"), "cluster_name"),
        zone=_require_str(raw.get("zone"), "zone"),
        kops_state_store=_require_str(raw.get("kops_state_store"), "kops_state_store"),
        ssh_key_path=expand_path(_require_str(raw.get("ssh_key_path"), "ssh_key_path"), base_dir),
        ssh_user=_require_str(raw.get("ssh_user", "ubuntu"), "ssh_user"),
        cluster_config_path=expand_path(
            _require_str(raw.get("cluster_config_path", "part3.yaml"), "cluster_config_path"),
            base_dir,
        ),
        results_root=expand_path(_require_str(raw.get("results_root", "runs"), "results_root"), base_dir),
        submission_group=submission_group,
        memcached_name=_require_str(raw.get("memcached_name", "some-memcached"), "memcached_name"),
        remote_repo_dir=_require_str(
            raw.get("remote_repo_dir", "/opt/cca/memcache-perf-dynamic"),
            "remote_repo_dir",
        ),
        measurement=measurement,
    )


def _load_job_overrides(raw: Any) -> dict[str, JobOverride]:
    overrides_raw = _require_mapping(raw or {}, "job_overrides")
    overrides: dict[str, JobOverride] = {}
    for job_id, override_raw in overrides_raw.items():
        _require_str(job_id, "job_overrides job id")
        if job_id not in JOB_CATALOG:
            raise ValueError(f"Unknown job override: {job_id}")
        override_map = _require_mapping(override_raw, f"job_overrides.{job_id}")
        threads_raw = override_map.get("threads")
        override = JobOverride(
            node=_optional_str(override_map.get("node"), f"job_overrides.{job_id}.node"),
            cores=_optional_str(override_map.get("cores"), f"job_overrides.{job_id}.cores"),
            threads=None if threads_raw is None else _require_int(threads_raw, f"job_overrides.{job_id}.threads"),
            cpu_request=_optional_str(override_map.get("cpu_request"), f"job_overrides.{job_id}.cpu_request"),
            memory_request=_optional_str(
                override_map.get("memory_request"),
                f"job_overrides.{job_id}.memory_request",
            ),
            memory_limit=_optional_str(override_map.get("memory_limit"), f"job_overrides.{job_id}.memory_limit"),
        )
        overrides[job_id] = override
    return overrides


def _job_override_from_simple_schedule(job_id: str, raw: Any) -> JobOverride:
    schedule_map = _require_mapping(raw, f"jobs.{job_id}")
    threads_raw = schedule_map.get("threads")
    return JobOverride(
        node=_optional_str(schedule_map.get("node"), f"jobs.{job_id}.node"),
        cores=_optional_str(schedule_map.get("cores"), f"jobs.{job_id}.cores"),
        threads=None if threads_raw is None else _require_int(threads_raw, f"jobs.{job_id}.threads"),
        cpu_request=_optional_str(schedule_map.get("cpu_request"), f"jobs.{job_id}.cpu_request"),
        memory_request=_optional_str(schedule_map.get("memory_request"), f"jobs.{job_id}.memory_request"),
        memory_limit=_optional_str(schedule_map.get("memory_limit"), f"jobs.{job_id}.memory_limit"),
    )


def _validate_job_override(job_id: str, override: JobOverride) -> None:
    catalog_entry = JOB_CATALOG[job_id]
    node = override.node or catalog_entry.default_node
    if node not in (NODE_A, NODE_B):
        raise ValueError(f"{job_id} uses unsupported node: {node}")
    cores = override.cores or catalog_entry.default_cores
    if cores not in catalog_entry.allowed_cores_by_node[node]:
        raise ValueError(f"{job_id} uses unsupported core set {cores} on {node}")
    threads = override.threads or catalog_entry.default_threads
    if threads <= 0:
        raise ValueError(f"{job_id} must use at least one thread")
    if threads > count_cores(cores):
        raise ValueError(f"{job_id} threads ({threads}) exceed pinned cores ({cores})")


def _load_phases(raw: Any) -> list[Phase]:
    phase_list = _require_list(raw, "phases")
    phases: list[Phase] = []
    phase_ids: set[str] = set()
    launched_jobs: set[str] = set()
    for idx, phase_raw in enumerate(phase_list):
        phase_map = _require_mapping(phase_raw, f"phases[{idx}]")
        phase_id = _require_str(phase_map.get("id"), f"phases[{idx}].id")
        if phase_id in phase_ids:
            raise ValueError(f"Duplicate phase id: {phase_id}")
        after = _require_str(phase_map.get("after", "start"), f"phases[{idx}].after")
        if not (after == "start" or after == "jobs_complete" or after.startswith("phase:")):
            raise ValueError(f"Unsupported phase dependency: {after}")
        if after.startswith("phase:") and after.split(":", 1)[1] not in phase_ids:
            raise ValueError(f"Phase {phase_id} depends on unknown earlier phase: {after}")
        jobs_complete = tuple(_require_list(phase_map.get("jobs_complete", []), f"phases[{idx}].jobs_complete"))
        launch = tuple(_require_list(phase_map.get("launch", []), f"phases[{idx}].launch"))
        if not launch:
            raise ValueError(f"Phase {phase_id} must launch at least one job")
        for job_id in jobs_complete:
            _require_str(job_id, f"phases[{idx}].jobs_complete job")
            if job_id not in launched_jobs:
                raise ValueError(
                    f"Phase {phase_id} waits for {job_id} before that job has been launched"
                )
        for job_id in launch:
            _require_str(job_id, f"phases[{idx}].launch job")
            if job_id not in JOB_CATALOG:
                raise ValueError(f"Unknown job in phase {phase_id}: {job_id}")
            if job_id in launched_jobs:
                raise ValueError(f"Job {job_id} is launched more than once")
        if after == "jobs_complete" and not jobs_complete:
            raise ValueError(f"Phase {phase_id} requires jobs_complete entries")
        if after != "jobs_complete" and jobs_complete:
            raise ValueError(f"Phase {phase_id} should not define jobs_complete with after={after}")
        delay_s = _require_int(phase_map.get("delay_s", 0), f"phases[{idx}].delay_s")
        if delay_s < 0:
            raise ValueError(f"Phase {phase_id} delay_s must be non-negative")
        phases.append(
            Phase(
                phase_id=phase_id,
                after=after,
                jobs_complete=jobs_complete,
                delay_s=delay_s,
                launch=launch,
            )
        )
        phase_ids.add(phase_id)
        launched_jobs.update(launch)
    return phases


def _translate_simple_schedule(raw: dict[str, Any]) -> tuple[dict[str, JobOverride], list[Phase]]:
    jobs_raw = _require_mapping(raw.get("jobs", {}), "jobs")
    if not jobs_raw:
        raise ValueError("Simple schedule policies must define a jobs mapping")

    overrides: dict[str, JobOverride] = {}
    phases: list[Phase] = []
    current_phase: Phase | None = None
    current_key: tuple[str, tuple[str, ...], int] | None = None

    for job_id, job_raw in jobs_raw.items():
        if job_id not in JOB_CATALOG:
            raise ValueError(f"Unknown job in jobs mapping: {job_id}")
        schedule_map = _require_mapping(job_raw, f"jobs.{job_id}")
        overrides[job_id] = _job_override_from_simple_schedule(job_id, schedule_map)

        after_raw = schedule_map.get("after", "start")
        if isinstance(after_raw, list):
            after_jobs = tuple(_require_list(after_raw, f"jobs.{job_id}.after"))
            for dependency in after_jobs:
                _require_str(dependency, f"jobs.{job_id}.after dependency")
            phase_after = "jobs_complete"
            jobs_complete = after_jobs
        else:
            after_value = _require_str(after_raw, f"jobs.{job_id}.after")
            if after_value == "start":
                phase_after = "start"
                jobs_complete = ()
            else:
                phase_after = "jobs_complete"
                jobs_complete = (after_value,)

        delay_s = _require_int(schedule_map.get("delay_s", 0), f"jobs.{job_id}.delay_s")
        phase_key = (phase_after, jobs_complete, delay_s)
        if current_phase is None or current_key != phase_key:
            current_phase = Phase(
                phase_id=f"phase-{len(phases) + 1:02d}",
                after=phase_after,
                jobs_complete=jobs_complete,
                delay_s=delay_s,
                launch=(job_id,),
            )
            phases.append(current_phase)
            current_key = phase_key
        else:
            phases[-1] = Phase(
                phase_id=current_phase.phase_id,
                after=current_phase.after,
                jobs_complete=current_phase.jobs_complete,
                delay_s=current_phase.delay_s,
                launch=current_phase.launch + (job_id,),
            )
            current_phase = phases[-1]
    return overrides, phases


def load_policy_config(path_str: str) -> PolicyConfig:
    path = expand_path(path_str)
    raw = _load_structured_file(path)
    memcached_raw = _require_mapping(raw.get("memcached", {}), "memcached")
    memcached = MemcachedConfig(
        node=_require_str(memcached_raw.get("node"), "memcached.node"),
        cores=_require_str(memcached_raw.get("cores"), "memcached.cores"),
        threads=_require_int(memcached_raw.get("threads", 1), "memcached.threads"),
    )
    if memcached.node not in (NODE_A, NODE_B):
        raise ValueError(f"Unsupported memcached node: {memcached.node}")
    if memcached.threads > count_cores(memcached.cores):
        raise ValueError("memcached threads exceed pinned cores")

    if "jobs" in raw and "phases" not in raw:
        job_overrides, phases = _translate_simple_schedule(raw)
    else:
        job_overrides = _load_job_overrides(raw.get("job_overrides", {}))
        phases = _load_phases(raw.get("phases"))

    for job_id, override in job_overrides.items():
        _validate_job_override(job_id, override)
    if len({job_id for phase in phases for job_id in phase.launch}) != len(JOB_CATALOG):
        raise ValueError("Policy must launch all seven batch jobs exactly once")

    return PolicyConfig(
        config_path=path,
        policy_name=_require_str(raw.get("policy_name", path.stem), "policy_name"),
        memcached=memcached,
        job_overrides=job_overrides,
        phases=phases,
    )
