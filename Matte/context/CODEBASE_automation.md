Project Path: automation

Source Tree:

```txt
automation
├── README.md
├── __init__.py
├── audit.py
├── catalog.py
├── cli.py
├── cluster.py
├── collect.py
├── config.py
├── debug.py
├── experiment.yaml
├── export.py
├── gui.py
├── manifests.py
├── metrics.py
├── part3.yaml
├── policies
│   └── baseline.yaml
├── provision.py
├── results.py
├── runner.py
├── schedule.yaml
├── tests
│   ├── __init__.py
│   ├── helpers.py
│   ├── test_audit.py
│   ├── test_cluster_labels.py
│   ├── test_config.py
│   ├── test_debug.py
│   ├── test_export.py
│   ├── test_live_integration.py
│   ├── test_manifests.py
│   ├── test_metrics.py
│   ├── test_provision.py
│   ├── test_results.py
│   └── test_runner.py
└── utils.py

```

`README.md`:

```md
# Part 3 Automation

Everything below assumes you are already inside:

```bash
cd risultatiPart3/Matte/automation
```

## TL;DR

### 1. Check local setup and auth

```bash
../../checkCredits.sh
```

### 2. If the cluster does not exist yet, or you deleted it, bring it up

```bash
python3 cli.py cluster up --config experiment.yaml
```

If `cluster up` fails or gets interrupted, clean up before retrying:

```bash
python3 cli.py cluster down --config experiment.yaml
../../checkCredits.sh
```

Only retry `cluster up` after `../../checkCredits.sh` shows no leftover Part 3 billable
resources or network artifacts.

You do **not** need to do this every time.

You only need `cluster up` when:
- the cluster has never been created
- the cluster was deleted
- you changed the cluster YAML and want to apply those changes

`run once` and `run batch` do **not** call `cluster up` for you. They assume the cluster
already exists and is reachable.

### 3. Check that the client VMs are ready

```bash
python3 cli.py provision check --config experiment.yaml
```

If `~/.ssh/cloud-computing` is passphrase-protected and not loaded in `ssh-agent`, this
command may ask for the passphrase up to **3 times**, roughly once for each client VM that
is checked. To avoid repeated prompts, run:

```bash
ssh-add ~/.ssh/cloud-computing
```

### 4. Inspect and validate the schedule

```bash
python3 cli.py show --policy schedule.yaml
python3 cli.py audit --policy schedule.yaml --times-csv ../../Part2summary_times.csv
```

### 5. Do a dry run first

```bash
python3 cli.py run once --config experiment.yaml --policy schedule.yaml --dry-run
```

### 6. Run one real experiment

```bash
python3 cli.py run once --config experiment.yaml --policy schedule.yaml
```

### 7. Run three repetitions

```bash
python3 cli.py run batch --config experiment.yaml --policy schedule.yaml --runs 3
```

### 8. See the best run

```bash
python3 cli.py results best --experiment part3-handcrafted
```

### 9. Export submission files

```bash
python3 cli.py export submission --experiment part3-handcrafted --group 054 --task 3_1
```

## What You Edit

Most of the time, you only edit:

```bash
schedule.yaml
```

That file decides:
- which node each job runs on
- which cores it uses
- how many threads it gets
- when it starts relative to other jobs

Only edit:

```bash
experiment.yaml
```

if you need to change:
- cluster name
- zone
- state store
- SSH key path
- results folder
- group number

The cluster definition itself lives in:

```bash
part3.yaml
```

Only edit that file if you want to change VM bootstrap behavior or the cluster layout.

## Step By Step

### Step 1. Preflight

Run:

```bash
../../checkCredits.sh
```

This checks:
- that the automation files exist
- that `cli.py` is runnable
- that `gcloud`, `kops`, `kubectl`, and `python3` exist
- that your Google auth is still valid
- whether `kubectl` is currently usable
- whether there are still billable GCP resources running
- whether stale Part 3 VPC/subnet/firewall/route artifacts are still around

This script does **not** create, update, or delete the cluster.

### Step 2. Create or refresh the cluster only when needed

Run:

```bash
python3 cli.py cluster up --config experiment.yaml
```

This does the full bring-up flow:
- creates or replaces the kOps cluster config
- ensures the SSH public key secret exists
- runs `kops update cluster`
- runs `kops validate cluster`
- exports kubeconfig locally
- labels the Kubernetes nodes with canonical `cca-project-nodetype` values such as
  `client-agent-a` and `node-a-8core`

You do **not** need to run this before every experiment.

If `cluster up` fails or is interrupted, do **not** immediately retry it on top of the
half-finished state. Run:

```bash
python3 cli.py cluster down --config experiment.yaml
../../checkCredits.sh
```

Use `../../checkCredits.sh` to confirm there are no leftover billable resources or Part 3
network artifacts before bringing the cluster up again.

### Step 3. Make sure the client VMs are bootstrapped

Run:

```bash
python3 cli.py provision check --config experiment.yaml
```

If your SSH key is passphrase-protected and not already loaded in `ssh-agent`, expect up to
**3 passphrase prompts** here, roughly one per checked client VM. If you want to unlock the
key once instead of on each SSH call, run:

```bash
ssh-add ~/.ssh/cloud-computing
```

This checks that:
- `client-agent-a` exists and has `mcperf`
- `client-agent-b` exists and has `mcperf`
- `client-measure` exists and has `mcperf`
- the `mcperf-agent.service` units are active on the agent VMs
- the randomized Kubernetes node names have the expected canonical
  `cca-project-nodetype` labels so jobs can schedule correctly

The CLI prints each node as `READY` or `WAITING`:
- `WAITING` means bootstrap is still in progress or some expected software/service is missing
- `READY` means that node is usable for experiments
- for `client-agent-a` and `client-agent-b`, `READY` requires `mcperf-agent.service active`
- for `client-measure`, `READY` only requires bootstrap and `mcperf`; `mcperf-agent.service`
  is not expected there

If a node stays in `WAITING`, print the ready-made debug commands with:

```bash
python3 cli.py debug commands --config experiment.yaml --policy schedule.yaml
```

### Step 4. Check the schedule before you spend credits

Run:

```bash
python3 cli.py show --policy schedule.yaml
python3 cli.py audit --policy schedule.yaml --times-csv ../../Part2summary_times.csv
```

Use `show` to read the launch order quickly.

Use `audit` to catch:
- overlapping cores
- unsupported core sets
- memcached collisions
- suspicious idle gaps

### Step 5. Dry run

Run:

```bash
python3 cli.py run once --config experiment.yaml --policy schedule.yaml --dry-run
```

This renders manifests and writes the phase plan, but does not touch the live cluster.

### Step 6. Real run

Run:

```bash
python3 cli.py run once --config experiment.yaml --policy schedule.yaml
```

This:
- cleans previous managed jobs and pods
- checks client provisioning
- launches memcached
- starts the `mcperf` measurement
- launches the batch phases in schedule order
- collects logs and results

### Step 7. Repeated runs

Run:

```bash
python3 cli.py run batch --config experiment.yaml --policy schedule.yaml --runs 3
```

Use this when you want the three measurement files needed for submission.

### Step 8. Pick the best run

Run:

```bash
python3 cli.py results best --experiment part3-handcrafted
```

This sorts the runs by:
1. passing runs first
2. lowest makespan
3. lowest observed p95

### Step 9. Export the submission folder

Run:

```bash
python3 cli.py export submission --experiment part3-handcrafted --group 054 --task 3_1
```

## What Each Command Does

### `python3 cli.py cluster up --config experiment.yaml`

Creates or refreshes the Part 3 cluster. Use it only when the cluster is missing or you
want to apply cluster-definition changes.

### `python3 cli.py provision check --config experiment.yaml`

Checks whether the three client VMs are bootstrapped correctly for Part 3.

### `python3 cli.py debug commands --config experiment.yaml --policy schedule.yaml`

Prints exact `gcloud compute ssh`, `kubectl`, `journalctl`, `tail -f`, and serial-console
commands for debugging the client VMs, memcached pod, and saved `mcperf` output. Add
`--run-id <run-id>` if you want the exact memcached pod name and `mcperf.txt` path for one
run.

### `python3 cli.py show --policy schedule.yaml`

Prints the current schedule in a human-readable format.

### `python3 cli.py audit --policy schedule.yaml --times-csv ../../Part2summary_times.csv`

Runs the static schedule checker using your Part 2 timing data.

### `python3 cli.py run once --config experiment.yaml --policy schedule.yaml --dry-run`

Builds the manifests and phase plan without touching the cluster.

### `python3 cli.py run once --config experiment.yaml --policy schedule.yaml`

Runs one full live experiment.

### `python3 cli.py run batch --config experiment.yaml --policy schedule.yaml --runs 3`

Runs the same experiment multiple times.

### `python3 cli.py results best --experiment part3-handcrafted`

Shows the best completed runs according to the built-in ranking.

### `python3 cli.py export submission --experiment part3-handcrafted --group 054 --task 3_1`

Creates the submission-ready results directory.

## Common Problems

### `kubectl` points to `localhost:8080`

This usually means:
- the cluster is not up yet, or
- kubeconfig was never exported, or
- kubeconfig is stale

First try:

```bash
python3 cli.py cluster up --config experiment.yaml
```

If the cluster already exists and you only need kubeconfig:

```bash
kops export kubecfg --admin --name part3.k8s.local
```

### `cluster up` failed halfway through

Clean up first:

```bash
python3 cli.py cluster down --config experiment.yaml
../../checkCredits.sh
```

Only retry `cluster up` after `../../checkCredits.sh` reports that there are no leftover
billable resources or Part 3 network artifacts.

### `run once` hangs at `Cleaning previous managed workloads`

That usually means `kubectl` cannot actually talk to the cluster, even though the
automation started.

Check:

```bash
kubectl get nodes -o wide
```

If that does not work, fix cluster access before running experiments.

### `provision check` or `run once` says a client is `WAITING`

That usually means the VM bootstrap script did not finish. In this project, the most likely
failure mode is:
- cloud-init started the bootstrap script
- `apt-get build-dep memcached --yes` failed because `deb-src` was not enabled correctly
- the script exited before `mcperf`, `mcperf-agent.service`, and `/opt/cca/bootstrap.done`

Run:

```bash
python3 cli.py debug commands --config experiment.yaml --policy schedule.yaml
```

Then inspect, in this order:
- `cloud-final.service`
- `/var/log/cca-bootstrap.log`
- the serial console output from `gcloud compute instances get-serial-port-output ...`

If provisioning is still failing, do **not** expect a memcached pod yet. `run once` only
applies the memcached manifest after provisioning passes.

If you changed `part3.yaml` to fix the startup script, remember that cloud-init only runs
when the VM is created. Existing client VMs will keep the old broken bootstrap state, so
you need to recreate the cluster with `python3 cli.py cluster down --config experiment.yaml`
followed by `python3 cli.py cluster up --config experiment.yaml`.

Use the command types like this:
- `gcloud compute ssh ...` opens a shell on the VM
- `kubectl exec -it <pod> -- sh` opens a shell inside the container
- `kubectl logs -f`, `journalctl -f`, and `tail -f` follow live output instead of opening a shell

## Important Notes

- The main scheduling file is `schedule.yaml`.
- `run once` does **not** create the cluster for you.
- `cluster up` is a separate step from `run once`.
- The Part 2 timing reference file is `../../Part2summary_times.csv` from this folder.

```

`__init__.py`:

```py
"""Automation framework for Part 3 experiments."""


```

`audit.py`:

```py
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

```

`catalog.py`:

```py
from __future__ import annotations

from dataclasses import dataclass


NODE_A = "node-a-8core"
NODE_B = "node-b-4core"

NODE_A_CORE_SETS = (
    "0-7",
    "0-3",
    "4-7",
    "0-1",
    "2-3",
    "4-5",
    "6-7",
    "0",
    "1",
    "2",
    "3",
    "4",
    "5",
    "6",
    "7",
)

NODE_B_CORE_SETS = (
    "0-3",
    "1-3",
    "0-2",
    "1-2",
    "2-3",
    "0-1",
    "0",
    "1",
    "2",
    "3",
)


@dataclass(frozen=True)
class JobCatalogEntry:
    job_id: str
    image: str
    suite: str
    program: str
    default_node: str
    default_cores: str
    default_threads: int
    allowed_cores_by_node: dict[str, tuple[str, ...]]
    default_cpu_request: str | None = None
    default_memory_request: str | None = None
    default_memory_limit: str | None = None


def count_cores(core_spec: str) -> int:
    total = 0
    for part in core_spec.split(","):
        part = part.strip()
        if not part:
            continue
        if "-" in part:
            start_text, end_text = part.split("-", 1)
            start = int(start_text)
            end = int(end_text)
            if end < start:
                raise ValueError(f"Invalid core range: {core_spec}")
            total += (end - start) + 1
        else:
            int(part)
            total += 1
    if total <= 0:
        raise ValueError(f"Invalid core set: {core_spec}")
    return total


JOB_CATALOG: dict[str, JobCatalogEntry] = {
    "barnes": JobCatalogEntry(
        job_id="barnes",
        image="anakli/cca:splash2x_barnes",
        suite="splash2x",
        program="barnes",
        default_node=NODE_A,
        default_cores="0-7",
        default_threads=8,
        allowed_cores_by_node={NODE_A: NODE_A_CORE_SETS, NODE_B: NODE_B_CORE_SETS},
    ),
    "blackscholes": JobCatalogEntry(
        job_id="blackscholes",
        image="anakli/cca:parsec_blackscholes",
        suite="parsec",
        program="blackscholes",
        default_node=NODE_B,
        default_cores="1-3",
        default_threads=3,
        allowed_cores_by_node={NODE_A: NODE_A_CORE_SETS, NODE_B: NODE_B_CORE_SETS},
    ),
    "canneal": JobCatalogEntry(
        job_id="canneal",
        image="anakli/cca:parsec_canneal",
        suite="parsec",
        program="canneal",
        default_node=NODE_A,
        default_cores="0-7",
        default_threads=8,
        allowed_cores_by_node={NODE_A: NODE_A_CORE_SETS, NODE_B: NODE_B_CORE_SETS},
    ),
    "freqmine": JobCatalogEntry(
        job_id="freqmine",
        image="anakli/cca:parsec_freqmine",
        suite="parsec",
        program="freqmine",
        default_node=NODE_B,
        default_cores="1-3",
        default_threads=3,
        allowed_cores_by_node={NODE_A: NODE_A_CORE_SETS, NODE_B: NODE_B_CORE_SETS},
    ),
    "radix": JobCatalogEntry(
        job_id="radix",
        image="anakli/cca:splash2x_radix",
        suite="splash2x",
        program="radix",
        default_node=NODE_A,
        default_cores="0-7",
        default_threads=8,
        allowed_cores_by_node={NODE_A: NODE_A_CORE_SETS, NODE_B: NODE_B_CORE_SETS},
    ),
    "streamcluster": JobCatalogEntry(
        job_id="streamcluster",
        image="anakli/cca:parsec_streamcluster",
        suite="parsec",
        program="streamcluster",
        default_node=NODE_A,
        default_cores="0-7",
        default_threads=8,
        allowed_cores_by_node={NODE_A: NODE_A_CORE_SETS, NODE_B: NODE_B_CORE_SETS},
    ),
    "vips": JobCatalogEntry(
        job_id="vips",
        image="anakli/cca:parsec_vips",
        suite="parsec",
        program="vips",
        default_node=NODE_A,
        default_cores="0-7",
        default_threads=8,
        allowed_cores_by_node={NODE_A: NODE_A_CORE_SETS, NODE_B: NODE_B_CORE_SETS},
    ),
}


```

`cli.py`:

```py
from __future__ import annotations

import argparse
from pathlib import Path
import sys

if __package__ in {None, ""}:
    package_dir = Path(__file__).resolve().parent
    package_parent = package_dir.parent
    if str(package_parent) not in sys.path:
        sys.path.insert(0, str(package_parent))
    __package__ = package_dir.name

from .audit import audit_schedule, load_runtime_table, load_schedule_model, render_audit_report
from .catalog import JOB_CATALOG
from .cluster import ClusterController
from .collect import collect_live_pods, summarize_run
from .config import load_experiment_config, load_policy_config
from .debug import format_debug_command_hint, render_debug_commands, summarize_provisioning_hints
from .export import export_submission
from .gui import launch_planner_gui
from .provision import (
    check_client_provisioning,
    render_provision_check_note,
    render_provision_expectations,
)
from .results import load_run_summaries, sort_best_runs
from .runner import ExperimentRunner
from .manifests import resolve_jobs


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Part 3 Python orchestrator")
    subparsers = parser.add_subparsers(dest="command", required=True)

    cluster_parser = subparsers.add_parser("cluster")
    cluster_sub = cluster_parser.add_subparsers(dest="cluster_command", required=True)
    cluster_up = cluster_sub.add_parser("up")
    cluster_up.add_argument("--config", required=True)
    cluster_down = cluster_sub.add_parser("down")
    cluster_down.add_argument("--config", required=True)

    debug_parser = subparsers.add_parser("debug")
    debug_sub = debug_parser.add_subparsers(dest="debug_command", required=True)
    debug_commands = debug_sub.add_parser("commands")
    debug_commands.add_argument("--config", required=True)
    debug_commands.add_argument("--policy")
    debug_commands.add_argument("--run-id")

    provision_parser = subparsers.add_parser("provision")
    provision_sub = provision_parser.add_subparsers(dest="provision_command", required=True)
    provision_check = provision_sub.add_parser("check")
    provision_check.add_argument("--config", required=True)

    run_parser = subparsers.add_parser("run")
    run_sub = run_parser.add_subparsers(dest="run_command", required=True)
    run_once = run_sub.add_parser("once")
    run_once.add_argument("--config", required=True)
    run_once.add_argument("--policy", required=True)
    run_once.add_argument("--dry-run", action="store_true")
    run_batch = run_sub.add_parser("batch")
    run_batch.add_argument("--config", required=True)
    run_batch.add_argument("--policy", required=True)
    run_batch.add_argument("--runs", type=int, default=3)
    run_batch.add_argument("--dry-run", action="store_true")

    collect_parser = subparsers.add_parser("collect")
    collect_parser.add_argument("--config", required=True)
    collect_parser.add_argument("--policy", required=True)
    collect_parser.add_argument("--run-dir", required=True)
    collect_parser.add_argument("--live", action="store_true")

    audit_parser = subparsers.add_parser("audit")
    audit_parser.add_argument("--policy", required=True)
    audit_parser.add_argument("--times-csv", required=True)

    gui_parser = subparsers.add_parser("gui")
    gui_parser.add_argument("--policy", required=True)
    gui_parser.add_argument("--times-csv", required=True)

    show_parser = subparsers.add_parser("show")
    show_parser.add_argument("--policy", required=True)

    results_parser = subparsers.add_parser("results")
    results_sub = results_parser.add_subparsers(dest="results_command", required=True)
    results_best = results_sub.add_parser("best")
    results_best.add_argument("--experiment", required=True)
    results_best.add_argument("--results-root", default="part3/automation/runs")

    export_parser = subparsers.add_parser("export")
    export_sub = export_parser.add_subparsers(dest="export_command", required=True)
    export_submission_parser = export_sub.add_parser("submission")
    export_submission_parser.add_argument("--experiment", required=True)
    export_submission_parser.add_argument("--group", required=True)
    export_submission_parser.add_argument("--task", required=True)
    export_submission_parser.add_argument("--results-root", default="part3/automation/runs")
    export_submission_parser.add_argument("--output-root", default=".")
    export_submission_parser.add_argument("--run-id", action="append", dest="run_ids")

    return parser


def main(argv: list[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)

    if args.command == "cluster":
        experiment = load_experiment_config(args.config)
        cluster = ClusterController(experiment)
        if args.cluster_command == "up":
            cluster.cluster_up()
        else:
            cluster.cluster_down()
        return 0

    if args.command == "debug" and args.debug_command == "commands":
        experiment = load_experiment_config(args.config)
        cluster = ClusterController(experiment)
        policy = load_policy_config(args.policy) if args.policy else None
        print(
            render_debug_commands(
                experiment=experiment,
                cluster=cluster,
                policy=policy,
                run_id=args.run_id,
            )
        )
        return 0

    if args.command == "provision":
        experiment = load_experiment_config(args.config)
        cluster = ClusterController(experiment)
        print(render_provision_check_note(experiment.ssh_key_path))
        try:
            statuses = check_client_provisioning(cluster)
        except RuntimeError:
            print(
                "Debug commands:",
                format_debug_command_hint(config_path=experiment.config_path),
            )
            raise
        for status in statuses.values():
            print(status)
        print(render_provision_expectations())
        if any(not status.is_ready for status in statuses.values()):
            for hint in summarize_provisioning_hints(statuses):
                print("Hint:", hint)
            print(
                "Debug commands:",
                format_debug_command_hint(config_path=experiment.config_path),
            )
        return 0

    if args.command == "run":
        experiment = load_experiment_config(args.config)
        policy = load_policy_config(args.policy)
        runner = ExperimentRunner(experiment, policy)
        if args.run_command == "once":
            run_dir = runner.run_once(dry_run=args.dry_run)
            print(run_dir)
        else:
            run_dirs = runner.run_batch(args.runs, dry_run=args.dry_run)
            for run_dir in run_dirs:
                print(run_dir)
        return 0

    if args.command == "collect":
        experiment = load_experiment_config(args.config)
        policy = load_policy_config(args.policy)
        run_dir = Path(args.run_dir).resolve()
        if args.live:
            cluster = ClusterController(experiment)
            collect_live_pods(cluster, run_dir)
        summary = summarize_run(
            run_dir,
            experiment_id=experiment.experiment_id,
            policy_name=policy.policy_name,
            run_id=run_dir.name,
            expected_jobs=set(JOB_CATALOG),
        )
        print(summary["overall_status"])
        return 0

    if args.command == "audit":
        model = load_schedule_model(args.policy)
        runtime_table = load_runtime_table(args.times_csv)
        report = audit_schedule(model, runtime_table)
        print(render_audit_report(report))
        return 1 if report.errors else 0

    if args.command == "gui":
        launch_planner_gui(policy_path_str=args.policy, times_csv_path_str=args.times_csv)
        return 0

    if args.command == "show":
        policy = load_policy_config(args.policy)
        jobs = resolve_jobs(policy, "preview")
        print(f"Policy: {policy.policy_name}")
        print(
            "Memcached:",
            f"vm={policy.memcached.node}",
            f"cores={policy.memcached.cores}",
            f"threads={policy.memcached.threads}",
        )
        for phase in policy.phases:
            dependency = phase.after
            if phase.jobs_complete:
                dependency += ":" + ",".join(phase.jobs_complete)
            print(f"{phase.phase_id} after {dependency} delay={phase.delay_s}s")
            for job_id in phase.launch:
                job = jobs[job_id]
                print(
                    "  -",
                    job_id,
                    f"vm={job.node}",
                    f"cores={job.cores}",
                    f"threads={job.threads}",
                )
        return 0

    if args.command == "results" and args.results_command == "best":
        summaries = sort_best_runs(
            load_run_summaries(Path(args.results_root).resolve(), args.experiment)
        )
        if not summaries:
            print("No completed run summaries found.")
            return 0
        for summary in summaries:
            print(
                summary.get("run_id"),
                summary.get("policy_name"),
                summary.get("overall_status"),
                f"makespan={summary.get('makespan_s')}",
                f"max_p95_us={summary.get('max_observed_p95_us')}",
                summary.get("run_dir"),
            )
        return 0

    if args.command == "export" and args.export_command == "submission":
        output_dir = export_submission(
            results_root=Path(args.results_root).resolve(),
            experiment_id=args.experiment,
            group=args.group,
            task=args.task,
            output_root=Path(args.output_root).resolve(),
            selected_run_ids=args.run_ids,
        )
        print(output_dir)
        return 0

    parser.error("Unknown command")
    return 2


if __name__ == "__main__":
    raise SystemExit(main())

```

`cluster.py`:

```py
from __future__ import annotations

import json
import shlex
import subprocess
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

from .config import ExperimentConfig
from .utils import CommandResult, run_command


@dataclass(frozen=True)
class NodeInfo:
    name: str
    nodetype: str
    internal_ip: str | None
    external_ip: str | None


CANONICAL_NODETYPES = (
    "client-agent-a",
    "client-agent-b",
    "client-measure",
    "node-a-8core",
    "node-b-4core",
)


class ClusterController:
    def __init__(self, config: ExperimentConfig):
        self.config = config

    @property
    def env(self) -> dict[str, str]:
        return {"KOPS_STATE_STORE": self.config.kops_state_store}

    def _public_key_path(self) -> Path:
        if self.config.ssh_key_path.suffix == ".pub":
            return self.config.ssh_key_path
        return Path(str(self.config.ssh_key_path) + ".pub")

    def kops(self, *args: str, check: bool = True) -> CommandResult:
        return run_command(["kops", *args], env=self.env, check=check)

    def kubectl(self, *args: str, check: bool = True) -> CommandResult:
        return run_command(["kubectl", *args], env=self.env, check=check)

    def _announce(self, message: str) -> None:
        print(f"[cluster] {message}")

    def kubectl_json(self, *args: str) -> dict[str, object]:
        result = self.kubectl(*args)
        return json.loads(result.stdout)

    def cluster_exists(self) -> bool:
        result = self.kops("get", "cluster", "--name", self.config.cluster_name, check=False)
        return result.returncode == 0

    def cluster_up(self) -> None:
        self._announce(f"Preparing cluster {self.config.cluster_name}")
        if not self.cluster_exists():
            self._announce(f"Creating cluster definition from {self.config.cluster_config_path}")
            run_command(
                ["kops", "create", "-f", str(self.config.cluster_config_path)],
                env=self.env,
                live_output=True,
                output_prefix="[kops] ",
            )
        else:
            self._announce(f"Updating existing cluster definition from {self.config.cluster_config_path}")
            run_command(
                ["kops", "replace", "-f", str(self.config.cluster_config_path), "--force"],
                env=self.env,
                live_output=True,
                output_prefix="[kops] ",
            )
        public_key = self._public_key_path()
        if not public_key.exists():
            raise FileNotFoundError(f"SSH public key not found: {public_key}")
        self._announce(f"Ensuring SSH admin key exists: {public_key}")
        create_secret = self.kops(
            "create",
            "secret",
            "--name",
            self.config.cluster_name,
            "sshpublickey",
            "admin",
            "-i",
            str(public_key),
            check=False,
        )
        if create_secret.returncode != 0 and "already exists" not in create_secret.combined_output.lower():
            raise RuntimeError(create_secret.combined_output)
        self._announce("Applying cloud changes with kops update")
        run_command(
            ["kops", "update", "cluster", "--name", self.config.cluster_name, "--yes", "--admin"],
            env=self.env,
            live_output=True,
            output_prefix="[kops] ",
        )
        self._announce("Waiting for cluster validation to succeed")
        run_command(
            ["kops", "validate", "cluster", "--wait", "10m"],
            env=self.env,
            live_output=True,
            output_prefix="[kops] ",
        )
        self._announce("Exporting kubeconfig for kubectl")
        run_command(
            ["kops", "export", "kubecfg", "--admin", "--name", self.config.cluster_name],
            env=self.env,
            live_output=True,
            output_prefix="[kops] ",
        )
        self._announce("Ensuring canonical node labels")
        self.ensure_canonical_node_labels()
        self._announce("Cluster is ready")

    def cluster_down(self) -> None:
        self._announce(f"Deleting cluster {self.config.cluster_name}")
        run_command(
            ["kops", "delete", "cluster", "--name", self.config.cluster_name, "--yes"],
            env=self.env,
            live_output=True,
            output_prefix="[kops] ",
        )

    def _infer_canonical_nodetype(self, node_name: str) -> str | None:
        for nodetype in sorted(CANONICAL_NODETYPES, key=len, reverse=True):
            if node_name == nodetype or node_name.startswith(f"{nodetype}-"):
                return nodetype
        return None

    def _node_info_from_payload(
        self,
        item: dict[str, object],
        *,
        nodetype: str,
    ) -> NodeInfo:
        metadata = item.get("metadata", {})
        status = item.get("status", {})
        addresses = status.get("addresses", [])
        internal_ip = None
        external_ip = None
        for address in addresses:
            address_type = address.get("type")
            if address_type == "InternalIP":
                internal_ip = address.get("address")
            elif address_type == "ExternalIP":
                external_ip = address.get("address")
        return NodeInfo(
            name=metadata.get("name"),
            nodetype=nodetype,
            internal_ip=internal_ip,
            external_ip=external_ip,
        )

    def _discover_nodes_from_payload(
        self,
        payload: dict[str, object],
        *,
        allow_name_inference: bool,
    ) -> dict[str, NodeInfo]:
        nodes: dict[str, NodeInfo] = {}
        for item in payload.get("items", []):
            labels = item.get("metadata", {}).get("labels", {})
            nodetype = labels.get("cca-project-nodetype")
            if not nodetype and allow_name_inference:
                node_name = item.get("metadata", {}).get("name")
                if isinstance(node_name, str) and node_name:
                    nodetype = self._infer_canonical_nodetype(node_name)
            if not nodetype:
                continue
            info = self._node_info_from_payload(item, nodetype=nodetype)
            existing = nodes.get(nodetype)
            if existing is not None and existing.name != info.name:
                raise RuntimeError(
                    "Multiple Kubernetes nodes map to the same canonical nodetype "
                    f"{nodetype}: {existing.name}, {info.name}"
                )
            nodes[nodetype] = info
        return nodes

    def discover_nodes(self) -> dict[str, NodeInfo]:
        payload = self.kubectl_json("get", "nodes", "-o", "json")
        return self._discover_nodes_from_payload(payload, allow_name_inference=True)

    def ensure_canonical_node_labels(self) -> dict[str, NodeInfo]:
        payload = self.kubectl_json("get", "nodes", "-o", "json")
        updated = False
        for item in payload.get("items", []):
            metadata = item.get("metadata", {})
            node_name = metadata.get("name")
            if not isinstance(node_name, str) or not node_name:
                continue
            desired_nodetype = self._infer_canonical_nodetype(node_name)
            if desired_nodetype is None:
                continue
            labels = metadata.get("labels", {})
            current_nodetype = labels.get("cca-project-nodetype")
            if current_nodetype == desired_nodetype:
                continue
            self._announce(
                f"Labeling node {node_name} with cca-project-nodetype={desired_nodetype}"
            )
            self.kubectl(
                "label",
                "nodes",
                node_name,
                f"cca-project-nodetype={desired_nodetype}",
                "--overwrite",
            )
            updated = True
        if updated:
            payload = self.kubectl_json("get", "nodes", "-o", "json")
        return self._discover_nodes_from_payload(payload, allow_name_inference=True)

    def ssh_args(
        self,
        node_name: str,
        *,
        command: str | None = None,
    ) -> list[str]:
        args = [
            "gcloud",
            "compute",
            "ssh",
            f"{self.config.ssh_user}@{node_name}",
            "--zone",
            self.config.zone,
            "--ssh-key-file",
            str(self.config.ssh_key_path),
        ]
        if command is not None:
            args.extend(["--command", command])
        return args

    def ssh_command_str(
        self,
        node_name: str,
        *,
        command: str | None = None,
    ) -> str:
        return shlex.join(self.ssh_args(node_name, command=command))

    def serial_port_output_args(
        self,
        node_name: str,
        *,
        port: int = 1,
    ) -> list[str]:
        return [
            "gcloud",
            "compute",
            "instances",
            "get-serial-port-output",
            node_name,
            "--zone",
            self.config.zone,
            f"--port={port}",
        ]

    def serial_port_output_command_str(
        self,
        node_name: str,
        *,
        port: int = 1,
    ) -> str:
        return shlex.join(self.serial_port_output_args(node_name, port=port))

    def ssh(
        self,
        node_name: str,
        command: str,
        *,
        check: bool = True,
    ) -> CommandResult:
        return run_command(self.ssh_args(node_name, command=command), check=check)

    def popen_ssh(
        self,
        node_name: str,
        command: str,
        *,
        stdout,
        stderr,
    ) -> subprocess.Popen[str]:
        return subprocess.Popen(
            self.ssh_args(node_name, command=command),
            text=True,
            stdout=stdout,
            stderr=stderr,
        )

    def apply_manifest(self, manifest_path: Path) -> None:
        self.kubectl("apply", "-f", str(manifest_path))

    def cleanup_managed_workloads(self) -> None:
        self._announce("Deleting previous managed jobs and pods")
        self.kubectl("delete", "jobs", "-l", "cca-project-managed=true", "--ignore-not-found=true", check=False)
        self.kubectl("delete", "pods", "-l", "cca-project-managed=true", "--ignore-not-found=true", check=False)
        deadline = time.time() + 180
        iteration = 0
        while time.time() < deadline:
            pods = self.kubectl_json("get", "pods", "-l", "cca-project-managed=true", "-o", "json")
            jobs = self.kubectl_json("get", "jobs", "-l", "cca-project-managed=true", "-o", "json")
            if not pods.get("items") and not jobs.get("items"):
                self._announce("Managed workload cleanup finished")
                return
            if iteration == 0 or iteration % 3 == 0:
                self._announce(
                    "Waiting for managed workloads to disappear: "
                    f"{len(pods.get('items', []))} pods, {len(jobs.get('items', []))} jobs remaining"
                )
            iteration += 1
            time.sleep(5)
        raise TimeoutError("Timed out while cleaning managed workloads")

    def wait_for_pod_ready(self, pod_name: str, timeout_s: int = 300) -> None:
        self._announce(f"Waiting for pod/{pod_name} to become Ready (timeout {timeout_s}s)")
        self.kubectl("wait", "--for=condition=Ready", f"pod/{pod_name}", f"--timeout={timeout_s}s")
        self._announce(f"pod/{pod_name} is Ready")

    def get_pod_by_run_role(self, run_id: str, role: str) -> dict[str, object]:
        payload = self.kubectl_json(
            "get",
            "pods",
            "-l",
            f"cca-project-run-id={run_id},cca-project-role={role}",
            "-o",
            "json",
        )
        items = payload.get("items", [])
        if not items:
            raise RuntimeError(f"No pod found for role={role} run_id={run_id}")
        return items[0]

    def _job_snapshot_from_payload(self, payload: dict[str, object]) -> dict[str, object]:
        status = payload.get("status", {})
        succeeded = status.get("succeeded", 0) or 0
        failed = status.get("failed", 0) or 0
        if succeeded >= 1:
            state = "completed"
        elif failed >= 1:
            state = "failed"
        else:
            state = "running"
        return {"status": state, "payload": payload}

    def get_run_jobs_snapshot(self, run_id: str) -> dict[str, dict[str, object]]:
        payload = self.kubectl_json(
            "get",
            "jobs",
            "-l",
            f"cca-project-run-id={run_id}",
            "-o",
            "json",
        )
        snapshots: dict[str, dict[str, object]] = {}
        for item in payload.get("items", []):
            metadata = item.get("metadata", {})
            job_name = metadata.get("name")
            if isinstance(job_name, str) and job_name:
                snapshots[job_name] = self._job_snapshot_from_payload(item)
        return snapshots

    def get_jobs_snapshot(self, job_names: Iterable[str]) -> dict[str, dict[str, object]]:
        snapshots: dict[str, dict[str, object]] = {}
        for job_name in job_names:
            result = self.kubectl("get", "job", job_name, "-o", "json", check=False)
            if result.returncode != 0:
                snapshots[job_name] = {"status": "missing"}
                continue
            payload = json.loads(result.stdout)
            snapshots[job_name] = self._job_snapshot_from_payload(payload)
        return snapshots

    def wait_for_jobs(self, job_names: list[str], timeout_s: int) -> None:
        deadline = time.time() + timeout_s
        while time.time() < deadline:
            snapshot = self.get_jobs_snapshot(job_names)
            statuses = {name: info["status"] for name, info in snapshot.items()}
            if any(status == "failed" for status in statuses.values()):
                raise RuntimeError(f"One or more jobs failed: {statuses}")
            if all(status == "completed" for status in statuses.values()):
                return
            time.sleep(5)
        raise TimeoutError(f"Timed out waiting for jobs: {job_names}")

    def capture_pods_json(self, destination: Path) -> None:
        result = self.kubectl("get", "pods", "-o", "json")
        destination.write_text(result.stdout, encoding="utf-8")

    def describe_job(self, job_name: str, destination: Path) -> None:
        result = self.kubectl("describe", "job", job_name, check=False)
        destination.write_text(result.combined_output + "\n", encoding="utf-8")

```

`collect.py`:

```py
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


```

`config.py`:

```py
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

```

`debug.py`:

```py
from __future__ import annotations

import shlex
from pathlib import Path

from .cluster import ClusterController, NodeInfo
from .config import ExperimentConfig, PolicyConfig
from .manifests import resolve_memcached
from .provision import ProvisionStatus, REQUIRED_CLIENT_NODETYPES


def _shell_join(args: list[str]) -> str:
    return shlex.join(args)


def _append_command(lines: list[str], label: str, command: str, *, indent: str = "  ") -> None:
    lines.append(f"{indent}{label}: {command}")


def format_debug_command_hint(
    *,
    config_path: Path,
    policy_path: Path | None = None,
    run_id: str | None = None,
) -> str:
    args = ["python3", "cli.py", "debug", "commands", "--config", str(config_path)]
    if policy_path is not None:
        args.extend(["--policy", str(policy_path)])
    if run_id is not None:
        args.extend(["--run-id", run_id])
    return _shell_join(args)


def summarize_provisioning_hints(statuses: dict[str, ProvisionStatus]) -> list[str]:
    hints: list[str] = []
    for nodetype, status in statuses.items():
        if not status.bootstrap_ready and not status.mcperf_present and status.agent_service_state == "not-installed":
            hints.append(
                f"{nodetype}: bootstrap appears to have failed before mcperf installation; "
                "inspect cloud-init and serial logs."
            )
        elif nodetype.startswith("client-agent") and status.bootstrap_ready and status.mcperf_present:
            if status.agent_service_state == "not-installed":
                hints.append(
                    f"{nodetype}: mcperf is installed, but mcperf-agent.service was not installed; "
                    "inspect cloud-init and serial logs."
                )
            elif status.agent_service_state not in {None, "active"}:
                hints.append(
                    f"{nodetype}: mcperf-agent.service is not active; inspect the service status and logs."
                )
    if any(not status.is_ready for status in statuses.values()):
        hints.append("No memcached pod is expected yet; `run once` only launches memcached after provisioning passes.")
    return hints


def _vm_command_lines(cluster: ClusterController, nodetype: str, node: NodeInfo | None) -> list[str]:
    lines = [f"{nodetype}:"]
    if node is None:
        lines.append("  Node is not currently discoverable via kubectl.")
        lines.append(f"  Expected canonical nodetype: {nodetype}")
        return lines

    lines[0] = f"{nodetype} ({node.name}):"
    _append_command(lines, "Open an SSH shell", cluster.ssh_command_str(node.name))
    _append_command(
        lines,
        "Check cloud-init status",
        cluster.ssh_command_str(node.name, command="sudo systemctl status cloud-final.service --no-pager -l"),
    )
    _append_command(
        lines,
        "Read bootstrap log",
        cluster.ssh_command_str(node.name, command="sudo tail -n 200 /var/log/cca-bootstrap.log"),
    )
    _append_command(
        lines,
        "Read cloud-init journal",
        cluster.ssh_command_str(node.name, command="sudo journalctl -u cloud-final.service -b --no-pager -n 200"),
    )
    if nodetype.startswith("client-agent"):
        _append_command(
            lines,
            "Check mcperf-agent.service status",
            cluster.ssh_command_str(node.name, command="sudo systemctl status mcperf-agent.service --no-pager -l"),
        )
        _append_command(
            lines,
            "Read mcperf-agent journal",
            cluster.ssh_command_str(node.name, command="sudo journalctl -u mcperf-agent.service -n 200 --no-pager"),
        )
        _append_command(
            lines,
            "Follow live mcperf-agent output",
            cluster.ssh_command_str(node.name, command="sudo journalctl -u mcperf-agent.service -f"),
        )
        _append_command(
            lines,
            "Follow /var/log/mcperf-agent.log",
            cluster.ssh_command_str(node.name, command="sudo tail -f /var/log/mcperf-agent.log"),
        )
    _append_command(
        lines,
        "Check running mcperf processes",
        cluster.ssh_command_str(node.name, command="pgrep -a mcperf"),
    )
    _append_command(lines, "Read serial console output", cluster.serial_port_output_command_str(node.name, port=1))
    return lines


def render_debug_commands(
    *,
    experiment: ExperimentConfig,
    cluster: ClusterController,
    policy: PolicyConfig | None = None,
    run_id: str | None = None,
) -> str:
    nodes = cluster.discover_nodes()
    mcperf_tail_command: str | None = None
    if run_id is not None:
        mcperf_path = experiment.results_root / experiment.experiment_id / run_id / "mcperf.txt"
        mcperf_tail_command = _shell_join(["tail", "-f", str(mcperf_path)])

    lines = [
        "Part 3 debug guide",
        "",
        "Resolved nodes:",
    ]
    for nodetype in REQUIRED_CLIENT_NODETYPES:
        node = nodes.get(nodetype)
        if node is None:
            lines.append(f"- {nodetype}: not currently discoverable")
        else:
            lines.append(f"- {nodetype}: {node.name}")

    lines.extend(
        [
            "",
            "What to run first:",
            "- To watch load generation, follow `mcperf-agent.service` on `client-agent-a` and `client-agent-b`.",
            "- To watch the benchmark output, follow the local `mcperf.txt` file created by `run once`.",
            "- To confirm the measurement process exists on `client-measure`, run `pgrep -a mcperf` there.",
            "- If provisioning is failing, start with `cloud-final.service`, `/var/log/cca-bootstrap.log`, and the serial console.",
            "",
            "How to read the commands:",
            "- `gcloud compute ssh ...` opens a shell on a VM.",
            "- `kubectl exec -it ... -- sh` opens a shell inside a container.",
            "- `kubectl logs -f`, `journalctl -f`, and `tail -f` stream live output.",
            "- `client-agent-a` and `client-agent-b` run the long-lived `mcperf-agent.service`.",
            "- `client-measure` does not run `mcperf-agent.service`; `run once` starts `./mcperf ...` over SSH and saves stdout into your local `mcperf.txt`.",
            "",
            "Live mcperf output:",
        ]
    )
    if nodes.get("client-agent-a") is not None:
        lines.append(
            "- Agent A output: "
            + cluster.ssh_command_str(
                nodes["client-agent-a"].name,
                command="sudo journalctl -u mcperf-agent.service -f",
            )
        )
    if nodes.get("client-agent-b") is not None:
        lines.append(
            "- Agent B output: "
            + cluster.ssh_command_str(
                nodes["client-agent-b"].name,
                command="sudo journalctl -u mcperf-agent.service -f",
            )
        )
    if mcperf_tail_command is not None:
        lines.append(f"- Measurement output on your workstation: {mcperf_tail_command}")
    else:
        lines.append(
            "- Measurement output on your workstation: add `--run-id <run-id>` to get the exact `tail -f .../mcperf.txt` command."
        )
    if nodes.get("client-measure") is not None:
        lines.append(
            "- Check whether the measurement process exists on client-measure: "
            + cluster.ssh_command_str(nodes["client-measure"].name, command="pgrep -a mcperf")
        )

    lines.extend(["", "Exact VM commands:"])
    for nodetype in REQUIRED_CLIENT_NODETYPES:
        lines.extend(_vm_command_lines(cluster, nodetype, nodes.get(nodetype)))

    lines.extend(
        [
            "",
            "Memcached:",
            "  First check whether a memcached pod exists: "
            + _shell_join(["kubectl", "get", "pods", "-A", "-l", "cca-project-role=memcached", "-o", "wide"]),
        ]
    )
    if policy is not None and run_id is not None:
        memcached_name = resolve_memcached(policy, run_id).kubernetes_name
        lines.append("  Describe the exact pod: " + _shell_join(["kubectl", "describe", "pod", memcached_name]))
        lines.append("  Follow the pod logs: " + _shell_join(["kubectl", "logs", "-f", memcached_name]))
        lines.append("  Open a shell in the pod: " + _shell_join(["kubectl", "exec", "-it", memcached_name, "--", "sh"]))
        lines.append(
            "  If the exact pod is missing, that is expected until provisioning passes and the run reaches memcached startup."
        )
    else:
        lines.append("  After you find a pod, describe it: kubectl describe pod <pod>")
        lines.append("  After you find a pod, follow its logs: kubectl logs -f <pod>")
        lines.append("  After you find a pod, open a shell inside it: kubectl exec -it <pod> -- sh")
        if run_id is not None and policy is None:
            lines.append("  Pass `--policy` as well if you want the exact memcached pod name for that run id.")
        lines.append("  No memcached pod is expected until provisioning passes.")

    lines.extend(["", "Measurement output:"])
    if mcperf_tail_command is not None:
        lines.append("  Follow the live measurement output on your workstation: " + mcperf_tail_command)
        lines.append("  This is usually the most useful place to watch the benchmark while `run once` is active.")
    else:
        experiment_root = experiment.results_root / experiment.experiment_id
        lines.append(
            f"  Find saved mcperf outputs on your workstation: find {shlex.quote(str(experiment_root))} -maxdepth 2 -name mcperf.txt | sort"
        )
        lines.append("  Add `--run-id` if you want the exact `tail -f` command for one run.")
    return "\n".join(lines)

```

`experiment.yaml`:

```yaml
{
  "experiment_id": "part3-handcrafted",
  "cluster_name": "part3.k8s.local",
  "zone": "europe-west1-b",
  "kops_state_store": "gs://cca-eth-2026-group-54-mpelossi",
  "ssh_key_path": "~/.ssh/cloud-computing",
  "ssh_user": "ubuntu",
  "cluster_config_path": "part3.yaml",
  "results_root": "runs",
  "submission_group": "054",
  "memcached_name": "some-memcached",
  "remote_repo_dir": "/opt/cca/memcache-perf-dynamic",
  "mcperf_measurement": {
    "agent_a_threads": 2,
    "agent_b_threads": 4,
    "measure_threads": 6,
    "connections": 4,
    "depth": 4,
    "qps_interval": 1000,
    "scan_start": 30000,
    "scan_stop": 30500,
    "scan_step": 5,
    "max_start_wait_s": 180,
    "completion_timeout_s": 3600
  }
}

```

`export.py`:

```py
from __future__ import annotations

import json
import shutil
from pathlib import Path


def _load_summary(path: Path) -> dict[str, object]:
    return json.loads(path.read_text(encoding="utf-8"))


def export_submission(
    *,
    results_root: Path,
    experiment_id: str,
    group: str,
    task: str,
    output_root: Path,
    selected_run_ids: list[str] | None = None,
) -> Path:
    if task != "3_1":
        raise ValueError("Only task 3_1 export is implemented")
    experiment_root = results_root / experiment_id
    if not experiment_root.exists():
        raise FileNotFoundError(f"Experiment directory not found: {experiment_root}")
    run_dirs = [path for path in experiment_root.iterdir() if path.is_dir()]
    summaries: list[tuple[Path, dict[str, object]]] = []
    for run_dir in sorted(run_dirs):
        summary_path = run_dir / "summary.json"
        if not summary_path.exists():
            continue
        summary = _load_summary(summary_path)
        if summary.get("overall_status") != "pass":
            continue
        summaries.append((run_dir, summary))
    if selected_run_ids:
        selected = [item for item in summaries if item[1].get("run_id") in selected_run_ids]
    else:
        selected = summaries[-3:]
    if len(selected) != 3:
        raise ValueError("Submission export requires exactly three successful runs")
    target_dir = output_root / f"part_3_1_results_group_{str(group).zfill(3)}"
    target_dir.mkdir(parents=True, exist_ok=True)
    for index, (run_dir, _) in enumerate(selected, start=1):
        shutil.copyfile(run_dir / "pods.json", target_dir / f"pods_{index}.json")
        shutil.copyfile(run_dir / "mcperf.txt", target_dir / f"mcperf_{index}.txt")
    return target_dir


```

`gui.py`:

```py
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

try:
    import tkinter as tk
    from tkinter import messagebox, ttk

    TKINTER_IMPORT_ERROR: Exception | None = None
except Exception as exc:  # pragma: no cover - depends on local Python build
    tk = None  # type: ignore[assignment]
    ttk = None  # type: ignore[assignment]
    messagebox = None  # type: ignore[assignment]
    TKINTER_IMPORT_ERROR = exc

from .audit import (
    AuditJob,
    AuditMemcached,
    AuditReport,
    audit_schedule,
    build_schedule_model,
    dependency_text,
    estimate_runtime,
    load_runtime_table,
    load_schedule_model,
    parse_dependency_text,
    write_policy_document,
)
from .catalog import JOB_CATALOG, NODE_A, NODE_A_CORE_SETS, NODE_B, NODE_B_CORE_SETS


TIMELINE_COLORS = (
    "#d95f02",
    "#1b9e77",
    "#7570b3",
    "#e7298a",
    "#66a61e",
    "#e6ab02",
    "#a6761d",
    "#1f78b4",
)


@dataclass(frozen=True)
class PlannerJobState:
    job_id: str
    order: int
    node: str
    cores: str
    threads: int
    after_text: str
    delay_s: int


@dataclass(frozen=True)
class PlannerState:
    policy_name: str
    memcached_node: str
    memcached_cores: str
    memcached_threads: int
    jobs: tuple[PlannerJobState, ...]


@dataclass
class JobRowWidgets:
    order_var: tk.StringVar
    node_var: tk.StringVar
    cores_var: tk.StringVar
    threads_var: tk.StringVar
    after_var: tk.StringVar
    delay_var: tk.StringVar
    duration_var: tk.StringVar
    cores_box: ttk.Combobox


def planner_state_from_model(model) -> PlannerState:
    ordered_jobs = sorted(model.jobs.values(), key=lambda item: (item.order, item.job_id))
    return PlannerState(
        policy_name=model.policy_name,
        memcached_node=model.memcached.node,
        memcached_cores=model.memcached.cores,
        memcached_threads=model.memcached.threads,
        jobs=tuple(
            PlannerJobState(
                job_id=job.job_id,
                order=index + 1,
                node=job.node,
                cores=job.cores,
                threads=job.threads,
                after_text=dependency_text(job.dependencies),
                delay_s=job.delay_s,
            )
            for index, job in enumerate(ordered_jobs)
        ),
    )


def build_model_from_planner_state(
    state: PlannerState,
    *,
    config_path: Path | None = None,
    parse_errors: tuple[str, ...] = (),
) -> object:
    jobs = {
        job.job_id: AuditJob(
            job_id=job.job_id,
            node=job.node,
            cores=job.cores,
            threads=job.threads,
            dependencies=parse_dependency_text(job.after_text),
            delay_s=job.delay_s,
            order=job.order,
        )
        for job in sorted(state.jobs, key=lambda item: (item.order, item.job_id))
    }
    return build_schedule_model(
        policy_name=state.policy_name,
        memcached=AuditMemcached(
            node=state.memcached_node,
            cores=state.memcached_cores,
            threads=state.memcached_threads,
        ),
        jobs=jobs,
        config_path=config_path,
        parse_errors=parse_errors,
    )


class PlannerApp:
    def __init__(self, root: Any, *, policy_path: Path, times_csv_path: Path) -> None:
        self.root = root
        self.policy_path = policy_path
        self.runtime_table = load_runtime_table(str(times_csv_path))
        self.status_var = tk.StringVar(value="Loading...")
        self.makespan_var = tk.StringVar(value="Estimated makespan: n/a")
        self.policy_name_var = tk.StringVar()
        self.memcached_node_var = tk.StringVar()
        self.memcached_cores_var = tk.StringVar()
        self.memcached_threads_var = tk.StringVar()
        self.validation_text: tk.Text | None = None
        self.node_canvases: dict[str, tk.Canvas] = {}
        self.job_rows: dict[str, JobRowWidgets] = {}
        self.color_by_job = {
            "memcached": "#9e9e9e",
            **{job_id: TIMELINE_COLORS[index % len(TIMELINE_COLORS)] for index, job_id in enumerate(sorted(JOB_CATALOG))},
        }
        self._refresh_after_id: str | None = None
        self._build_ui()
        self.reload_from_disk()

    def _build_ui(self) -> None:
        self.root.title("Part 3 Schedule Planner")
        self.root.geometry("1360x980")
        self.root.columnconfigure(0, weight=1)
        self.root.rowconfigure(3, weight=1)

        top_bar = ttk.Frame(self.root, padding=12)
        top_bar.grid(row=0, column=0, sticky="ew")
        top_bar.columnconfigure(1, weight=1)
        ttk.Label(top_bar, text="Policy file:").grid(row=0, column=0, sticky="w")
        ttk.Label(top_bar, text=str(self.policy_path)).grid(row=0, column=1, sticky="w")
        ttk.Button(top_bar, text="Reload", command=self.reload_from_disk).grid(row=0, column=2, padx=(12, 0))
        ttk.Button(top_bar, text="Save", command=self.save_to_disk).grid(row=0, column=3, padx=(8, 0))
        ttk.Label(top_bar, textvariable=self.status_var).grid(row=1, column=0, columnspan=2, sticky="w", pady=(8, 0))
        ttk.Label(top_bar, textvariable=self.makespan_var).grid(row=1, column=2, columnspan=2, sticky="e", pady=(8, 0))

        general_frame = ttk.LabelFrame(self.root, text="Policy", padding=12)
        general_frame.grid(row=1, column=0, sticky="ew", padx=12, pady=(0, 12))
        general_frame.columnconfigure(1, weight=1)
        ttk.Label(general_frame, text="Policy name").grid(row=0, column=0, sticky="w")
        ttk.Entry(general_frame, textvariable=self.policy_name_var, width=40).grid(row=0, column=1, sticky="ew")
        ttk.Label(general_frame, text="Memcached node").grid(row=0, column=2, sticky="w", padx=(12, 0))
        memcached_node_box = ttk.Combobox(
            general_frame,
            textvariable=self.memcached_node_var,
            values=(NODE_A, NODE_B),
            width=16,
            state="readonly",
        )
        memcached_node_box.grid(row=0, column=3, sticky="w")
        ttk.Label(general_frame, text="Memcached cores").grid(row=0, column=4, sticky="w", padx=(12, 0))
        self.memcached_cores_box = ttk.Combobox(general_frame, textvariable=self.memcached_cores_var, width=12)
        self.memcached_cores_box.grid(row=0, column=5, sticky="w")
        ttk.Label(general_frame, text="Memcached threads").grid(row=0, column=6, sticky="w", padx=(12, 0))
        tk.Spinbox(general_frame, from_=1, to=8, width=6, textvariable=self.memcached_threads_var).grid(row=0, column=7, sticky="w")

        jobs_frame = ttk.LabelFrame(self.root, text="Jobs", padding=12)
        jobs_frame.grid(row=2, column=0, sticky="ew", padx=12, pady=(0, 12))
        headers = ("Job", "Order", "Node", "Cores", "Threads", "After", "Delay", "Est. runtime")
        for column, header in enumerate(headers):
            ttk.Label(jobs_frame, text=header).grid(row=0, column=column, sticky="w", padx=(0, 8), pady=(0, 8))
        for row_index, job_id in enumerate(sorted(JOB_CATALOG), start=1):
            ttk.Label(jobs_frame, text=job_id).grid(row=row_index, column=0, sticky="w", padx=(0, 8))
            widgets = JobRowWidgets(
                order_var=tk.StringVar(),
                node_var=tk.StringVar(),
                cores_var=tk.StringVar(),
                threads_var=tk.StringVar(),
                after_var=tk.StringVar(),
                delay_var=tk.StringVar(),
                duration_var=tk.StringVar(value="n/a"),
                cores_box=ttk.Combobox(jobs_frame, width=12),
            )
            self.job_rows[job_id] = widgets
            tk.Spinbox(jobs_frame, from_=1, to=99, width=6, textvariable=widgets.order_var).grid(row=row_index, column=1, sticky="w", padx=(0, 8))
            node_box = ttk.Combobox(
                jobs_frame,
                textvariable=widgets.node_var,
                values=(NODE_A, NODE_B),
                width=16,
                state="readonly",
            )
            node_box.grid(row=row_index, column=2, sticky="w", padx=(0, 8))
            widgets.cores_box.grid(row=row_index, column=3, sticky="w", padx=(0, 8))
            tk.Spinbox(jobs_frame, from_=1, to=8, width=6, textvariable=widgets.threads_var).grid(row=row_index, column=4, sticky="w", padx=(0, 8))
            ttk.Entry(jobs_frame, textvariable=widgets.after_var, width=24).grid(row=row_index, column=5, sticky="w", padx=(0, 8))
            tk.Spinbox(jobs_frame, from_=0, to=3600, width=6, textvariable=widgets.delay_var).grid(row=row_index, column=6, sticky="w", padx=(0, 8))
            ttk.Label(jobs_frame, textvariable=widgets.duration_var, width=12).grid(row=row_index, column=7, sticky="w")
            node_box.bind("<<ComboboxSelected>>", lambda _event, current_job=job_id: self._update_job_core_values(current_job))

        feedback_frame = ttk.Frame(self.root, padding=(12, 0, 12, 12))
        feedback_frame.grid(row=3, column=0, sticky="nsew")
        feedback_frame.columnconfigure(0, weight=1)
        feedback_frame.columnconfigure(1, weight=1)
        feedback_frame.rowconfigure(0, weight=1)

        validation_frame = ttk.LabelFrame(feedback_frame, text="Validation", padding=12)
        validation_frame.grid(row=0, column=0, sticky="nsew", padx=(0, 6))
        validation_frame.rowconfigure(0, weight=1)
        validation_frame.columnconfigure(0, weight=1)
        self.validation_text = tk.Text(validation_frame, wrap="word", width=58, height=18)
        self.validation_text.grid(row=0, column=0, sticky="nsew")
        self.validation_text.tag_configure("error", foreground="#b71c1c")
        self.validation_text.tag_configure("warning", foreground="#a35d00")
        self.validation_text.tag_configure("info", foreground="#1b1b1b")

        timeline_frame = ttk.LabelFrame(feedback_frame, text="Estimated node timelines", padding=12)
        timeline_frame.grid(row=0, column=1, sticky="nsew", padx=(6, 0))
        timeline_frame.columnconfigure(0, weight=1)
        timeline_frame.columnconfigure(1, weight=1)
        for column, node in enumerate((NODE_A, NODE_B)):
            node_frame = ttk.LabelFrame(timeline_frame, text=node, padding=8)
            node_frame.grid(row=0, column=column, sticky="nsew", padx=(0 if column == 0 else 8, 0))
            canvas = tk.Canvas(node_frame, width=520, height=320, background="white", highlightthickness=1, highlightbackground="#cccccc")
            canvas.pack(fill="both", expand=True)
            self.node_canvases[node] = canvas

        all_vars: list[tk.Variable] = [
            self.policy_name_var,
            self.memcached_node_var,
            self.memcached_cores_var,
            self.memcached_threads_var,
        ]
        for widgets in self.job_rows.values():
            all_vars.extend(
                (
                    widgets.order_var,
                    widgets.node_var,
                    widgets.cores_var,
                    widgets.threads_var,
                    widgets.after_var,
                    widgets.delay_var,
                )
            )
        for variable in all_vars:
            variable.trace_add("write", lambda *_args: self._schedule_refresh())
        self.memcached_node_var.trace_add("write", lambda *_args: self._update_memcached_core_values())

    def _update_memcached_core_values(self) -> None:
        node = self.memcached_node_var.get() or NODE_B
        values = NODE_A_CORE_SETS if node == NODE_A else NODE_B_CORE_SETS
        self.memcached_cores_box.configure(values=values)
        if self.memcached_cores_var.get() not in values:
            self.memcached_cores_var.set(values[0])

    def _update_job_core_values(self, job_id: str) -> None:
        widgets = self.job_rows[job_id]
        node = widgets.node_var.get() or JOB_CATALOG[job_id].default_node
        values = JOB_CATALOG[job_id].allowed_cores_by_node[node]
        widgets.cores_box.configure(values=values)
        if widgets.cores_var.get() not in values:
            widgets.cores_var.set(values[0])

    def _safe_int(self, raw: str, fallback: int) -> int:
        try:
            return int(raw.strip())
        except (TypeError, ValueError, AttributeError):
            return fallback

    def _collect_state(self) -> tuple[PlannerState, tuple[str, ...]]:
        parse_errors: list[str] = []
        jobs: list[PlannerJobState] = []
        for job_id, widgets in self.job_rows.items():
            order = self._safe_int(widgets.order_var.get(), 0)
            if order <= 0:
                parse_errors.append(f"{job_id} order must be a positive integer")
                order = 1
            threads = self._safe_int(widgets.threads_var.get(), 0)
            if threads <= 0:
                parse_errors.append(f"{job_id} threads must be a positive integer")
            delay_s = self._safe_int(widgets.delay_var.get(), 0)
            jobs.append(
                PlannerJobState(
                    job_id=job_id,
                    order=order,
                    node=widgets.node_var.get().strip(),
                    cores=widgets.cores_var.get().strip(),
                    threads=threads,
                    after_text=widgets.after_var.get().strip() or "start",
                    delay_s=delay_s,
                )
            )
        memcached_threads = self._safe_int(self.memcached_threads_var.get(), 0)
        if memcached_threads <= 0:
            parse_errors.append("memcached threads must be a positive integer")
        state = PlannerState(
            policy_name=self.policy_name_var.get().strip() or "planner-policy",
            memcached_node=self.memcached_node_var.get().strip(),
            memcached_cores=self.memcached_cores_var.get().strip(),
            memcached_threads=memcached_threads,
            jobs=tuple(jobs),
        )
        return state, tuple(parse_errors)

    def _schedule_refresh(self) -> None:
        if self._refresh_after_id is not None:
            self.root.after_cancel(self._refresh_after_id)
        self._refresh_after_id = self.root.after(75, self.refresh_view)

    def reload_from_disk(self) -> None:
        model = load_schedule_model(str(self.policy_path))
        state = planner_state_from_model(model)
        self.policy_name_var.set(state.policy_name)
        self.memcached_node_var.set(state.memcached_node)
        self.memcached_cores_var.set(state.memcached_cores)
        self.memcached_threads_var.set(str(state.memcached_threads))
        self._update_memcached_core_values()
        for job in state.jobs:
            widgets = self.job_rows[job.job_id]
            widgets.order_var.set(str(job.order))
            widgets.node_var.set(job.node)
            self._update_job_core_values(job.job_id)
            widgets.cores_var.set(job.cores)
            widgets.threads_var.set(str(job.threads))
            widgets.after_var.set(job.after_text)
            widgets.delay_var.set(str(job.delay_s))
        self.refresh_view()

    def _report_to_text(self, report: AuditReport) -> None:
        assert self.validation_text is not None
        self.validation_text.configure(state="normal")
        self.validation_text.delete("1.0", "end")
        self.validation_text.insert("end", f"Status: {report.status}\n", "info")
        self.validation_text.insert("end", f"Estimated makespan: {report.makespan_s:.2f}s\n\n" if report.makespan_s is not None else "Estimated makespan: n/a\n\n", "info")
        if report.errors:
            self.validation_text.insert("end", "Errors\n", "error")
            for issue in report.errors:
                self.validation_text.insert("end", f"- {issue.message}\n", "error")
            self.validation_text.insert("end", "\n", "info")
        if report.warnings:
            self.validation_text.insert("end", "Warnings\n", "warning")
            for issue in report.warnings:
                self.validation_text.insert("end", f"- {issue.message}\n", "warning")
            self.validation_text.insert("end", "\n", "info")
        self.validation_text.insert("end", "Jobs\n", "info")
        for window in sorted(report.jobs.values(), key=lambda item: (item.start_s, item.end_s, item.label)):
            self.validation_text.insert(
                "end",
                (
                    f"- {window.label}: node={window.node} cores={window.cores} threads={window.threads} "
                    f"after={dependency_text(window.dependencies)} start={window.start_s:.2f}s "
                    f"end={window.end_s:.2f}s duration={window.duration_s:.2f}s\n"
                ),
                "info",
            )
        self.validation_text.configure(state="disabled")

    def _draw_node_timeline(self, node: str, report: AuditReport) -> None:
        canvas = self.node_canvases[node]
        canvas.delete("all")
        width = int(canvas.cget("width"))
        height = int(canvas.cget("height"))
        left = 50
        right = width - 20
        top = 24
        bottom = height - 30
        core_count = 8 if node == NODE_A else 4
        usable_width = max(right - left, 1)
        usable_height = max(bottom - top, 1)
        row_height = usable_height / core_count
        scale_max = max(report.makespan_s or 0.0, 1.0)
        error_jobs = {job_id for issue in report.errors for job_id in issue.jobs}

        for core in range(core_count + 1):
            y = top + (core * row_height)
            canvas.create_line(left, y, right, y, fill="#dddddd")
            if core < core_count:
                canvas.create_text(22, y + (row_height / 2), text=str(core), fill="#555555")
        canvas.create_line(left, top, left, bottom, fill="#999999")
        canvas.create_line(left, bottom, right, bottom, fill="#999999")
        canvas.create_text(left, bottom + 14, text="0s", anchor="w", fill="#555555")
        canvas.create_text(right, bottom + 14, text=f"{scale_max:.1f}s", anchor="e", fill="#555555")

        for window in report.windows_by_node.get(node, []):
            x1 = left + ((window.start_s / scale_max) * usable_width)
            x2 = left + ((window.end_s / scale_max) * usable_width)
            if x2 - x1 < 2:
                x2 = x1 + 2
            fill = self.color_by_job.get(window.job_id, "#64b5f6")
            outline = "#c62828" if window.job_id in error_jobs else "#333333"
            text_color = "#ffffff" if window.job_id != "memcached" else "#111111"
            segments: list[tuple[int, int]] = []
            start_core = window.core_ids[0]
            end_core = window.core_ids[0]
            for core_id in window.core_ids[1:]:
                if core_id == end_core + 1:
                    end_core = core_id
                    continue
                segments.append((start_core, end_core))
                start_core = core_id
                end_core = core_id
            segments.append((start_core, end_core))
            for segment_start, segment_end in segments:
                y1 = top + (segment_start * row_height)
                y2 = top + ((segment_end + 1) * row_height)
                canvas.create_rectangle(x1, y1, x2, y2, fill=fill, outline=outline, width=2 if window.job_id in error_jobs else 1)
                if (x2 - x1) >= 50:
                    canvas.create_text((x1 + x2) / 2, (y1 + y2) / 2, text=window.label, fill=text_color)

    def refresh_view(self) -> None:
        self._refresh_after_id = None
        state, parse_errors = self._collect_state()
        model = build_model_from_planner_state(state, config_path=self.policy_path, parse_errors=parse_errors)
        report = audit_schedule(model, self.runtime_table)
        self.status_var.set(f"Status: {report.status}")
        self.makespan_var.set(
            f"Estimated makespan: {report.makespan_s:.2f}s" if report.makespan_s is not None else "Estimated makespan: n/a"
        )
        for job_id, widgets in self.job_rows.items():
            runtime = estimate_runtime(job_id, self._safe_int(widgets.threads_var.get(), 0), self.runtime_table)
            widgets.duration_var.set("n/a" if runtime is None else f"{runtime:.2f}s")
        self._report_to_text(report)
        for node in (NODE_A, NODE_B):
            self._draw_node_timeline(node, report)

    def save_to_disk(self) -> None:
        state, parse_errors = self._collect_state()
        model = build_model_from_planner_state(state, config_path=self.policy_path, parse_errors=parse_errors)
        report = audit_schedule(model, self.runtime_table)
        if report.errors:
            messagebox.showerror("Cannot save schedule", "Fix the validation errors before saving the policy.")
            self.refresh_view()
            return
        write_policy_document(model, self.policy_path)
        self.status_var.set(f"Saved {self.policy_path.name}")
        self.refresh_view()


def launch_planner_gui(*, policy_path_str: str, times_csv_path_str: str) -> None:
    if TKINTER_IMPORT_ERROR is not None:
        raise RuntimeError("Tkinter is not available or could not be imported in this Python environment.") from TKINTER_IMPORT_ERROR
    policy_path = Path(policy_path_str).resolve()
    times_csv_path = Path(times_csv_path_str).resolve()
    try:
        root = tk.Tk()
    except tk.TclError as exc:
        raise RuntimeError("Tkinter GUI could not start. Make sure a graphical display is available.") from exc
    PlannerApp(root, policy_path=policy_path, times_csv_path=times_csv_path)
    root.mainloop()

```

`manifests.py`:

```py
from __future__ import annotations

from dataclasses import dataclass

from .catalog import JOB_CATALOG, JobCatalogEntry
from .config import JobOverride, PolicyConfig


@dataclass(frozen=True)
class ResolvedBatchJob:
    job_id: str
    kubernetes_name: str
    image: str
    suite: str
    program: str
    node: str
    cores: str
    threads: int
    cpu_request: str | None
    memory_request: str | None
    memory_limit: str | None


@dataclass(frozen=True)
class ResolvedMemcached:
    kubernetes_name: str
    node: str
    cores: str
    threads: int


def _manifest_name(prefix: str, slug: str, run_id: str) -> str:
    base = f"{prefix}-{slug}-{run_id.lower()}"
    return base[:63].rstrip("-")


def _resolve_job(catalog_entry: JobCatalogEntry, override: JobOverride | None, run_id: str) -> ResolvedBatchJob:
    node = override.node if override and override.node else catalog_entry.default_node
    cores = override.cores if override and override.cores else catalog_entry.default_cores
    threads = override.threads if override and override.threads else catalog_entry.default_threads
    cpu_request = override.cpu_request if override and override.cpu_request else catalog_entry.default_cpu_request
    memory_request = (
        override.memory_request if override and override.memory_request else catalog_entry.default_memory_request
    )
    memory_limit = override.memory_limit if override and override.memory_limit else catalog_entry.default_memory_limit
    return ResolvedBatchJob(
        job_id=catalog_entry.job_id,
        kubernetes_name=_manifest_name("parsec", catalog_entry.job_id, run_id),
        image=catalog_entry.image,
        suite=catalog_entry.suite,
        program=catalog_entry.program,
        node=node,
        cores=cores,
        threads=threads,
        cpu_request=cpu_request,
        memory_request=memory_request,
        memory_limit=memory_limit,
    )


def resolve_jobs(policy: PolicyConfig, run_id: str) -> dict[str, ResolvedBatchJob]:
    launched = {job_id for phase in policy.phases for job_id in phase.launch}
    resolved: dict[str, ResolvedBatchJob] = {}
    for job_id in sorted(launched):
        resolved[job_id] = _resolve_job(JOB_CATALOG[job_id], policy.job_overrides.get(job_id), run_id)
    return resolved


def resolve_memcached(policy: PolicyConfig, run_id: str) -> ResolvedMemcached:
    memcached = policy.memcached
    return ResolvedMemcached(
        kubernetes_name=_manifest_name("memcached", "server", run_id),
        node=memcached.node,
        cores=memcached.cores,
        threads=memcached.threads,
    )


def _resource_block(job: ResolvedBatchJob) -> str:
    requests: list[str] = []
    limits: list[str] = []
    if job.cpu_request:
        requests.append(f'        cpu: "{job.cpu_request}"')
    if job.memory_request:
        requests.append(f'        memory: "{job.memory_request}"')
    if job.memory_limit:
        limits.append(f'        memory: "{job.memory_limit}"')
    if not requests and not limits:
        return ""
    lines = ["      resources:"]
    if requests:
        lines.append("        requests:")
        lines.extend(requests)
    if limits:
        lines.append("        limits:")
        lines.extend(limits)
    return "\n".join(lines) + "\n"


def render_memcached_manifest(
    memcached: ResolvedMemcached,
    *,
    experiment_id: str,
    run_id: str,
) -> str:
    return f"""apiVersion: v1
kind: Pod
metadata:
  name: {memcached.kubernetes_name}
  labels:
    cca-project-managed: "true"
    cca-project-experiment: "{experiment_id}"
    cca-project-run-id: "{run_id}"
    cca-project-role: "memcached"
spec:
  containers:
  - image: anakli/memcached:t1
    name: memcached
    imagePullPolicy: Always
    command: ["/bin/sh"]
    args: ["-c", "taskset -c {memcached.cores} ./memcached -t {memcached.threads} -u memcache"]
  nodeSelector:
    cca-project-nodetype: "{memcached.node}"
"""


def render_batch_job_manifest(
    job: ResolvedBatchJob,
    *,
    experiment_id: str,
    run_id: str,
) -> str:
    resources = _resource_block(job)
    return f"""apiVersion: batch/v1
kind: Job
metadata:
  name: {job.kubernetes_name}
  labels:
    cca-project-managed: "true"
    cca-project-experiment: "{experiment_id}"
    cca-project-run-id: "{run_id}"
    cca-project-job-id: "{job.job_id}"
spec:
  backoffLimit: 0
  template:
    metadata:
      labels:
        cca-project-managed: "true"
        cca-project-experiment: "{experiment_id}"
        cca-project-run-id: "{run_id}"
        cca-project-job-id: "{job.job_id}"
    spec:
      containers:
      - image: {job.image}
        name: parsec-{job.job_id}
        imagePullPolicy: Always
        command: ["/bin/sh"]
        args: ["-c", "taskset -c {job.cores} ./run -a run -S {job.suite} -p {job.program} -i native -n {job.threads}"]
{resources}      restartPolicy: Never
      nodeSelector:
        cca-project-nodetype: "{job.node}"
"""


```

`metrics.py`:

```py
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

```

`part3.yaml`:

```yaml
apiVersion: kops.k8s.io/v1alpha2
kind: Cluster
metadata:
  creationTimestamp: null
  name: part3.k8s.local
spec:
  api:
    loadBalancer:
      type: Public
  authorization:
    rbac: {}
  channel: stable
  cloudConfig:
    gceServiceAccount: default
  cloudProvider: gce
  configBase: gs://cca-eth-2026-group-54-mpelossi/part3.k8s.local
  containerRuntime: containerd
  etcdClusters:
  - cpuRequest: 200m
    etcdMembers:
    - instanceGroup: master-europe-west1-b
      name: a
    memoryRequest: 100Mi
    name: main
  - cpuRequest: 100m
    etcdMembers:
    - instanceGroup: master-europe-west1-b
      name: a
    memoryRequest: 100Mi
    name: events
  iam:
    allowContainerRegistry: true
    legacy: false
  kubelet:
    anonymousAuth: false
  kubernetesApiAccess:
  - 0.0.0.0/0
  kubernetesVersion: 1.31.5
  masterPublicName: api.part3.k8s.local
  networking:
    kubenet: {}
  nonMasqueradeCIDR: 100.64.0.0/10
  project: cca-eth-2026-group-54
  sshAccess:
  - 0.0.0.0/0
  subnets:
  - name: europe-west1
    region: europe-west1
    type: Public
  topology:
    dns:
      type: None
    masters: public
    nodes: public
  cloudControllerManager:
    image: gcr.io/k8s-staging-cloud-provider-gcp/cloud-controller-manager:master@sha256:e125f4e6792978125546e64279a13de18fdf6b704edfec8400cac1254d3adf88

---

apiVersion: kops.k8s.io/v1alpha2
kind: InstanceGroup
metadata:
  creationTimestamp: null
  labels:
    kops.k8s.io/cluster: part3.k8s.local
  name: master-europe-west1-b
spec:
  image: ubuntu-os-cloud/ubuntu-2404-noble-amd64-v20250130
  machineType: e2-standard-2
  maxSize: 1
  minSize: 1
  nodeLabels:
    cloud.google.com/metadata-proxy-ready: "true"
    kops.k8s.io/instancegroup: master-europe-west1-b
  role: Master
  subnets:
  - europe-west1
  zones:
  - europe-west1-b

---

apiVersion: kops.k8s.io/v1alpha2
kind: InstanceGroup
metadata:
  creationTimestamp: null
  labels:
    kops.k8s.io/cluster: part3.k8s.local
  name: client-measure
spec:
  additionalUserData:
  - name: 00-client-measure-bootstrap.sh
    type: text/x-shellscript
    content: |
      #!/bin/bash
      mkdir -p /opt/cca
      exec > >(tee -a /var/log/cca-bootstrap.log | logger -t cca-bootstrap -s) 2>&1
      set -euxo pipefail

      if [[ -f /opt/cca/bootstrap.done ]]; then
        echo "Bootstrap already completed"
        exit 0
      fi

      export DEBIAN_FRONTEND=noninteractive
      prepare_memcached_build_dependencies() {
        local sources_file=/etc/apt/sources.list.d/ubuntu.sources
        if [[ ! -f "${sources_file}" ]]; then
          echo "ERROR: ${sources_file} is missing; cannot enable deb-src for memcached build dependencies."
          return 1
        fi

        awk '
          $1 == "Types:" {
            has_deb_src = 0
            for (i = 2; i <= NF; ++i) {
              if ($i == "deb-src") {
                has_deb_src = 1
              }
            }
            if (!has_deb_src) {
              print $0 " deb-src"
              next
            }
          }
          { print }
        ' "${sources_file}" > "${sources_file}.tmp"
        mv "${sources_file}.tmp" "${sources_file}"

        apt-get update
        if ! apt-cache showsrc memcached >/dev/null 2>&1; then
          echo "ERROR: memcached source metadata is unavailable after enabling deb-src in ${sources_file}."
          echo "ERROR: Inspect /var/log/cca-bootstrap.log and the cloud-init logs before retrying."
          return 1
        fi

        apt-get install libevent-dev libzmq3-dev git make g++ --yes
        apt-get build-dep memcached --yes
      }

      prepare_memcached_build_dependencies

      if [[ ! -d /opt/cca/memcache-perf-dynamic/.git ]]; then
        rm -rf /opt/cca/memcache-perf-dynamic
        git clone https://github.com/eth-easl/memcache-perf-dynamic.git /opt/cca/memcache-perf-dynamic
      fi
      make -C /opt/cca/memcache-perf-dynamic
      touch /opt/cca/bootstrap.done
  image: ubuntu-os-cloud/ubuntu-2404-noble-amd64-v20250130
  machineType: e2-standard-2
  maxSize: 1
  minSize: 1
  nodeLabels:
    cloud.google.com/metadata-proxy-ready: "true"
    kops.k8s.io/instancegroup: nodes-europe-west1-b
    cca-project-nodetype: "client-measure"
  role: Node
  subnets:
  - europe-west1
  zones:
  - europe-west1-b
---

apiVersion: kops.k8s.io/v1alpha2
kind: InstanceGroup
metadata:
  creationTimestamp: null
  labels:
    kops.k8s.io/cluster: part3.k8s.local
  name: client-agent-a
spec:
  additionalUserData:
  - name: 00-client-agent-a-bootstrap.sh
    type: text/x-shellscript
    content: |
      #!/bin/bash
      mkdir -p /opt/cca
      exec > >(tee -a /var/log/cca-bootstrap.log | logger -t cca-bootstrap -s) 2>&1
      set -euxo pipefail

      if [[ -f /opt/cca/bootstrap.done ]]; then
        echo "Bootstrap already completed"
        exit 0
      fi

      export DEBIAN_FRONTEND=noninteractive
      prepare_memcached_build_dependencies() {
        local sources_file=/etc/apt/sources.list.d/ubuntu.sources
        if [[ ! -f "${sources_file}" ]]; then
          echo "ERROR: ${sources_file} is missing; cannot enable deb-src for memcached build dependencies."
          return 1
        fi

        awk '
          $1 == "Types:" {
            has_deb_src = 0
            for (i = 2; i <= NF; ++i) {
              if ($i == "deb-src") {
                has_deb_src = 1
              }
            }
            if (!has_deb_src) {
              print $0 " deb-src"
              next
            }
          }
          { print }
        ' "${sources_file}" > "${sources_file}.tmp"
        mv "${sources_file}.tmp" "${sources_file}"

        apt-get update
        if ! apt-cache showsrc memcached >/dev/null 2>&1; then
          echo "ERROR: memcached source metadata is unavailable after enabling deb-src in ${sources_file}."
          echo "ERROR: Inspect /var/log/cca-bootstrap.log and the cloud-init logs before retrying."
          return 1
        fi

        apt-get install libevent-dev libzmq3-dev git make g++ --yes
        apt-get build-dep memcached --yes
      }

      prepare_memcached_build_dependencies

      if [[ ! -d /opt/cca/memcache-perf-dynamic/.git ]]; then
        rm -rf /opt/cca/memcache-perf-dynamic
        git clone https://github.com/eth-easl/memcache-perf-dynamic.git /opt/cca/memcache-perf-dynamic
      fi
      make -C /opt/cca/memcache-perf-dynamic

      cat >/etc/systemd/system/mcperf-agent.service <<'EOF'
      [Unit]
      Description=CCA mcperf load agent
      After=network-online.target
      Wants=network-online.target

      [Service]
      Type=simple
      WorkingDirectory=/opt/cca/memcache-perf-dynamic
      ExecStart=/opt/cca/memcache-perf-dynamic/mcperf -T 2 -A
      Restart=always
      RestartSec=2
      StandardOutput=append:/var/log/mcperf-agent.log
      StandardError=append:/var/log/mcperf-agent.log

      [Install]
      WantedBy=multi-user.target
      EOF

      systemctl daemon-reload
      systemctl enable --now mcperf-agent.service
      touch /opt/cca/bootstrap.done
  image: ubuntu-os-cloud/ubuntu-2404-noble-amd64-v20250130
  machineType: e2-standard-2
  maxSize: 1
  minSize: 1
  nodeLabels:
    cloud.google.com/metadata-proxy-ready: "true"
    kops.k8s.io/instancegroup: nodes-europe-west1-b
    cca-project-nodetype: "client-agent-a"
  role: Node
  subnets:
  - europe-west1
  zones:
  - europe-west1-b

---

apiVersion: kops.k8s.io/v1alpha2
kind: InstanceGroup
metadata:
  creationTimestamp: null
  labels:
    kops.k8s.io/cluster: part3.k8s.local
  name: client-agent-b
spec:
  additionalUserData:
  - name: 00-client-agent-b-bootstrap.sh
    type: text/x-shellscript
    content: |
      #!/bin/bash
      mkdir -p /opt/cca
      exec > >(tee -a /var/log/cca-bootstrap.log | logger -t cca-bootstrap -s) 2>&1
      set -euxo pipefail

      if [[ -f /opt/cca/bootstrap.done ]]; then
        echo "Bootstrap already completed"
        exit 0
      fi

      export DEBIAN_FRONTEND=noninteractive
      prepare_memcached_build_dependencies() {
        local sources_file=/etc/apt/sources.list.d/ubuntu.sources
        if [[ ! -f "${sources_file}" ]]; then
          echo "ERROR: ${sources_file} is missing; cannot enable deb-src for memcached build dependencies."
          return 1
        fi

        awk '
          $1 == "Types:" {
            has_deb_src = 0
            for (i = 2; i <= NF; ++i) {
              if ($i == "deb-src") {
                has_deb_src = 1
              }
            }
            if (!has_deb_src) {
              print $0 " deb-src"
              next
            }
          }
          { print }
        ' "${sources_file}" > "${sources_file}.tmp"
        mv "${sources_file}.tmp" "${sources_file}"

        apt-get update
        if ! apt-cache showsrc memcached >/dev/null 2>&1; then
          echo "ERROR: memcached source metadata is unavailable after enabling deb-src in ${sources_file}."
          echo "ERROR: Inspect /var/log/cca-bootstrap.log and the cloud-init logs before retrying."
          return 1
        fi

        apt-get install libevent-dev libzmq3-dev git make g++ --yes
        apt-get build-dep memcached --yes
      }

      prepare_memcached_build_dependencies

      if [[ ! -d /opt/cca/memcache-perf-dynamic/.git ]]; then
        rm -rf /opt/cca/memcache-perf-dynamic
        git clone https://github.com/eth-easl/memcache-perf-dynamic.git /opt/cca/memcache-perf-dynamic
      fi
      make -C /opt/cca/memcache-perf-dynamic

      cat >/etc/systemd/system/mcperf-agent.service <<'EOF'
      [Unit]
      Description=CCA mcperf load agent
      After=network-online.target
      Wants=network-online.target

      [Service]
      Type=simple
      WorkingDirectory=/opt/cca/memcache-perf-dynamic
      ExecStart=/opt/cca/memcache-perf-dynamic/mcperf -T 4 -A
      Restart=always
      RestartSec=2
      StandardOutput=append:/var/log/mcperf-agent.log
      StandardError=append:/var/log/mcperf-agent.log

      [Install]
      WantedBy=multi-user.target
      EOF

      systemctl daemon-reload
      systemctl enable --now mcperf-agent.service
      touch /opt/cca/bootstrap.done
  image: ubuntu-os-cloud/ubuntu-2404-noble-amd64-v20250130
  machineType: e2-standard-4
  maxSize: 1
  minSize: 1
  nodeLabels:
    cloud.google.com/metadata-proxy-ready: "true"
    kops.k8s.io/instancegroup: nodes-europe-west1-b
    cca-project-nodetype: "client-agent-b"
  role: Node
  subnets:
  - europe-west1
  zones:
  - europe-west1-b

---
apiVersion: kops.k8s.io/v1alpha2
kind: InstanceGroup
metadata:
  creationTimestamp: null
  labels:
    kops.k8s.io/cluster: part3.k8s.local
  name: node-a-8core
spec:
  image: ubuntu-os-cloud/ubuntu-2404-noble-amd64-v20250130
  machineType: e2-standard-8
  maxSize: 1
  minSize: 1
  nodeLabels:
    cloud.google.com/metadata-proxy-ready: "true"
    kops.k8s.io/instancegroup: nodes-europe-west1-b
    cca-project-nodetype: "node-a-8core"
  role: Node
  subnets:
  - europe-west1
  zones:
  - europe-west1-b

---
apiVersion: kops.k8s.io/v1alpha2
kind: InstanceGroup
metadata:
  creationTimestamp: null
  labels:
    kops.k8s.io/cluster: part3.k8s.local
  name: node-b-4core
spec:
  image: ubuntu-os-cloud/ubuntu-2404-noble-amd64-v20250130
  machineType: n2d-highcpu-4
  maxSize: 1
  minSize: 1
  nodeLabels:
    cloud.google.com/metadata-proxy-ready: "true"
    kops.k8s.io/instancegroup: nodes-europe-west1-b
    cca-project-nodetype: "node-b-4core"
  role: Node
  subnets:
  - europe-west1
  zones:
  - europe-west1-b

```

`policies/baseline.yaml`:

```yaml
{
  "policy_name": "baseline-two-queues",
  "memcached": {
    "node": "node-b-4core",
    "cores": "0",
    "threads": 1
  },
  "job_overrides": {},
  "phases": [
    {
      "id": "start-streamcluster-and-blackscholes",
      "after": "start",
      "delay_s": 0,
      "launch": ["streamcluster", "blackscholes"]
    },
    {
      "id": "launch-freqmine",
      "after": "jobs_complete",
      "jobs_complete": ["blackscholes"],
      "delay_s": 0,
      "launch": ["freqmine"]
    },
    {
      "id": "launch-canneal",
      "after": "jobs_complete",
      "jobs_complete": ["streamcluster"],
      "delay_s": 0,
      "launch": ["canneal"]
    },
    {
      "id": "launch-barnes",
      "after": "jobs_complete",
      "jobs_complete": ["canneal"],
      "delay_s": 0,
      "launch": ["barnes"]
    },
    {
      "id": "launch-vips",
      "after": "jobs_complete",
      "jobs_complete": ["barnes"],
      "delay_s": 0,
      "launch": ["vips"]
    },
    {
      "id": "launch-radix",
      "after": "jobs_complete",
      "jobs_complete": ["vips"],
      "delay_s": 0,
      "launch": ["radix"]
    }
  ]
}

```

`provision.py`:

```py
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from .cluster import ClusterController


BOOTSTRAP_SENTINEL = "/opt/cca/bootstrap.done"
REMOTE_MCperf = "/opt/cca/memcache-perf-dynamic/mcperf"
REQUIRED_CLIENT_NODETYPES = ("client-agent-a", "client-agent-b", "client-measure")


@dataclass(frozen=True)
class ProvisionStatus:
    nodetype: str
    node_name: str
    bootstrap_ready: bool
    mcperf_present: bool
    agent_service_state: str | None

    @property
    def is_ready(self) -> bool:
        if not self.bootstrap_ready or not self.mcperf_present:
            return False
        if self.nodetype.startswith("client-agent"):
            return self.agent_service_state == "active"
        return True

    def pending_reasons(self) -> list[str]:
        reasons: list[str] = []
        if not self.bootstrap_ready:
            reasons.append("bootstrap not finished")
        if not self.mcperf_present:
            reasons.append("mcperf missing")
        if self.nodetype.startswith("client-agent") and self.agent_service_state != "active":
            if self.agent_service_state == "not-installed":
                reasons.append("mcperf-agent.service not installed")
            elif self.agent_service_state:
                reasons.append(f"mcperf-agent.service is {self.agent_service_state}")
            else:
                reasons.append("mcperf-agent.service state unknown")
        return reasons

    def __str__(self) -> str:
        state = "READY" if self.is_ready else "WAITING"
        reasons = self.pending_reasons()
        if reasons:
            detail = "; ".join(reasons)
        elif self.nodetype.startswith("client-agent"):
            detail = "bootstrap ready; mcperf present; mcperf-agent.service active"
        else:
            detail = "bootstrap ready; mcperf present"
        return f"{self.nodetype} ({self.node_name}): {state} - {detail}"


class ProvisioningError(RuntimeError):
    def __init__(self, message: str, *, statuses: dict[str, ProvisionStatus]):
        super().__init__(message)
        self.statuses = statuses


def render_provision_check_note(ssh_key_path: Path) -> str:
    prompt_count = len(REQUIRED_CLIENT_NODETYPES)
    return (
        f"Provision check will SSH into {prompt_count} client VMs. "
        f"If {ssh_key_path} is passphrase-protected and not loaded in ssh-agent, "
        f"expect up to {prompt_count} passphrase prompts, roughly one per VM. "
        f"Run `ssh-add {ssh_key_path}` first if you want to avoid repeated prompts."
    )


def render_provision_expectations() -> str:
    return (
        "Expected READY state: client-agent-a/client-agent-b need bootstrap ready, "
        "mcperf present, and mcperf-agent.service active; client-measure only needs "
        "bootstrap ready and mcperf present."
    )


def check_client_provisioning(cluster: ClusterController) -> dict[str, ProvisionStatus]:
    nodes = cluster.ensure_canonical_node_labels()
    statuses: dict[str, ProvisionStatus] = {}
    for nodetype in REQUIRED_CLIENT_NODETYPES:
        if nodetype not in nodes:
            discovered = ", ".join(sorted(nodes)) or "none"
            raise RuntimeError(
                f"Expected node not found after canonical labeling: {nodetype}. "
                f"Discovered canonical nodes: {discovered}"
            )
        node = nodes[nodetype]
        command = (
            "bash -lc '"
            f"if [ -f {BOOTSTRAP_SENTINEL} ]; then echo bootstrap=ready; else echo bootstrap=missing; fi; "
            f"if [ -x {REMOTE_MCperf} ]; then echo mcperf=present; else echo mcperf=missing; fi; "
            "if systemctl list-unit-files mcperf-agent.service >/dev/null 2>&1; "
            "then systemctl is-active mcperf-agent.service || true; "
            "else echo not-installed; fi'"
        )
        result = cluster.ssh(node.name, command)
        lines = [line.strip() for line in result.stdout.splitlines() if line.strip()]
        bootstrap_ready = "bootstrap=ready" in lines
        mcperf_present = "mcperf=present" in lines
        service_state = None
        for line in reversed(lines):
            if line in {"active", "inactive", "failed", "not-installed"}:
                service_state = line
                break
        statuses[nodetype] = ProvisionStatus(
            nodetype=nodetype,
            node_name=node.name,
            bootstrap_ready=bootstrap_ready,
            mcperf_present=mcperf_present,
            agent_service_state=service_state,
        )
    return statuses


def assert_client_provisioning(cluster: ClusterController) -> dict[str, ProvisionStatus]:
    statuses = check_client_provisioning(cluster)
    for nodetype, status in statuses.items():
        if not status.bootstrap_ready or not status.mcperf_present:
            raise ProvisioningError(f"{nodetype} is not fully bootstrapped: {status}", statuses=statuses)
        if nodetype.startswith("client-agent") and status.agent_service_state != "active":
            raise ProvisioningError(
                f"{nodetype} agent service is not active: {status.agent_service_state}",
                statuses=statuses,
            )
    return statuses

```

`results.py`:

```py
from __future__ import annotations

import json
from pathlib import Path


def load_run_summaries(results_root: Path, experiment_id: str) -> list[dict[str, object]]:
    experiment_root = results_root / experiment_id
    if not experiment_root.exists():
        raise FileNotFoundError(f"Experiment directory not found: {experiment_root}")
    summaries: list[dict[str, object]] = []
    for run_dir in sorted(path for path in experiment_root.iterdir() if path.is_dir()):
        summary_path = run_dir / "summary.json"
        if not summary_path.exists():
            continue
        summary = json.loads(summary_path.read_text(encoding="utf-8"))
        summary["run_dir"] = str(run_dir)
        summaries.append(summary)
    return summaries


def sort_best_runs(summaries: list[dict[str, object]]) -> list[dict[str, object]]:
    def sort_key(summary: dict[str, object]) -> tuple[int, float, float]:
        is_pass = 0 if summary.get("overall_status") == "pass" else 1
        makespan = float(summary.get("makespan_s") or 1e18)
        p95 = float(summary.get("max_observed_p95_us") or 1e18)
        return (is_pass, makespan, p95)

    return sorted(summaries, key=sort_key)

```

`runner.py`:

```py
from __future__ import annotations

import shutil
import subprocess
import threading
import time
import json
from dataclasses import dataclass
from pathlib import Path

from .catalog import JOB_CATALOG
from .cluster import ClusterController
from .collect import collect_describes, collect_live_pods, summarize_run
from .config import ExperimentConfig, Phase, PolicyConfig
from .debug import format_debug_command_hint, summarize_provisioning_hints
from .manifests import (
    ResolvedBatchJob,
    render_batch_job_manifest,
    render_memcached_manifest,
    resolve_jobs,
    resolve_memcached,
)
from .provision import ProvisioningError, assert_client_provisioning
from .utils import append_log, ensure_directory, utc_timestamp


@dataclass
class MeasurementHandle:
    process: subprocess.Popen[str]
    reader_thread: threading.Thread
    ready_event: threading.Event


class ExperimentRunner:
    poll_interval_s = 1.0
    scheduler_status_interval_s = 15.0

    def __init__(self, experiment: ExperimentConfig, policy: PolicyConfig):
        self.experiment = experiment
        self.policy = policy
        self.cluster = ClusterController(experiment)

    def _create_run_dir(self) -> tuple[str, Path, Path]:
        run_id = utc_timestamp().lower()
        run_dir = ensure_directory(self.experiment.results_root / self.experiment.experiment_id / run_id)
        manifests_dir = ensure_directory(run_dir / "rendered_manifests")
        return run_id, run_dir, manifests_dir

    def _log(self, log_path: Path, message: str) -> None:
        append_log(log_path, message)
        print(message)

    def _log_run_prefix(self, run_id: str, message: str) -> None:
        print(f"[run {run_id}] {message}")

    def _write_policy_snapshot(self, run_dir: Path) -> None:
        shutil.copyfile(self.experiment.config_path, run_dir / "experiment.yaml")
        shutil.copyfile(self.policy.config_path, run_dir / "policy.yaml")

    def _render_manifests(
        self,
        *,
        run_id: str,
        manifests_dir: Path,
    ) -> tuple[Path, dict[str, ResolvedBatchJob]]:
        resolved_jobs = resolve_jobs(self.policy, run_id)
        memcached = resolve_memcached(self.policy, run_id)
        memcached_path = manifests_dir / "memcached.yaml"
        memcached_path.write_text(
            render_memcached_manifest(
                memcached,
                experiment_id=self.experiment.experiment_id,
                run_id=run_id,
            ),
            encoding="utf-8",
        )
        for job in resolved_jobs.values():
            manifest_path = manifests_dir / f"{job.job_id}.yaml"
            manifest_path.write_text(
                render_batch_job_manifest(
                    job,
                    experiment_id=self.experiment.experiment_id,
                    run_id=run_id,
                ),
                encoding="utf-8",
            )
        return memcached_path, resolved_jobs

    def _phase_plan(self, resolved_jobs: dict[str, ResolvedBatchJob]) -> list[dict[str, object]]:
        return [
            {
                "id": phase.phase_id,
                "after": phase.after,
                "jobs_complete": list(phase.jobs_complete),
                "delay_s": phase.delay_s,
                "launch": [resolved_jobs[job_id].kubernetes_name for job_id in phase.launch],
            }
            for phase in self.policy.phases
        ]

    def _start_measurement(
        self,
        *,
        run_dir: Path,
        run_id: str,
        memcached_ip: str,
        agent_a_ip: str,
        agent_b_ip: str,
        log_path: Path,
    ) -> MeasurementHandle:
        mcperf_path = run_dir / "mcperf.txt"
        measurement = self.experiment.measurement
        nodes = self.cluster.discover_nodes()
        measure_node = nodes["client-measure"]
        command = (
            "bash -lc 'set -euo pipefail; "
            f"cd {self.experiment.remote_repo_dir}; "
            f"./mcperf -s {memcached_ip} --loadonly; "
            f"./mcperf -s {memcached_ip} "
            f"-a {agent_a_ip} -a {agent_b_ip} "
            "--noload "
            f"-T {measurement.measure_threads} "
            f"-C {measurement.connections} "
            f"-D {measurement.depth} "
            f"-Q {measurement.qps_interval} "
            f"-c {measurement.connections} "
            "-t 10 "
            f"--scan {measurement.scan_start}:{measurement.scan_stop}:{measurement.scan_step}'"
        )
        process = self.cluster.popen_ssh(
            measure_node.name,
            command,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
        )
        ready_event = threading.Event()

        def _reader() -> None:
            assert process.stdout is not None
            with mcperf_path.open("w", encoding="utf-8") as handle:
                for line in process.stdout:
                    handle.write(line)
                    handle.flush()
                    if line.startswith("#type"):
                        ready_event.set()
                        self._log(log_path, "mcperf measurement header observed")

        reader_thread = threading.Thread(target=_reader, daemon=True)
        reader_thread.start()
        return MeasurementHandle(process=process, reader_thread=reader_thread, ready_event=ready_event)

    def _wait_for_measurement_start(self, handle: MeasurementHandle) -> None:
        if not handle.ready_event.wait(timeout=self.experiment.measurement.max_start_wait_s):
            handle.process.terminate()
            raise TimeoutError("mcperf measurement did not become ready in time")

    def _wait_for_measurement_finish(self, handle: MeasurementHandle) -> None:
        return_code = handle.process.wait(timeout=self.experiment.measurement.completion_timeout_s)
        handle.reader_thread.join(timeout=30)
        if return_code != 0:
            raise RuntimeError(f"mcperf measurement exited with code {return_code}")

    def _current_time(self) -> float:
        return time.monotonic()

    def _sleep(self, seconds: float) -> None:
        time.sleep(seconds)

    def _phase_dependency_job_ids(
        self,
        phase: Phase,
        *,
        phase_jobs: dict[str, tuple[str, ...]],
    ) -> tuple[str, ...]:
        if phase.after == "start":
            return ()
        if phase.after == "jobs_complete":
            return phase.jobs_complete
        if phase.after.startswith("phase:"):
            referenced_phase = phase.after.split(":", 1)[1]
            return phase_jobs[referenced_phase]
        raise RuntimeError(f"Unsupported phase dependency at runtime: {phase.after}")

    def _update_launched_job_states(
        self,
        *,
        snapshot: dict[str, dict[str, object]],
        launched_jobs: dict[str, ResolvedBatchJob],
        completed_jobs: set[str],
        failed_jobs: set[str],
        log_path: Path,
    ) -> None:
        for job_id, job in launched_jobs.items():
            info = snapshot.get(job.kubernetes_name)
            if info is None:
                continue
            status = info["status"]
            if status == "completed" and job_id not in completed_jobs:
                completed_jobs.add(job_id)
                self._log(log_path, f"Job completed: {job.kubernetes_name}")
            elif status == "failed" and job_id not in failed_jobs:
                failed_jobs.add(job_id)
                self._log(log_path, f"Job failed: {job.kubernetes_name}")

    def _launch_phase(
        self,
        phase: Phase,
        *,
        resolved_jobs: dict[str, ResolvedBatchJob],
        manifests_dir: Path,
        launched_jobs: dict[str, ResolvedBatchJob],
        log_path: Path,
    ) -> None:
        self._log(log_path, f"Launching phase {phase.phase_id}: {', '.join(phase.launch)}")
        for job_id in phase.launch:
            job = resolved_jobs[job_id]
            manifest_path = manifests_dir / f"{job.job_id}.yaml"
            self.cluster.apply_manifest(manifest_path)
            launched_jobs[job_id] = job

    def _run_phase_scheduler(
        self,
        *,
        run_id: str,
        resolved_jobs: dict[str, ResolvedBatchJob],
        manifests_dir: Path,
        log_path: Path,
    ) -> dict[str, ResolvedBatchJob]:
        phases = list(self.policy.phases)
        phase_jobs = {phase.phase_id: phase.launch for phase in phases}
        launched_phase_ids: set[str] = set()
        launched_jobs: dict[str, ResolvedBatchJob] = {}
        completed_jobs: set[str] = set()
        failed_jobs: set[str] = set()
        dependency_ready_at: dict[str, float] = {}
        deadline = self._current_time() + self.experiment.measurement.completion_timeout_s
        next_status_log_at = self._current_time()

        while True:
            now = self._current_time()
            if now >= deadline:
                pending_phases = [phase.phase_id for phase in phases if phase.phase_id not in launched_phase_ids]
                raise TimeoutError(
                    "Timed out waiting for scheduler completion: "
                    f"pending_phases={pending_phases} completed_jobs={sorted(completed_jobs)}"
                )

            snapshot = self.cluster.get_run_jobs_snapshot(run_id)
            self._update_launched_job_states(
                snapshot=snapshot,
                launched_jobs=launched_jobs,
                completed_jobs=completed_jobs,
                failed_jobs=failed_jobs,
                log_path=log_path,
            )
            if failed_jobs:
                raise RuntimeError(f"One or more jobs failed: {sorted(failed_jobs)}")

            now = self._current_time()
            launched_this_cycle = False
            for phase in phases:
                if phase.phase_id in launched_phase_ids or phase.phase_id in dependency_ready_at:
                    continue
                dependency_job_ids = self._phase_dependency_job_ids(phase, phase_jobs=phase_jobs)
                if all(job_id in completed_jobs for job_id in dependency_job_ids):
                    dependency_ready_at[phase.phase_id] = now
                    dependency_text = "start" if not dependency_job_ids else ",".join(dependency_job_ids)
                    self._log(log_path, f"Phase dependency satisfied for {phase.phase_id}: {dependency_text}")
                    if phase.delay_s:
                        self._log(log_path, f"Delay window started for phase {phase.phase_id}: {phase.delay_s}s")

            for phase in phases:
                if phase.phase_id in launched_phase_ids:
                    continue
                ready_at = dependency_ready_at.get(phase.phase_id)
                if ready_at is None or now < ready_at + phase.delay_s:
                    continue
                self._launch_phase(
                    phase,
                    resolved_jobs=resolved_jobs,
                    manifests_dir=manifests_dir,
                    launched_jobs=launched_jobs,
                    log_path=log_path,
                )
                launched_phase_ids.add(phase.phase_id)
                launched_this_cycle = True

            if len(launched_phase_ids) == len(phases) and len(completed_jobs) == len(launched_jobs):
                return launched_jobs

            if not launched_this_cycle:
                if now >= next_status_log_at:
                    running_jobs = sorted(
                        job_name
                        for job_name, info in snapshot.items()
                        if info.get("status") == "running"
                    )
                    pending_phases = [
                        phase.phase_id for phase in phases if phase.phase_id not in launched_phase_ids
                    ]
                    self._log(
                        log_path,
                        "Scheduler heartbeat: "
                        f"launched_phases={sorted(launched_phase_ids)} "
                        f"pending_phases={pending_phases} "
                        f"completed_jobs={sorted(completed_jobs)} "
                        f"running_jobs={running_jobs}",
                    )
                    next_status_log_at = now + self.scheduler_status_interval_s
                self._sleep(min(self.poll_interval_s, max(deadline - self._current_time(), 0.0)))

    def run_once(self, *, dry_run: bool = False) -> Path:
        run_id, run_dir, manifests_dir = self._create_run_dir()
        log_path = run_dir / "events.log"
        self._log_run_prefix(run_id, f"Preparing run in {run_dir}")
        self._write_policy_snapshot(run_dir)
        memcached_manifest, resolved_jobs = self._render_manifests(run_id=run_id, manifests_dir=manifests_dir)
        plan_path = run_dir / "phase_plan.json"
        plan_path.write_text(json.dumps(self._phase_plan(resolved_jobs), indent=2) + "\n", encoding="utf-8")
        self._log(log_path, f"Run directory prepared: {run_dir}")
        self._log(log_path, f"Rendered {1 + len(resolved_jobs)} manifests into {manifests_dir}")

        if dry_run:
            self._log(log_path, f"Dry run prepared at {run_dir}")
            return run_dir

        self._log(log_path, "Cleaning previous managed workloads")
        self.cluster.cleanup_managed_workloads()
        self._log(log_path, "Ensuring canonical node labels and checking client provisioning")
        try:
            assert_client_provisioning(self.cluster)
        except ProvisioningError as exc:
            for status in exc.statuses.values():
                self._log(log_path, str(status))
            for hint in summarize_provisioning_hints(exc.statuses):
                self._log(log_path, f"Hint: {hint}")
            self._log(
                log_path,
                "Debug commands: "
                + format_debug_command_hint(
                    config_path=self.experiment.config_path,
                    policy_path=self.policy.config_path,
                    run_id=run_id,
                ),
            )
            raise
        except RuntimeError:
            self._log(
                log_path,
                "Debug commands: "
                + format_debug_command_hint(
                    config_path=self.experiment.config_path,
                    policy_path=self.policy.config_path,
                    run_id=run_id,
                ),
            )
            raise

        self._log(log_path, "Applying memcached manifest")
        self.cluster.apply_manifest(memcached_manifest)
        memcached_name = resolve_memcached(self.policy, run_id).kubernetes_name
        self.cluster.wait_for_pod_ready(memcached_name)
        memcached_pod = self.cluster.get_pod_by_run_role(run_id, "memcached")
        memcached_ip = memcached_pod.get("status", {}).get("podIP")
        if not isinstance(memcached_ip, str) or not memcached_ip:
            raise RuntimeError("memcached pod IP is missing")

        nodes = self.cluster.discover_nodes()
        agent_a_ip = nodes["client-agent-a"].internal_ip
        agent_b_ip = nodes["client-agent-b"].internal_ip
        if not agent_a_ip or not agent_b_ip:
            raise RuntimeError("Agent internal IPs are missing")

        self._log(log_path, f"Starting measurement against memcached IP {memcached_ip}")
        measurement = self._start_measurement(
            run_dir=run_dir,
            run_id=run_id,
            memcached_ip=memcached_ip,
            agent_a_ip=agent_a_ip,
            agent_b_ip=agent_b_ip,
            log_path=log_path,
        )
        self._log(log_path, "Waiting for mcperf measurement header")
        self._wait_for_measurement_start(measurement)
        self._log(log_path, "mcperf measurement is live")

        self._log(log_path, "Starting phase scheduler")
        launched_jobs = self._run_phase_scheduler(
            run_id=run_id,
            resolved_jobs=resolved_jobs,
            manifests_dir=manifests_dir,
            log_path=log_path,
        )
        self._log(log_path, "All phases launched; waiting for mcperf to finish")
        self._wait_for_measurement_finish(measurement)

        self._log(log_path, "Collecting live pod snapshot and summarizing run")
        collect_live_pods(self.cluster, run_dir)
        summary = summarize_run(
            run_dir,
            experiment_id=self.experiment.experiment_id,
            policy_name=self.policy.policy_name,
            run_id=run_id,
            expected_jobs=set(JOB_CATALOG),
        )
        if summary["overall_status"] != "pass":
            collect_describes(
                self.cluster,
                run_dir,
                job_name_map={job_id: job.kubernetes_name for job_id, job in launched_jobs.items()},
                summary=summary,
            )
        self._log(
            log_path,
            "Run completed with status "
            f"{summary['overall_status']} makespan={summary.get('makespan_s')} "
            f"max_p95_us={summary.get('max_observed_p95_us')}",
        )
        return run_dir

    def run_batch(self, runs: int, *, dry_run: bool = False) -> list[Path]:
        run_dirs: list[Path] = []
        print(f"Starting batch of {runs} run(s)")
        for index in range(1, runs + 1):
            print(f"Starting run {index}/{runs}")
            run_dir = self.run_once(dry_run=dry_run)
            run_dirs.append(run_dir)
            print(f"Finished run {index}/{runs}: {run_dir}")
        print("Batch complete")
        return run_dirs

```

`schedule.yaml`:

```yaml
{
  "policy_name": "edit-this-file",
  "memcached": {
    "node": "node-b-4core",
    "cores": "0",
    "threads": 1
  },
  "jobs": {
    "streamcluster": {
      "node": "node-a-8core",
      "cores": "0-7",
      "threads": 8,
      "after": "start"
    },
    "canneal": {
      "node": "node-b-4core",
      "cores": "1-3",
      "threads": 3,
      "after": "start"
    },
    "freqmine": {
      "node": "node-a-8core",
      "cores": "0-7",
      "threads": 8,
      "after": "streamcluster"
    },
    "blackscholes": {
      "node": "node-b-4core",
      "cores": "1-3",
      "threads": 3,
      "after": "canneal"
    },
    "barnes": {
      "node": "node-a-8core",
      "cores": "0-3",
      "threads": 4,
      "after": "freqmine"
    },
    "vips": {
      "node": "node-a-8core",
      "cores": "4-7",
      "threads": 4,
      "after": "freqmine"
    },
    "radix": {
      "node": "node-b-4core",
      "cores": "1-3",
      "threads": 3,
      "after": "blackscholes"
    }
  }
}

```

`tests/__init__.py`:

```py
"""Unit and opt-in integration tests for the Part 3 framework."""


```

`tests/helpers.py`:

```py
from __future__ import annotations

import json
from pathlib import Path
from tempfile import TemporaryDirectory


def write_json_config(path: Path, payload: dict[str, object]) -> None:
    path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")


def temp_workspace() -> TemporaryDirectory[str]:
    return TemporaryDirectory(prefix="part3-tests-")


```

`tests/test_audit.py`:

```py
from __future__ import annotations

import json
import unittest
from pathlib import Path

from part3.automation.audit import (
    AuditJob,
    AuditMemcached,
    audit_schedule,
    build_schedule_model,
    estimate_runtime,
    load_runtime_table,
    serialize_policy_document,
)
from part3.automation.catalog import NODE_A, NODE_B
from part3.automation.gui import build_model_from_planner_state, planner_state_from_model


ROOT = Path("/home/carti/ETH/Msc/CCA")
TIMES_CSV = ROOT / "risultatiPart3/Part2summary_times.csv"


def _base_jobs() -> dict[str, AuditJob]:
    return {
        "blackscholes": AuditJob(
            job_id="blackscholes",
            node=NODE_A,
            cores="0-3",
            threads=4,
            dependencies=(),
            delay_s=0,
            order=1,
        ),
        "barnes": AuditJob(
            job_id="barnes",
            node=NODE_A,
            cores="4-7",
            threads=4,
            dependencies=(),
            delay_s=0,
            order=2,
        ),
        "streamcluster": AuditJob(
            job_id="streamcluster",
            node=NODE_A,
            cores="0-7",
            threads=8,
            dependencies=("blackscholes", "barnes"),
            delay_s=0,
            order=3,
        ),
        "canneal": AuditJob(
            job_id="canneal",
            node=NODE_A,
            cores="0-7",
            threads=8,
            dependencies=("streamcluster",),
            delay_s=0,
            order=4,
        ),
        "vips": AuditJob(
            job_id="vips",
            node=NODE_A,
            cores="0-7",
            threads=8,
            dependencies=("canneal",),
            delay_s=0,
            order=5,
        ),
        "radix": AuditJob(
            job_id="radix",
            node=NODE_A,
            cores="0-7",
            threads=8,
            dependencies=("vips",),
            delay_s=0,
            order=6,
        ),
        "freqmine": AuditJob(
            job_id="freqmine",
            node=NODE_B,
            cores="1-3",
            threads=3,
            dependencies=(),
            delay_s=0,
            order=7,
        ),
    }


class AuditTests(unittest.TestCase):
    def setUp(self) -> None:
        self.runtime_table = load_runtime_table(str(TIMES_CSV))

    def _build_model(self, jobs: dict[str, AuditJob]):
        return build_schedule_model(
            policy_name="test-policy",
            memcached=AuditMemcached(node=NODE_B, cores="0", threads=1),
            jobs=jobs,
        )

    def test_interpolates_three_thread_runtime(self) -> None:
        estimate = estimate_runtime("freqmine", 3, self.runtime_table)
        self.assertIsNotNone(estimate)
        assert estimate is not None
        self.assertGreater(estimate, 206.718)
        self.assertLess(estimate, 266.438)

    def test_allows_split_four_core_jobs_on_node_a(self) -> None:
        report = audit_schedule(self._build_model(_base_jobs()), self.runtime_table)
        self.assertEqual(report.errors, [])

    def test_detects_overlap_for_concurrent_jobs(self) -> None:
        jobs = _base_jobs()
        jobs["barnes"] = AuditJob(
            job_id="barnes",
            node=NODE_A,
            cores="2-3",
            threads=2,
            dependencies=(),
            delay_s=0,
            order=2,
        )
        report = audit_schedule(self._build_model(jobs), self.runtime_table)
        self.assertTrue(report.errors)
        self.assertTrue(any("Core overlap on node-a-8core" in issue.message for issue in report.errors))

    def test_rejects_unsupported_core_set_before_overlap(self) -> None:
        jobs = _base_jobs()
        jobs["barnes"] = AuditJob(
            job_id="barnes",
            node=NODE_A,
            cores="1-5",
            threads=5,
            dependencies=(),
            delay_s=0,
            order=2,
        )
        report = audit_schedule(self._build_model(jobs), self.runtime_table)
        self.assertTrue(any("unsupported core set 1-5 on node-a-8core" in issue.message for issue in report.errors))

    def test_reports_idle_gaps_as_warnings_only(self) -> None:
        jobs = _base_jobs()
        jobs["canneal"] = AuditJob(
            job_id="canneal",
            node=NODE_A,
            cores="0-7",
            threads=8,
            dependencies=("streamcluster",),
            delay_s=15,
            order=4,
        )
        report = audit_schedule(self._build_model(jobs), self.runtime_table)
        self.assertEqual(report.errors, [])
        self.assertTrue(any("Idle gap on node-a-8core" in issue.message for issue in report.warnings))


class PlannerRoundTripTests(unittest.TestCase):
    def test_round_trip_serializes_explicit_policy(self) -> None:
        original_model = self._base_model()
        planner_state = planner_state_from_model(original_model)
        rebuilt_model = build_model_from_planner_state(planner_state)
        payload = json.loads(serialize_policy_document(rebuilt_model))
        self.assertIn("job_overrides", payload)
        self.assertIn("phases", payload)
        self.assertEqual(payload["phases"][0]["launch"], ["blackscholes", "barnes", "freqmine"])

    def _base_model(self):
        return build_schedule_model(
            policy_name="planner-test",
            memcached=AuditMemcached(node=NODE_B, cores="0", threads=1),
            jobs=_base_jobs(),
        )

```

`tests/test_cluster_labels.py`:

```py
from __future__ import annotations

import copy
import unittest
from pathlib import Path

from Matte.automation.cluster import ClusterController
from Matte.automation.config import ExperimentConfig, MeasurementConfig
from Matte.automation.utils import CommandResult


def _experiment_config() -> ExperimentConfig:
    return ExperimentConfig(
        config_path=Path("/tmp/experiment.yaml"),
        experiment_id="demo",
        cluster_name="part3.k8s.local",
        zone="europe-west1-b",
        kops_state_store="gs://bucket",
        ssh_key_path=Path("/tmp/cloud-computing"),
        ssh_user="ubuntu",
        cluster_config_path=Path("/tmp/part3.yaml"),
        results_root=Path("/tmp/runs"),
        submission_group="054",
        memcached_name="some-memcached",
        remote_repo_dir="/opt/cca/memcache-perf-dynamic",
        measurement=MeasurementConfig(
            agent_a_threads=2,
            agent_b_threads=4,
            measure_threads=6,
            connections=4,
            depth=4,
            qps_interval=1000,
            scan_start=30000,
            scan_stop=30500,
            scan_step=5,
            max_start_wait_s=180,
            completion_timeout_s=3600,
        ),
    )


class FakeClusterController(ClusterController):
    def __init__(self, payload: dict[str, object]):
        super().__init__(_experiment_config())
        self.payload = copy.deepcopy(payload)
        self.label_calls: list[tuple[str, ...]] = []

    def kubectl_json(self, *args: str) -> dict[str, object]:
        self.last_kubectl_json_args = args
        return copy.deepcopy(self.payload)

    def kubectl(self, *args: str, check: bool = True) -> CommandResult:
        if args[:2] == ("label", "nodes"):
            node_name = args[2]
            label_assignment = args[3]
            key, value = label_assignment.split("=", 1)
            for item in self.payload["items"]:
                metadata = item.setdefault("metadata", {})
                if metadata.get("name") != node_name:
                    continue
                metadata.setdefault("labels", {})[key] = value
                break
            self.label_calls.append(args)
            return CommandResult(args=list(args), returncode=0, stdout="", stderr="")
        raise AssertionError(f"Unexpected kubectl call: {args}")


class ClusterLabelRepairTests(unittest.TestCase):
    def test_discover_nodes_infers_canonical_nodetype_from_randomized_name(self) -> None:
        cluster = FakeClusterController(
            {
                "items": [
                    {
                        "metadata": {"name": "client-agent-a-fn6b", "labels": {}},
                        "status": {
                            "addresses": [
                                {"type": "InternalIP", "address": "10.0.16.5"},
                                {"type": "ExternalIP", "address": "35.189.215.31"},
                            ]
                        },
                    },
                    {
                        "metadata": {"name": "node-a-8core-7jrx", "labels": {}},
                        "status": {
                            "addresses": [
                                {"type": "InternalIP", "address": "10.0.16.8"},
                            ]
                        },
                    },
                ]
            }
        )

        nodes = cluster.discover_nodes()

        self.assertEqual(nodes["client-agent-a"].name, "client-agent-a-fn6b")
        self.assertEqual(nodes["client-agent-a"].internal_ip, "10.0.16.5")
        self.assertEqual(nodes["node-a-8core"].name, "node-a-8core-7jrx")
        self.assertEqual(nodes["node-a-8core"].internal_ip, "10.0.16.8")

    def test_ensure_canonical_node_labels_repairs_unlabeled_nodes(self) -> None:
        cluster = FakeClusterController(
            {
                "items": [
                    {
                        "metadata": {"name": "client-agent-a-fn6b", "labels": {}},
                        "status": {"addresses": []},
                    },
                    {
                        "metadata": {"name": "client-agent-b-rw1c", "labels": {}},
                        "status": {"addresses": []},
                    },
                    {
                        "metadata": {
                            "name": "node-b-4core-h3sc",
                            "labels": {"cca-project-nodetype": "node-b-4core"},
                        },
                        "status": {"addresses": []},
                    },
                ]
            }
        )

        nodes = cluster.ensure_canonical_node_labels()

        self.assertEqual(nodes["client-agent-a"].name, "client-agent-a-fn6b")
        self.assertEqual(nodes["client-agent-b"].name, "client-agent-b-rw1c")
        self.assertEqual(
            cluster.label_calls,
            [
                (
                    "label",
                    "nodes",
                    "client-agent-a-fn6b",
                    "cca-project-nodetype=client-agent-a",
                    "--overwrite",
                ),
                (
                    "label",
                    "nodes",
                    "client-agent-b-rw1c",
                    "cca-project-nodetype=client-agent-b",
                    "--overwrite",
                ),
            ],
        )


if __name__ == "__main__":
    unittest.main()

```

`tests/test_config.py`:

```py
from __future__ import annotations

import unittest
from pathlib import Path

from part3.automation.config import load_experiment_config, load_policy_config
from part3.automation.tests.helpers import temp_workspace, write_json_config


class ConfigTests(unittest.TestCase):
    def test_experiment_config_resolves_relative_paths(self) -> None:
        with temp_workspace() as workspace:
            root = Path(workspace)
            config_path = root / "experiment.yaml"
            write_json_config(
                config_path,
                {
                    "experiment_id": "demo",
                    "cluster_name": "part3.k8s.local",
                    "zone": "europe-west1-b",
                    "kops_state_store": "gs://bucket",
                    "ssh_key_path": "~/.ssh/cloud-computing",
                    "cluster_config_path": "part3.yaml",
                    "results_root": "runs",
                    "submission_group": "054",
                },
            )
            config = load_experiment_config(str(config_path))
            self.assertEqual(config.experiment_id, "demo")
            self.assertEqual(config.results_root, (root / "runs").resolve())

    def test_policy_rejects_invalid_phase_dependency(self) -> None:
        with temp_workspace() as workspace:
            path = Path(workspace) / "policy.yaml"
            write_json_config(
                path,
                {
                    "policy_name": "bad",
                    "memcached": {"node": "node-b-4core", "cores": "0", "threads": 1},
                    "job_overrides": {},
                    "phases": [
                        {
                            "id": "phase-1",
                            "after": "phase:phase-1",
                            "delay_s": 0,
                            "launch": [
                                "barnes",
                                "blackscholes",
                                "canneal",
                                "freqmine",
                                "radix",
                                "streamcluster",
                                "vips",
                            ],
                        }
                    ],
                },
            )
            with self.assertRaises(ValueError):
                load_policy_config(str(path))

    def test_policy_rejects_invalid_core_set(self) -> None:
        with temp_workspace() as workspace:
            path = Path(workspace) / "policy.yaml"
            write_json_config(
                path,
                {
                    "policy_name": "bad-cores",
                    "memcached": {"node": "node-b-4core", "cores": "0", "threads": 1},
                    "job_overrides": {"blackscholes": {"node": "node-b-4core", "cores": "7-9"}},
                    "phases": [
                        {"id": "p1", "after": "start", "delay_s": 0, "launch": ["barnes", "blackscholes"]},
                        {"id": "p2", "after": "jobs_complete", "jobs_complete": ["barnes"], "delay_s": 0, "launch": ["canneal"]},
                        {"id": "p3", "after": "jobs_complete", "jobs_complete": ["canneal"], "delay_s": 0, "launch": ["freqmine"]},
                        {"id": "p4", "after": "jobs_complete", "jobs_complete": ["freqmine"], "delay_s": 0, "launch": ["radix"]},
                        {"id": "p5", "after": "jobs_complete", "jobs_complete": ["radix"], "delay_s": 0, "launch": ["streamcluster"]},
                        {"id": "p6", "after": "jobs_complete", "jobs_complete": ["streamcluster"], "delay_s": 0, "launch": ["vips"]},
                    ],
                },
            )
            with self.assertRaises(ValueError):
                load_policy_config(str(path))

    def test_simple_schedule_is_compiled_into_policy(self) -> None:
        with temp_workspace() as workspace:
            path = Path(workspace) / "schedule.yaml"
            write_json_config(
                path,
                {
                    "policy_name": "simple-schedule",
                    "memcached": {"node": "node-b-4core", "cores": "0", "threads": 1},
                    "jobs": {
                        "streamcluster": {
                            "node": "node-a-8core",
                            "cores": "0-7",
                            "threads": 8,
                            "after": "start",
                        },
                        "blackscholes": {
                            "node": "node-b-4core",
                            "cores": "1-3",
                            "threads": 3,
                            "after": "start",
                        },
                        "freqmine": {
                            "node": "node-b-4core",
                            "cores": "1-3",
                            "threads": 3,
                            "after": "blackscholes",
                        },
                        "canneal": {
                            "node": "node-a-8core",
                            "cores": "0-7",
                            "threads": 8,
                            "after": "streamcluster",
                        },
                        "barnes": {
                            "node": "node-a-8core",
                            "cores": "0-7",
                            "threads": 8,
                            "after": "canneal",
                        },
                        "vips": {
                            "node": "node-a-8core",
                            "cores": "0-7",
                            "threads": 8,
                            "after": "barnes",
                        },
                        "radix": {
                            "node": "node-a-8core",
                            "cores": "0-7",
                            "threads": 8,
                            "after": "vips",
                        },
                    },
                },
            )
            policy = load_policy_config(str(path))
            self.assertEqual(policy.policy_name, "simple-schedule")
            self.assertEqual(policy.phases[0].launch, ("streamcluster", "blackscholes"))
            self.assertEqual(policy.phases[1].jobs_complete, ("blackscholes",))

```

`tests/test_debug.py`:

```py
from __future__ import annotations

import copy
import io
import unittest
from contextlib import redirect_stdout
from pathlib import Path
from unittest.mock import patch

from Matte.automation import cli
from Matte.automation.cluster import ClusterController
from Matte.automation.config import ExperimentConfig, MeasurementConfig, MemcachedConfig, PolicyConfig
from Matte.automation.debug import render_debug_commands, summarize_provisioning_hints
from Matte.automation.provision import ProvisionStatus, ProvisioningError
from Matte.automation.runner import ExperimentRunner
from Matte.automation.tests.helpers import temp_workspace, write_json_config


PART3_YAML = Path("/home/carti/ETH/Msc/CCA/risultatiPart3/Matte/automation/part3.yaml")


def _experiment_config() -> ExperimentConfig:
    return ExperimentConfig(
        config_path=Path("/tmp/experiment.yaml"),
        experiment_id="demo",
        cluster_name="part3.k8s.local",
        zone="europe-west1-b",
        kops_state_store="gs://bucket",
        ssh_key_path=Path("/tmp/cloud-computing"),
        ssh_user="ubuntu",
        cluster_config_path=Path("/tmp/part3.yaml"),
        results_root=Path("/tmp/runs"),
        submission_group="054",
        memcached_name="some-memcached",
        remote_repo_dir="/opt/cca/memcache-perf-dynamic",
        measurement=MeasurementConfig(
            agent_a_threads=2,
            agent_b_threads=4,
            measure_threads=6,
            connections=4,
            depth=4,
            qps_interval=1000,
            scan_start=30000,
            scan_stop=30500,
            scan_step=5,
            max_start_wait_s=180,
            completion_timeout_s=3600,
        ),
    )


def _waiting_statuses() -> dict[str, ProvisionStatus]:
    return {
        "client-agent-a": ProvisionStatus(
            nodetype="client-agent-a",
            node_name="client-agent-a-fn6b",
            bootstrap_ready=False,
            mcperf_present=False,
            agent_service_state="not-installed",
        ),
        "client-agent-b": ProvisionStatus(
            nodetype="client-agent-b",
            node_name="client-agent-b-rw1c",
            bootstrap_ready=False,
            mcperf_present=False,
            agent_service_state="not-installed",
        ),
        "client-measure": ProvisionStatus(
            nodetype="client-measure",
            node_name="client-measure-2dll",
            bootstrap_ready=True,
            mcperf_present=True,
            agent_service_state="not-installed",
        ),
    }


def _simple_schedule_jobs() -> dict[str, object]:
    return {
        "barnes": {"after": "start", "delay_s": 0},
        "blackscholes": {"after": "start", "delay_s": 0},
        "canneal": {"after": "start", "delay_s": 0},
        "freqmine": {"after": "start", "delay_s": 0},
        "radix": {"after": "start", "delay_s": 0},
        "streamcluster": {"after": "start", "delay_s": 0},
        "vips": {"after": "start", "delay_s": 0},
    }


class FakeClusterController(ClusterController):
    def __init__(self, payload: dict[str, object]):
        super().__init__(_experiment_config())
        self.payload = copy.deepcopy(payload)

    def kubectl_json(self, *args: str) -> dict[str, object]:
        return copy.deepcopy(self.payload)


class MinimalRunnerCluster:
    def cleanup_managed_workloads(self) -> None:
        return


class DebugCommandRenderingTests(unittest.TestCase):
    def test_render_debug_commands_uses_resolved_vm_names(self) -> None:
        cluster = FakeClusterController(
            {
                "items": [
                    {"metadata": {"name": "client-agent-a-fn6b", "labels": {}}, "status": {"addresses": []}},
                    {"metadata": {"name": "client-agent-b-rw1c", "labels": {}}, "status": {"addresses": []}},
                    {"metadata": {"name": "client-measure-2dll", "labels": {}}, "status": {"addresses": []}},
                ]
            }
        )

        rendered = render_debug_commands(experiment=_experiment_config(), cluster=cluster)

        self.assertIn("Resolved nodes:", rendered)
        self.assertIn("- client-agent-a: client-agent-a-fn6b", rendered)
        self.assertIn("client-agent-a (client-agent-a-fn6b):", rendered)
        self.assertIn("ubuntu@client-agent-a-fn6b", rendered)
        self.assertIn("ubuntu@client-agent-b-rw1c", rendered)
        self.assertIn("ubuntu@client-measure-2dll", rendered)
        self.assertIn("gcloud compute instances get-serial-port-output client-agent-a-fn6b", rendered)
        self.assertIn("client-measure` does not run `mcperf-agent.service`", rendered)

    def test_render_debug_commands_includes_exact_memcached_and_mcperf_paths(self) -> None:
        cluster = FakeClusterController({"items": []})
        policy = PolicyConfig(
            config_path=Path("/tmp/policy.yaml"),
            policy_name="test-policy",
            memcached=MemcachedConfig(node="node-b-4core", cores="0", threads=1),
            job_overrides={},
            phases=[],
        )

        rendered = render_debug_commands(
            experiment=_experiment_config(),
            cluster=cluster,
            policy=policy,
            run_id="run-1",
        )

        self.assertIn("kubectl describe pod memcached-server-run-1", rendered)
        self.assertIn("kubectl logs -f memcached-server-run-1", rendered)
        self.assertIn("kubectl exec -it memcached-server-run-1 -- sh", rendered)
        self.assertIn("tail -f /tmp/runs/demo/run-1/mcperf.txt", rendered)
        self.assertIn("This is usually the most useful place to watch the benchmark", rendered)

    def test_summarize_provisioning_hints_explains_bootstrap_failure(self) -> None:
        hints = summarize_provisioning_hints(_waiting_statuses())

        self.assertTrue(any("bootstrap appears to have failed before mcperf installation" in hint for hint in hints))
        self.assertTrue(any("No memcached pod is expected yet" in hint for hint in hints))


class FailureSurfaceTests(unittest.TestCase):
    def test_provision_check_prints_hints_and_debug_pointer(self) -> None:
        experiment = _experiment_config()
        output = io.StringIO()

        with patch("Matte.automation.cli.load_experiment_config", return_value=experiment), patch(
            "Matte.automation.cli.ClusterController"
        ), patch(
            "Matte.automation.cli.check_client_provisioning",
            return_value=_waiting_statuses(),
        ):
            with redirect_stdout(output):
                cli.main(["provision", "check", "--config", "experiment.yaml"])

        rendered = output.getvalue()
        self.assertIn("Hint: client-agent-a: bootstrap appears to have failed before mcperf installation", rendered)
        self.assertIn("Debug commands: python3 cli.py debug commands --config /tmp/experiment.yaml", rendered)

    def test_run_once_logs_debug_pointer_when_provisioning_blocks(self) -> None:
        statuses = _waiting_statuses()
        error = ProvisioningError(
            "client-agent-a is not fully bootstrapped: client-agent-a (client-agent-a-fn6b): WAITING",
            statuses=statuses,
        )
        with temp_workspace() as workspace:
            root = Path(workspace)
            experiment_path = root / "experiment.yaml"
            policy_path = root / "policy.yaml"
            write_json_config(
                experiment_path,
                {
                    "experiment_id": "demo",
                    "cluster_name": "part3.k8s.local",
                    "zone": "europe-west1-b",
                    "kops_state_store": "gs://bucket",
                    "ssh_key_path": "~/.ssh/cloud-computing",
                    "cluster_config_path": str(PART3_YAML),
                    "results_root": str(root / "runs"),
                    "submission_group": "054",
                },
            )
            write_json_config(
                policy_path,
                {
                    "policy_name": "test-policy",
                    "memcached": {"node": "node-b-4core", "cores": "0", "threads": 1},
                    "jobs": _simple_schedule_jobs(),
                },
            )
            runner = ExperimentRunner(
                cli.load_experiment_config(str(experiment_path)),
                cli.load_policy_config(str(policy_path)),
            )
            runner.cluster = MinimalRunnerCluster()

            with patch("Matte.automation.runner.utc_timestamp", return_value="20260417T010203Z"), patch(
                "Matte.automation.runner.assert_client_provisioning",
                side_effect=error,
            ), self.assertRaises(ProvisioningError):
                runner.run_once()

            events_log = (
                root / "runs" / "demo" / "20260417t010203z" / "events.log"
            ).read_text(encoding="utf-8")
            self.assertIn("Hint: client-agent-a: bootstrap appears to have failed before mcperf installation", events_log)
            self.assertIn("Debug commands: python3 cli.py debug commands --config", events_log)
            self.assertIn("--policy", events_log)
            self.assertIn("--run-id 20260417t010203z", events_log)


class BootstrapScriptTests(unittest.TestCase):
    def test_all_client_bootstrap_scripts_share_the_new_dependency_helper(self) -> None:
        payload = PART3_YAML.read_text(encoding="utf-8")

        self.assertEqual(payload.count("prepare_memcached_build_dependencies() {"), 3)
        self.assertEqual(payload.count("apt-cache showsrc memcached >/dev/null 2>&1"), 3)
        self.assertEqual(payload.count("memcached source metadata is unavailable after enabling deb-src"), 3)


if __name__ == "__main__":
    unittest.main()

```

`tests/test_export.py`:

```py
from __future__ import annotations

import json
import unittest
from pathlib import Path

from part3.automation.export import export_submission
from part3.automation.tests.helpers import temp_workspace


class ExportTests(unittest.TestCase):
    def test_export_submission_creates_required_filenames(self) -> None:
        with temp_workspace() as workspace:
            root = Path(workspace)
            experiment_root = root / "runs" / "demo"
            experiment_root.mkdir(parents=True)
            for index in range(1, 4):
                run_dir = experiment_root / f"run-{index}"
                run_dir.mkdir()
                (run_dir / "pods.json").write_text("{}", encoding="utf-8")
                (run_dir / "mcperf.txt").write_text("#type p95\n", encoding="utf-8")
                (run_dir / "summary.json").write_text(
                    json.dumps({"run_id": f"run-{index}", "overall_status": "pass"}) + "\n",
                    encoding="utf-8",
                )
            target_dir = export_submission(
                results_root=root / "runs",
                experiment_id="demo",
                group="054",
                task="3_1",
                output_root=root,
            )
            self.assertTrue((target_dir / "pods_1.json").exists())
            self.assertTrue((target_dir / "pods_2.json").exists())
            self.assertTrue((target_dir / "pods_3.json").exists())
            self.assertTrue((target_dir / "mcperf_1.txt").exists())
            self.assertTrue((target_dir / "mcperf_2.txt").exists())
            self.assertTrue((target_dir / "mcperf_3.txt").exists())

```

`tests/test_live_integration.py`:

```py
from __future__ import annotations

import os
import unittest


@unittest.skipUnless(os.getenv("PART3_LIVE_TESTS") == "1", "live cluster tests are opt-in")
class LiveIntegrationTests(unittest.TestCase):
    def test_placeholder_for_live_cluster_smoke(self) -> None:
        self.assertEqual(os.getenv("PART3_LIVE_TESTS"), "1")


```

`tests/test_manifests.py`:

```py
from __future__ import annotations

import unittest

from part3.automation.config import load_policy_config
from part3.automation.manifests import render_batch_job_manifest, resolve_jobs


class ManifestTests(unittest.TestCase):
    def test_splash_jobs_render_with_splash_suite(self) -> None:
        policy = load_policy_config("/home/carti/ETH/Msc/CCA/part3/automation/policies/baseline.yaml")
        jobs = resolve_jobs(policy, "testrun")
        barnes_manifest = render_batch_job_manifest(jobs["barnes"], experiment_id="exp", run_id="testrun")
        radix_manifest = render_batch_job_manifest(jobs["radix"], experiment_id="exp", run_id="testrun")
        self.assertIn("anakli/cca:splash2x_barnes", barnes_manifest)
        self.assertIn("-S splash2x -p barnes", barnes_manifest)
        self.assertIn("anakli/cca:splash2x_radix", radix_manifest)
        self.assertIn("-S splash2x -p radix", radix_manifest)

    def test_parsec_jobs_render_with_parsec_suite(self) -> None:
        policy = load_policy_config("/home/carti/ETH/Msc/CCA/part3/automation/policies/baseline.yaml")
        jobs = resolve_jobs(policy, "testrun")
        manifest = render_batch_job_manifest(jobs["blackscholes"], experiment_id="exp", run_id="testrun")
        self.assertIn("anakli/cca:parsec_blackscholes", manifest)
        self.assertIn("-S parsec -p blackscholes", manifest)

```

`tests/test_metrics.py`:

```py
from __future__ import annotations

import unittest
from pathlib import Path

from part3.automation.metrics import build_summary, parse_mcperf_output


ROOT = Path("/home/carti/ETH/Msc/CCA")


class MetricsTests(unittest.TestCase):
    def test_parse_mcperf_output_counts_slo_violations(self) -> None:
        output = parse_mcperf_output(ROOT / "part3/results/firstRun/run1_mcperf.txt")
        self.assertEqual(output["slo_violations"], 0)
        self.assertIsNotNone(output["max_p95_us"])

    def test_build_summary_marks_first_run_as_pass(self) -> None:
        summary = build_summary(
            ROOT / "part3/results/firstRun/results.json",
            ROOT / "part3/results/firstRun/run1_mcperf.txt",
            {"barnes", "blackscholes", "canneal", "freqmine", "radix", "streamcluster", "vips"},
            run_id="sample-run",
            experiment_id="sample-experiment",
            policy_name="sample-policy",
        )
        self.assertEqual(summary["overall_status"], "pass")
        self.assertAlmostEqual(summary["makespan_s"], 259.0, places=1)

```

`tests/test_provision.py`:

```py
from __future__ import annotations

import unittest
from pathlib import Path

from Matte.automation.provision import (
    ProvisionStatus,
    render_provision_check_note,
    render_provision_expectations,
)


class ProvisionPresentationTests(unittest.TestCase):
    def test_render_provision_check_note_mentions_three_prompts(self) -> None:
        note = render_provision_check_note(Path("/home/carti/.ssh/cloud-computing"))

        self.assertIn("3 client VMs", note)
        self.assertIn("up to 3 passphrase prompts", note)
        self.assertIn("ssh-add /home/carti/.ssh/cloud-computing", note)

    def test_agent_status_string_explains_waiting_state(self) -> None:
        status = ProvisionStatus(
            nodetype="client-agent-a",
            node_name="client-agent-a-fn6b",
            bootstrap_ready=False,
            mcperf_present=False,
            agent_service_state="not-installed",
        )

        rendered = str(status)

        self.assertFalse(status.is_ready)
        self.assertIn("WAITING", rendered)
        self.assertIn("bootstrap not finished", rendered)
        self.assertIn("mcperf missing", rendered)
        self.assertIn("mcperf-agent.service not installed", rendered)

    def test_measure_node_ready_does_not_require_agent_service(self) -> None:
        status = ProvisionStatus(
            nodetype="client-measure",
            node_name="client-measure-2dll",
            bootstrap_ready=True,
            mcperf_present=True,
            agent_service_state="not-installed",
        )

        self.assertTrue(status.is_ready)
        self.assertEqual(
            str(status),
            "client-measure (client-measure-2dll): READY - bootstrap ready; mcperf present",
        )

    def test_expectations_text_mentions_agents_and_measure_node(self) -> None:
        expectations = render_provision_expectations()

        self.assertIn("client-agent-a/client-agent-b", expectations)
        self.assertIn("client-measure", expectations)


if __name__ == "__main__":
    unittest.main()

```

`tests/test_results.py`:

```py
from __future__ import annotations

import unittest

from part3.automation.results import sort_best_runs


class ResultsTests(unittest.TestCase):
    def test_best_runs_sort_passes_first_then_makespan(self) -> None:
        summaries = [
            {"run_id": "c", "overall_status": "slo_fail", "makespan_s": 10, "max_observed_p95_us": 1200},
            {"run_id": "b", "overall_status": "pass", "makespan_s": 200, "max_observed_p95_us": 800},
            {"run_id": "a", "overall_status": "pass", "makespan_s": 150, "max_observed_p95_us": 900},
        ]
        ordered = sort_best_runs(summaries)
        self.assertEqual([entry["run_id"] for entry in ordered], ["a", "b", "c"])

```

`tests/test_runner.py`:

```py
from __future__ import annotations

import unittest
from dataclasses import dataclass
from pathlib import Path
from unittest.mock import patch

from part3.automation.cluster import NodeInfo
from part3.automation.config import (
    JobOverride,
    MemcachedConfig,
    Phase,
    PolicyConfig,
    load_experiment_config,
    load_policy_config,
)
from part3.automation.runner import ExperimentRunner
from part3.automation.tests.helpers import temp_workspace, write_json_config


BASE_POLICY = "/home/carti/ETH/Msc/CCA/part3/automation/policies/baseline.yaml"


@dataclass(frozen=True)
class JobOutcome:
    duration_s: float
    failed: bool = False


class FakeClock:
    def __init__(self) -> None:
        self.now = 0.0

    def advance(self, seconds: float) -> None:
        self.now += seconds


class FakeCluster:
    def __init__(self, clock: FakeClock, outcomes: dict[str, JobOutcome]):
        self.clock = clock
        self.outcomes = outcomes
        self.applied_job_ids: list[str] = []
        self.job_launch_times: dict[str, float] = {}
        self.job_names: dict[str, str] = {}
        self.job_run_ids: dict[str, str] = {}

    def cleanup_managed_workloads(self) -> None:
        return

    def apply_manifest(self, manifest_path: Path) -> None:
        manifest = self._parse_manifest(manifest_path)
        if manifest["kind"] != "Job":
            return
        job_id = manifest["labels"]["cca-project-job-id"]
        self.applied_job_ids.append(job_id)
        self.job_launch_times[job_id] = self.clock.now
        self.job_names[job_id] = manifest["name"]
        self.job_run_ids[job_id] = manifest["labels"]["cca-project-run-id"]

    def wait_for_pod_ready(self, pod_name: str, timeout_s: int = 300) -> None:
        return

    def get_pod_by_run_role(self, run_id: str, role: str) -> dict[str, object]:
        return {"status": {"podIP": "10.0.0.10"}}

    def discover_nodes(self) -> dict[str, NodeInfo]:
        return {
            "client-agent-a": NodeInfo("client-agent-a-node", "client-agent-a", "10.0.0.11", None),
            "client-agent-b": NodeInfo("client-agent-b-node", "client-agent-b", "10.0.0.12", None),
            "client-measure": NodeInfo("client-measure-node", "client-measure", "10.0.0.13", None),
        }

    def get_run_jobs_snapshot(self, run_id: str) -> dict[str, dict[str, object]]:
        snapshots: dict[str, dict[str, object]] = {}
        for job_id, launch_time in self.job_launch_times.items():
            if self.job_run_ids[job_id] != run_id:
                continue
            outcome = self.outcomes[job_id]
            if self.clock.now - launch_time >= outcome.duration_s:
                status = {"failed": 1} if outcome.failed else {"succeeded": 1}
                state = "failed" if outcome.failed else "completed"
            else:
                status = {"active": 1}
                state = "running"
            snapshots[self.job_names[job_id]] = {
                "status": state,
                "payload": {"metadata": {"name": self.job_names[job_id]}, "status": status},
            }
        return snapshots

    def _parse_manifest(self, manifest_path: Path) -> dict[str, object]:
        kind = ""
        name = manifest_path.stem
        labels: dict[str, str] = {}
        in_metadata = False
        in_labels = False
        for line in manifest_path.read_text(encoding="utf-8").splitlines():
            if line.startswith("kind: "):
                kind = line.split(":", 1)[1].strip()
            if line == "metadata:":
                in_metadata = True
                in_labels = False
                continue
            if in_metadata and line == "spec:":
                in_metadata = False
                in_labels = False
                continue
            if not in_metadata:
                continue
            if line.startswith("  name: "):
                name = line.split(":", 1)[1].strip()
            elif line.startswith("  labels:"):
                in_labels = True
            elif in_labels and line.startswith("    "):
                key, value = line.strip().split(":", 1)
                labels[key] = value.strip().strip('"')
            elif line.startswith("  ") and not line.startswith("    "):
                in_labels = False
        return {"kind": kind, "name": name, "labels": labels}


class FakeMeasurementRunner(ExperimentRunner):
    def __init__(self, *args, clock: FakeClock, **kwargs):
        super().__init__(*args, **kwargs)
        self.clock = clock

    def _start_measurement(self, **kwargs):  # type: ignore[override]
        return object()

    def _wait_for_measurement_start(self, handle) -> None:  # type: ignore[override]
        return

    def _wait_for_measurement_finish(self, handle) -> None:  # type: ignore[override]
        return

    def _current_time(self) -> float:
        return self.clock.now

    def _sleep(self, seconds: float) -> None:
        self.clock.advance(seconds)


class RunnerDryRunTests(unittest.TestCase):
    def test_dry_run_creates_plan_and_manifests(self) -> None:
        with temp_workspace() as workspace:
            root = Path(workspace)
            experiment_path = root / "experiment.yaml"
            write_json_config(
                experiment_path,
                {
                    "experiment_id": "demo",
                    "cluster_name": "part3.k8s.local",
                    "zone": "europe-west1-b",
                    "kops_state_store": "gs://bucket",
                    "ssh_key_path": "~/.ssh/cloud-computing",
                    "cluster_config_path": "/home/carti/ETH/Msc/CCA/part3/part3.yaml",
                    "results_root": str(root / "runs"),
                    "submission_group": "054",
                },
            )
            runner = ExperimentRunner(load_experiment_config(str(experiment_path)), load_policy_config(BASE_POLICY))
            run_dir = runner.run_once(dry_run=True)
            self.assertTrue((run_dir / "phase_plan.json").exists())
            self.assertTrue((run_dir / "rendered_manifests" / "memcached.yaml").exists())
            self.assertTrue((run_dir / "rendered_manifests" / "barnes.yaml").exists())


class RunnerAsyncSchedulerTests(unittest.TestCase):
    def _write_experiment(self, root: Path):
        experiment_path = root / "experiment.yaml"
        write_json_config(
            experiment_path,
            {
                "experiment_id": "demo",
                "cluster_name": "part3.k8s.local",
                "zone": "europe-west1-b",
                "kops_state_store": "gs://bucket",
                "ssh_key_path": "~/.ssh/cloud-computing",
                "cluster_config_path": "/home/carti/ETH/Msc/CCA/part3/part3.yaml",
                "results_root": str(root / "runs"),
                "submission_group": "054",
                "mcperf_measurement": {
                    "completion_timeout_s": 30,
                },
            },
        )
        return load_experiment_config(str(experiment_path))

    def _write_policy_placeholder(self, root: Path) -> Path:
        policy_path = root / "policy.yaml"
        write_json_config(policy_path, {"policy_name": "test-policy"})
        return policy_path

    def _build_runner(
        self,
        root: Path,
        *,
        phases: list[Phase],
        outcomes: dict[str, JobOutcome],
        job_overrides: dict[str, JobOverride] | None = None,
    ) -> tuple[FakeMeasurementRunner, FakeCluster]:
        experiment = self._write_experiment(root)
        policy = PolicyConfig(
            config_path=self._write_policy_placeholder(root),
            policy_name="test-policy",
            memcached=MemcachedConfig(node="node-b-4core", cores="0", threads=1),
            job_overrides=job_overrides or {},
            phases=phases,
        )
        clock = FakeClock()
        cluster = FakeCluster(clock, outcomes)
        runner = FakeMeasurementRunner(experiment, policy, clock=clock)
        runner.cluster = cluster
        return runner, cluster

    def _run_once(self, runner: ExperimentRunner) -> Path:
        with patch("part3.automation.runner.assert_client_provisioning"), patch(
            "part3.automation.runner.collect_live_pods"
        ), patch("part3.automation.runner.collect_describes"), patch(
            "part3.automation.runner.summarize_run",
            return_value={"overall_status": "pass"},
        ):
            return runner.run_once()

    def test_later_phase_can_launch_before_earlier_blocked_phase(self) -> None:
        with temp_workspace() as workspace:
            root = Path(workspace)
            runner, cluster = self._build_runner(
                root,
                phases=[
                    Phase("p1", "start", (), 0, ("streamcluster", "blackscholes")),
                    Phase("p2", "jobs_complete", ("streamcluster",), 0, ("canneal",)),
                    Phase("p3", "jobs_complete", ("blackscholes",), 0, ("freqmine",)),
                ],
                outcomes={
                    "streamcluster": JobOutcome(10),
                    "blackscholes": JobOutcome(2),
                    "canneal": JobOutcome(1),
                    "freqmine": JobOutcome(1),
                },
            )

            run_dir = self._run_once(runner)

            self.assertEqual(cluster.applied_job_ids, ["streamcluster", "blackscholes", "freqmine", "canneal"])
            events_log = (run_dir / "events.log").read_text(encoding="utf-8")
            self.assertIn("Phase dependency satisfied for p3: blackscholes", events_log)
            self.assertIn("Job completed: parsec-blackscholes", events_log)

    def test_phase_dependency_waits_for_every_job_in_referenced_phase(self) -> None:
        with temp_workspace() as workspace:
            root = Path(workspace)
            runner, cluster = self._build_runner(
                root,
                phases=[
                    Phase("p1", "start", (), 0, ("blackscholes", "freqmine")),
                    Phase("p2", "phase:p1", (), 0, ("barnes",)),
                ],
                outcomes={
                    "blackscholes": JobOutcome(2),
                    "freqmine": JobOutcome(5),
                    "barnes": JobOutcome(1),
                },
            )

            self._run_once(runner)

            self.assertEqual(cluster.job_launch_times["barnes"], 5.0)

    def test_split_core_follow_up_can_start_while_other_half_is_still_busy(self) -> None:
        with temp_workspace() as workspace:
            root = Path(workspace)
            runner, cluster = self._build_runner(
                root,
                phases=[
                    Phase("p1", "start", (), 0, ("blackscholes", "barnes")),
                    Phase("p2", "jobs_complete", ("blackscholes",), 0, ("freqmine",)),
                ],
                outcomes={
                    "blackscholes": JobOutcome(2),
                    "barnes": JobOutcome(10),
                    "freqmine": JobOutcome(1),
                },
                job_overrides={
                    "blackscholes": JobOverride(node="node-a-8core", cores="0-3", threads=4),
                    "barnes": JobOverride(node="node-a-8core", cores="4-7", threads=4),
                    "freqmine": JobOverride(node="node-a-8core", cores="0-3", threads=4),
                },
            )

            self._run_once(runner)

            self.assertEqual(cluster.job_launch_times["blackscholes"], 0.0)
            self.assertEqual(cluster.job_launch_times["barnes"], 0.0)
            self.assertEqual(cluster.job_launch_times["freqmine"], 2.0)
            self.assertLess(cluster.job_launch_times["freqmine"], cluster.job_launch_times["barnes"] + 10.0)

    def test_failed_job_aborts_before_dependent_phase_launches(self) -> None:
        with temp_workspace() as workspace:
            root = Path(workspace)
            runner, cluster = self._build_runner(
                root,
                phases=[
                    Phase("p1", "start", (), 0, ("blackscholes", "streamcluster")),
                    Phase("p2", "jobs_complete", ("blackscholes",), 0, ("freqmine",)),
                ],
                outcomes={
                    "blackscholes": JobOutcome(2, failed=True),
                    "streamcluster": JobOutcome(10),
                    "freqmine": JobOutcome(1),
                },
            )

            with self.assertRaisesRegex(RuntimeError, "blackscholes"):
                self._run_once(runner)

            self.assertEqual(cluster.applied_job_ids, ["blackscholes", "streamcluster"])


if __name__ == "__main__":
    unittest.main()

```

`utils.py`:

```py
from __future__ import annotations

import json
import os
import subprocess
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Mapping, Sequence


@dataclass
class CommandResult:
    args: Sequence[str]
    returncode: int
    stdout: str
    stderr: str

    @property
    def combined_output(self) -> str:
        return "\n".join(part for part in (self.stdout, self.stderr) if part).strip()


def utc_timestamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def ensure_directory(path: Path) -> Path:
    path.mkdir(parents=True, exist_ok=True)
    return path


def write_json(path: Path, data: object) -> None:
    path.write_text(json.dumps(data, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def append_log(path: Path, message: str) -> None:
    timestamp = datetime.now(timezone.utc).isoformat()
    with path.open("a", encoding="utf-8") as handle:
        handle.write(f"{timestamp} {message}\n")


def expand_path(path_str: str, base_dir: Path | None = None) -> Path:
    expanded = Path(os.path.expanduser(path_str))
    if not expanded.is_absolute() and base_dir is not None:
        expanded = base_dir / expanded
    return expanded.resolve()


def run_command(
    args: Sequence[str],
    *,
    env: Mapping[str, str] | None = None,
    cwd: Path | None = None,
    input_text: str | None = None,
    check: bool = True,
    live_output: bool = False,
    output_prefix: str | None = None,
) -> CommandResult:
    merged_env = os.environ.copy()
    if env:
        merged_env.update(env)

    if live_output:
        process = subprocess.Popen(
            list(args),
            cwd=str(cwd) if cwd else None,
            env=merged_env,
            stdin=subprocess.PIPE if input_text is not None else None,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
        )
        captured_lines: list[str] = []
        if input_text is not None and process.stdin is not None:
            process.stdin.write(input_text)
            process.stdin.close()
        assert process.stdout is not None
        for line in process.stdout:
            captured_lines.append(line)
            text = line.rstrip()
            if output_prefix:
                print(f"{output_prefix}{text}")
            else:
                print(text)
        returncode = process.wait()
        stdout = "".join(captured_lines)
        result = CommandResult(
            args=list(args),
            returncode=returncode,
            stdout=stdout,
            stderr="",
        )
        if check and result.returncode != 0:
            joined = " ".join(args)
            raise RuntimeError(f"Command failed ({joined}):\n{result.combined_output}")
        return result

    completed = subprocess.run(
        list(args),
        cwd=str(cwd) if cwd else None,
        env=merged_env,
        input=input_text,
        text=True,
        capture_output=True,
        check=False,
    )
    result = CommandResult(
        args=list(args),
        returncode=completed.returncode,
        stdout=completed.stdout,
        stderr=completed.stderr,
    )
    if check and result.returncode != 0:
        joined = " ".join(args)
        raise RuntimeError(f"Command failed ({joined}):\n{result.combined_output}")
    return result

```