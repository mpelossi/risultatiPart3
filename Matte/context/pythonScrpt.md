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
├── cpu_sets.py
├── debug.py
├── experiment.yaml
├── export.py
├── gui.py
├── manifests.py
├── metrics.py
├── part3.yaml
├── provision.py
├── results.py
├── runner.py
├── runtime_stats.py
├── schedule.yaml
├── schedule_queue.yaml
├── schedule_viewer_data.py
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
│   ├── test_queue.py
│   ├── test_results.py
│   ├── test_runner.py
│   ├── test_runtime_stats.py
│   ├── test_schedule_viewer.py
│   └── test_viewer.py
├── timing.py
├── utils.py
├── viewer.py
└── viewer_data.py

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

`audit` still uses the old Part 2 CSV as a simple static checker. The schedule viewer uses
run-derived `runs/runtime_stats.json` when that file exists.

If you want to edit the schedule visually, open the planner GUI from this directory with:

```bash
python3 gui.py
```

That opens `schedule.yaml` and `../../Part2summary_times.csv` by default. If you want to
use different files, run:

```bash
python3 gui.py --policy other-schedule.yaml --times-csv /path/to/Part2summary_times.csv
```

### 5. Do a dry run first

```bash
python3 cli.py run once --config experiment.yaml --policy schedule.yaml --dry-run
```

### 6. Run one real experiment

```bash
python3 cli.py run once --config experiment.yaml --policy schedule.yaml --precache
```

`--precache` is recommended before serious benchmark runs. It warms all benchmark and
memcached images on both benchmark nodes so the measured run does not spend time pulling
containers.

After the first warm run on an unchanged cluster, repeating `run once --precache` is safe
but usually optional because the images should already be present. For `run batch
--precache`, the automation warms images once before the first repetition only.

### 7. Run three repetitions

```bash
python3 cli.py run batch --config experiment.yaml --policy schedule.yaml --runs 3 --precache
```

### 7b. Run multiple schedules as a queue

Create a queue file such as:

```yaml
queue_name: "part3-candidates"
entries:
  - policy: "schedules/schedule1.yaml"
    runs: 1
  - policy: "schedules/schedule2.yaml"
    runs: 3
  - policy: "schedules/schedule3.yaml"
    runs: 1
```

Then run:

```bash
python3 cli.py run queue --config experiment.yaml --queue schedule_queue.yaml --precache
```

Each `policy` path is resolved relative to the queue file. `runs: 1` behaves like
`run once`; larger values behave like `run batch`. With `--precache`, images are warmed
only before the first real queued run.

### 7c. Rebuild runtime stats from saved runs

Real runs refresh `runs/runtime_stats.json` automatically. If you want to backfill from
old runs, or you edited/copied run artifacts by hand, rebuild it explicitly:

```bash
python3 cli.py stats rebuild --results-root runs
```

The schedule viewer uses this file for theoretical predictions before falling back to
`../../Part2summary_times.csv`.

### 8. See the best run

```bash
python3 cli.py results best --experiment part3-handcrafted
```

Or open the run viewer:

```bash
python3 cli.py results viewer --experiment part3-handcrafted
```

The viewer serves the frontend from this automation directory, reads `runs/` by default,
uses `runs/runtime_stats.json` for schedule predictions when available, and opens your
browser automatically.

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

The `cores` field accepts any valid Linux CPU-set string that fits on the chosen node, for
example `0-4`, `5-7`, `1-5`, or `0,2,4`.

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

If you want to edit the schedule in a GUI instead of hand-editing YAML, run:

```bash
python3 gui.py
```

or, equivalently:

```bash
python3 cli.py gui --policy schedule.yaml --times-csv ../../Part2summary_times.csv
```

How to use the planner correctly:
- run it from a graphical desktop session; plain headless SSH will not open a Tk window
- `Reload` re-reads the policy file from disk
- `Save` validates the schedule first and refuses to write if there are errors
- the `Threads` spinboxes are loaded from the `threads:` values in the policy file
- the `Cores` combobox suggests contiguous presets, but you can also type any valid CPU-set
  string manually
- after saving, run `python3 cli.py show --policy schedule.yaml` if you want to confirm the
  exact node/core/thread assignments that will be used

One important detail: the GUI always saves back an explicit policy file with
`job_overrides` and `phases`. That is still supported by all the automation commands, but
the file will no longer be in the shorter `jobs:` format after you save from the GUI.

### Step 5. Dry run

Run:

```bash
python3 cli.py run once --config experiment.yaml --policy schedule.yaml --dry-run
```

This renders manifests and writes the phase plan, but does not touch the live cluster.

### Step 6. Real run

Run:

```bash
python3 cli.py run once --config experiment.yaml --policy schedule.yaml --precache
```

This:
- cleans previous managed jobs and pods
- checks client provisioning
- optionally pre-pulls all benchmark images on both benchmark nodes
- launches memcached
- starts the `mcperf` measurement
- launches the batch phases in schedule order
- stops `mcperf` when the last batch job completes
- captures `results.json`, `mcperf.txt`, and `summary.json`

`--precache` is recommended for serious timing runs. It warms the images once and then
deletes the transient warmup pods before memcached and the benchmark jobs start.

`results.json` is the raw `kubectl get pods -o json` snapshot that matches the assignment
workflow. `summary.json` is a derived convenience report built from `results.json` and
`mcperf.txt`.

Every real run also writes `node_platforms.json` and copies that data into `summary.json`.
This records the GCP `cpuPlatform` and machine type for the benchmark nodes
`node-a-8core` and `node-b-4core`, so n2d Rome/Milan placement is visible after the run.

Every real run also refreshes `runs/runtime_stats.json` from the saved run history. This
file stores per-job observed runtimes grouped by job, node, thread count, and memcached
placement. Refresh failure is logged as a warning and does not mark the benchmark run as
failed.

If you already warmed the images on this cluster, you may omit `--precache` on later
single runs. Re-running it is not harmful; it just creates short-lived image-warmup pods
and deletes them before memcached starts.

When the run stops measurement, the signal is sent only to the temporary `mcperf` wrapper on
`client-measure`. It does not target the memcached pod or the long-lived
`mcperf-agent.service` processes on `client-agent-a` / `client-agent-b`.

### Step 7. Repeated runs

Run:

```bash
python3 cli.py run batch --config experiment.yaml --policy schedule.yaml --runs 3 --precache
```

Use this when you want the three measurement files needed for submission. With `--precache`,
the warmup happens once before the first run only.

### Step 8. Pick the best run

If this checkout already contains run artifacts, or you copied run folders in manually,
first rebuild the run-derived runtime statistics:

```bash
python3 cli.py stats rebuild --results-root runs
```

The rebuild scans `runs/<experiment>/<run-id>/`, reads `summary.json`, `policy.yaml`, and
`node_platforms.json` when present, and writes `runs/runtime_stats.json`. The schedule
viewer uses median observed runtime from this file, grouped by job, node, thread count,
and memcached node. If no matching sample exists, it falls back to broader run-derived
groups and then to `../../Part2summary_times.csv`.

Run:

```bash
python3 cli.py results best --experiment part3-handcrafted
```

This sorts the runs by:
1. passing runs first
2. lowest makespan
3. lowest observed p95

By default it reads from this automation directory's own `runs/` folder, so you usually do
not need to pass `--results-root`.

### Step 9. Export the submission folder

Run:

```bash
python3 cli.py export submission --experiment part3-handcrafted --group 054 --task 3_1
```

The export step reads each selected run's local `results.json`, but writes the submission
bundle using the assignment filenames `pods_1.json`, `pods_2.json`, `pods_3.json`.

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

### `python3 cli.py gui --policy schedule.yaml --times-csv ../../Part2summary_times.csv`

Opens the Tkinter planner GUI for the current schedule. From inside this directory,
`python3 gui.py` does the same thing with the default `schedule.yaml` and
`../../Part2summary_times.csv` paths.

### `python3 cli.py run once --config experiment.yaml --policy schedule.yaml --dry-run`

Builds the manifests and phase plan without touching the cluster.

### `python3 cli.py run once --config experiment.yaml --policy schedule.yaml --precache`

Runs one full live experiment. `--precache` is recommended.

### `python3 cli.py run batch --config experiment.yaml --policy schedule.yaml --runs 3 --precache`

Runs the same experiment multiple times.

### `python3 cli.py run queue --config experiment.yaml --queue schedule_queue.yaml --precache`

Runs each schedule listed in a queue file, stopping on the first runner exception.

### `python3 cli.py stats rebuild --results-root runs`

Rebuilds `runs/runtime_stats.json` from saved run artifacts. Use it after copying old runs
into `runs/` or whenever you want to force a deterministic full refresh.

### `python3 cli.py results best --experiment part3-handcrafted`

Shows the best completed runs according to the built-in ranking.

### `python3 cli.py results viewer --experiment part3-handcrafted`

Starts a small local file server for the run viewer, using this directory's `runs/` folder
by default. It prints the URL and tries to open the browser automatically.

Useful options:
- `--no-open` keeps the server-only behavior for SSH/headless sessions
- `--host 0.0.0.0` makes the viewer reachable from another machine that can access the port
- `--port 8080` chooses a different port
- `--results-root /path/to/runs` reads a different results directory
- `--runtime-stats /path/to/runtime_stats.json` reads a different run-derived stats file
- `--times-csv /path/to/Part2summary_times.csv` changes the legacy fallback timing file

You can also run the same viewer directly:

```bash
python3 viewer.py --experiment part3-handcrafted
```

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

### `gui.py` does not open a window

The planner is a Tkinter desktop app. It needs:
- a Python build with Tkinter available
- a graphical display session

If `python3 gui.py` says Tkinter could not be imported, install the Tkinter package for
your Python distribution. If it says no graphical display is available, run it locally in
a desktop session or use X11 forwarding.

## Important Notes

- The main scheduling file is `schedule.yaml`.
- `python3 gui.py` opens `schedule.yaml` and `../../Part2summary_times.csv` by default.
- The schedule viewer uses `runs/runtime_stats.json` first for predictions, then falls
  back to `../../Part2summary_times.csv` when needed.
- `run once` does **not** create the cluster for you.
- `cluster up` is a separate step from `run once`.
- The Part 2 timing reference file is still `../../Part2summary_times.csv` from this
  folder, but it is now the compatibility fallback rather than the primary predictor.

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

from .catalog import JOB_CATALOG, NODE_A, NODE_B, validate_node_core_spec
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
class RuntimeEstimate:
    duration_s: float
    source: str
    match_type: str
    sample_count: int | None = None
    message: str | None = None


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


def estimate_runtime_detail(
    job_id: str,
    threads: int,
    runtime_source,
    *,
    node: str | None = None,
    memcached_node: str | None = None,
) -> RuntimeEstimate | None:
    estimator = getattr(runtime_source, "estimate", None)
    if callable(estimator):
        raw_estimate = estimator(
            job_id=job_id,
            node=node,
            threads=threads,
            memcached_node=memcached_node,
        )
        if raw_estimate is None:
            return None
        return RuntimeEstimate(
            duration_s=float(_estimate_value(raw_estimate, "duration_s")),
            source=str(_estimate_value(raw_estimate, "source", "")),
            match_type=str(_estimate_value(raw_estimate, "match_type", "unknown")),
            sample_count=_optional_estimate_int(raw_estimate, "sample_count"),
            message=_optional_estimate_str(raw_estimate, "message"),
        )

    duration = estimate_runtime(job_id, threads, runtime_source)
    if duration is None:
        return None
    return RuntimeEstimate(
        duration_s=duration,
        source=str(runtime_source.source_path),
        match_type="csv",
    )


def _estimate_value(raw_estimate, key: str, default=None):
    if isinstance(raw_estimate, dict):
        return raw_estimate.get(key, default)
    return getattr(raw_estimate, key, default)


def _optional_estimate_int(raw_estimate, key: str) -> int | None:
    value = _estimate_value(raw_estimate, key)
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _optional_estimate_str(raw_estimate, key: str) -> str | None:
    value = _estimate_value(raw_estimate, key)
    if value is None:
        return None
    text = str(value)
    return text if text else None


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
    errors: list[AuditIssue],
) -> tuple[int, ...] | None:
    if node not in (NODE_A, NODE_B):
        errors.append(AuditIssue(level="error", message=f"{label} uses unsupported node: {node}", jobs=(label,)))
        return None
    try:
        core_ids = validate_node_core_spec(cores, node)
    except ValueError as exc:
        errors.append(AuditIssue(level="error", message=str(exc), node=node, jobs=(label,)))
        return None
    if threads <= 0:
        errors.append(AuditIssue(level="error", message=f"{label} must use at least one thread", node=node, jobs=(label,)))
        return None
    if threads > len(core_ids):
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
        if job_id not in JOB_CATALOG:
            continue
        core_ids = _validate_core_assignment(
            label=job_id,
            node=job.node,
            cores=job.cores,
            threads=job.threads,
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
        memcached_cores = _validate_core_assignment(
            label="memcached",
            node=model.memcached.node,
            cores=model.memcached.cores,
            threads=model.memcached.threads,
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
        estimate = estimate_runtime_detail(
            job_id,
            job.threads,
            runtime_table,
            node=job.node,
            memcached_node=model.memcached.node,
        )
        if estimate is None:
            errors.append(
                AuditIssue(
                    level="error",
                    message=(
                        f"Missing runtime estimate for {job_id} on {job.node} "
                        f"with {job.threads} thread(s)"
                    ),
                    jobs=(job_id,),
                )
            )
            continue
        if estimate.match_type != "exact":
            warnings.append(
                AuditIssue(
                    level="warning",
                    node=job.node,
                    jobs=(job_id,),
                    message=estimate.message or _runtime_fallback_message(job, estimate),
                )
            )
        duration = estimate.duration_s
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


def _runtime_fallback_message(job: AuditJob, estimate: RuntimeEstimate) -> str:
    sample_text = ""
    if estimate.sample_count is not None:
        sample_text = f" from {estimate.sample_count} sample(s)"
    return (
        f"Using {estimate.match_type} runtime estimate for {job.job_id} on {job.node} "
        f"with {job.threads} thread(s){sample_text}."
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

from .cpu_sets import contiguous_core_sets, validate_core_spec


NODE_A = "node-a-8core"
NODE_B = "node-b-4core"
MEMCACHED_IMAGE = "anakli/memcached:t1"

NODE_CORE_COUNTS = {
    NODE_A: 8,
    NODE_B: 4,
}

NODE_A_CORE_PRESETS = contiguous_core_sets(NODE_CORE_COUNTS[NODE_A])
NODE_B_CORE_PRESETS = contiguous_core_sets(NODE_CORE_COUNTS[NODE_B])

NODE_CORE_PRESETS = {
    NODE_A: NODE_A_CORE_PRESETS,
    NODE_B: NODE_B_CORE_PRESETS,
}


@dataclass(frozen=True)
class JobCatalogEntry:
    job_id: str
    image: str
    suite: str
    program: str
    default_node: str
    default_cores: str
    default_threads: int
    suggested_cores_by_node: dict[str, tuple[str, ...]]
    default_cpu_request: str | None = None
    default_memory_request: str | None = None
    default_memory_limit: str | None = None


def suggested_core_sets(node: str) -> tuple[str, ...]:
    if node not in NODE_CORE_PRESETS:
        raise ValueError(f"Unsupported node: {node}")
    return NODE_CORE_PRESETS[node]


def validate_node_core_spec(core_spec: str, node: str) -> tuple[int, ...]:
    if node not in NODE_CORE_COUNTS:
        raise ValueError(f"Unsupported node: {node}")
    try:
        return validate_core_spec(core_spec, max_core_id=NODE_CORE_COUNTS[node] - 1)
    except ValueError as exc:
        raise ValueError(f"Invalid core set {core_spec} on {node}: {exc}") from exc


JOB_CATALOG: dict[str, JobCatalogEntry] = {
    "barnes": JobCatalogEntry(
        job_id="barnes",
        image="anakli/cca:splash2x_barnes",
        suite="splash2x",
        program="barnes",
        default_node=NODE_A,
        default_cores="0-7",
        default_threads=8,
        suggested_cores_by_node={NODE_A: NODE_A_CORE_PRESETS, NODE_B: NODE_B_CORE_PRESETS},
    ),
    "blackscholes": JobCatalogEntry(
        job_id="blackscholes",
        image="anakli/cca:parsec_blackscholes",
        suite="parsec",
        program="blackscholes",
        default_node=NODE_B,
        default_cores="1-3",
        default_threads=3,
        suggested_cores_by_node={NODE_A: NODE_A_CORE_PRESETS, NODE_B: NODE_B_CORE_PRESETS},
    ),
    "canneal": JobCatalogEntry(
        job_id="canneal",
        image="anakli/cca:parsec_canneal",
        suite="parsec",
        program="canneal",
        default_node=NODE_A,
        default_cores="0-7",
        default_threads=8,
        suggested_cores_by_node={NODE_A: NODE_A_CORE_PRESETS, NODE_B: NODE_B_CORE_PRESETS},
    ),
    "freqmine": JobCatalogEntry(
        job_id="freqmine",
        image="anakli/cca:parsec_freqmine",
        suite="parsec",
        program="freqmine",
        default_node=NODE_B,
        default_cores="1-3",
        default_threads=3,
        suggested_cores_by_node={NODE_A: NODE_A_CORE_PRESETS, NODE_B: NODE_B_CORE_PRESETS},
    ),
    "radix": JobCatalogEntry(
        job_id="radix",
        image="anakli/cca:splash2x_radix",
        suite="splash2x",
        program="radix",
        default_node=NODE_A,
        default_cores="0-7",
        default_threads=8,
        suggested_cores_by_node={NODE_A: NODE_A_CORE_PRESETS, NODE_B: NODE_B_CORE_PRESETS},
    ),
    "streamcluster": JobCatalogEntry(
        job_id="streamcluster",
        image="anakli/cca:parsec_streamcluster",
        suite="parsec",
        program="streamcluster",
        default_node=NODE_A,
        default_cores="0-7",
        default_threads=8,
        suggested_cores_by_node={NODE_A: NODE_A_CORE_PRESETS, NODE_B: NODE_B_CORE_PRESETS},
    ),
    "vips": JobCatalogEntry(
        job_id="vips",
        image="anakli/cca:parsec_vips",
        suite="parsec",
        program="vips",
        default_node=NODE_A,
        default_cores="0-7",
        default_threads=8,
        suggested_cores_by_node={NODE_A: NODE_A_CORE_PRESETS, NODE_B: NODE_B_CORE_PRESETS},
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
from .config import load_experiment_config, load_policy_config, load_run_queue_config
from .debug import format_debug_command_hint, render_debug_commands, summarize_provisioning_hints
from .export import export_submission
from .gui import launch_planner_gui
from .provision import (
    check_client_provisioning,
    render_provision_check_note,
    render_provision_expectations,
)
from .results import load_run_summaries, sort_best_runs
from .runner import ExperimentRunner, run_policy_queue
from .manifests import resolve_jobs
from .viewer import (
    DEFAULT_RESULTS_ROOT,
    DEFAULT_RUNTIME_STATS_PATH,
    DEFAULT_SCHEDULE_QUEUE_PATH,
    DEFAULT_SCHEDULES_DIR,
    DEFAULT_TIMES_CSV_PATH,
    launch_run_viewer,
)
from .runtime_stats import rebuild_runtime_stats_file


def _default_results_root() -> Path:
    return DEFAULT_RESULTS_ROOT


def _default_schedules_dir() -> Path:
    return DEFAULT_SCHEDULES_DIR


def _default_schedule_queue_path() -> Path:
    return DEFAULT_SCHEDULE_QUEUE_PATH


def _default_times_csv_path() -> Path:
    return DEFAULT_TIMES_CSV_PATH


def _default_runtime_stats_path() -> Path:
    return DEFAULT_RUNTIME_STATS_PATH


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
    run_once.add_argument("--precache", action="store_true")
    run_batch = run_sub.add_parser("batch")
    run_batch.add_argument("--config", required=True)
    run_batch.add_argument("--policy", required=True)
    run_batch.add_argument("--runs", type=int, default=3)
    run_batch.add_argument("--dry-run", action="store_true")
    run_batch.add_argument("--precache", action="store_true")
    run_queue = run_sub.add_parser("queue")
    run_queue.add_argument("--config", required=True)
    run_queue.add_argument("--queue", required=True)
    run_queue.add_argument("--dry-run", action="store_true")
    run_queue.add_argument("--precache", action="store_true")

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
    results_best.add_argument("--results-root", default=str(_default_results_root()))
    results_viewer = results_sub.add_parser("viewer")
    results_viewer.add_argument("--experiment")
    results_viewer.add_argument("--results-root", default=str(_default_results_root()))
    results_viewer.add_argument("--schedules-dir", default=str(_default_schedules_dir()))
    results_viewer.add_argument("--schedule-queue", default=str(_default_schedule_queue_path()))
    results_viewer.add_argument("--times-csv", default=str(_default_times_csv_path()))
    results_viewer.add_argument("--runtime-stats", default=str(_default_runtime_stats_path()))
    results_viewer.add_argument("--host", default="127.0.0.1")
    results_viewer.add_argument("--port", type=int, default=8000)
    results_viewer.add_argument("--no-open", action="store_true")

    stats_parser = subparsers.add_parser("stats")
    stats_sub = stats_parser.add_subparsers(dest="stats_command", required=True)
    stats_rebuild = stats_sub.add_parser("rebuild")
    stats_rebuild.add_argument("--results-root", default=str(_default_results_root()))
    stats_rebuild.add_argument("--output")

    export_parser = subparsers.add_parser("export")
    export_sub = export_parser.add_subparsers(dest="export_command", required=True)
    export_submission_parser = export_sub.add_parser("submission")
    export_submission_parser.add_argument("--experiment", required=True)
    export_submission_parser.add_argument("--group", required=True)
    export_submission_parser.add_argument("--task", required=True)
    export_submission_parser.add_argument("--results-root", default=str(_default_results_root()))
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
        if args.dry_run and args.precache:
            parser.error("--precache cannot be combined with --dry-run")
        experiment = load_experiment_config(args.config)
        if args.run_command == "queue":
            queue = load_run_queue_config(args.queue)
            run_dirs = run_policy_queue(experiment, queue, dry_run=args.dry_run, precache=args.precache)
            for run_dir in run_dirs:
                print(run_dir)
            return 0
        policy = load_policy_config(args.policy)
        runner = ExperimentRunner(experiment, policy)
        if args.run_command == "once":
            run_dir = runner.run_once(dry_run=args.dry_run, precache=args.precache)
            print(run_dir)
        else:
            run_dirs = runner.run_batch(args.runs, dry_run=args.dry_run, precache=args.precache)
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
            run_id = summary.get("run_id")
            run_label = summary.get("run_label")
            run_display = str(run_id)
            if run_label and run_label != run_id:
                run_display = f"{run_id} ({run_label})"
            print(
                run_display,
                summary.get("policy_name"),
                summary.get("overall_status"),
                f"makespan={summary.get('makespan_s')}",
                f"max_p95_us={summary.get('max_observed_p95_us')}",
                summary.get("run_dir"),
            )
        return 0

    if args.command == "results" and args.results_command == "viewer":
        return launch_run_viewer(
            results_root=Path(args.results_root).resolve(),
            schedules_dir=Path(args.schedules_dir).resolve(),
            schedule_queue_path=Path(args.schedule_queue).resolve() if args.schedule_queue else None,
            times_csv_path=Path(args.times_csv).resolve(),
            runtime_stats_path=Path(args.runtime_stats).resolve() if args.runtime_stats else None,
            experiment_id=args.experiment,
            host=args.host,
            port=args.port,
            open_browser=not args.no_open,
        )

    if args.command == "stats" and args.stats_command == "rebuild":
        payload = rebuild_runtime_stats_file(
            Path(args.results_root).resolve(),
            output_path=Path(args.output).resolve() if args.output else None,
        )
        print(
            "Runtime stats rebuilt:",
            payload.get("output_path"),
            f"samples={payload.get('sample_count')}",
            f"eligible_runs={payload.get('eligible_run_count')}",
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
BENCHMARK_NODETYPES = ("node-a-8core", "node-b-4core")


class ClusterController:
    kubectl_read_retry_attempts = 4
    kubectl_read_retry_delay_s = 3.0

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
        command_text = "kubectl " + " ".join(args)
        last_output = ""
        for attempt in range(1, self.kubectl_read_retry_attempts + 1):
            result = self.kubectl(*args, check=False)
            if result.returncode == 0:
                return json.loads(result.stdout)
            last_output = result.combined_output
            if attempt >= self.kubectl_read_retry_attempts or not self._is_transient_kubectl_read_error(last_output):
                break
            self._announce(
                f"Transient Kubernetes API read failure for `{command_text}`; "
                f"retrying in {self.kubectl_read_retry_delay_s:.1f}s "
                f"(attempt {attempt}/{self.kubectl_read_retry_attempts})"
            )
            time.sleep(self.kubectl_read_retry_delay_s)
        raise RuntimeError(
            "Kubernetes API connectivity was lost during a read command "
            f"({command_text}):\n{last_output}"
        )

    def _is_transient_kubectl_read_error(self, output: str) -> bool:
        lowered = output.lower()
        stripped = lowered.strip()
        if stripped == "eof" or stripped.endswith(": eof"):
            return True
        return any(
            snippet in lowered
            for snippet in (
                "unable to connect to the server",
                "network is unreachable",
                "no route to host",
                "i/o timeout",
                "tls handshake timeout",
            )
        )

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

    def instance_describe_args(self, node_name: str) -> list[str]:
        return [
            "gcloud",
            "compute",
            "instances",
            "describe",
            node_name,
            "--zone",
            self.config.zone,
            "--format=json(name,machineType,cpuPlatform,zone,status)",
        ]

    def _short_resource_name(self, value: object) -> str | None:
        if not isinstance(value, str) or not value:
            return None
        return value.rsplit("/", 1)[-1]

    def describe_instance_json(self, node_name: str) -> dict[str, object]:
        result = run_command(self.instance_describe_args(node_name), check=False)
        if result.returncode != 0:
            suffix = f": {result.combined_output}" if result.combined_output else ""
            raise RuntimeError(f"gcloud compute instances describe failed for {node_name}{suffix}")
        try:
            loaded = json.loads(result.stdout)
        except json.JSONDecodeError as exc:
            raise RuntimeError(f"gcloud returned invalid JSON for {node_name}: {exc}") from exc
        if not isinstance(loaded, dict):
            raise RuntimeError(f"gcloud returned a non-object payload for {node_name}")
        return loaded

    def capture_benchmark_node_platforms(
        self,
        *,
        nodes: dict[str, NodeInfo] | None = None,
    ) -> dict[str, object]:
        discovered_nodes = self.discover_nodes() if nodes is None else nodes
        platforms_by_node: dict[str, dict[str, object]] = {}
        errors: list[str] = []
        success_count = 0

        for nodetype in BENCHMARK_NODETYPES:
            node = discovered_nodes.get(nodetype)
            node_name = node.name if node is not None else None
            if not isinstance(node_name, str) or not node_name:
                message = f"Missing Kubernetes node for {nodetype}"
                platforms_by_node[nodetype] = {
                    "capture_status": "error",
                    "node_type": nodetype,
                    "node_name": node_name,
                    "error": message,
                }
                errors.append(message)
                continue
            try:
                payload = self.describe_instance_json(node_name)
            except Exception as exc:
                message = str(exc)
                platforms_by_node[nodetype] = {
                    "capture_status": "error",
                    "node_type": nodetype,
                    "node_name": node_name,
                    "error": message,
                }
                errors.append(f"{nodetype}: {message}")
                continue

            success_count += 1
            machine_type_uri = payload.get("machineType")
            zone_uri = payload.get("zone")
            platforms_by_node[nodetype] = {
                "capture_status": "ok",
                "node_type": nodetype,
                "node_name": node_name,
                "gcp_name": payload.get("name"),
                "machine_type": self._short_resource_name(machine_type_uri),
                "machine_type_uri": machine_type_uri,
                "cpu_platform": payload.get("cpuPlatform"),
                "zone": self._short_resource_name(zone_uri) or self.config.zone,
                "zone_uri": zone_uri,
                "gcp_status": payload.get("status"),
            }

        if errors and success_count:
            capture_status = "partial"
        elif errors:
            capture_status = "error"
        else:
            capture_status = "ok"
        return {
            "capture_status": capture_status,
            "zone": self.config.zone,
            "nodes": platforms_by_node,
            "errors": errors,
        }

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

    def delete_manifest(self, manifest_path: Path) -> None:
        self.kubectl("delete", "-f", str(manifest_path), "--ignore-not-found=true", check=False)

    def get_pods_payload_by_selector(self, selector: str) -> dict[str, object]:
        return self.kubectl_json("get", "pods", "-l", selector, "-o", "json")

    def _pod_failure_message(self, item: dict[str, object]) -> str | None:
        metadata = item.get("metadata", {})
        status = item.get("status", {})
        pod_name = metadata.get("name", "<unknown>")
        phase = status.get("phase")
        container_statuses = status.get("containerStatuses") or []
        for container_status in container_statuses:
            container_name = container_status.get("name", "<unknown>")
            state = container_status.get("state", {})
            waiting = state.get("waiting", {})
            reason = waiting.get("reason")
            message = waiting.get("message", "")
            if reason in {"ErrImagePull", "ImagePullBackOff"}:
                suffix = f": {message}" if isinstance(message, str) and message else ""
                return f"Image pull failed for pod/{pod_name} container {container_name} ({reason}){suffix}"
            terminated = state.get("terminated", {})
            exit_code = terminated.get("exitCode")
            if isinstance(exit_code, int) and exit_code != 0:
                reason_text = terminated.get("reason")
                message_text = terminated.get("message")
                details = []
                if isinstance(reason_text, str) and reason_text:
                    details.append(reason_text)
                if isinstance(message_text, str) and message_text:
                    details.append(message_text)
                suffix = f" ({'; '.join(details)})" if details else ""
                return f"Precache pod/{pod_name} container {container_name} exited with code {exit_code}{suffix}"
        if phase == "Failed":
            reason = status.get("reason")
            message = status.get("message")
            details = [detail for detail in (reason, message) if isinstance(detail, str) and detail]
            suffix = f" ({'; '.join(details)})" if details else ""
            return f"Precache pod/{pod_name} failed{suffix}"
        return None

    def wait_for_pods_completion(
        self,
        selector: str,
        *,
        expected_names: set[str],
        timeout_s: int = 600,
    ) -> None:
        deadline = time.time() + timeout_s
        while time.time() < deadline:
            payload = self.get_pods_payload_by_selector(selector)
            pods_by_name: dict[str, dict[str, object]] = {}
            for item in payload.get("items", []):
                metadata = item.get("metadata", {})
                pod_name = metadata.get("name")
                if isinstance(pod_name, str) and pod_name:
                    pods_by_name[pod_name] = item
            for item in pods_by_name.values():
                failure = self._pod_failure_message(item)
                if failure is not None:
                    raise RuntimeError(failure)
            missing = sorted(expected_names - set(pods_by_name))
            if not missing and all(
                item.get("status", {}).get("phase") == "Succeeded" for item in pods_by_name.values()
            ):
                return
            time.sleep(2)
        raise TimeoutError(
            "Timed out waiting for pods to complete: "
            + ", ".join(sorted(expected_names))
        )

    def wait_for_pods_deleted(self, selector: str, *, timeout_s: int = 120) -> None:
        deadline = time.time() + timeout_s
        while time.time() < deadline:
            payload = self.get_pods_payload_by_selector(selector)
            if not payload.get("items"):
                return
            time.sleep(2)
        raise TimeoutError(f"Timed out waiting for pods with selector {selector} to disappear")

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

    def get_run_pods_payload(self, run_id: str) -> dict[str, object]:
        return self.kubectl_json(
            "get",
            "pods",
            "-l",
            f"cca-project-run-id={run_id}",
            "-o",
            "json",
        )

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
        payload = self.kubectl_json("get", "pods", "-o", "json")
        destination.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")

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
from .utils import resolve_existing_run_results_path, run_results_path, write_json


def collect_live_pods(cluster: ClusterController, run_dir: Path) -> Path:
    results_path = run_results_path(run_dir)
    cluster.capture_pods_json(results_path)
    return results_path


def summarize_run(
    run_dir: Path,
    *,
    experiment_id: str,
    policy_name: str,
    run_id: str,
    expected_jobs: set[str],
    node_platforms: dict[str, object] | None = None,
) -> dict[str, object]:
    pods_path = resolve_existing_run_results_path(run_dir)
    mcperf_path = run_dir / "mcperf.txt"
    summary = build_summary(
        pods_path,
        mcperf_path if mcperf_path.exists() else None,
        expected_jobs,
        run_id=run_id,
        experiment_id=experiment_id,
        policy_name=policy_name,
        node_platforms=node_platforms,
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

from .catalog import JOB_CATALOG, NODE_A, NODE_B, validate_node_core_spec
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


@dataclass(frozen=True)
class QueueEntry:
    policy_path: Path
    runs: int


@dataclass(frozen=True)
class RunQueueConfig:
    config_path: Path
    queue_name: str
    entries: tuple[QueueEntry, ...]


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
    node = override.node if override.node is not None else catalog_entry.default_node
    if node not in (NODE_A, NODE_B):
        raise ValueError(f"{job_id} uses unsupported node: {node}")
    cores = override.cores if override.cores is not None else catalog_entry.default_cores
    core_ids = validate_node_core_spec(cores, node)
    threads = override.threads if override.threads is not None else catalog_entry.default_threads
    if threads <= 0:
        raise ValueError(f"{job_id} must use at least one thread")
    if threads > len(core_ids):
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
    memcached_core_ids = validate_node_core_spec(memcached.cores, memcached.node)
    if memcached.threads > len(memcached_core_ids):
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


def load_run_queue_config(path_str: str) -> RunQueueConfig:
    path = expand_path(path_str)
    raw = _load_structured_file(path)
    entries_raw = _require_list(raw.get("entries"), "entries")
    if not entries_raw:
        raise ValueError("entries must contain at least one queue entry")

    base_dir = path.parent
    entries: list[QueueEntry] = []
    for index, entry_raw in enumerate(entries_raw):
        entry_map = _require_mapping(entry_raw, f"entries[{index}]")
        policy_path = expand_path(
            _require_str(entry_map.get("policy"), f"entries[{index}].policy"),
            base_dir,
        )
        if not policy_path.exists():
            raise FileNotFoundError(f"Queue policy does not exist: {policy_path}")
        runs = _require_int(entry_map.get("runs", 1), f"entries[{index}].runs")
        if runs < 1:
            raise ValueError(f"entries[{index}].runs must be at least 1")
        entries.append(QueueEntry(policy_path=policy_path, runs=runs))

    return RunQueueConfig(
        config_path=path,
        queue_name=_require_str(raw.get("queue_name", path.stem), "queue_name"),
        entries=tuple(entries),
    )

```

`cpu_sets.py`:

```py
from __future__ import annotations


def _parse_core_token(token: str) -> tuple[int, ...]:
    if not token:
        raise ValueError("empty token")
    if "-" in token:
        if token.count("-") != 1:
            raise ValueError(f"invalid token `{token}`")
        start_text, end_text = token.split("-", 1)
        if not start_text.isdigit() or not end_text.isdigit():
            raise ValueError(f"invalid token `{token}`")
        start = int(start_text)
        end = int(end_text)
        if end < start:
            raise ValueError(f"invalid range `{token}`: end before start")
        return tuple(range(start, end + 1))
    if not token.isdigit():
        raise ValueError(f"invalid token `{token}`")
    return (int(token),)


def parse_core_spec(core_spec: str) -> tuple[int, ...]:
    if not isinstance(core_spec, str) or not core_spec.strip():
        raise ValueError("empty core specification")
    core_ids: list[int] = []
    seen: set[int] = set()
    for raw_token in core_spec.split(","):
        token = raw_token.strip()
        if not token:
            raise ValueError("empty token")
        try:
            expanded = _parse_core_token(token)
        except ValueError as exc:
            raise ValueError(str(exc)) from exc
        for core_id in expanded:
            if core_id in seen:
                raise ValueError(f"duplicate or overlapping core {core_id}")
            seen.add(core_id)
            core_ids.append(core_id)
    if not core_ids:
        raise ValueError("empty core specification")
    return tuple(sorted(core_ids))


def validate_core_spec(core_spec: str, *, max_core_id: int) -> tuple[int, ...]:
    core_ids = parse_core_spec(core_spec)
    for core_id in core_ids:
        if core_id < 0 or core_id > max_core_id:
            raise ValueError(f"core {core_id} is out of range 0-{max_core_id}")
    return core_ids


def count_cores(core_spec: str) -> int:
    return len(parse_core_spec(core_spec))


def contiguous_core_sets(core_count: int) -> tuple[str, ...]:
    if core_count <= 0:
        raise ValueError(f"core_count must be positive, got {core_count}")
    presets: list[str] = []
    for length in range(core_count, 0, -1):
        for start in range(0, core_count - length + 1):
            end = start + length - 1
            presets.append(str(start) if start == end else f"{start}-{end}")
    return tuple(presets)

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

from .results import resolve_experiment_root
from .utils import resolve_existing_run_results_path


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
    experiment_root = resolve_experiment_root(results_root, experiment_id)
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
        shutil.copyfile(resolve_existing_run_results_path(run_dir), target_dir / f"pods_{index}.json")
        shutil.copyfile(run_dir / "mcperf.txt", target_dir / f"mcperf_{index}.txt")
    return target_dir

```

`gui.py`:

```py
from __future__ import annotations

import argparse
from dataclasses import dataclass
from pathlib import Path
import sys
from typing import Any

if __package__ in {None, ""}:
    package_dir = Path(__file__).resolve().parent
    package_parent = package_dir.parent
    if str(package_parent) not in sys.path:
        sys.path.insert(0, str(package_parent))
    __package__ = package_dir.name

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
from .catalog import JOB_CATALOG, NODE_A, NODE_B, suggested_core_sets, validate_node_core_spec


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

DEFAULT_POLICY_PATH = Path(__file__).resolve().with_name("schedule.yaml")
DEFAULT_TIMES_CSV_PATH = Path(__file__).resolve().parents[2] / "Part2summary_times.csv"


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
        values = suggested_core_sets(node)
        self.memcached_cores_box.configure(values=values)
        current_value = self.memcached_cores_var.get().strip()
        try:
            is_valid = bool(current_value) and bool(validate_node_core_spec(current_value, node))
        except ValueError:
            is_valid = False
        if not is_valid:
            self.memcached_cores_var.set(values[0])

    def _update_job_core_values(self, job_id: str) -> None:
        widgets = self.job_rows[job_id]
        node = widgets.node_var.get() or JOB_CATALOG[job_id].default_node
        values = JOB_CATALOG[job_id].suggested_cores_by_node[node]
        widgets.cores_box.configure(values=values)
        current_value = widgets.cores_var.get().strip()
        try:
            is_valid = bool(current_value) and bool(validate_node_core_spec(current_value, node))
        except ValueError:
            is_valid = False
        if not is_valid:
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


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Launch the Part 3 schedule planner GUI")
    parser.add_argument(
        "--policy",
        default=str(DEFAULT_POLICY_PATH),
        help="Path to the schedule or policy file (default: %(default)s)",
    )
    parser.add_argument(
        "--times-csv",
        default=str(DEFAULT_TIMES_CSV_PATH),
        help="Path to the Part 2 runtime CSV (default: %(default)s)",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)
    try:
        launch_planner_gui(policy_path_str=args.policy, times_csv_path_str=args.times_csv)
    except RuntimeError as exc:
        parser.exit(1, f"{exc}\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

```

`manifests.py`:

```py
from __future__ import annotations

from dataclasses import dataclass

from .catalog import JOB_CATALOG, MEMCACHED_IMAGE, NODE_A, NODE_B, JobCatalogEntry
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


@dataclass(frozen=True)
class ResolvedPrecachePod:
    kubernetes_name: str
    node: str
    images: tuple[str, ...]


def _manifest_name(prefix: str, slug: str, run_id: str) -> str:
    base = f"{prefix}-{slug}-{run_id.lower()}"
    return base[:63].rstrip("-")


def _resolve_job(catalog_entry: JobCatalogEntry, override: JobOverride | None, run_id: str) -> ResolvedBatchJob:
    node = override.node if override and override.node is not None else catalog_entry.default_node
    cores = override.cores if override and override.cores is not None else catalog_entry.default_cores
    threads = override.threads if override and override.threads is not None else catalog_entry.default_threads
    cpu_request = override.cpu_request if override and override.cpu_request is not None else catalog_entry.default_cpu_request
    memory_request = (
        override.memory_request if override and override.memory_request is not None else catalog_entry.default_memory_request
    )
    memory_limit = override.memory_limit if override and override.memory_limit is not None else catalog_entry.default_memory_limit
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


def resolve_precache_pods(run_id: str) -> tuple[ResolvedPrecachePod, ...]:
    images = tuple(sorted({entry.image for entry in JOB_CATALOG.values()} | {MEMCACHED_IMAGE}))
    return (
        ResolvedPrecachePod(
            kubernetes_name=_manifest_name("precache", NODE_A, run_id),
            node=NODE_A,
            images=images,
        ),
        ResolvedPrecachePod(
            kubernetes_name=_manifest_name("precache", NODE_B, run_id),
            node=NODE_B,
            images=images,
        ),
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
  - image: {MEMCACHED_IMAGE}
    name: memcached
    imagePullPolicy: Always
    command: ["/bin/sh"]
    args: ["-c", "taskset -c {memcached.cores} ./memcached -t {memcached.threads} -u memcache"]
  nodeSelector:
    cca-project-nodetype: "{memcached.node}"
"""


def render_precache_pod_manifest(
    pod: ResolvedPrecachePod,
    *,
    experiment_id: str,
    run_id: str,
) -> str:
    container_lines: list[str] = []
    for index, image in enumerate(pod.images, start=1):
        container_lines.extend(
            (
                f"  - image: {image}",
                f"    name: image-{index:02d}",
                "    imagePullPolicy: IfNotPresent",
                '    command: ["/bin/sh"]',
                '    args: ["-c", "true"]',
            )
        )
    containers_block = "\n".join(container_lines)
    return f"""apiVersion: v1
kind: Pod
metadata:
  name: {pod.kubernetes_name}
  labels:
    cca-project-managed: "true"
    cca-project-experiment: "{experiment_id}"
    cca-project-role: "precache"
    cca-project-precache-run: "{run_id}"
spec:
  restartPolicy: Never
  containers:
{containers_block}
  nodeSelector:
    cca-project-nodetype: "{pod.node}"
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

from .metrics import parse_mcperf_output
from .utils import format_run_id_label


def resolve_experiment_root(results_root: Path, experiment_id: str) -> Path:
    experiment_root = results_root / experiment_id
    if not experiment_root.exists():
        raise FileNotFoundError(
            f"Experiment directory not found: {experiment_root}. "
            "Pass --results-root if your runs directory lives somewhere else."
        )
    return experiment_root


def load_run_summaries(results_root: Path, experiment_id: str) -> list[dict[str, object]]:
    experiment_root = resolve_experiment_root(results_root, experiment_id)
    summaries: list[dict[str, object]] = []
    for run_dir in sorted(path for path in experiment_root.iterdir() if path.is_dir()):
        summary_path = run_dir / "summary.json"
        if not summary_path.exists():
            continue
        summary = json.loads(summary_path.read_text(encoding="utf-8"))
        _refresh_measurement_summary(summary, run_dir / "mcperf.txt")
        summary["run_dir"] = str(run_dir)
        summary["run_label"] = format_run_id_label(str(summary.get("run_id", run_dir.name)))
        summaries.append(summary)
    return summaries


def _refresh_measurement_summary(summary: dict[str, object], mcperf_path: Path) -> None:
    try:
        mcperf_summary = parse_mcperf_output(mcperf_path if mcperf_path.exists() else None)
    except ValueError:
        mcperf_summary = {
            "samples": [],
            "max_p95_us": None,
            "slo_violations": None,
            "measurement_status": "parse_error",
        }
    if mcperf_summary["measurement_status"] == "ok":
        summary["max_observed_p95_us"] = mcperf_summary["max_p95_us"]
        summary["slo_violations"] = mcperf_summary["slo_violations"]
        summary["sample_count"] = len(mcperf_summary["samples"])
        return
    summary["measurement_status"] = mcperf_summary["measurement_status"]
    summary["max_observed_p95_us"] = mcperf_summary["max_p95_us"]
    summary["slo_violations"] = mcperf_summary["slo_violations"]
    summary["sample_count"] = len(mcperf_summary["samples"])
    if summary.get("overall_status") == "pass":
        summary["overall_status"] = "infra_fail"


def sort_best_runs(summaries: list[dict[str, object]]) -> list[dict[str, object]]:
    def is_valid_best_candidate(summary: dict[str, object]) -> bool:
        sample_count = summary.get("sample_count")
        try:
            parsed_sample_count = int(sample_count or 0)
        except (TypeError, ValueError):
            parsed_sample_count = 0
        return (
            summary.get("overall_status") == "pass"
            and summary.get("measurement_status") == "ok"
            and summary.get("makespan_s") is not None
            and summary.get("max_observed_p95_us") is not None
            and summary.get("timing_complete") is not False
            and parsed_sample_count > 0
        )

    def sort_key(summary: dict[str, object]) -> tuple[int, float, float]:
        is_pass = 0 if is_valid_best_candidate(summary) else 1
        makespan = float(summary.get("makespan_s") or 1e18)
        p95 = float(summary.get("max_observed_p95_us") or 1e18)
        return (is_pass, makespan, p95)

    return sorted(summaries, key=sort_key)

```

`runner.py`:

```py
from __future__ import annotations

import shlex
import shutil
import subprocess
import threading
import time
import json
from dataclasses import dataclass, field
from pathlib import Path

from .catalog import JOB_CATALOG
from .cluster import BENCHMARK_NODETYPES, ClusterController
from .collect import collect_describes, collect_live_pods, summarize_run
from .config import ExperimentConfig, Phase, PolicyConfig, RunQueueConfig, load_policy_config
from .debug import format_debug_command_hint, summarize_provisioning_hints
from .manifests import (
    ResolvedBatchJob,
    ResolvedPrecachePod,
    render_batch_job_manifest,
    render_memcached_manifest,
    render_precache_pod_manifest,
    resolve_jobs,
    resolve_memcached,
    resolve_precache_pods,
)
from .provision import ProvisioningError, assert_client_provisioning
from .runtime_stats import rebuild_runtime_stats_file
from .utils import append_log, ensure_directory, run_id_timestamp, write_json


@dataclass
class MeasurementHandle:
    process: subprocess.Popen[str]
    reader_thread: threading.Thread
    ready_event: threading.Event
    sample_event: threading.Event
    error_event: threading.Event
    node_name: str
    remote_pid_file: str
    error_messages: list[str] = field(default_factory=list)
    stop_requested: bool = False


class ExperimentRunner:
    poll_interval_s = 1.0
    scheduler_status_interval_s = 15.0
    final_pod_metadata_wait_s = 30.0
    measurement_stop_int_grace_s = 10.0
    measurement_stop_term_grace_s = 5.0
    mcperf_agent_start_timeout_s = 20.0
    precache_completion_timeout_s = 900
    precache_cleanup_timeout_s = 120

    def __init__(self, experiment: ExperimentConfig, policy: PolicyConfig):
        self.experiment = experiment
        self.policy = policy
        self.cluster = ClusterController(experiment)

    def _create_run_dir(self) -> tuple[str, Path, Path]:
        experiment_root = ensure_directory(self.experiment.results_root / self.experiment.experiment_id)
        base_run_id = run_id_timestamp()
        run_id = base_run_id
        suffix = 2
        run_dir = experiment_root / run_id
        while run_dir.exists():
            run_id = f"{base_run_id}-{suffix:02d}"
            run_dir = experiment_root / run_id
            suffix += 1
        run_dir = ensure_directory(run_dir)
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

    def _render_precache_manifests(
        self,
        *,
        run_id: str,
        manifests_dir: Path,
    ) -> tuple[tuple[Path, ...], tuple[ResolvedPrecachePod, ...]]:
        precache_pods = resolve_precache_pods(run_id)
        manifest_paths: list[Path] = []
        for pod in precache_pods:
            manifest_path = manifests_dir / f"{pod.kubernetes_name}.yaml"
            manifest_path.write_text(
                render_precache_pod_manifest(
                    pod,
                    experiment_id=self.experiment.experiment_id,
                    run_id=run_id,
                ),
                encoding="utf-8",
            )
            manifest_paths.append(manifest_path)
        return tuple(manifest_paths), precache_pods

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

    def _capture_node_platforms(
        self,
        *,
        run_dir: Path,
        log_path: Path,
        nodes: dict[str, object],
    ) -> dict[str, object]:
        self._log(log_path, "Capturing benchmark node CPU platforms")
        try:
            node_platforms = self.cluster.capture_benchmark_node_platforms(nodes=nodes)
        except Exception as exc:
            node_platforms = {
                "capture_status": "error",
                "zone": self.experiment.zone,
                "nodes": {},
                "errors": [str(exc)],
            }
            self._log(log_path, f"Warning: failed to capture benchmark node CPU platforms: {exc}")
        else:
            status = node_platforms.get("capture_status")
            if status == "ok":
                platforms_by_node = node_platforms.get("nodes", {})
                details: list[str] = []
                if isinstance(platforms_by_node, dict):
                    for nodetype in BENCHMARK_NODETYPES:
                        raw_info = platforms_by_node.get(nodetype)
                        if not isinstance(raw_info, dict):
                            continue
                        machine_type = raw_info.get("machine_type") or "machine n/a"
                        cpu_platform = raw_info.get("cpu_platform") or "CPU platform n/a"
                        details.append(f"{nodetype}={machine_type}/{cpu_platform}")
                suffix = ": " + ", ".join(details) if details else ""
                self._log(log_path, f"Benchmark node CPU platform capture complete{suffix}")
            else:
                errors = node_platforms.get("errors", [])
                if isinstance(errors, list) and errors:
                    error_text = "; ".join(str(error) for error in errors)
                else:
                    error_text = f"status={status}"
                self._log(log_path, f"Warning: benchmark node CPU platform capture incomplete: {error_text}")
        write_json(run_dir / "node_platforms.json", node_platforms)
        return node_platforms

    def _refresh_runtime_stats(self, *, log_path: Path) -> None:
        try:
            payload = rebuild_runtime_stats_file(self.experiment.results_root)
        except Exception as exc:
            self._log(log_path, f"Warning: failed to refresh runtime stats: {exc}")
            return
        self._log(
            log_path,
            "Runtime stats refreshed: "
            f"{payload.get('output_path')} "
            f"samples={payload.get('sample_count')} "
            f"eligible_runs={payload.get('eligible_run_count')}",
        )

    def _bash_lc(self, script: str) -> str:
        return f"bash -lc {shlex.quote(script)}"

    def _measurement_pid_file(self, run_id: str) -> str:
        return f"/tmp/cca-mcperf-{run_id}.pid"

    def _precache_selector(self, run_id: str) -> str:
        return f"cca-project-role=precache,cca-project-precache-run={run_id}"

    def _cleanup_precache_manifests(
        self,
        *,
        manifest_paths: tuple[Path, ...],
        selector: str,
        log_path: Path,
    ) -> None:
        for manifest_path in manifest_paths:
            self.cluster.delete_manifest(manifest_path)
        self.cluster.wait_for_pods_deleted(selector, timeout_s=self.precache_cleanup_timeout_s)
        self._log(log_path, "Pre-cache pods deleted")

    def _precache_images(
        self,
        *,
        run_id: str,
        manifests_dir: Path,
        log_path: Path,
    ) -> None:
        manifest_paths, precache_pods = self._render_precache_manifests(run_id=run_id, manifests_dir=manifests_dir)
        selector = self._precache_selector(run_id)
        expected_names = {pod.kubernetes_name for pod in precache_pods}
        image_count = len(precache_pods[0].images) if precache_pods else 0
        self._log(
            log_path,
            f"Pre-caching {image_count} images on benchmark nodes via {len(precache_pods)} transient pod(s)",
        )
        primary_error: Exception | None = None
        try:
            for manifest_path in manifest_paths:
                self.cluster.apply_manifest(manifest_path)
            self.cluster.wait_for_pods_completion(
                selector,
                expected_names=expected_names,
                timeout_s=self.precache_completion_timeout_s,
            )
            self._log(log_path, "Pre-cache pods completed successfully")
        except Exception as exc:
            primary_error = exc
            raise
        finally:
            try:
                self._cleanup_precache_manifests(
                    manifest_paths=manifest_paths,
                    selector=selector,
                    log_path=log_path,
                )
            except Exception as exc:
                self._log(log_path, f"Warning: failed to clean up pre-cache pods: {exc}")
                if primary_error is None:
                    raise

    def _send_measurement_signal(self, handle: MeasurementHandle, signal_name: str) -> None:
        if handle.process.poll() is not None:
            return
        script = "\n".join(
            [
                "set -euo pipefail",
                f"pid_file={shlex.quote(handle.remote_pid_file)}",
                'if [ ! -s "$pid_file" ]; then',
                "  exit 0",
                "fi",
                'pid="$(cat "$pid_file")"',
                'if kill -0 "$pid" >/dev/null 2>&1; then',
                f'  kill -{signal_name} "$pid"',
                "fi",
            ]
        )
        result = self.cluster.ssh(handle.node_name, self._bash_lc(script), check=False)
        if result.returncode != 0:
            raise RuntimeError(
                f"Failed to send SIG{signal_name} to the mcperf measurement wrapper: {result.combined_output}"
            )

    def _abort_measurement_start(self, handle: MeasurementHandle) -> None:
        if handle.process.poll() is not None:
            return
        handle.stop_requested = True
        try:
            self._send_measurement_signal(handle, "TERM")
        except RuntimeError:
            handle.process.terminate()
            return
        try:
            handle.process.wait(timeout=self.measurement_stop_term_grace_s)
        except subprocess.TimeoutExpired:
            handle.process.terminate()
        handle.reader_thread.join(timeout=5)

    def _mcperf_agent_is_active(self, node_name: str) -> bool:
        result = self.cluster.ssh(
            node_name,
            self._bash_lc("sudo systemctl is-active --quiet mcperf-agent.service"),
            check=False,
        )
        return result.returncode == 0

    def _mcperf_agent_diagnostics(self, node_name: str) -> str:
        script = "\n".join(
            [
                "set +e",
                'echo "--- systemctl status mcperf-agent.service ---"',
                "sudo systemctl status mcperf-agent.service --no-pager -l",
                'echo "--- journalctl -u mcperf-agent.service ---"',
                "sudo journalctl -u mcperf-agent.service -n 80 --no-pager",
                'echo "--- pgrep -a mcperf ---"',
                "pgrep -a mcperf",
                "exit 0",
            ]
        )
        result = self.cluster.ssh(node_name, self._bash_lc(script), check=False)
        return result.combined_output

    def _ensure_mcperf_agents_active(
        self,
        *,
        nodes: dict[str, object],
        log_path: Path,
    ) -> None:
        for nodetype in ("client-agent-a", "client-agent-b"):
            node = nodes.get(nodetype)
            if node is None:
                raise RuntimeError(f"Missing node for {nodetype}")
            node_name = getattr(node, "name", None)
            if not isinstance(node_name, str) or not node_name:
                raise RuntimeError(f"Missing Kubernetes node name for {nodetype}")

            self._log(log_path, f"Restarting mcperf-agent.service on {nodetype} ({node_name})")
            script = "\n".join(
                [
                    "set +e",
                    "sudo systemctl reset-failed mcperf-agent.service",
                    'reset_status="$?"',
                    "sudo systemctl restart mcperf-agent.service",
                    'restart_status="$?"',
                    'echo "reset_failed_status=$reset_status restart_status=$restart_status"',
                    'if [ "$reset_status" -ne 0 ] || [ "$restart_status" -ne 0 ]; then',
                    "  exit 1",
                    "fi",
                ]
            )
            result = self.cluster.ssh(node_name, self._bash_lc(script), check=False)
            if result.returncode != 0:
                suffix = f": {result.combined_output}" if result.combined_output else ""
                self._log(
                    log_path,
                    "Warning: mcperf-agent.service restart command returned nonzero on "
                    f"{nodetype} ({node_name}); continuing to poll active state{suffix}",
                )

            deadline = self._current_time() + self.mcperf_agent_start_timeout_s
            while self._current_time() < deadline:
                if self._mcperf_agent_is_active(node_name):
                    self._log(log_path, f"mcperf-agent.service active on {nodetype} ({node_name})")
                    break
                self._sleep(min(self.poll_interval_s, max(deadline - self._current_time(), 0.0)))
            else:
                if self._mcperf_agent_is_active(node_name):
                    self._log(log_path, f"mcperf-agent.service active on {nodetype} ({node_name})")
                    continue
                diagnostics = self._mcperf_agent_diagnostics(node_name)
                suffix = f"\n{diagnostics}" if diagnostics else ""
                raise RuntimeError(
                    f"mcperf-agent.service did not become active on {nodetype} ({node_name})"
                    f"{suffix}"
                )

    def _line_has_mcperf_sync_error(self, line: str) -> bool:
        return "sync_agent" in line or "ERROR during synchronization" in line

    def _line_looks_like_mcperf_sample(self, line: str, p95_index: int | None) -> bool:
        stripped = line.strip()
        if not stripped or stripped.startswith("#") or stripped.startswith("mcperf.cc"):
            return False
        columns = stripped.split()
        index = 12 if p95_index is None else p95_index
        if len(columns) <= index:
            return False
        try:
            float(columns[index])
        except ValueError:
            return False
        return True

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
        remote_pid_file = self._measurement_pid_file(run_id)
        load_command = shlex.join(["./mcperf", "-s", memcached_ip, "--loadonly"])
        scan_command = shlex.join(
            [
                "./mcperf",
                "-s",
                memcached_ip,
                "-a",
                agent_a_ip,
                "-a",
                agent_b_ip,
                "--noload",
                "-T",
                str(measurement.measure_threads),
                "-C",
                str(measurement.connections),
                "-D",
                str(measurement.depth),
                "-Q",
                str(measurement.qps_interval),
                "-c",
                str(measurement.connections),
                "-t",
                "10",
                "--scan",
                f"{measurement.scan_start}:{measurement.scan_stop}:{measurement.scan_step}",
            ]
        )
        script = "\n".join(
            [
                "set -euo pipefail",
                f"cd {shlex.quote(self.experiment.remote_repo_dir)}",
                f"pid_file={shlex.quote(remote_pid_file)}",
                'child_pid=""',
                "stop_requested=0",
                'rm -f "$pid_file"',
                "cleanup() {",
                '  rm -f "$pid_file"',
                "}",
                "forward_stop() {",
                '  stop_requested=1',
                '  signal_name="$1"',
                '  if [ -n "${child_pid:-}" ] && kill -0 "$child_pid" >/dev/null 2>&1; then',
                '    kill "-$signal_name" "$child_pid" >/dev/null 2>&1 || true',
                "  fi",
                "}",
                "trap cleanup EXIT",
                "trap 'forward_stop INT' INT",
                "trap 'forward_stop TERM' TERM",
                load_command,
                'echo "$$" > "$pid_file"',
                "set +e",
                f"{scan_command} &",
                'child_pid="$!"',
                "while true; do",
                '  wait "$child_pid"',
                '  status="$?"',
                '  if [ "$stop_requested" -eq 1 ] && kill -0 "$child_pid" >/dev/null 2>&1; then',
                "    continue",
                "  fi",
                "  break",
                "done",
                "set -e",
                'if [ "$stop_requested" -eq 1 ]; then',
                "  exit 0",
                "fi",
                'if [ "$status" -eq 130 ] || [ "$status" -eq 143 ]; then',
                "  exit 0",
                "fi",
                'exit "$status"',
            ]
        )
        process = self.cluster.popen_ssh(
            measure_node.name,
            self._bash_lc(script),
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
        )
        ready_event = threading.Event()
        sample_event = threading.Event()
        error_event = threading.Event()
        error_messages: list[str] = []

        def _reader() -> None:
            assert process.stdout is not None
            sample_logged = False
            p95_index: int | None = None
            with mcperf_path.open("w", encoding="utf-8") as handle:
                for line in process.stdout:
                    handle.write(line)
                    handle.flush()
                    if self._line_has_mcperf_sync_error(line):
                        error_messages.append(line.strip())
                        error_event.set()
                    if line.startswith("#type"):
                        header = line.split()
                        if "p95" in header:
                            p95_index = header.index("p95")
                        ready_event.set()
                        self._log(log_path, "mcperf measurement header observed")
                    if self._line_looks_like_mcperf_sample(line, p95_index):
                        sample_event.set()
                        if not sample_logged:
                            sample_logged = True
                            self._log(log_path, "mcperf measurement sample observed")

        reader_thread = threading.Thread(target=_reader, daemon=True)
        reader_thread.start()
        return MeasurementHandle(
            process=process,
            reader_thread=reader_thread,
            ready_event=ready_event,
            sample_event=sample_event,
            error_event=error_event,
            error_messages=error_messages,
            node_name=measure_node.name,
            remote_pid_file=remote_pid_file,
        )

    def _wait_for_measurement_start(self, handle: MeasurementHandle) -> None:
        deadline = time.monotonic() + self.experiment.measurement.max_start_wait_s
        while time.monotonic() < deadline:
            if handle.error_event.is_set():
                self._abort_measurement_start(handle)
                details = "; ".join(message for message in handle.error_messages if message)
                suffix = f": {details}" if details else ""
                raise RuntimeError(f"mcperf measurement failed during agent synchronization{suffix}")
            if handle.sample_event.wait(timeout=0.5):
                return
            return_code = handle.process.poll()
            if return_code is not None:
                handle.reader_thread.join(timeout=5)
                if handle.error_event.is_set():
                    details = "; ".join(message for message in handle.error_messages if message)
                    suffix = f": {details}" if details else ""
                    raise RuntimeError(f"mcperf measurement failed during agent synchronization{suffix}")
                raise RuntimeError(f"mcperf measurement exited before the first sample with code {return_code}")
        self._abort_measurement_start(handle)
        raise TimeoutError("mcperf measurement did not produce a latency sample in time")

    def _wait_for_measurement_finish(self, handle: MeasurementHandle, *, timeout_s: float | None = None) -> None:
        timeout = self.experiment.measurement.completion_timeout_s if timeout_s is None else timeout_s
        try:
            return_code = handle.process.wait(timeout=timeout)
        except subprocess.TimeoutExpired as exc:
            raise TimeoutError(f"mcperf measurement did not finish within {timeout:.1f}s") from exc
        handle.reader_thread.join(timeout=30)
        if return_code != 0 and not handle.stop_requested:
            raise RuntimeError(f"mcperf measurement exited with code {return_code}")

    def _stop_measurement(self, handle: MeasurementHandle, *, log_path: Path) -> None:
        if handle.process.poll() is not None:
            return
        handle.stop_requested = True
        self._log(log_path, f"Stopping mcperf measurement wrapper on {handle.node_name} with SIGINT")
        self._send_measurement_signal(handle, "INT")
        try:
            self._wait_for_measurement_finish(handle, timeout_s=self.measurement_stop_int_grace_s)
            return
        except TimeoutError:
            self._log(
                log_path,
                f"mcperf is still running on {handle.node_name} after SIGINT; escalating to SIGTERM",
            )
        self._send_measurement_signal(handle, "TERM")
        try:
            self._wait_for_measurement_finish(handle, timeout_s=self.measurement_stop_term_grace_s)
        except TimeoutError as exc:
            raise TimeoutError("mcperf measurement did not stop after SIGINT/SIGTERM") from exc

    def _jobs_missing_termination_metadata(
        self,
        payload: dict[str, object],
        *,
        expected_job_ids: set[str],
    ) -> list[str]:
        terminated_job_ids: set[str] = set()
        for item in payload.get("items", []):
            metadata = item.get("metadata", {})
            labels = metadata.get("labels", {})
            job_id = labels.get("cca-project-job-id")
            if not isinstance(job_id, str) or job_id not in expected_job_ids:
                continue
            container_status = (item.get("status", {}).get("containerStatuses") or [{}])[0]
            terminated = container_status.get("state", {}).get("terminated", {})
            if terminated.get("startedAt") and terminated.get("finishedAt"):
                terminated_job_ids.add(job_id)
        return sorted(expected_job_ids - terminated_job_ids)

    def _wait_for_final_job_pod_metadata(
        self,
        *,
        run_id: str,
        expected_job_ids: set[str],
        log_path: Path,
    ) -> None:
        if not expected_job_ids:
            return
        self._log(log_path, "Waiting briefly for final pod termination metadata")
        deadline = self._current_time() + self.final_pod_metadata_wait_s
        while True:
            payload = self.cluster.get_run_pods_payload(run_id)
            missing_job_ids = self._jobs_missing_termination_metadata(
                payload,
                expected_job_ids=expected_job_ids,
            )
            if not missing_job_ids:
                return
            now = self._current_time()
            if now >= deadline:
                self._log(
                    log_path,
                    "Proceeding with pod snapshot even though termination metadata is still missing for: "
                    + ", ".join(missing_job_ids),
                )
                return
            self._sleep(min(self.poll_interval_s, max(deadline - now, 0.0)))

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

    def run_once(self, *, dry_run: bool = False, precache: bool = False) -> Path:
        if dry_run and precache:
            raise ValueError("--precache cannot be combined with --dry-run")
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

        if precache:
            self._precache_images(run_id=run_id, manifests_dir=manifests_dir, log_path=log_path)

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
        node_platforms = self._capture_node_platforms(run_dir=run_dir, log_path=log_path, nodes=nodes)
        self._ensure_mcperf_agents_active(nodes=nodes, log_path=log_path)

        self._log(log_path, f"Starting measurement against memcached IP {memcached_ip}")
        measurement = self._start_measurement(
            run_dir=run_dir,
            run_id=run_id,
            memcached_ip=memcached_ip,
            agent_a_ip=agent_a_ip,
            agent_b_ip=agent_b_ip,
            log_path=log_path,
        )
        self._log(log_path, "Waiting for first mcperf measurement sample")
        self._wait_for_measurement_start(measurement)
        self._log(log_path, "mcperf measurement is live")

        self._log(log_path, "Starting phase scheduler")
        launched_jobs = self._run_phase_scheduler(
            run_id=run_id,
            resolved_jobs=resolved_jobs,
            manifests_dir=manifests_dir,
            log_path=log_path,
        )
        self._log(log_path, "All batch jobs completed; capturing results.json and stopping mcperf")
        self._wait_for_final_job_pod_metadata(
            run_id=run_id,
            expected_job_ids=set(launched_jobs),
            log_path=log_path,
        )
        collect_live_pods(self.cluster, run_dir)
        self._stop_measurement(measurement, log_path=log_path)
        self._wait_for_measurement_finish(measurement)

        self._log(log_path, "Summarizing run from captured pod snapshot and mcperf output")
        summary = summarize_run(
            run_dir,
            experiment_id=self.experiment.experiment_id,
            policy_name=self.policy.policy_name,
            run_id=run_id,
            expected_jobs=set(JOB_CATALOG),
            node_platforms=node_platforms,
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
        self._refresh_runtime_stats(log_path=log_path)
        return run_dir

    def run_batch(self, runs: int, *, dry_run: bool = False, precache: bool = False) -> list[Path]:
        if dry_run and precache:
            raise ValueError("--precache cannot be combined with --dry-run")
        run_dirs: list[Path] = []
        print(f"Starting batch of {runs} run(s)")
        for index in range(1, runs + 1):
            print(f"Starting run {index}/{runs}")
            run_dir = self.run_once(dry_run=dry_run, precache=precache and index == 1)
            run_dirs.append(run_dir)
            print(f"Finished run {index}/{runs}: {run_dir}")
        print("Batch complete")
        return run_dirs


def run_policy_queue(
    experiment: ExperimentConfig,
    queue: RunQueueConfig,
    *,
    dry_run: bool = False,
    precache: bool = False,
) -> list[Path]:
    if dry_run and precache:
        raise ValueError("--precache cannot be combined with --dry-run")
    run_dirs: list[Path] = []
    entry_label = "entry" if len(queue.entries) == 1 else "entries"
    print(f"Starting queue {queue.queue_name} with {len(queue.entries)} {entry_label}")
    precache_consumed = False
    for index, entry in enumerate(queue.entries, start=1):
        policy = load_policy_config(str(entry.policy_path))
        runner = ExperimentRunner(experiment, policy)
        entry_precache = precache and not precache_consumed
        print(
            "Starting queue entry "
            f"{index}/{len(queue.entries)}: {entry.policy_path} ({entry.runs} run(s))"
        )
        if entry.runs == 1:
            entry_run_dirs = [runner.run_once(dry_run=dry_run, precache=entry_precache)]
        else:
            entry_run_dirs = runner.run_batch(entry.runs, dry_run=dry_run, precache=entry_precache)
        run_dirs.extend(entry_run_dirs)
        if precache and not dry_run:
            precache_consumed = True
        print(
            "Finished queue entry "
            f"{index}/{len(queue.entries)}: {entry.policy_path} ({len(entry_run_dirs)} run(s))"
        )
    print("Queue complete")
    return run_dirs

```

`runtime_stats.py`:

```py
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

```

`schedule.yaml`:

```yaml
policy_name: "ultimate-6-2-split"
memcached:
  node: "node-b-4core"
  cores: "0"
  threads: 1
jobs:
  streamcluster:
    node: "node-a-8core"
    cores: "0-7"
    threads: 8
    after: "start"
  blackscholes:
    node: "node-b-4core"
    cores: "1-3"
    threads: 3
    after: "start"
    
  freqmine:
    node: "node-a-8core"
    cores: "0-7"
    threads: 8
    after: "streamcluster"
  canneal:
    node: "node-b-4core"
    cores: "1-3"
    threads: 3
    after: "blackscholes"
    
  barnes:
    node: "node-a-8core"
    cores: "0-5"
    threads: 6
    after: "freqmine"
  radix:
    node: "node-a-8core"
    cores: "6-7"
    threads: 2
    after: "freqmine"
    
  vips:
    node: "node-b-4core"
    cores: "1-3"
    threads: 3
    after: "canneal"
```

`schedule_queue.yaml`:

```yaml
queue_name: "part3-candidates"
entries:
  - policy: "schedules/schedule6bis.yaml"
    runs: 2
  - policy: "schedules/schedule7bis.yaml"
    runs: 2
  - policy: "schedules/schedule7bisbis.yaml"
    runs: 2
  - policy: "schedules/schedule7bis3.yaml"
    runs: 2
  - policy: "schedules/schedule8bis.yaml"
    runs: 2
  - policy: "schedules/schedule8bisbis.yaml"
    runs: 2
  - policy: "schedules/schedule7B2.yaml"
    runs: 2
  - policy: "schedules/schedule7B.yaml"
    runs: 2


# NEW

  - policy: "schedules/final1.yaml"
    runs: 2
  - policy: "schedules/final2.yaml"
    runs: 2
  - policy: "schedules/final3.yaml"
    runs: 2
  - policy: "schedules/final4.yaml"
    runs: 2
  - policy: "schedules/final5.yaml"
    runs: 2
  - policy: "schedules/final6.yaml"
    runs: 2
```

`schedule_viewer_data.py`:

```py
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

from Matte.automation.audit import (
    AuditJob,
    AuditMemcached,
    audit_schedule,
    build_schedule_model,
    estimate_runtime,
    load_runtime_table,
    serialize_policy_document,
)
from Matte.automation.catalog import NODE_A, NODE_B
from Matte.automation.gui import build_model_from_planner_state, planner_state_from_model


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
            cores="0-3,2-4",
            threads=5,
            dependencies=(),
            delay_s=0,
            order=2,
        )
        report = audit_schedule(self._build_model(jobs), self.runtime_table)
        self.assertTrue(any("duplicate or overlapping core 2" in issue.message for issue in report.errors))

    def test_accepts_arbitrary_valid_core_specs(self) -> None:
        jobs = _base_jobs()
        jobs["blackscholes"] = AuditJob(
            job_id="blackscholes",
            node=NODE_A,
            cores="0,2,4",
            threads=3,
            dependencies=(),
            delay_s=0,
            order=1,
        )
        jobs["barnes"] = AuditJob(
            job_id="barnes",
            node=NODE_A,
            cores="5-7",
            threads=3,
            dependencies=(),
            delay_s=0,
            order=2,
        )
        jobs["streamcluster"] = AuditJob(
            job_id="streamcluster",
            node=NODE_A,
            cores="1-5",
            threads=5,
            dependencies=("blackscholes", "barnes"),
            delay_s=0,
            order=3,
        )
        report = audit_schedule(self._build_model(jobs), self.runtime_table)
        self.assertFalse(report.errors)

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

    def test_round_trip_preserves_custom_core_specs(self) -> None:
        jobs = _base_jobs()
        jobs["blackscholes"] = AuditJob(
            job_id="blackscholes",
            node=NODE_A,
            cores="0,2,4",
            threads=3,
            dependencies=(),
            delay_s=0,
            order=1,
        )
        model = build_schedule_model(
            policy_name="planner-test",
            memcached=AuditMemcached(node=NODE_B, cores="0", threads=1),
            jobs=jobs,
        )
        planner_state = planner_state_from_model(model)
        rebuilt_model = build_model_from_planner_state(planner_state)

        self.assertEqual(rebuilt_model.jobs["blackscholes"].cores, "0,2,4")

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
import json
import unittest
from pathlib import Path
from unittest.mock import patch

from Matte.automation.cluster import ClusterController, NodeInfo
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


class RetryingClusterController(ClusterController):
    def __init__(self, responses: list[CommandResult]):
        super().__init__(_experiment_config())
        self.responses = list(responses)
        self.calls: list[tuple[str, ...]] = []

    def kubectl(self, *args: str, check: bool = True) -> CommandResult:
        self.calls.append(args)
        if not self.responses:
            raise AssertionError("No more fake kubectl responses configured")
        return self.responses.pop(0)


class PlatformClusterController(ClusterController):
    def __init__(self):
        super().__init__(_experiment_config())

    def discover_nodes(self) -> dict[str, NodeInfo]:
        return {
            "node-a-8core": NodeInfo("node-a-8core-abcd", "node-a-8core", "10.0.0.21", None),
            "node-b-4core": NodeInfo("node-b-4core-wxyz", "node-b-4core", "10.0.0.22", None),
        }


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

    def test_kubectl_json_retries_transient_connectivity_failures(self) -> None:
        cluster = RetryingClusterController(
            [
                CommandResult(
                    args=["kubectl", "get", "jobs", "-o", "json"],
                    returncode=1,
                    stdout="",
                    stderr="Unable to connect to the server: dial tcp 34.77.122.98:443: connect: network is unreachable",
                ),
                CommandResult(
                    args=["kubectl", "get", "jobs", "-o", "json"],
                    returncode=0,
                    stdout=json.dumps({"items": []}),
                    stderr="",
                ),
            ]
        )

        with patch("Matte.automation.cluster.time.sleep"):
            payload = cluster.kubectl_json("get", "jobs", "-o", "json")

        self.assertEqual(payload, {"items": []})
        self.assertEqual(cluster.calls, [("get", "jobs", "-o", "json"), ("get", "jobs", "-o", "json")])

    def test_kubectl_json_raises_clear_error_after_retry_budget(self) -> None:
        attempts = ClusterController.kubectl_read_retry_attempts
        cluster = RetryingClusterController(
            [
                CommandResult(
                    args=["kubectl", "get", "jobs", "-o", "json"],
                    returncode=1,
                    stdout="",
                    stderr="Unable to connect to the server: dial tcp 34.77.122.98:443: connect: network is unreachable",
                )
                for _ in range(attempts)
            ]
        )

        with patch("Matte.automation.cluster.time.sleep"):
            with self.assertRaisesRegex(RuntimeError, "Kubernetes API connectivity was lost"):
                cluster.kubectl_json("get", "jobs", "-o", "json")

    def test_capture_benchmark_node_platforms_parses_gcloud_json(self) -> None:
        cluster = PlatformClusterController()

        def fake_run_command(args, *, check=True, **_kwargs):  # type: ignore[no-untyped-def]
            node_name = args[4]
            payloads = {
                "node-a-8core-abcd": {
                    "name": "node-a-8core-abcd",
                    "machineType": "zones/europe-west1-b/machineTypes/e2-standard-8",
                    "cpuPlatform": "Intel Broadwell",
                    "zone": "zones/europe-west1-b",
                    "status": "RUNNING",
                },
                "node-b-4core-wxyz": {
                    "name": "node-b-4core-wxyz",
                    "machineType": "zones/europe-west1-b/machineTypes/n2d-highcpu-4",
                    "cpuPlatform": "AMD Milan",
                    "zone": "zones/europe-west1-b",
                    "status": "RUNNING",
                },
            }
            return CommandResult(args=args, returncode=0, stdout=json.dumps(payloads[node_name]), stderr="")

        with patch("Matte.automation.cluster.run_command", side_effect=fake_run_command):
            payload = cluster.capture_benchmark_node_platforms()

        self.assertEqual(payload["capture_status"], "ok")
        nodes = payload["nodes"]
        self.assertEqual(nodes["node-a-8core"]["machine_type"], "e2-standard-8")
        self.assertEqual(nodes["node-a-8core"]["cpu_platform"], "Intel Broadwell")
        self.assertEqual(nodes["node-b-4core"]["machine_type"], "n2d-highcpu-4")
        self.assertEqual(nodes["node-b-4core"]["cpu_platform"], "AMD Milan")

    def test_capture_benchmark_node_platforms_records_gcloud_errors(self) -> None:
        cluster = PlatformClusterController()

        def fake_run_command(args, *, check=True, **_kwargs):  # type: ignore[no-untyped-def]
            node_name = args[4]
            if node_name == "node-b-4core-wxyz":
                return CommandResult(args=args, returncode=1, stdout="", stderr="permission denied")
            return CommandResult(
                args=args,
                returncode=0,
                stdout=json.dumps(
                    {
                        "name": node_name,
                        "machineType": "zones/europe-west1-b/machineTypes/e2-standard-8",
                        "cpuPlatform": "Intel Broadwell",
                        "zone": "zones/europe-west1-b",
                        "status": "RUNNING",
                    }
                ),
                stderr="",
            )

        with patch("Matte.automation.cluster.run_command", side_effect=fake_run_command):
            payload = cluster.capture_benchmark_node_platforms()

        self.assertEqual(payload["capture_status"], "partial")
        nodes = payload["nodes"]
        self.assertEqual(nodes["node-a-8core"]["capture_status"], "ok")
        self.assertEqual(nodes["node-b-4core"]["capture_status"], "error")
        self.assertIn("permission denied", nodes["node-b-4core"]["error"])


if __name__ == "__main__":
    unittest.main()

```

`tests/test_config.py`:

```py
from __future__ import annotations

import unittest
from pathlib import Path

from Matte.automation.config import load_experiment_config, load_policy_config, load_run_queue_config
from Matte.automation.manifests import resolve_jobs
from Matte.automation.tests.helpers import temp_workspace, write_json_config


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

    def test_queue_config_resolves_relative_policy_paths(self) -> None:
        with temp_workspace() as workspace:
            root = Path(workspace)
            schedules_dir = root / "schedules"
            schedules_dir.mkdir()
            first_policy = schedules_dir / "schedule1.yaml"
            second_policy = schedules_dir / "schedule2.yaml"
            write_json_config(first_policy, {"policy_name": "candidate-1"})
            write_json_config(second_policy, {"policy_name": "candidate-2"})
            queue_path = root / "schedule_queue.yaml"
            write_json_config(
                queue_path,
                {
                    "queue_name": "candidates",
                    "entries": [
                        {"policy": "schedules/schedule1.yaml"},
                        {"policy": "schedules/schedule2.yaml", "runs": 3},
                    ],
                },
            )

            queue = load_run_queue_config(str(queue_path))

            self.assertEqual(queue.queue_name, "candidates")
            self.assertEqual(queue.entries[0].policy_path, first_policy.resolve())
            self.assertEqual(queue.entries[0].runs, 1)
            self.assertEqual(queue.entries[1].policy_path, second_policy.resolve())
            self.assertEqual(queue.entries[1].runs, 3)

    def test_queue_config_rejects_invalid_entries(self) -> None:
        with temp_workspace() as workspace:
            root = Path(workspace)
            policy_path = root / "schedule.yaml"
            write_json_config(policy_path, {"policy_name": "candidate"})
            queue_path = root / "schedule_queue.yaml"

            write_json_config(queue_path, {"queue_name": "empty", "entries": []})
            with self.assertRaisesRegex(ValueError, "entries must contain"):
                load_run_queue_config(str(queue_path))

            write_json_config(queue_path, {"entries": [{"policy": "schedule.yaml", "runs": 0}]})
            with self.assertRaisesRegex(ValueError, "runs must be at least 1"):
                load_run_queue_config(str(queue_path))

            write_json_config(queue_path, {"entries": [{"policy": "missing.yaml"}]})
            with self.assertRaises(FileNotFoundError):
                load_run_queue_config(str(queue_path))

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

    def test_simple_schedule_preserves_thread_overrides(self) -> None:
        with temp_workspace() as workspace:
            path = Path(workspace) / "schedule.yaml"
            write_json_config(
                path,
                {
                    "policy_name": "threaded-schedule",
                    "memcached": {"node": "node-b-4core", "cores": "0", "threads": 1},
                    "jobs": {
                        "streamcluster": {
                            "node": "node-a-8core",
                            "cores": "0-7",
                            "threads": 6,
                            "after": "start",
                        },
                        "blackscholes": {
                            "node": "node-b-4core",
                            "cores": "1-3",
                            "threads": 2,
                            "after": "start",
                        },
                        "freqmine": {
                            "node": "node-b-4core",
                            "cores": "1-3",
                            "threads": 2,
                            "after": "blackscholes",
                        },
                        "canneal": {
                            "node": "node-a-8core",
                            "cores": "0-7",
                            "threads": 7,
                            "after": "streamcluster",
                        },
                        "barnes": {
                            "node": "node-a-8core",
                            "cores": "0-7",
                            "threads": 5,
                            "after": "canneal",
                        },
                        "vips": {
                            "node": "node-a-8core",
                            "cores": "0-7",
                            "threads": 4,
                            "after": "barnes",
                        },
                        "radix": {
                            "node": "node-a-8core",
                            "cores": "0-7",
                            "threads": 3,
                            "after": "vips",
                        },
                    },
                },
            )
            policy = load_policy_config(str(path))
            jobs = resolve_jobs(policy, "preview")
            self.assertEqual(policy.job_overrides["streamcluster"].threads, 6)
            self.assertEqual(policy.job_overrides["blackscholes"].threads, 2)
            self.assertEqual(policy.job_overrides["radix"].threads, 3)
            self.assertEqual(jobs["streamcluster"].threads, 6)
            self.assertEqual(jobs["blackscholes"].threads, 2)
            self.assertEqual(jobs["radix"].threads, 3)

    def test_simple_schedule_rejects_zero_thread_override(self) -> None:
        with temp_workspace() as workspace:
            path = Path(workspace) / "schedule.yaml"
            write_json_config(
                path,
                {
                    "policy_name": "bad-zero-threads",
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
                            "threads": 0,
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
            with self.assertRaisesRegex(ValueError, "blackscholes must use at least one thread"):
                load_policy_config(str(path))

    def test_policy_accepts_arbitrary_valid_core_specs(self) -> None:
        with temp_workspace() as workspace:
            path = Path(workspace) / "schedule.yaml"
            write_json_config(
                path,
                {
                    "policy_name": "custom-cores",
                    "memcached": {"node": "node-b-4core", "cores": "0", "threads": 1},
                    "jobs": {
                        "streamcluster": {
                            "node": "node-a-8core",
                            "cores": "0-4",
                            "threads": 5,
                            "after": "start",
                        },
                        "blackscholes": {
                            "node": "node-b-4core",
                            "cores": "1-3",
                            "threads": 3,
                            "after": "start",
                        },
                        "freqmine": {
                            "node": "node-a-8core",
                            "cores": "0,2,4",
                            "threads": 3,
                            "after": "streamcluster",
                        },
                        "canneal": {
                            "node": "node-b-4core",
                            "cores": "1-2",
                            "threads": 2,
                            "after": "blackscholes",
                        },
                        "barnes": {
                            "node": "node-a-8core",
                            "cores": "5-7",
                            "threads": 3,
                            "after": "freqmine",
                        },
                        "vips": {
                            "node": "node-a-8core",
                            "cores": "1-5",
                            "threads": 5,
                            "after": "barnes",
                        },
                        "radix": {
                            "node": "node-b-4core",
                            "cores": "0-2",
                            "threads": 3,
                            "after": "canneal",
                        },
                    },
                },
            )

            policy = load_policy_config(str(path))
            jobs = resolve_jobs(policy, "preview")

            self.assertEqual(jobs["streamcluster"].cores, "0-4")
            self.assertEqual(jobs["freqmine"].cores, "0,2,4")
            self.assertEqual(jobs["vips"].cores, "1-5")

    def test_policy_rejects_duplicate_or_overlapping_core_specs(self) -> None:
        with temp_workspace() as workspace:
            path = Path(workspace) / "schedule.yaml"
            write_json_config(
                path,
                {
                    "policy_name": "overlap-cores",
                    "memcached": {"node": "node-b-4core", "cores": "0", "threads": 1},
                    "jobs": {
                        "streamcluster": {
                            "node": "node-a-8core",
                            "cores": "0-3,2-4",
                            "threads": 5,
                            "after": "start",
                        },
                        "blackscholes": {"node": "node-b-4core", "cores": "1-3", "threads": 3, "after": "start"},
                        "freqmine": {"node": "node-a-8core", "cores": "0-4", "threads": 5, "after": "streamcluster"},
                        "canneal": {"node": "node-b-4core", "cores": "1-3", "threads": 3, "after": "blackscholes"},
                        "barnes": {"node": "node-a-8core", "cores": "5-7", "threads": 3, "after": "freqmine"},
                        "vips": {"node": "node-a-8core", "cores": "0-7", "threads": 8, "after": "barnes"},
                        "radix": {"node": "node-b-4core", "cores": "1-3", "threads": 3, "after": "canneal"},
                    },
                },
            )

            with self.assertRaisesRegex(ValueError, "duplicate or overlapping core 2"):
                load_policy_config(str(path))

    def test_policy_rejects_out_of_range_reversed_and_empty_core_specs(self) -> None:
        with temp_workspace() as workspace:
            path = Path(workspace) / "schedule.yaml"
            base_payload = {
                "policy_name": "bad-cores",
                "memcached": {"node": "node-b-4core", "cores": "0", "threads": 1},
                "jobs": {
                    "streamcluster": {"node": "node-a-8core", "cores": "0-7", "threads": 8, "after": "start"},
                    "blackscholes": {"node": "node-b-4core", "cores": "1-3", "threads": 3, "after": "start"},
                    "freqmine": {"node": "node-a-8core", "cores": "0-7", "threads": 8, "after": "streamcluster"},
                    "canneal": {"node": "node-b-4core", "cores": "1-3", "threads": 3, "after": "blackscholes"},
                    "barnes": {"node": "node-a-8core", "cores": "0-7", "threads": 8, "after": "freqmine"},
                    "vips": {"node": "node-a-8core", "cores": "0-7", "threads": 8, "after": "barnes"},
                    "radix": {"node": "node-b-4core", "cores": "1-3", "threads": 3, "after": "canneal"},
                },
            }

            payload = dict(base_payload)
            payload["jobs"] = dict(base_payload["jobs"])
            payload["jobs"]["streamcluster"] = dict(payload["jobs"]["streamcluster"], cores="7-9")
            write_json_config(path, payload)
            with self.assertRaisesRegex(ValueError, "out of range 0-7"):
                load_policy_config(str(path))

            payload["jobs"]["streamcluster"] = dict(payload["jobs"]["streamcluster"], cores="5-3")
            write_json_config(path, payload)
            with self.assertRaisesRegex(ValueError, "end before start"):
                load_policy_config(str(path))

            payload["jobs"]["streamcluster"] = dict(payload["jobs"]["streamcluster"], cores="")
            write_json_config(path, payload)
            with self.assertRaisesRegex(ValueError, "must be a non-empty string"):
                load_policy_config(str(path))

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
from Matte.automation import viewer
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

            with patch("Matte.automation.runner.run_id_timestamp", return_value="2026-04-17-03h02m03s"), patch(
                "Matte.automation.runner.assert_client_provisioning",
                side_effect=error,
            ), self.assertRaises(ProvisioningError):
                runner.run_once()

            events_log = (
                root / "runs" / "demo" / "2026-04-17-03h02m03s" / "events.log"
            ).read_text(encoding="utf-8")
            self.assertIn("Hint: client-agent-a: bootstrap appears to have failed before mcperf installation", events_log)
            self.assertIn("Debug commands: python3 cli.py debug commands --config", events_log)
            self.assertIn("--policy", events_log)
            self.assertIn("--run-id 2026-04-17-03h02m03s", events_log)

    def test_results_best_uses_automation_runs_directory_by_default(self) -> None:
        output = io.StringIO()
        expected_root = Path(cli.__file__).resolve().parent / "runs"

        with patch("Matte.automation.cli.load_run_summaries", return_value=[] ) as load_summaries:
            with redirect_stdout(output):
                cli.main(["results", "best", "--experiment", "demo"])

        self.assertEqual(load_summaries.call_args.args[0], expected_root.resolve())
        self.assertEqual(load_summaries.call_args.args[1], "demo")
        self.assertIn("No completed run summaries found.", output.getvalue())

    def test_export_submission_uses_automation_runs_directory_by_default(self) -> None:
        output = io.StringIO()
        expected_root = Path(cli.__file__).resolve().parent / "runs"

        with patch("Matte.automation.cli.export_submission", return_value=Path("/tmp/submission")) as export_submission:
            with redirect_stdout(output):
                cli.main(["export", "submission", "--experiment", "demo", "--group", "054", "--task", "3_1"])

        self.assertEqual(export_submission.call_args.kwargs["results_root"], expected_root.resolve())
        self.assertEqual(output.getvalue().strip(), "/tmp/submission")

    def test_results_viewer_uses_automation_runs_directory_by_default(self) -> None:
        expected_root = Path(cli.__file__).resolve().parent / "runs"

        with patch("Matte.automation.cli.launch_run_viewer", return_value=0) as launch_run_viewer:
            cli.main(["results", "viewer"])

        self.assertEqual(launch_run_viewer.call_args.kwargs["results_root"], expected_root.resolve())
        self.assertEqual(launch_run_viewer.call_args.kwargs["experiment_id"], None)
        self.assertEqual(launch_run_viewer.call_args.kwargs["host"], "127.0.0.1")
        self.assertEqual(launch_run_viewer.call_args.kwargs["port"], 8000)
        self.assertEqual(launch_run_viewer.call_args.kwargs["open_browser"], True)

    def test_stats_rebuild_uses_automation_runs_directory_by_default(self) -> None:
        output = io.StringIO()
        expected_root = Path(cli.__file__).resolve().parent / "runs"

        with patch(
            "Matte.automation.cli.rebuild_runtime_stats_file",
            return_value={
                "output_path": str(expected_root / "runtime_stats.json"),
                "sample_count": 7,
                "eligible_run_count": 1,
            },
        ) as rebuild_runtime_stats:
            with redirect_stdout(output):
                cli.main(["stats", "rebuild"])

        self.assertEqual(rebuild_runtime_stats.call_args.args[0], expected_root.resolve())
        self.assertIn("Runtime stats rebuilt:", output.getvalue())
        self.assertIn("samples=7", output.getvalue())

    def test_results_viewer_can_disable_browser_open(self) -> None:
        with patch("Matte.automation.cli.launch_run_viewer", return_value=0) as launch_run_viewer:
            cli.main(["results", "viewer", "--no-open"])

        self.assertEqual(launch_run_viewer.call_args.kwargs["open_browser"], False)

    def test_viewer_py_main_uses_same_defaults(self) -> None:
        expected_root = Path(viewer.__file__).resolve().parent / "runs"

        with patch("Matte.automation.viewer.launch_run_viewer", return_value=0) as launch_run_viewer:
            viewer.main(["--experiment", "demo", "--no-open"])

        self.assertEqual(launch_run_viewer.call_args.kwargs["results_root"], expected_root.resolve())
        self.assertEqual(launch_run_viewer.call_args.kwargs["experiment_id"], "demo")
        self.assertEqual(launch_run_viewer.call_args.kwargs["host"], "127.0.0.1")
        self.assertEqual(launch_run_viewer.call_args.kwargs["port"], 8000)
        self.assertEqual(launch_run_viewer.call_args.kwargs["open_browser"], False)

    def test_run_cli_rejects_dry_run_with_precache(self) -> None:
        with self.assertRaises(SystemExit) as exc:
            cli.main(
                [
                    "run",
                    "once",
                    "--config",
                    "experiment.yaml",
                    "--policy",
                    "policy.yaml",
                    "--dry-run",
                    "--precache",
                ]
            )

        self.assertEqual(exc.exception.code, 2)

    def test_run_cli_passes_precache_to_runner(self) -> None:
        experiment = _experiment_config()
        policy = PolicyConfig(
            config_path=Path("/tmp/policy.yaml"),
            policy_name="test-policy",
            memcached=MemcachedConfig(node="node-b-4core", cores="0", threads=1),
            job_overrides={},
            phases=[],
        )
        output = io.StringIO()

        with patch("Matte.automation.cli.load_experiment_config", return_value=experiment), patch(
            "Matte.automation.cli.load_policy_config",
            return_value=policy,
        ), patch("Matte.automation.cli.ExperimentRunner") as runner_cls:
            runner_cls.return_value.run_once.return_value = Path("/tmp/run")
            with redirect_stdout(output):
                cli.main(
                    [
                        "run",
                        "once",
                        "--config",
                        "experiment.yaml",
                        "--policy",
                        "policy.yaml",
                        "--precache",
                    ]
                )

        runner_cls.return_value.run_once.assert_called_once_with(dry_run=False, precache=True)
        self.assertEqual(output.getvalue().strip(), "/tmp/run")


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

from Matte.automation.export import export_submission
from Matte.automation.tests.helpers import temp_workspace


class ExportTests(unittest.TestCase):
    def test_export_submission_creates_required_filenames_from_results_json(self) -> None:
        with temp_workspace() as workspace:
            root = Path(workspace)
            experiment_root = root / "runs" / "demo"
            experiment_root.mkdir(parents=True)
            for index in range(1, 4):
                run_dir = experiment_root / f"run-{index}"
                run_dir.mkdir()
                (run_dir / "results.json").write_text("{}", encoding="utf-8")
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

    def test_export_submission_falls_back_to_legacy_pods_json(self) -> None:
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

from Matte.automation.config import load_policy_config
from Matte.automation.manifests import render_batch_job_manifest, resolve_jobs


class ManifestTests(unittest.TestCase):
    def test_splash_jobs_render_with_splash_suite(self) -> None:
        policy = load_policy_config("/home/carti/ETH/Msc/CCA/risultatiPart3/Matte/automation/policies/baseline.yaml")
        jobs = resolve_jobs(policy, "testrun")
        barnes_manifest = render_batch_job_manifest(jobs["barnes"], experiment_id="exp", run_id="testrun")
        radix_manifest = render_batch_job_manifest(jobs["radix"], experiment_id="exp", run_id="testrun")
        self.assertIn("anakli/cca:splash2x_barnes", barnes_manifest)
        self.assertIn("-S splash2x -p barnes", barnes_manifest)
        self.assertIn("anakli/cca:splash2x_radix", radix_manifest)
        self.assertIn("-S splash2x -p radix", radix_manifest)

    def test_parsec_jobs_render_with_parsec_suite(self) -> None:
        policy = load_policy_config("/home/carti/ETH/Msc/CCA/risultatiPart3/Matte/automation/policies/baseline.yaml")
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

from Matte.automation.metrics import build_summary, parse_mcperf_output
from Matte.automation.tests.helpers import temp_workspace
from Matte.automation.timing import build_get_time_report


ROOT = Path("/home/carti/ETH/Msc/CCA")


class MetricsTests(unittest.TestCase):
    def test_parse_mcperf_output_counts_slo_violations(self) -> None:
        output = parse_mcperf_output(ROOT / "part3/results/firstRun/run1_mcperf.txt")
        self.assertEqual(output["slo_violations"], 0)
        self.assertIsNotNone(output["max_p95_us"])

    def test_parse_mcperf_output_marks_header_only_as_no_samples(self) -> None:
        with temp_workspace() as workspace:
            path = Path(workspace) / "mcperf.txt"
            path.write_text("#type p95\n", encoding="utf-8")

            output = parse_mcperf_output(path)

            self.assertEqual(output["measurement_status"], "no_samples")
            self.assertEqual(output["samples"], [])

    def test_parse_mcperf_output_marks_sync_error_as_parse_error(self) -> None:
        with temp_workspace() as workspace:
            path = Path(workspace) / "mcperf.txt"
            path.write_text(
                "#type       avg     std     min      p5     p10     p50     p67     p75     p80     p85     p90     p95\n"
                "mcperf.cc(757): sync_agent[M]: out of sync [1] for agent 1 expected sync got \n",
                encoding="utf-8",
            )

            output = parse_mcperf_output(path)

            self.assertEqual(output["measurement_status"], "parse_error")
            self.assertEqual(output["samples"], [])

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

    def test_build_summary_uses_shared_get_time_makespan(self) -> None:
        results_path = ROOT / "part3/results/firstRun/results.json"
        report = build_get_time_report(results_path)
        summary = build_summary(
            results_path,
            ROOT / "part3/results/firstRun/run1_mcperf.txt",
            {"barnes", "blackscholes", "canneal", "freqmine", "radix", "streamcluster", "vips"},
            run_id="sample-run",
            experiment_id="sample-experiment",
            policy_name="sample-policy",
        )

        self.assertTrue(report.is_complete)
        self.assertEqual(summary["completed_job_count"], report.completed_job_count)
        self.assertAlmostEqual(summary["makespan_s"], report.total_runtime.total_seconds(), places=1)

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

`tests/test_queue.py`:

```py
from __future__ import annotations

import unittest
from pathlib import Path
from unittest.mock import patch

from Matte.automation.catalog import JOB_CATALOG, NODE_A
from Matte.automation.config import load_experiment_config, load_run_queue_config
from Matte.automation.runner import run_policy_queue
from Matte.automation.tests.helpers import temp_workspace, write_json_config


def _write_experiment(root: Path):
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
    return load_experiment_config(str(experiment_path))


def _write_policy(path: Path, policy_name: str) -> None:
    write_json_config(
        path,
        {
            "policy_name": policy_name,
            "memcached": {"node": NODE_A, "cores": "0", "threads": 1},
            "jobs": {
                job_id: {
                    "node": NODE_A,
                    "cores": "0-7",
                    "threads": min(8, entry.default_threads),
                    "after": "start",
                }
                for job_id, entry in JOB_CATALOG.items()
            },
        },
    )


class QueueRunnerTests(unittest.TestCase):
    def test_run_policy_queue_executes_entries_in_order(self) -> None:
        with temp_workspace() as workspace:
            root = Path(workspace)
            schedules_dir = root / "schedules"
            schedules_dir.mkdir()
            _write_policy(schedules_dir / "schedule1.yaml", "candidate-1")
            _write_policy(schedules_dir / "schedule2.yaml", "candidate-2")
            _write_policy(schedules_dir / "schedule3.yaml", "candidate-3")
            queue_path = root / "schedule_queue.yaml"
            write_json_config(
                queue_path,
                {
                    "queue_name": "candidates",
                    "entries": [
                        {"policy": "schedules/schedule1.yaml", "runs": 1},
                        {"policy": "schedules/schedule2.yaml", "runs": 3},
                        {"policy": "schedules/schedule3.yaml", "runs": 1},
                    ],
                },
            )
            experiment = _write_experiment(root)
            queue = load_run_queue_config(str(queue_path))
            calls: list[tuple[str, str, int, bool, bool]] = []

            class FakeRunner:
                def __init__(self, _experiment, policy):
                    self.policy = policy

                def run_once(self, *, dry_run: bool = False, precache: bool = False) -> Path:
                    calls.append((self.policy.policy_name, "once", 1, dry_run, precache))
                    return root / f"{self.policy.policy_name}-once"

                def run_batch(self, runs: int, *, dry_run: bool = False, precache: bool = False) -> list[Path]:
                    calls.append((self.policy.policy_name, "batch", runs, dry_run, precache))
                    return [root / f"{self.policy.policy_name}-{index}" for index in range(runs)]

            with patch("Matte.automation.runner.ExperimentRunner", FakeRunner):
                run_dirs = run_policy_queue(experiment, queue, precache=True)

            self.assertEqual(
                calls,
                [
                    ("candidate-1", "once", 1, False, True),
                    ("candidate-2", "batch", 3, False, False),
                    ("candidate-3", "once", 1, False, False),
                ],
            )
            self.assertEqual(len(run_dirs), 5)

    def test_run_policy_queue_stops_on_runner_exception(self) -> None:
        with temp_workspace() as workspace:
            root = Path(workspace)
            schedules_dir = root / "schedules"
            schedules_dir.mkdir()
            _write_policy(schedules_dir / "schedule1.yaml", "candidate-1")
            _write_policy(schedules_dir / "schedule2.yaml", "candidate-2")
            _write_policy(schedules_dir / "schedule3.yaml", "candidate-3")
            queue_path = root / "schedule_queue.yaml"
            write_json_config(
                queue_path,
                {
                    "entries": [
                        {"policy": "schedules/schedule1.yaml"},
                        {"policy": "schedules/schedule2.yaml"},
                        {"policy": "schedules/schedule3.yaml"},
                    ],
                },
            )
            experiment = _write_experiment(root)
            queue = load_run_queue_config(str(queue_path))
            calls: list[str] = []

            class FakeRunner:
                def __init__(self, _experiment, policy):
                    self.policy = policy

                def run_once(self, *, dry_run: bool = False, precache: bool = False) -> Path:
                    calls.append(self.policy.policy_name)
                    if self.policy.policy_name == "candidate-2":
                        raise RuntimeError("runner failed")
                    return root / self.policy.policy_name

                def run_batch(self, runs: int, *, dry_run: bool = False, precache: bool = False) -> list[Path]:
                    raise AssertionError("unexpected batch call")

            with patch("Matte.automation.runner.ExperimentRunner", FakeRunner):
                with self.assertRaisesRegex(RuntimeError, "runner failed"):
                    run_policy_queue(experiment, queue)

            self.assertEqual(calls, ["candidate-1", "candidate-2"])


if __name__ == "__main__":
    unittest.main()

```

`tests/test_results.py`:

```py
from __future__ import annotations

import json
import unittest
from pathlib import Path

from Matte.automation.results import load_run_summaries, sort_best_runs
from Matte.automation.tests.helpers import temp_workspace


class ResultsTests(unittest.TestCase):
    def test_best_runs_sort_passes_first_then_makespan(self) -> None:
        summaries = [
            {"run_id": "c", "overall_status": "slo_fail", "makespan_s": 10, "max_observed_p95_us": 1200},
            {
                "run_id": "b",
                "overall_status": "pass",
                "measurement_status": "ok",
                "sample_count": 3,
                "timing_complete": True,
                "makespan_s": 200,
                "max_observed_p95_us": 800,
            },
            {
                "run_id": "a",
                "overall_status": "pass",
                "measurement_status": "ok",
                "sample_count": 3,
                "timing_complete": True,
                "makespan_s": 150,
                "max_observed_p95_us": 900,
            },
        ]
        ordered = sort_best_runs(summaries)
        self.assertEqual([entry["run_id"] for entry in ordered], ["a", "b", "c"])

    def test_best_runs_does_not_treat_zero_sample_pass_as_candidate(self) -> None:
        summaries = [
            {
                "run_id": "bad",
                "overall_status": "pass",
                "measurement_status": "ok",
                "sample_count": 0,
                "timing_complete": True,
                "makespan_s": 10,
                "max_observed_p95_us": None,
            },
            {
                "run_id": "good",
                "overall_status": "pass",
                "measurement_status": "ok",
                "sample_count": 2,
                "timing_complete": True,
                "makespan_s": 30,
                "max_observed_p95_us": 450,
            },
        ]

        ordered = sort_best_runs(summaries)

        self.assertEqual([entry["run_id"] for entry in ordered], ["good", "bad"])

    def test_load_run_summaries_reclassifies_stale_sync_error_summary(self) -> None:
        with temp_workspace() as workspace:
            root = Path(workspace)
            run_dir = root / "runs" / "demo" / "run-1"
            run_dir.mkdir(parents=True)
            (run_dir / "summary.json").write_text(
                json.dumps(
                    {
                        "run_id": "run-1",
                        "overall_status": "pass",
                        "measurement_status": "ok",
                        "sample_count": 0,
                        "makespan_s": 10,
                        "max_observed_p95_us": None,
                        "timing_complete": True,
                    }
                ),
                encoding="utf-8",
            )
            (run_dir / "mcperf.txt").write_text(
                "#type       avg     std     min      p5     p10     p50     p67     p75     p80     p85     p90     p95\n"
                "mcperf.cc(757): sync_agent[M]: out of sync [1] for agent 1 expected sync got \n",
                encoding="utf-8",
            )

            summaries = load_run_summaries(root / "runs", "demo")

            self.assertEqual(summaries[0]["overall_status"], "infra_fail")
            self.assertEqual(summaries[0]["measurement_status"], "parse_error")

```

`tests/test_runner.py`:

```py
from __future__ import annotations

import json
import unittest
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch

from Matte.automation.catalog import JOB_CATALOG
from Matte.automation.cluster import NodeInfo
from Matte.automation.config import (
    JobOverride,
    MemcachedConfig,
    Phase,
    PolicyConfig,
    load_experiment_config,
    load_policy_config,
)
from Matte.automation.runner import ExperimentRunner
from Matte.automation.tests.helpers import temp_workspace, write_json_config
from Matte.automation.utils import CommandResult


BASE_POLICY = "/home/carti/ETH/Msc/CCA/risultatiPart3/Matte/automation/schedule.yaml"


@dataclass(frozen=True)
class JobOutcome:
    duration_s: float
    failed: bool = False


@dataclass
class FakeMeasurementHandle:
    started_at_s: float
    stopped: bool = False


class FakeClock:
    def __init__(self) -> None:
        self.now = 0.0

    def advance(self, seconds: float) -> None:
        self.now += seconds


class FakeCluster:
    def __init__(
        self,
        clock: FakeClock,
        outcomes: dict[str, JobOutcome],
        *,
        pod_metadata_delay_s: float = 0.0,
    ):
        self.clock = clock
        self.outcomes = outcomes
        self.pod_metadata_delay_s = pod_metadata_delay_s
        self.applied_job_ids: list[str] = []
        self.job_launch_times: dict[str, float] = {}
        self.job_names: dict[str, str] = {}
        self.job_run_ids: dict[str, str] = {}
        self.memcached_name: str | None = None
        self.memcached_run_id: str | None = None
        self.precache_pod_names: set[str] = set()
        self.precache_wait_calls: list[tuple[str, tuple[str, ...], int]] = []
        self.precache_deleted_selectors: list[tuple[str, int]] = []
        self.precache_wait_error: Exception | None = None
        self.node_platforms_error: Exception | None = None
        self.node_platform_capture_calls: list[dict[str, NodeInfo] | None] = []
        self.node_platforms_payload: dict[str, object] = {
            "capture_status": "ok",
            "zone": "europe-west1-b",
            "nodes": {
                "node-a-8core": {
                    "capture_status": "ok",
                    "node_type": "node-a-8core",
                    "node_name": "node-a-8core-node",
                    "machine_type": "e2-standard-8",
                    "cpu_platform": "Intel Broadwell",
                },
                "node-b-4core": {
                    "capture_status": "ok",
                    "node_type": "node-b-4core",
                    "node_name": "node-b-4core-node",
                    "machine_type": "n2d-highcpu-4",
                    "cpu_platform": "AMD Milan",
                },
            },
            "errors": [],
        }
        self.base_time = datetime(2026, 1, 1, tzinfo=timezone.utc)

    def cleanup_managed_workloads(self) -> None:
        return

    def apply_manifest(self, manifest_path: Path) -> None:
        manifest = self._parse_manifest(manifest_path)
        if manifest["kind"] == "Pod" and manifest["labels"].get("cca-project-role") == "memcached":
            self.memcached_name = manifest["name"]
            self.memcached_run_id = manifest["labels"]["cca-project-run-id"]
            return
        if manifest["kind"] == "Pod" and manifest["labels"].get("cca-project-role") == "precache":
            self.precache_pod_names.add(manifest["name"])
            return
        if manifest["kind"] != "Job":
            return
        job_id = manifest["labels"]["cca-project-job-id"]
        self.applied_job_ids.append(job_id)
        self.job_launch_times[job_id] = self.clock.now
        self.job_names[job_id] = manifest["name"]
        self.job_run_ids[job_id] = manifest["labels"]["cca-project-run-id"]

    def delete_manifest(self, manifest_path: Path) -> None:
        manifest = self._parse_manifest(manifest_path)
        self.precache_pod_names.discard(manifest["name"])

    def wait_for_pods_completion(
        self,
        selector: str,
        *,
        expected_names: set[str],
        timeout_s: int = 600,
    ) -> None:
        self.precache_wait_calls.append((selector, tuple(sorted(expected_names)), timeout_s))
        if self.precache_wait_error is not None:
            raise self.precache_wait_error

    def wait_for_pods_deleted(self, selector: str, *, timeout_s: int = 120) -> None:
        self.precache_deleted_selectors.append((selector, timeout_s))
        return

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

    def capture_benchmark_node_platforms(
        self,
        *,
        nodes: dict[str, NodeInfo] | None = None,
    ) -> dict[str, object]:
        self.node_platform_capture_calls.append(nodes)
        if self.node_platforms_error is not None:
            raise self.node_platforms_error
        return json.loads(json.dumps(self.node_platforms_payload))

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

    def _format_time(self, seconds: float) -> str:
        instant = self.base_time + timedelta(seconds=seconds)
        return instant.strftime("%Y-%m-%dT%H:%M:%SZ")

    def _build_pods_payload(self, run_id: str | None = None) -> dict[str, object]:
        items: list[dict[str, object]] = []
        if self.memcached_name is not None and self.memcached_run_id is not None:
            if run_id is None or self.memcached_run_id == run_id:
                items.append(
                    {
                        "metadata": {
                            "name": self.memcached_name,
                            "labels": {
                                "cca-project-role": "memcached",
                                "cca-project-run-id": self.memcached_run_id,
                            },
                        },
                        "spec": {"nodeName": "node-b-4core-node"},
                        "status": {
                            "phase": "Running",
                            "podIP": "10.0.0.10",
                            "containerStatuses": [
                                {
                                    "name": "memcached",
                                    "state": {"running": {"startedAt": self._format_time(0.0)}},
                                }
                            ],
                        },
                    }
                )

        for index, job_id in enumerate(sorted(self.job_launch_times)):
            if run_id is not None and self.job_run_ids[job_id] != run_id:
                continue
            launch_time = self.job_launch_times[job_id]
            outcome = self.outcomes[job_id]
            finish_time = launch_time + outcome.duration_s
            metadata_visible_at = finish_time + self.pod_metadata_delay_s
            labels = {
                "cca-project-run-id": self.job_run_ids[job_id],
                "cca-project-job-id": job_id,
            }
            container_state: dict[str, object]
            phase: str
            if self.clock.now >= metadata_visible_at:
                container_state = {
                    "terminated": {
                        "startedAt": self._format_time(launch_time),
                        "finishedAt": self._format_time(finish_time),
                        "exitCode": 1 if outcome.failed else 0,
                    }
                }
                phase = "Failed" if outcome.failed else "Succeeded"
            else:
                container_state = {"running": {"startedAt": self._format_time(launch_time)}}
                phase = "Running"
            items.append(
                {
                    "metadata": {
                        "name": f"{self.job_names[job_id]}-pod",
                        "labels": labels,
                    },
                    "spec": {"nodeName": f"node-{index}"},
                    "status": {
                        "phase": phase,
                        "podIP": f"10.0.1.{index + 10}",
                        "containerStatuses": [
                            {
                                "name": f"parsec-{job_id}",
                                "state": container_state,
                            }
                        ],
                    },
                }
            )
        return {"items": items}

    def get_run_pods_payload(self, run_id: str) -> dict[str, object]:
        return self._build_pods_payload(run_id)

    def capture_pods_json(self, destination: Path) -> None:
        destination.write_text(json.dumps(self._build_pods_payload(), indent=2), encoding="utf-8")

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


class FakeAgentGateCluster:
    def __init__(
        self,
        active_sequences: dict[str, list[bool]],
        *,
        restart_returncodes: dict[str, int] | None = None,
    ):
        self.active_sequences = active_sequences
        self.restart_returncodes = restart_returncodes or {}
        self.active_checks: dict[str, int] = {}
        self.restart_calls: list[str] = []
        self.commands: list[tuple[str, str]] = []

    def ssh(self, node_name: str, command: str, *, check: bool = True) -> CommandResult:
        self.commands.append((node_name, command))
        if "systemctl is-active --quiet mcperf-agent.service" in command:
            count = self.active_checks.get(node_name, 0)
            self.active_checks[node_name] = count + 1
            sequence = self.active_sequences.get(node_name, [True])
            is_active = sequence[min(count, len(sequence) - 1)]
            return CommandResult(
                args=[],
                returncode=0 if is_active else 3,
                stdout="active\n" if is_active else "inactive\n",
                stderr="",
            )
        if "systemctl restart mcperf-agent.service" in command:
            self.restart_calls.append(node_name)
            returncode = self.restart_returncodes.get(node_name, 0)
            return CommandResult(
                args=[],
                returncode=returncode,
                stdout=f"restart_returncode={returncode}\n",
                stderr="",
            )
        if "systemctl status mcperf-agent.service" in command and "journalctl" in command:
            return CommandResult(
                args=[],
                returncode=0,
                stdout=(
                    "--- systemctl status mcperf-agent.service ---\n"
                    "status output\n"
                    "--- journalctl -u mcperf-agent.service ---\n"
                    "journal output\n"
                    "--- pgrep -a mcperf ---\n"
                    "pgrep output\n"
                ),
                stderr="",
            )
        return CommandResult(args=[], returncode=0, stdout="", stderr="")


class FakeMeasurementRunner(ExperimentRunner):
    def __init__(
        self,
        *args,
        clock: FakeClock,
        measurement_finish_s: float = 120.0,
        measurement_shutdown_s: float = 0.25,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.clock = clock
        self.measurement_finish_s = measurement_finish_s
        self.measurement_shutdown_s = measurement_shutdown_s
        self.measurement_events: list[tuple[str, float]] = []
        self.agent_gate_events: list[float] = []

    def _ensure_mcperf_agents_active(self, *, nodes: dict[str, object], log_path: Path) -> None:  # type: ignore[override]
        self.agent_gate_events.append(self.clock.now)

    def _start_measurement(self, *, run_dir: Path, **kwargs):  # type: ignore[override]
        (run_dir / "mcperf.txt").write_text("#type p95\nread 500\n", encoding="utf-8")
        self.measurement_events.append(("start", self.clock.now))
        return FakeMeasurementHandle(started_at_s=self.clock.now)

    def _wait_for_measurement_start(self, handle) -> None:  # type: ignore[override]
        return

    def _stop_measurement(self, handle, *, log_path: Path) -> None:  # type: ignore[override]
        self.measurement_events.append(("stop", self.clock.now))
        handle.stopped = True

    def _wait_for_measurement_finish(self, handle, *, timeout_s: float | None = None) -> None:  # type: ignore[override]
        self.measurement_events.append(("finish", self.clock.now))
        if handle.stopped:
            self.clock.advance(self.measurement_shutdown_s)
            return
        remaining = max(self.measurement_finish_s - self.clock.now, 0.0)
        if timeout_s is not None and remaining > timeout_s:
            self.clock.advance(timeout_s)
            raise TimeoutError(f"mcperf measurement did not finish within {timeout_s:.1f}s")
        self.clock.advance(remaining)

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

    def test_dry_run_uses_human_readable_run_id(self) -> None:
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

            with patch("Matte.automation.runner.run_id_timestamp", return_value="2026-04-23-04h12m41s"):
                run_dir = runner.run_once(dry_run=True)

            self.assertEqual(run_dir.name, "2026-04-23-04h12m41s")

    def test_dry_run_appends_suffix_when_same_second_run_dir_exists(self) -> None:
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
            existing = root / "runs" / "demo" / "2026-04-23-04h12m41s"
            existing.mkdir(parents=True)
            runner = ExperimentRunner(load_experiment_config(str(experiment_path)), load_policy_config(BASE_POLICY))

            with patch("Matte.automation.runner.run_id_timestamp", return_value="2026-04-23-04h12m41s"):
                run_dir = runner.run_once(dry_run=True)

            self.assertEqual(run_dir.name, "2026-04-23-04h12m41s-02")

    def test_dry_run_rejects_precache(self) -> None:
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

            with self.assertRaisesRegex(ValueError, "--precache"):
                runner.run_once(dry_run=True, precache=True)


class RunnerAsyncSchedulerTests(unittest.TestCase):
    def _agent_nodes(self) -> dict[str, NodeInfo]:
        return {
            "client-agent-a": NodeInfo("client-agent-a-node", "client-agent-a", "10.0.0.11", None),
            "client-agent-b": NodeInfo("client-agent-b-node", "client-agent-b", "10.0.0.12", None),
        }

    def _run_real_agent_gate(self, runner: ExperimentRunner, *, log_path: Path) -> None:
        ExperimentRunner._ensure_mcperf_agents_active(runner, nodes=self._agent_nodes(), log_path=log_path)

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
        pod_metadata_delay_s: float = 0.0,
        measurement_finish_s: float = 120.0,
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
        cluster = FakeCluster(clock, outcomes, pod_metadata_delay_s=pod_metadata_delay_s)
        runner = FakeMeasurementRunner(
            experiment,
            policy,
            clock=clock,
            measurement_finish_s=measurement_finish_s,
        )
        runner.cluster = cluster
        return runner, cluster

    def test_mcperf_agent_gate_restarts_agents_even_when_active(self) -> None:
        with temp_workspace() as workspace:
            root = Path(workspace)
            runner, _cluster = self._build_runner(root, phases=[], outcomes={})
            agent_cluster = FakeAgentGateCluster(
                {
                    "client-agent-a-node": [True],
                    "client-agent-b-node": [True],
                }
            )
            runner.cluster = agent_cluster  # type: ignore[assignment]
            log_path = root / "events.log"

            self._run_real_agent_gate(runner, log_path=log_path)

            self.assertEqual(agent_cluster.restart_calls, ["client-agent-a-node", "client-agent-b-node"])
            events_log = log_path.read_text(encoding="utf-8")
            self.assertIn("Restarting mcperf-agent.service on client-agent-a", events_log)
            self.assertIn("Restarting mcperf-agent.service on client-agent-b", events_log)
            self.assertIn("mcperf-agent.service active on client-agent-a", events_log)
            self.assertIn("mcperf-agent.service active on client-agent-b", events_log)

    def test_mcperf_agent_gate_restarts_inactive_agent_and_accepts_active(self) -> None:
        with temp_workspace() as workspace:
            root = Path(workspace)
            runner, _cluster = self._build_runner(root, phases=[], outcomes={})
            agent_cluster = FakeAgentGateCluster(
                {
                    "client-agent-a-node": [False, True],
                    "client-agent-b-node": [True],
                }
            )
            runner.cluster = agent_cluster  # type: ignore[assignment]
            log_path = root / "events.log"

            self._run_real_agent_gate(runner, log_path=log_path)

            self.assertEqual(agent_cluster.restart_calls, ["client-agent-a-node", "client-agent-b-node"])
            events_log = log_path.read_text(encoding="utf-8")
            self.assertIn("Restarting mcperf-agent.service on client-agent-a", events_log)
            self.assertIn("mcperf-agent.service active on client-agent-a", events_log)

    def test_mcperf_agent_gate_tolerates_restart_error_if_agent_becomes_active(self) -> None:
        with temp_workspace() as workspace:
            root = Path(workspace)
            runner, _cluster = self._build_runner(root, phases=[], outcomes={})
            agent_cluster = FakeAgentGateCluster(
                {
                    "client-agent-a-node": [False, True],
                    "client-agent-b-node": [True],
                },
                restart_returncodes={"client-agent-a-node": 1},
            )
            runner.cluster = agent_cluster  # type: ignore[assignment]
            log_path = root / "events.log"

            self._run_real_agent_gate(runner, log_path=log_path)

            self.assertEqual(agent_cluster.restart_calls, ["client-agent-a-node", "client-agent-b-node"])
            self.assertIn("Warning: mcperf-agent.service restart command returned nonzero", log_path.read_text())

    def test_mcperf_agent_gate_reports_diagnostics_when_agent_never_becomes_active(self) -> None:
        with temp_workspace() as workspace:
            root = Path(workspace)
            runner, _cluster = self._build_runner(root, phases=[], outcomes={})
            runner.mcperf_agent_start_timeout_s = 2.0
            agent_cluster = FakeAgentGateCluster(
                {
                    "client-agent-a-node": [False],
                    "client-agent-b-node": [True],
                }
            )
            runner.cluster = agent_cluster  # type: ignore[assignment]

            with self.assertRaises(RuntimeError) as raised:
                self._run_real_agent_gate(runner, log_path=root / "events.log")

            message = str(raised.exception)
            self.assertIn("mcperf-agent.service did not become active on client-agent-a", message)
            self.assertIn("systemctl status mcperf-agent.service", message)
            self.assertIn("journalctl -u mcperf-agent.service", message)
            self.assertIn("pgrep -a mcperf", message)
            self.assertEqual(agent_cluster.restart_calls, ["client-agent-a-node"])

    def _run_once(self, runner: ExperimentRunner, *, precache: bool = False) -> Path:
        with patch("Matte.automation.runner.assert_client_provisioning"), patch(
            "Matte.automation.runner.collect_describes"
        ), patch(
            "Matte.automation.runner.summarize_run",
            return_value={"overall_status": "pass"},
        ):
            return runner.run_once(precache=precache)

    def _run_once_with_real_summary(self, runner: ExperimentRunner, *, precache: bool = False) -> Path:
        with patch("Matte.automation.runner.assert_client_provisioning"), patch(
            "Matte.automation.runner.collect_describes"
        ):
            return runner.run_once(precache=precache)

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

    def test_run_once_stops_measurement_after_batch_completion(self) -> None:
        with temp_workspace() as workspace:
            root = Path(workspace)
            runner, _cluster = self._build_runner(
                root,
                phases=[Phase("p1", "start", (), 0, ("blackscholes", "freqmine"))],
                outcomes={
                    "blackscholes": JobOutcome(2),
                    "freqmine": JobOutcome(3),
                },
                measurement_finish_s=120,
            )

            self._run_once(runner)

            self.assertEqual([event for event, _time in runner.measurement_events], ["start", "stop", "finish"])
            self.assertLess(runner.clock.now, 10.0)

    def test_run_once_waits_for_final_pod_metadata_before_capture(self) -> None:
        with temp_workspace() as workspace:
            root = Path(workspace)
            runner, _cluster = self._build_runner(
                root,
                phases=[Phase("p1", "start", (), 0, ("blackscholes",))],
                outcomes={"blackscholes": JobOutcome(2)},
                pod_metadata_delay_s=3.0,
            )
            capture_times: list[float] = []

            def _capture_side_effect(cluster, run_dir):
                capture_times.append(runner.clock.now)
                return None

            with patch("Matte.automation.runner.assert_client_provisioning"), patch(
                "Matte.automation.runner.collect_live_pods",
                side_effect=_capture_side_effect,
            ), patch("Matte.automation.runner.collect_describes"), patch(
                "Matte.automation.runner.summarize_run",
                return_value={"overall_status": "pass"},
            ):
                runner.run_once()

            self.assertEqual(len(capture_times), 1)
            self.assertGreaterEqual(capture_times[0], 5.0)

    def test_run_once_precaches_before_memcached_and_jobs(self) -> None:
        with temp_workspace() as workspace:
            root = Path(workspace)
            runner, cluster = self._build_runner(
                root,
                phases=[Phase("p1", "start", (), 0, ("blackscholes",))],
                outcomes={"blackscholes": JobOutcome(1)},
            )

            self._run_once(runner, precache=True)

            self.assertEqual(len(cluster.precache_wait_calls), 1)
            self.assertTrue(cluster.precache_deleted_selectors)
            self.assertEqual(cluster.memcached_name is not None, True)
            self.assertEqual(cluster.applied_job_ids, ["blackscholes"])
            self.assertEqual(cluster.precache_pod_names, set())

    def test_run_once_refreshes_runtime_stats_after_real_run(self) -> None:
        with temp_workspace() as workspace:
            root = Path(workspace)
            runner, _cluster = self._build_runner(
                root,
                phases=[Phase("p1", "start", (), 0, ("blackscholes",))],
                outcomes={"blackscholes": JobOutcome(1)},
            )
            stats_path = root / "runs" / "runtime_stats.json"

            with patch(
                "Matte.automation.runner.rebuild_runtime_stats_file",
                return_value={
                    "output_path": str(stats_path),
                    "sample_count": 7,
                    "eligible_run_count": 1,
                },
            ) as rebuild_runtime_stats:
                run_dir = self._run_once(runner)

            self.assertEqual(rebuild_runtime_stats.call_args.args[0], runner.experiment.results_root)
            events_log = (run_dir / "events.log").read_text(encoding="utf-8")
            self.assertIn("Runtime stats refreshed:", events_log)
            self.assertIn("samples=7", events_log)

    def test_runtime_stats_refresh_failure_is_warning_only(self) -> None:
        with temp_workspace() as workspace:
            root = Path(workspace)
            runner, _cluster = self._build_runner(
                root,
                phases=[Phase("p1", "start", (), 0, ("blackscholes",))],
                outcomes={"blackscholes": JobOutcome(1)},
            )

            with patch(
                "Matte.automation.runner.rebuild_runtime_stats_file",
                side_effect=RuntimeError("stats unavailable"),
            ):
                run_dir = self._run_once(runner)

            events_log = (run_dir / "events.log").read_text(encoding="utf-8")
            self.assertIn("Warning: failed to refresh runtime stats: stats unavailable", events_log)

    def test_run_once_precache_failure_aborts_before_memcached(self) -> None:
        with temp_workspace() as workspace:
            root = Path(workspace)
            runner, cluster = self._build_runner(
                root,
                phases=[Phase("p1", "start", (), 0, ("blackscholes",))],
                outcomes={"blackscholes": JobOutcome(1)},
            )
            cluster.precache_wait_error = RuntimeError("Image pull failed for pod/precache")

            with self.assertRaisesRegex(RuntimeError, "Image pull failed"):
                self._run_once(runner, precache=True)

            self.assertIsNone(cluster.memcached_name)
            self.assertEqual(cluster.applied_job_ids, [])
            self.assertTrue(cluster.precache_deleted_selectors)

    def test_run_batch_precaches_only_before_first_run(self) -> None:
        with temp_workspace() as workspace:
            root = Path(workspace)
            runner, cluster = self._build_runner(
                root,
                phases=[Phase("p1", "start", (), 0, ("blackscholes",))],
                outcomes={"blackscholes": JobOutcome(1)},
            )

            with patch("Matte.automation.runner.assert_client_provisioning"), patch(
                "Matte.automation.runner.collect_describes"
            ), patch(
                "Matte.automation.runner.summarize_run",
                return_value={"overall_status": "pass"},
            ):
                run_dirs = runner.run_batch(2, precache=True)

            self.assertEqual(len(run_dirs), 2)
            self.assertEqual(len(cluster.precache_wait_calls), 1)

    def test_run_batch_checks_mcperf_agents_before_every_measurement(self) -> None:
        with temp_workspace() as workspace:
            root = Path(workspace)
            runner, _cluster = self._build_runner(
                root,
                phases=[Phase("p1", "start", (), 0, ("blackscholes",))],
                outcomes={"blackscholes": JobOutcome(1)},
            )

            with patch("Matte.automation.runner.assert_client_provisioning"), patch(
                "Matte.automation.runner.collect_describes"
            ), patch(
                "Matte.automation.runner.summarize_run",
                return_value={"overall_status": "pass"},
            ):
                run_dirs = runner.run_batch(2)

            self.assertEqual(len(run_dirs), 2)
            self.assertEqual(len(runner.agent_gate_events), 2)
            self.assertLessEqual(runner.agent_gate_events[0], runner.measurement_events[0][1])
            self.assertLessEqual(runner.agent_gate_events[1], runner.measurement_events[3][1])

    def test_intentional_measurement_shutdown_still_summarizes_as_pass(self) -> None:
        with temp_workspace() as workspace:
            root = Path(workspace)
            all_jobs = tuple(sorted(JOB_CATALOG))
            runner, _cluster = self._build_runner(
                root,
                phases=[Phase("p1", "start", (), 0, all_jobs)],
                outcomes={job_id: JobOutcome(1) for job_id in all_jobs},
            )

            run_dir = self._run_once_with_real_summary(runner)

            summary = json.loads((run_dir / "summary.json").read_text(encoding="utf-8"))
            self.assertEqual(summary["overall_status"], "pass")
            self.assertEqual(summary["measurement_status"], "ok")

    def test_real_run_writes_results_json(self) -> None:
        with temp_workspace() as workspace:
            root = Path(workspace)
            runner, _cluster = self._build_runner(
                root,
                phases=[Phase("p1", "start", (), 0, ("blackscholes",))],
                outcomes={"blackscholes": JobOutcome(1)},
            )

            run_dir = self._run_once_with_real_summary(runner)

            self.assertTrue((run_dir / "results.json").exists())

    def test_real_run_writes_node_platforms_artifact_and_summary(self) -> None:
        with temp_workspace() as workspace:
            root = Path(workspace)
            runner, cluster = self._build_runner(
                root,
                phases=[Phase("p1", "start", (), 0, ("blackscholes",))],
                outcomes={"blackscholes": JobOutcome(1)},
            )

            run_dir = self._run_once_with_real_summary(runner)

            artifact = json.loads((run_dir / "node_platforms.json").read_text(encoding="utf-8"))
            summary = json.loads((run_dir / "summary.json").read_text(encoding="utf-8"))
            self.assertEqual(artifact["capture_status"], "ok")
            self.assertEqual(artifact["nodes"]["node-b-4core"]["cpu_platform"], "AMD Milan")
            self.assertEqual(summary["node_platforms"], artifact)
            self.assertEqual(len(cluster.node_platform_capture_calls), 1)

    def test_node_platform_capture_failure_is_diagnostic(self) -> None:
        with temp_workspace() as workspace:
            root = Path(workspace)
            runner, cluster = self._build_runner(
                root,
                phases=[Phase("p1", "start", (), 0, ("blackscholes",))],
                outcomes={"blackscholes": JobOutcome(1)},
            )
            cluster.node_platforms_error = RuntimeError("gcloud unavailable")

            run_dir = self._run_once_with_real_summary(runner)

            artifact = json.loads((run_dir / "node_platforms.json").read_text(encoding="utf-8"))
            summary = json.loads((run_dir / "summary.json").read_text(encoding="utf-8"))
            self.assertEqual(artifact["capture_status"], "error")
            self.assertIn("gcloud unavailable", artifact["errors"][0])
            self.assertEqual(summary["node_platforms"], artifact)
            self.assertIn(
                "Warning: failed to capture benchmark node CPU platforms",
                (run_dir / "events.log").read_text(encoding="utf-8"),
            )


if __name__ == "__main__":
    unittest.main()

```

`tests/test_runtime_stats.py`:

```py
from __future__ import annotations

import json
import unittest
from pathlib import Path

from Matte.automation.catalog import JOB_CATALOG, NODE_A, NODE_B
from Matte.automation.runtime_stats import (
    build_runtime_stats,
    load_runtime_stats,
    rebuild_runtime_stats_file,
)
from Matte.automation.tests.helpers import temp_workspace, write_json_config


def _policy_payload(name: str, *, memcached_node: str = NODE_B) -> dict[str, object]:
    jobs: dict[str, object] = {}
    previous_job: str | None = None
    for job_id, entry in JOB_CATALOG.items():
        jobs[job_id] = {
            "node": NODE_B if job_id == "blackscholes" else NODE_A,
            "cores": "1-3" if job_id == "blackscholes" else "0-7",
            "threads": 3 if job_id == "blackscholes" else entry.default_threads,
            "after": previous_job or "start",
        }
        previous_job = job_id
    return {
        "policy_name": name,
        "memcached": {"node": memcached_node, "cores": "0", "threads": 1},
        "jobs": jobs,
    }


def _summary_payload(
    *,
    run_id: str,
    policy_name: str,
    memcached_node_name: str,
    blackscholes_runtime_s: float,
) -> dict[str, object]:
    jobs: dict[str, object] = {}
    for index, job_id in enumerate(JOB_CATALOG):
        runtime_s = blackscholes_runtime_s if job_id == "blackscholes" else float(100 + index)
        node_name = "node-b-4core-node" if job_id == "blackscholes" else "node-a-8core-node"
        jobs[job_id] = {
            "pod_name": f"parsec-{job_id}-{run_id}",
            "node_name": node_name,
            "phase": "Succeeded",
            "status": "completed",
            "started_at": "2026-01-01T00:00:00Z",
            "finished_at": "2026-01-01T00:01:00Z",
            "runtime_s": runtime_s,
        }
    return {
        "experiment_id": "demo",
        "run_id": run_id,
        "policy_name": policy_name,
        "overall_status": "pass",
        "measurement_status": "ok",
        "timing_complete": True,
        "completed_job_count": len(JOB_CATALOG),
        "expected_job_count": len(JOB_CATALOG),
        "makespan_s": 200.0,
        "max_observed_p95_us": 500.0,
        "slo_violations": 0,
        "sample_count": 10,
        "memcached": {
            "pod_name": f"memcached-{run_id}",
            "node_name": memcached_node_name,
            "phase": "Running",
        },
        "jobs": jobs,
        "node_platforms": {
            "capture_status": "ok",
            "nodes": {
                NODE_A: {
                    "node_name": "node-a-8core-node",
                    "machine_type": "e2-standard-8",
                    "cpu_platform": "Intel Broadwell",
                },
                NODE_B: {
                    "node_name": "node-b-4core-node",
                    "machine_type": "n2d-highcpu-4",
                    "cpu_platform": "AMD Milan",
                },
            },
            "errors": [],
        },
    }


def _write_run(
    results_root: Path,
    run_id: str,
    *,
    memcached_node: str,
    blackscholes_runtime_s: float,
) -> Path:
    run_dir = results_root / "demo" / run_id
    run_dir.mkdir(parents=True)
    policy_name = f"policy-{run_id}"
    write_json_config(run_dir / "policy.yaml", _policy_payload(policy_name, memcached_node=memcached_node))
    memcached_node_name = "node-a-8core-node" if memcached_node == NODE_A else "node-b-4core-node"
    (run_dir / "summary.json").write_text(
        json.dumps(
            _summary_payload(
                run_id=run_id,
                policy_name=policy_name,
                memcached_node_name=memcached_node_name,
                blackscholes_runtime_s=blackscholes_runtime_s,
            ),
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )
    return run_dir


class RuntimeStatsTests(unittest.TestCase):
    def test_build_runtime_stats_extracts_samples_with_context(self) -> None:
        with temp_workspace() as workspace:
            results_root = Path(workspace) / "runs"
            _write_run(results_root, "run-1", memcached_node=NODE_B, blackscholes_runtime_s=12.0)

            payload = build_runtime_stats(results_root)

            self.assertEqual(payload["sample_count"], len(JOB_CATALOG))
            sample = next(sample for sample in payload["samples"] if sample["job"] == "blackscholes")
            self.assertEqual(sample["job_node"], NODE_B)
            self.assertEqual(sample["threads"], 3)
            self.assertEqual(sample["memcached_node"], NODE_B)
            self.assertTrue(sample["memcached_same_node"])
            self.assertEqual(sample["job_cpu_platform"], "AMD Milan")

    def test_runtime_stats_index_uses_median_and_memcached_placement(self) -> None:
        with temp_workspace() as workspace:
            results_root = Path(workspace) / "runs"
            _write_run(results_root, "run-1", memcached_node=NODE_B, blackscholes_runtime_s=10.0)
            _write_run(results_root, "run-2", memcached_node=NODE_B, blackscholes_runtime_s=30.0)
            _write_run(results_root, "run-3", memcached_node=NODE_B, blackscholes_runtime_s=20.0)
            _write_run(results_root, "run-4", memcached_node=NODE_A, blackscholes_runtime_s=5.0)

            output_path = results_root / "runtime_stats.json"
            rebuild_runtime_stats_file(results_root, output_path=output_path)
            index = load_runtime_stats(output_path)

            mem_b = index.estimate(
                job_id="blackscholes",
                node=NODE_B,
                threads=3,
                memcached_node=NODE_B,
            )
            mem_a = index.estimate(
                job_id="blackscholes",
                node=NODE_B,
                threads=3,
                memcached_node=NODE_A,
            )

            self.assertIsNotNone(mem_b)
            self.assertIsNotNone(mem_a)
            assert mem_b is not None
            assert mem_a is not None
            self.assertEqual(mem_b.duration_s, 20.0)
            self.assertEqual(mem_b.sample_count, 3)
            self.assertEqual(mem_b.match_type, "exact")
            self.assertEqual(mem_a.duration_s, 5.0)
            self.assertEqual(mem_a.match_type, "exact")


if __name__ == "__main__":
    unittest.main()

```

`tests/test_schedule_viewer.py`:

```py
from __future__ import annotations

import json
import unittest
from pathlib import Path

from Matte.automation.catalog import JOB_CATALOG, NODE_A, NODE_B
from Matte.automation.config import load_policy_config
from Matte.automation.schedule_viewer_data import (
    list_schedule_view,
    load_schedule_view,
    preview_schedule_view,
)
from Matte.automation.tests.helpers import temp_workspace, write_json_config


def _write_times_csv(path: Path) -> None:
    lines = ["job,threads,real_time_seconds"]
    for index, job_id in enumerate(JOB_CATALOG, start=1):
        lines.append(f"{job_id},1,{10 * index}.0")
        lines.append(f"{job_id},2,{8 * index}.0")
        lines.append(f"{job_id},4,{5 * index}.0")
        lines.append(f"{job_id},8,{3 * index}.0")
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _write_runtime_stats(path: Path) -> None:
    exact = []
    for index, job_id in enumerate(JOB_CATALOG, start=1):
        for memcached_node, multiplier in ((NODE_B, 1), (NODE_A, 10)):
            duration = float(index * multiplier)
            exact.append(
                {
                    "key": {
                        "job": job_id,
                        "node": NODE_A,
                        "threads": 1,
                        "memcached_node": memcached_node,
                    },
                    "sample_count": 1,
                    "median_s": duration,
                    "mean_s": duration,
                    "min_s": duration,
                    "max_s": duration,
                    "source_runs": ["demo/run-1"],
                }
            )
    path.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "generated_at": "2026-01-01T00:00:00Z",
                "results_root": str(path.parent),
                "sample_count": len(exact),
                "samples": [],
                "aggregates": {"exact": exact, "same_node": [], "node": []},
                "skipped_runs": [],
            },
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )


def _policy_payload(name: str, *, memcached_node: str = NODE_B) -> dict[str, object]:
    jobs: dict[str, object] = {}
    previous_job: str | None = None
    for job_id in JOB_CATALOG:
        jobs[job_id] = {
            "node": NODE_A,
            "cores": "1",
            "threads": 1,
            "after": previous_job or "start",
        }
        previous_job = job_id
    return {
        "policy_name": name,
        "memcached": {"node": memcached_node, "cores": "0", "threads": 1},
        "jobs": jobs,
    }


class ScheduleViewerDataTests(unittest.TestCase):
    def test_lists_non_hidden_schedules_and_queue_entries(self) -> None:
        with temp_workspace() as workspace:
            root = Path(workspace)
            schedules_dir = root / "schedules"
            schedules_dir.mkdir()
            times_csv = root / "times.csv"
            _write_times_csv(times_csv)
            write_json_config(schedules_dir / "schedule1.yaml", _policy_payload("candidate-1"))
            write_json_config(schedules_dir / "schedule2.yaml", _policy_payload("candidate-2"))
            write_json_config(schedules_dir / ".hidden.yaml", _policy_payload("hidden"))
            queue_path = root / "schedule_queue.yaml"
            write_json_config(
                queue_path,
                {
                    "queue_name": "candidates",
                    "entries": [
                        {"policy": "schedules/schedule2.yaml", "runs": 3},
                        {"policy": "schedules/schedule1.yaml", "runs": 1},
                    ],
                },
            )

            payload = list_schedule_view(
                schedules_dir=schedules_dir,
                schedule_queue_path=queue_path,
                times_csv_path=times_csv,
            )

            self.assertEqual([item["schedule_id"] for item in payload["schedules"]], ["schedule1.yaml", "schedule2.yaml"])
            self.assertEqual(payload["default_schedule_id"], "schedule2.yaml")
            self.assertEqual(payload["queue"]["queue_name"], "candidates")
            self.assertEqual([entry["schedule_id"] for entry in payload["queue"]["entries"]], ["schedule2.yaml", "schedule1.yaml"])
            schedule2 = next(item for item in payload["schedules"] if item["schedule_id"] == "schedule2.yaml")
            self.assertTrue(schedule2["in_queue"])
            self.assertEqual(schedule2["queued_runs"], 3)

    def test_load_schedule_view_predicts_timeline_and_generates_loadable_simple_yaml(self) -> None:
        with temp_workspace() as workspace:
            root = Path(workspace)
            schedules_dir = root / "schedules"
            schedules_dir.mkdir()
            times_csv = root / "times.csv"
            _write_times_csv(times_csv)
            write_json_config(schedules_dir / "schedule1.yaml", _policy_payload("candidate-1"))

            payload = load_schedule_view(
                schedules_dir=schedules_dir,
                schedule_queue_path=None,
                times_csv_path=times_csv,
                schedule_id="schedule1.yaml",
            )

            self.assertEqual(payload["policy_name"], "candidate-1")
            self.assertEqual(payload["prediction"]["status"], "ok")
            self.assertTrue(payload["prediction"]["timeline"]["has_data"])
            segments = {
                segment["job_id"]: segment
                for lane in payload["prediction"]["timeline"]["lanes"]
                for segment in lane["segments"]
            }
            self.assertEqual(segments["memcached"]["planned_node"], NODE_B)
            self.assertEqual(segments["memcached"]["kind"], "memcached")

            generated_path = root / "generated.yaml"
            generated_path.write_text(str(payload["yaml"]), encoding="utf-8")
            generated_policy = load_policy_config(str(generated_path))
            self.assertEqual(generated_policy.policy_name, "candidate-1")

    def test_preview_timeline_supports_memcached_on_node_a(self) -> None:
        with temp_workspace() as workspace:
            root = Path(workspace)
            schedules_dir = root / "schedules"
            schedules_dir.mkdir()
            times_csv = root / "times.csv"
            _write_times_csv(times_csv)
            write_json_config(schedules_dir / "schedule1.yaml", _policy_payload("candidate-1"))
            payload = load_schedule_view(
                schedules_dir=schedules_dir,
                schedule_queue_path=None,
                times_csv_path=times_csv,
                schedule_id="schedule1.yaml",
            )
            editor = payload["editor"]
            editor["memcached"] = {"node": NODE_A, "cores": "0", "threads": 1}
            for job in editor["jobs"]:
                job["node"] = NODE_B
                job["cores"] = "1"
                job["threads"] = 1

            preview = preview_schedule_view(times_csv_path=times_csv, payload={"editor": editor})

            self.assertEqual(preview["prediction"]["status"], "ok")
            node_a_segments = preview["prediction"]["timeline"]["lanes"][0]["segments"]
            self.assertEqual([segment["job_id"] for segment in node_a_segments], ["memcached"])
            self.assertEqual(node_a_segments[0]["planned_node"], NODE_A)

    def test_runtime_stats_make_predictions_sensitive_to_memcached_node(self) -> None:
        with temp_workspace() as workspace:
            root = Path(workspace)
            schedules_dir = root / "schedules"
            schedules_dir.mkdir()
            times_csv = root / "times.csv"
            runtime_stats = root / "runtime_stats.json"
            _write_times_csv(times_csv)
            _write_runtime_stats(runtime_stats)
            write_json_config(schedules_dir / "schedule1.yaml", _policy_payload("candidate-1"))
            payload = load_schedule_view(
                schedules_dir=schedules_dir,
                schedule_queue_path=None,
                times_csv_path=times_csv,
                runtime_stats_path=runtime_stats,
                schedule_id="schedule1.yaml",
            )

            mem_b_makespan = payload["prediction"]["makespan_s"]
            editor = payload["editor"]
            editor["memcached"] = {"node": NODE_A, "cores": "0", "threads": 1}
            preview = preview_schedule_view(
                times_csv_path=times_csv,
                runtime_stats_path=runtime_stats,
                payload={"editor": editor},
            )

            self.assertEqual(payload["prediction"]["status"], "ok")
            self.assertEqual(preview["prediction"]["status"], "ok")
            self.assertNotEqual(preview["prediction"]["makespan_s"], mem_b_makespan)
            self.assertGreater(preview["prediction"]["makespan_s"], mem_b_makespan)

    def test_preview_reports_core_overlap_validation_errors(self) -> None:
        with temp_workspace() as workspace:
            root = Path(workspace)
            times_csv = root / "times.csv"
            _write_times_csv(times_csv)
            editor = {
                "policy_name": "bad-overlap",
                "memcached": {"node": NODE_B, "cores": "0", "threads": 1},
                "jobs": [],
            }
            for index, job_id in enumerate(JOB_CATALOG):
                editor["jobs"].append(
                    {
                        "job_id": job_id,
                        "order": index + 1,
                        "node": NODE_A,
                        "cores": "1",
                        "threads": 1,
                        "after": "start" if index < 2 else list(JOB_CATALOG)[index - 1],
                        "delay_s": 0,
                    }
                )

            preview = preview_schedule_view(times_csv_path=times_csv, payload={"editor": editor})

            self.assertEqual(preview["prediction"]["status"], "error")
            self.assertTrue(
                any("Core overlap on node-a-8core" in issue["message"] for issue in preview["prediction"]["errors"])
            )


if __name__ == "__main__":
    unittest.main()

```

`tests/test_viewer.py`:

```py
from __future__ import annotations

import json
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

from Matte.automation.catalog import JOB_CATALOG, NODE_A, NODE_B
from Matte.automation.metrics import build_summary
from Matte.automation.tests.helpers import temp_workspace, write_json_config
from Matte.automation.viewer_data import list_run_experiments, load_experiment_view, load_run_policy_view, load_run_view


BASE_TIME = datetime(2026, 4, 23, 3, 0, 0, tzinfo=timezone.utc)


def _format_time(offset_s: int) -> str:
    return (BASE_TIME + timedelta(seconds=offset_s)).strftime("%Y-%m-%dT%H:%M:%SZ")


def _policy_payload(name: str) -> dict[str, object]:
    jobs: dict[str, object] = {}
    for job_id, entry in JOB_CATALOG.items():
        jobs[job_id] = {
            "node": entry.default_node,
            "cores": entry.default_cores,
            "threads": entry.default_threads,
            "after": "start",
        }
    return {
        "policy_name": name,
        "memcached": {"node": NODE_B, "cores": "0", "threads": 1},
        "jobs": jobs,
    }


def _pods_payload(run_id: str, durations_by_job: dict[str, int]) -> dict[str, object]:
    items = [
        {
            "metadata": {
                "name": f"memcached-server-{run_id}",
                "labels": {
                    "cca-project-role": "memcached",
                    "cca-project-run-id": run_id,
                },
            },
            "spec": {"nodeName": "node-b-4core-demo"},
            "status": {
                "phase": "Running",
                "podIP": "10.0.0.10",
                "containerStatuses": [
                    {
                        "name": "memcached",
                        "state": {"running": {"startedAt": _format_time(-5)}},
                    }
                ],
            },
        }
    ]

    for job_id, entry in JOB_CATALOG.items():
        node_name = "node-a-8core-demo" if entry.default_node == NODE_A else "node-b-4core-demo"
        duration_s = durations_by_job[job_id]
        items.append(
            {
                "metadata": {
                    "name": f"parsec-{job_id}-{run_id}",
                    "labels": {
                        "cca-project-run-id": run_id,
                        "cca-project-job-id": job_id,
                    },
                },
                "spec": {"nodeName": node_name},
                "status": {
                    "phase": "Succeeded",
                    "podIP": f"10.0.1.{len(items) + 10}",
                    "containerStatuses": [
                        {
                            "name": f"parsec-{job_id}",
                            "state": {
                                "terminated": {
                                    "startedAt": _format_time(0),
                                    "finishedAt": _format_time(duration_s),
                                    "exitCode": 0,
                                }
                            },
                        }
                    ],
                },
            }
        )

    return {"apiVersion": "v1", "items": items}


def _write_mcperf(path: Path, p95_values: list[float]) -> None:
    lines = ["#type p95"]
    for index, value in enumerate(p95_values, start=1):
        lines.append(f"read-{index} {value}")
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _write_run(
    experiment_root: Path,
    run_id: str,
    *,
    policy_name: str,
    durations_by_job: dict[str, int] | None = None,
    snapshot_filename: str | None = None,
    mcperf_values: list[float] | None = None,
    mcperf_raw: str | None = None,
    write_summary_file: bool = False,
) -> Path:
    run_dir = experiment_root / run_id
    run_dir.mkdir(parents=True)
    write_json_config(run_dir / "policy.yaml", _policy_payload(policy_name))

    if snapshot_filename is not None and durations_by_job is not None:
        payload = _pods_payload(run_id, durations_by_job)
        (run_dir / snapshot_filename).write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")

    if mcperf_values is not None:
        _write_mcperf(run_dir / "mcperf.txt", mcperf_values)
    elif mcperf_raw is not None:
        (run_dir / "mcperf.txt").write_text(mcperf_raw, encoding="utf-8")

    if write_summary_file and snapshot_filename is not None and durations_by_job is not None and mcperf_values is not None:
        summary = build_summary(
            run_dir / snapshot_filename,
            run_dir / "mcperf.txt",
            set(JOB_CATALOG),
            run_id=run_id,
            experiment_id="demo",
            policy_name=policy_name,
        )
        (run_dir / "summary.json").write_text(json.dumps(summary, indent=2) + "\n", encoding="utf-8")

    return run_dir


class ViewerDataTests(unittest.TestCase):
    def test_load_run_view_supports_human_readable_summary_backed_runs(self) -> None:
        with temp_workspace() as workspace:
            root = Path(workspace)
            experiment_root = root / "runs" / "demo"
            durations = {
                "barnes": 60,
                "blackscholes": 50,
                "canneal": 100,
                "freqmine": 180,
                "radix": 25,
                "streamcluster": 170,
                "vips": 30,
            }
            _write_run(
                experiment_root,
                "2026-04-23-16h42m02s",
                policy_name="summary-backed",
                durations_by_job=durations,
                snapshot_filename="results.json",
                mcperf_values=[430.0, 470.0],
                write_summary_file=True,
            )

            run = load_run_view(root / "runs", "demo", "2026-04-23-16h42m02s")

            self.assertFalse(run["is_reconstructed"])
            self.assertEqual(run["measurement_status"], "ok")
            self.assertTrue(run["artifact_flags"]["summary"])
            self.assertTrue(run["timeline"]["has_data"])
            self.assertEqual(run["run_label"], "2026-04-23 16:42:02 CEST")
            self.assertEqual(run["timestamp_iso"], "2026-04-23T16:42:02+02:00")
            self.assertEqual(run["jobs"]["streamcluster"]["planned_cores"], JOB_CATALOG["streamcluster"].default_cores)
            self.assertEqual(run["jobs"]["streamcluster"]["planned_threads"], JOB_CATALOG["streamcluster"].default_threads)

    def test_load_run_view_exposes_node_platform_metadata(self) -> None:
        with temp_workspace() as workspace:
            root = Path(workspace)
            experiment_root = root / "runs" / "demo"
            durations = {
                "barnes": 60,
                "blackscholes": 50,
                "canneal": 100,
                "freqmine": 180,
                "radix": 25,
                "streamcluster": 170,
                "vips": 30,
            }
            run_dir = _write_run(
                experiment_root,
                "2026-04-23-16h42m02s",
                policy_name="summary-backed",
                durations_by_job=durations,
                snapshot_filename="results.json",
                mcperf_values=[430.0, 470.0],
                write_summary_file=True,
            )
            node_platforms = {
                "capture_status": "ok",
                "zone": "europe-west1-b",
                "nodes": {
                    "node-a-8core": {
                        "capture_status": "ok",
                        "machine_type": "e2-standard-8",
                        "cpu_platform": "Intel Broadwell",
                    },
                    "node-b-4core": {
                        "capture_status": "ok",
                        "machine_type": "n2d-highcpu-4",
                        "cpu_platform": "AMD Milan",
                    },
                },
                "errors": [],
            }
            (run_dir / "node_platforms.json").write_text(
                json.dumps(node_platforms, indent=2) + "\n",
                encoding="utf-8",
            )
            summary = json.loads((run_dir / "summary.json").read_text(encoding="utf-8"))
            summary["node_platforms"] = node_platforms
            (run_dir / "summary.json").write_text(json.dumps(summary, indent=2) + "\n", encoding="utf-8")

            run = load_run_view(root / "runs", "demo", "2026-04-23-16h42m02s")

            self.assertTrue(run["artifact_flags"]["node_platforms"])
            self.assertEqual(run["node_platforms"], node_platforms)
            self.assertEqual(run["node_platforms"]["nodes"]["node-b-4core"]["cpu_platform"], "AMD Milan")

    def test_load_run_view_reconstructs_legacy_pods_runs(self) -> None:
        with temp_workspace() as workspace:
            root = Path(workspace)
            experiment_root = root / "runs" / "demo"
            durations = {
                "barnes": 61,
                "blackscholes": 53,
                "canneal": 104,
                "freqmine": 190,
                "radix": 11,
                "streamcluster": 121,
                "vips": 30,
            }
            _write_run(
                experiment_root,
                "20260423t030618z",
                policy_name="legacy-run",
                durations_by_job=durations,
                snapshot_filename="pods.json",
                mcperf_values=[833.1, 840.2],
            )

            run = load_run_view(root / "runs", "demo", "20260423t030618z")

            self.assertTrue(run["is_reconstructed"])
            self.assertFalse(run["artifact_flags"]["results"])
            self.assertTrue(run["artifact_flags"]["pods"])
            self.assertEqual(run["overall_status"], "pass")
            self.assertEqual(run["run_label"], "2026-04-23 05:06:18 CEST")
            self.assertEqual(run["timestamp_iso"], "2026-04-23T03:06:18+00:00")

    def test_load_run_view_exposes_policy_core_assignments(self) -> None:
        with temp_workspace() as workspace:
            root = Path(workspace)
            experiment_root = root / "runs" / "demo"
            durations = {
                "barnes": 61,
                "blackscholes": 53,
                "canneal": 104,
                "freqmine": 77,
                "radix": 11,
                "streamcluster": 121,
                "vips": 30,
            }
            run_dir = _write_run(
                experiment_root,
                "20260423t030618z",
                policy_name="custom-cores",
                durations_by_job=durations,
                snapshot_filename="pods.json",
                mcperf_values=[833.1, 840.2],
            )
            policy = _policy_payload("custom-cores")
            policy["jobs"]["vips"] = {**policy["jobs"]["vips"], "cores": "0-3", "threads": 4}
            policy["jobs"]["radix"] = {**policy["jobs"]["radix"], "cores": "4-7", "threads": 4}
            write_json_config(run_dir / "policy.yaml", policy)

            run = load_run_view(root / "runs", "demo", "20260423t030618z")

            self.assertEqual(run["jobs"]["vips"]["planned_cores"], "0-3")
            self.assertEqual(run["jobs"]["vips"]["planned_core_ids"], [0, 1, 2, 3])
            self.assertEqual(run["jobs"]["vips"]["planned_threads"], 4)
            self.assertEqual(run["jobs"]["radix"]["planned_cores"], "4-7")
            segments = {
                segment["job_id"]: segment
                for lane in run["timeline"]["lanes"]
                for segment in lane["segments"]
            }
            self.assertEqual(segments["vips"]["cores"], "0-3")
            self.assertEqual(segments["vips"]["threads"], 4)
            self.assertEqual(segments["radix"]["core_ids"], [4, 5, 6, 7])
            self.assertEqual(segments["memcached"]["cores"], "0")
            self.assertEqual(segments["memcached"]["threads"], 1)
            self.assertIn("node-a-8core-demo", run["timeline"]["lanes"][0]["node_names"])

    def test_load_run_view_handles_runs_without_pod_snapshot(self) -> None:
        with temp_workspace() as workspace:
            root = Path(workspace)
            experiment_root = root / "runs" / "demo"
            _write_run(
                experiment_root,
                "20260423t015053z",
                policy_name="missing-pods",
                snapshot_filename=None,
                mcperf_values=[512.0, 560.0],
            )

            run = load_run_view(root / "runs", "demo", "20260423t015053z")

            self.assertFalse(run["timeline"]["has_data"])
            self.assertFalse(run["artifact_flags"]["snapshot"])
            self.assertIn("No results.json or pods.json snapshot found.", run["issues"])

    def test_load_run_view_marks_malformed_mcperf_as_parse_error(self) -> None:
        with temp_workspace() as workspace:
            root = Path(workspace)
            experiment_root = root / "runs" / "demo"
            durations = {
                "barnes": 60,
                "blackscholes": 50,
                "canneal": 100,
                "freqmine": 237,
                "radix": 11,
                "streamcluster": 121,
                "vips": 30,
            }
            _write_run(
                experiment_root,
                "20260423t024853z",
                policy_name="broken-mcperf",
                durations_by_job=durations,
                snapshot_filename="pods.json",
                mcperf_raw="#type p95\nread THANKS\n",
            )

            run = load_run_view(root / "runs", "demo", "20260423t024853z")

            self.assertEqual(run["measurement_status"], "parse_error")
            self.assertEqual(run["overall_status"], "infra_fail")
            self.assertFalse(run["eligible_for_best"])
            self.assertTrue(any("malformed latency data" in issue for issue in run["issues"]))

    def test_load_run_view_overrides_stale_summary_when_mcperf_has_sync_error(self) -> None:
        with temp_workspace() as workspace:
            root = Path(workspace)
            experiment_root = root / "runs" / "demo"
            durations = {
                "barnes": 60,
                "blackscholes": 50,
                "canneal": 100,
                "freqmine": 237,
                "radix": 11,
                "streamcluster": 121,
                "vips": 30,
            }
            run_dir = _write_run(
                experiment_root,
                "2026-04-23-17h07m12s",
                policy_name="stale-summary",
                durations_by_job=durations,
                snapshot_filename="results.json",
                mcperf_values=[430.0, 440.0],
                write_summary_file=True,
            )
            (run_dir / "mcperf.txt").write_text(
                "#type       avg     std     min      p5     p10     p50     p67     p75     p80     p85     p90     p95\n"
                "mcperf.cc(757): sync_agent[M]: out of sync [1] for agent 1 expected sync got \n",
                encoding="utf-8",
            )

            run = load_run_view(root / "runs", "demo", "2026-04-23-17h07m12s")

            self.assertEqual(run["measurement_status"], "parse_error")
            self.assertEqual(run["overall_status"], "infra_fail")
            self.assertFalse(run["eligible_for_best"])
            self.assertTrue(any("synchronization errors" in issue for issue in run["issues"]))

    def test_load_experiment_view_picks_best_across_summary_and_reconstructed_runs(self) -> None:
        with temp_workspace() as workspace:
            root = Path(workspace)
            experiment_root = root / "runs" / "demo"

            _write_run(
                experiment_root,
                "2026-04-23-16h42m02s",
                policy_name="summary-backed",
                durations_by_job={
                    "barnes": 60,
                    "blackscholes": 50,
                    "canneal": 104,
                    "freqmine": 240,
                    "radix": 11,
                    "streamcluster": 121,
                    "vips": 30,
                },
                snapshot_filename="results.json",
                mcperf_values=[470.0, 480.0],
                write_summary_file=True,
            )
            _write_run(
                experiment_root,
                "20260423t023159z",
                policy_name="legacy-best",
                durations_by_job={
                    "barnes": 61,
                    "blackscholes": 53,
                    "canneal": 104,
                    "freqmine": 230,
                    "radix": 11,
                    "streamcluster": 121,
                    "vips": 30,
                },
                snapshot_filename="pods.json",
                mcperf_values=[430.0, 440.0],
            )
            _write_run(
                experiment_root,
                "20260423t030618z",
                policy_name="broken-mcperf",
                durations_by_job={
                    "barnes": 50,
                    "blackscholes": 40,
                    "canneal": 60,
                    "freqmine": 120,
                    "radix": 10,
                    "streamcluster": 70,
                    "vips": 25,
                },
                snapshot_filename="pods.json",
                mcperf_raw="#type p95\nread THANKS\n",
            )

            view = load_experiment_view(root / "runs", "demo")

            self.assertEqual(view["best_run_id"], "20260423t023159z")
            self.assertEqual(view["runs"][0]["run_id"], "2026-04-23-16h42m02s")

    def test_load_run_policy_view_reports_parsed_exact_schedule_match(self) -> None:
        with temp_workspace() as workspace:
            root = Path(workspace)
            experiment_root = root / "runs" / "demo"
            schedules_dir = root / "schedules"
            schedules_dir.mkdir()
            run_dir = _write_run(
                experiment_root,
                "2026-04-23-16h42m02s",
                policy_name="summary-backed",
                snapshot_filename=None,
            )
            (schedules_dir / "matching.yaml").write_text(
                "# same policy with different formatting comments\n"
                + (run_dir / "policy.yaml").read_text(encoding="utf-8"),
                encoding="utf-8",
            )

            payload = load_run_policy_view(root / "runs", schedules_dir, "demo", "2026-04-23-16h42m02s")

            self.assertEqual(payload["match_status"], "matched")
            self.assertEqual(payload["matches"], [
                {
                    "schedule_id": "matching.yaml",
                    "label": "matching.yaml",
                    "path": str((schedules_dir / "matching.yaml").resolve()),
                }
            ])
            self.assertIn("summary-backed", payload["policy_yaml"])

    def test_load_run_policy_view_reports_unmatched_policy(self) -> None:
        with temp_workspace() as workspace:
            root = Path(workspace)
            experiment_root = root / "runs" / "demo"
            schedules_dir = root / "schedules"
            schedules_dir.mkdir()
            _write_run(
                experiment_root,
                "2026-04-23-16h42m02s",
                policy_name="summary-backed",
                snapshot_filename=None,
            )
            write_json_config(schedules_dir / "different.yaml", _policy_payload("different-policy"))

            payload = load_run_policy_view(root / "runs", schedules_dir, "demo", "2026-04-23-16h42m02s")

            self.assertEqual(payload["match_status"], "unmatched")
            self.assertEqual(payload["matches"], [])

    def test_load_run_policy_view_reports_missing_or_malformed_policy(self) -> None:
        with temp_workspace() as workspace:
            root = Path(workspace)
            schedules_dir = root / "schedules"
            schedules_dir.mkdir()
            missing_run = root / "runs" / "demo" / "missing-policy"
            missing_run.mkdir(parents=True)
            malformed_run = root / "runs" / "demo" / "bad-policy"
            malformed_run.mkdir(parents=True)
            (malformed_run / "policy.yaml").write_text("policy_name: [unterminated\n", encoding="utf-8")

            missing = load_run_policy_view(root / "runs", schedules_dir, "demo", "missing-policy")
            malformed = load_run_policy_view(root / "runs", schedules_dir, "demo", "bad-policy")

            self.assertEqual(missing["match_status"], "missing_policy")
            self.assertTrue(missing["errors"])
            self.assertEqual(malformed["match_status"], "parse_error")
            self.assertTrue(malformed["errors"])


class ViewerExperimentListingTests(unittest.TestCase):
    def test_list_run_experiments_reports_available_experiments(self) -> None:
        with temp_workspace() as workspace:
            root = Path(workspace)
            experiment_root = root / "runs" / "demo"
            _write_run(
                experiment_root,
                "20260423t023159z",
                policy_name="http-check",
                durations_by_job={
                    "barnes": 61,
                    "blackscholes": 53,
                    "canneal": 104,
                    "freqmine": 230,
                    "radix": 11,
                    "streamcluster": 121,
                    "vips": 30,
                },
                snapshot_filename="pods.json",
                mcperf_values=[430.0, 440.0],
            )
            _write_run(
                root / "runs" / "demo-two",
                "2026-04-23-16h42m02s",
                policy_name="second-experiment",
                durations_by_job={
                    "barnes": 50,
                    "blackscholes": 40,
                    "canneal": 90,
                    "freqmine": 160,
                    "radix": 10,
                    "streamcluster": 130,
                    "vips": 25,
                },
                snapshot_filename="results.json",
                mcperf_values=[470.0, 480.0],
            )

            experiments = list_run_experiments(root / "runs")

            self.assertEqual([entry["experiment_id"] for entry in experiments], ["demo", "demo-two"])
            self.assertEqual([entry["run_count"] for entry in experiments], [1, 1])


if __name__ == "__main__":
    unittest.main()

```

`timing.py`:

```py
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

```

`utils.py`:

```py
from __future__ import annotations

import json
import os
import re
import subprocess
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Mapping, Sequence
from zoneinfo import ZoneInfo


RAW_RESULTS_FILENAME = "results.json"
LEGACY_RAW_RESULTS_FILENAME = "pods.json"
RUN_ID_TIMEZONE = ZoneInfo("Europe/Zurich")
_LEGACY_RUN_ID_PATTERN = re.compile(r"^(?P<date>\d{8})t(?P<time>\d{6})z$", re.IGNORECASE)
_HUMAN_RUN_ID_PATTERN = re.compile(
    r"^(?P<date>\d{4}-\d{2}-\d{2})-(?P<hour>\d{2})h(?P<minute>\d{2})m(?P<second>\d{2})s(?:-(?P<suffix>\d{2}))?$"
)


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


def run_id_timestamp(now: datetime | None = None) -> str:
    instant = datetime.now(RUN_ID_TIMEZONE) if now is None else now.astimezone(RUN_ID_TIMEZONE)
    return instant.strftime("%Y-%m-%d-%Hh%Mm%Ss").lower()


def ensure_directory(path: Path) -> Path:
    path.mkdir(parents=True, exist_ok=True)
    return path


def run_results_path(run_dir: Path) -> Path:
    return run_dir / RAW_RESULTS_FILENAME


def resolve_existing_run_results_path(run_dir: Path) -> Path:
    results_path = run_results_path(run_dir)
    if results_path.exists():
        return results_path
    legacy_path = run_dir / LEGACY_RAW_RESULTS_FILENAME
    if legacy_path.exists():
        return legacy_path
    return results_path


def parse_run_id_timestamp(run_id: str) -> datetime | None:
    legacy_match = _LEGACY_RUN_ID_PATTERN.match(run_id)
    if legacy_match:
        return datetime.strptime(run_id.upper(), "%Y%m%dT%H%M%SZ").replace(tzinfo=timezone.utc)
    human_match = _HUMAN_RUN_ID_PATTERN.match(run_id)
    if human_match:
        return datetime.strptime(
            (
                f"{human_match.group('date')} "
                f"{human_match.group('hour')}:{human_match.group('minute')}:{human_match.group('second')}"
            ),
            "%Y-%m-%d %H:%M:%S",
        ).replace(tzinfo=RUN_ID_TIMEZONE)
    return None


def format_run_id_label(run_id: str) -> str:
    parsed = parse_run_id_timestamp(run_id)
    if parsed is None:
        return run_id
    return parsed.astimezone(RUN_ID_TIMEZONE).strftime("%Y-%m-%d %H:%M:%S %Z")


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

`viewer.py`:

```py
from __future__ import annotations

import argparse
import json
from functools import partial
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
import sys
from urllib.parse import parse_qs, unquote, urlparse
import webbrowser

if __package__ in {None, ""}:
    package_dir = Path(__file__).resolve().parent
    package_parent = package_dir.parent
    if str(package_parent) not in sys.path:
        sys.path.insert(0, str(package_parent))
    __package__ = package_dir.name

from .results import resolve_experiment_root
from .schedule_viewer_data import list_schedule_view, load_schedule_view, preview_schedule_view
from .viewer_data import list_run_experiments, load_experiment_view, load_run_policy_view, load_run_view


STATIC_DIR = Path(__file__).resolve().parent / "viewer_static"
DEFAULT_RESULTS_ROOT = Path(__file__).resolve().parent / "runs"
DEFAULT_SCHEDULES_DIR = Path(__file__).resolve().parent / "schedules"
DEFAULT_SCHEDULE_QUEUE_PATH = Path(__file__).resolve().parent / "schedule_queue.yaml"
DEFAULT_TIMES_CSV_PATH = Path(__file__).resolve().parents[2] / "Part2summary_times.csv"
DEFAULT_RUNTIME_STATS_PATH = DEFAULT_RESULTS_ROOT / "runtime_stats.json"


class _RunViewerHandler(SimpleHTTPRequestHandler):
    def __init__(
        self,
        *args,
        results_root: Path,
        schedules_dir: Path,
        schedule_queue_path: Path | None,
        times_csv_path: Path,
        runtime_stats_path: Path | None,
        default_experiment_id: str | None,
        **kwargs,
    ) -> None:
        self.results_root = results_root
        self.schedules_dir = schedules_dir
        self.schedule_queue_path = schedule_queue_path
        self.times_csv_path = times_csv_path
        self.runtime_stats_path = runtime_stats_path
        self.default_experiment_id = default_experiment_id
        super().__init__(*args, directory=str(STATIC_DIR), **kwargs)

    def do_GET(self) -> None:  # noqa: N802
        parsed = urlparse(self.path)
        if parsed.path.startswith("/api/"):
            self._handle_api(parsed)
            return
        if parsed.path == "/":
            self.path = "/index.html"
        else:
            self.path = parsed.path
        super().do_GET()

    def do_POST(self) -> None:  # noqa: N802
        parsed = urlparse(self.path)
        if parsed.path.startswith("/api/"):
            self._handle_api_post(parsed)
            return
        self._write_json(404, {"error": "Not found"})

    def end_headers(self) -> None:
        self.send_header("Cache-Control", "no-store")
        super().end_headers()

    def log_message(self, format: str, *args) -> None:
        return

    def _handle_api(self, parsed) -> None:
        try:
            if parsed.path == "/api/experiments":
                self._write_json(
                    200,
                    {
                        "experiments": list_run_experiments(self.results_root),
                        "default_experiment_id": self._default_experiment_id(),
                    },
                )
                return

            if parsed.path == "/api/runs":
                experiment_id = self._resolve_experiment_id(parse_qs(parsed.query))
                self._write_json(200, load_experiment_view(self.results_root, experiment_id))
                return

            if parsed.path.startswith("/api/runs/") and parsed.path.endswith("/policy"):
                run_id = unquote(parsed.path.removeprefix("/api/runs/").removesuffix("/policy"))
                experiment_id = self._resolve_experiment_id(parse_qs(parsed.query))
                self._write_json(200, load_run_policy_view(self.results_root, self.schedules_dir, experiment_id, run_id))
                return

            if parsed.path.startswith("/api/runs/"):
                run_id = unquote(parsed.path.removeprefix("/api/runs/"))
                experiment_id = self._resolve_experiment_id(parse_qs(parsed.query))
                self._write_json(200, load_run_view(self.results_root, experiment_id, run_id))
                return

            if parsed.path == "/api/schedules":
                self._write_json(
                    200,
                    list_schedule_view(
                        schedules_dir=self.schedules_dir,
                        schedule_queue_path=self.schedule_queue_path,
                        times_csv_path=self.times_csv_path,
                        runtime_stats_path=self.runtime_stats_path,
                    ),
                )
                return

            if parsed.path.startswith("/api/schedules/"):
                schedule_id = unquote(parsed.path.removeprefix("/api/schedules/"))
                self._write_json(
                    200,
                    load_schedule_view(
                        schedules_dir=self.schedules_dir,
                        schedule_queue_path=self.schedule_queue_path,
                        times_csv_path=self.times_csv_path,
                        runtime_stats_path=self.runtime_stats_path,
                        schedule_id=schedule_id,
                    ),
                )
                return

            self._write_json(404, {"error": "Not found"})
        except FileNotFoundError as exc:
            self._write_json(404, {"error": str(exc)})
        except ValueError as exc:
            self._write_json(400, {"error": str(exc)})
        except Exception as exc:  # pragma: no cover - defensive HTTP surface
            self._write_json(500, {"error": f"Unexpected server error: {exc}"})

    def _handle_api_post(self, parsed) -> None:
        try:
            if parsed.path == "/api/schedules/preview":
                self._write_json(
                    200,
                    preview_schedule_view(
                        times_csv_path=self.times_csv_path,
                        runtime_stats_path=self.runtime_stats_path,
                        payload=self._read_json_body(),
                    ),
                )
                return
            self._write_json(404, {"error": "Not found"})
        except json.JSONDecodeError as exc:
            self._write_json(400, {"error": f"Request body is not valid JSON: {exc}"})
        except ValueError as exc:
            self._write_json(400, {"error": str(exc)})
        except Exception as exc:  # pragma: no cover - defensive HTTP surface
            self._write_json(500, {"error": f"Unexpected server error: {exc}"})

    def _read_json_body(self) -> dict[str, object]:
        length = int(self.headers.get("Content-Length", "0"))
        raw = self.rfile.read(length)
        payload = json.loads(raw.decode("utf-8") if raw else "{}")
        if not isinstance(payload, dict):
            raise ValueError("Request body must contain a JSON object")
        return payload

    def _default_experiment_id(self) -> str | None:
        if self.default_experiment_id is not None:
            return self.default_experiment_id
        experiments = list_run_experiments(self.results_root)
        if not experiments:
            return None
        return str(experiments[0]["experiment_id"])

    def _resolve_experiment_id(self, query_params: dict[str, list[str]]) -> str:
        experiment_values = query_params.get("experiment", [])
        if experiment_values and experiment_values[0]:
            return experiment_values[0]
        default_experiment_id = self._default_experiment_id()
        if default_experiment_id is None:
            raise FileNotFoundError(f"No experiment directories found in {self.results_root}")
        return default_experiment_id

    def _write_json(self, status: int, payload: dict[str, object]) -> None:
        body = json.dumps(payload, indent=2).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)


def launch_run_viewer(
    *,
    results_root: Path,
    schedules_dir: Path = DEFAULT_SCHEDULES_DIR,
    schedule_queue_path: Path | None = DEFAULT_SCHEDULE_QUEUE_PATH,
    times_csv_path: Path = DEFAULT_TIMES_CSV_PATH,
    runtime_stats_path: Path | None = DEFAULT_RUNTIME_STATS_PATH,
    experiment_id: str | None = None,
    host: str = "127.0.0.1",
    port: int = 8000,
    open_browser: bool = True,
) -> int:
    if not results_root.exists():
        raise FileNotFoundError(f"Results root not found: {results_root}")
    if not STATIC_DIR.exists():
        raise FileNotFoundError(f"Viewer assets not found: {STATIC_DIR}")
    if experiment_id is not None:
        resolve_experiment_root(results_root, experiment_id)

    server = ThreadingHTTPServer(
        (host, port),
        partial(
            _RunViewerHandler,
            results_root=results_root,
            schedules_dir=schedules_dir,
            schedule_queue_path=schedule_queue_path,
            times_csv_path=times_csv_path,
            runtime_stats_path=runtime_stats_path,
            default_experiment_id=experiment_id,
        ),
    )

    bound_host, bound_port = server.server_address[:2]
    display_host = "127.0.0.1" if bound_host in {"0.0.0.0", "::"} else bound_host
    url = f"http://{display_host}:{bound_port}/"
    print(f"Run viewer available at {url}")
    if open_browser:
        try:
            webbrowser.open(url, new=2)
        except Exception as exc:  # pragma: no cover - browser integration is platform-specific
            print(f"Could not open browser automatically: {exc}")
    print("Press Ctrl+C to stop the server.")

    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nStopping run viewer.")
    finally:
        server.server_close()
    return 0


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Serve the Part 3 run timing viewer")
    parser.add_argument("--experiment")
    parser.add_argument("--results-root", default=str(DEFAULT_RESULTS_ROOT))
    parser.add_argument("--schedules-dir", default=str(DEFAULT_SCHEDULES_DIR))
    parser.add_argument("--schedule-queue", default=str(DEFAULT_SCHEDULE_QUEUE_PATH))
    parser.add_argument("--times-csv", default=str(DEFAULT_TIMES_CSV_PATH))
    parser.add_argument("--runtime-stats", default=str(DEFAULT_RUNTIME_STATS_PATH))
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8000)
    parser.add_argument("--no-open", action="store_true", help="Print the URL without opening a browser")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    return launch_run_viewer(
        results_root=Path(args.results_root).resolve(),
        schedules_dir=Path(args.schedules_dir).resolve(),
        schedule_queue_path=Path(args.schedule_queue).resolve() if args.schedule_queue else None,
        times_csv_path=Path(args.times_csv).resolve(),
        runtime_stats_path=Path(args.runtime_stats).resolve() if args.runtime_stats else None,
        experiment_id=args.experiment,
        host=args.host,
        port=args.port,
        open_browser=not args.no_open,
    )


if __name__ == "__main__":
    raise SystemExit(main())

```

`viewer_data.py`:

```py
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

```