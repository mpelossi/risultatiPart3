Project Path: part3_openEvolve

Source Tree:

```txt
part3_openEvolve
├── README.md
├── config.yaml
├── evaluator.py
└── initial_program.py

```

`README.md`:

```md
# Part 3 OpenEvolve

This directory contains the OpenEvolve setup for Part 3 task 3.2. It evolves a Python
schedule dictionary in `initial_program.py`, evaluates candidates through
`evaluator.py`, and uses the existing sibling automation framework in `../automation`.

## Files

- `initial_program.py`: baseline schedule. OpenEvolve edits only the code between
  `# EVOLVE-BLOCK-START` and `# EVOLVE-BLOCK-END`.
- `evaluator.py`: imports an evolved program, writes its schedule to a temporary YAML
  policy, runs `cli.py audit`, then runs one real Kubernetes experiment for valid
  schedules.
- `config.yaml`: OpenEvolve configuration, Swiss AI endpoint, model, prompt, and
  evaluator timeout.
- `.env`: local API key environment file. Do not commit or print this file.

## Do I Need Kubernetes Running?

Yes. The evaluator runs:

```bash
python3 cli.py run once --config experiment.yaml --policy <generated-policy>
```

That command expects the Part 3 Kubernetes cluster to already exist, be reachable, and
have the client VMs provisioned. OpenEvolve does not bring the cluster up for you.

You only need to create the cluster when it does not exist yet or after you deleted it.
Once it is running, you can reuse it for OpenEvolve iterations.

## Setup

From this directory:

```bash
cd /home/carti/ETH/Msc/CCA/risultatiPart3/Matte/part3_openEvolve
```

Use the local virtual environment that contains the pinned course version:

```bash
source .venv/bin/activate
python -c "import openevolve; print(openevolve.__version__)"
```

If the venv is missing, recreate it:

```bash
python3 -m venv .venv
.venv/bin/pip install openevolve==0.2.26
```

Load the Swiss AI API key:

```bash
set -a
source .env
set +a
```

## Cluster Preflight

From the automation directory:

```bash
cd /home/carti/ETH/Msc/CCA/risultatiPart3/Matte/automation
../../checkCredits.sh
python3 cli.py cluster up --config experiment.yaml
python3 cli.py provision check --config experiment.yaml
```

If the cluster is already running, you usually only need:

```bash
cd /home/carti/ETH/Msc/CCA/risultatiPart3/Matte/automation
python3 cli.py provision check --config experiment.yaml
```

Before a serious OpenEvolve run, warm the Kubernetes images once without doing a full
benchmark run:

```bash
python3 cli.py run precache --config experiment.yaml --policy schedules/schedule7bis.yaml
```

This creates transient pre-cache pods on both benchmark nodes, waits for the benchmark
and memcached images to be present, deletes the pods, and exits before launching
memcached, `mcperf`, or benchmark jobs. Its artifacts live under
`../automation/runs/__precache/...`, separate from real benchmark results.

The OpenEvolve evaluator intentionally does not use `--precache` or run this pre-cache
step on every iteration, because doing that repeatedly wastes time once images are warm.

## Run OpenEvolve

Use a fresh output directory each time. Do not reuse old output directories, because
OpenEvolve may overwrite checkpoints and logs needed for submission.

```bash
cd /home/carti/ETH/Msc/CCA/risultatiPart3/Matte/part3_openEvolve
set -a
source .env
set +a
.venv/bin/openevolve-run --config config.yaml -o run_$(date +%Y%m%d_%H%M%S) initial_program.py evaluator.py
```

For a one-iteration smoke test:

```bash
.venv/bin/openevolve-run --config config.yaml --iterations 1 -o smoke_$(date +%Y%m%d_%H%M%S) initial_program.py evaluator.py
```

## Visualizer

Yes, you can run the OpenEvolve visualizer while the evolution is running. Use a
separate terminal and point it at the OpenEvolve output directory. It reads logs and
checkpoints; it does not run Kubernetes jobs itself.

It is normal for the visualizer to show little or nothing before the first checkpoint.
This config has `checkpoint_interval: 1`, so it should update after each completed
iteration.

Set it up outside this submission/work directory:

```bash
cd /tmp
git clone https://github.com/algorithmicsuperintelligence/openevolve.git
cd openevolve
python3 -m venv .venv
source .venv/bin/activate
pip install -r scripts/requirements.txt
```

Then, while OpenEvolve is running, start the visualizer with the output directory:

```bash
python3 scripts/visualizer.py --path /home/carti/ETH/Msc/CCA/risultatiPart3/Matte/part3_openEvolve/<run-output-dir>
```

You can also point it at a specific checkpoint directory:

```bash
python3 scripts/visualizer.py --path /home/carti/ETH/Msc/CCA/risultatiPart3/Matte/part3_openEvolve/<run-output-dir>/checkpoints/<checkpoint-dir>
```

## After Evolution

The best evolved program is saved under the OpenEvolve output directory, usually in
`best/` and in the latest checkpoint. Convert the best program's `get_schedule()` output
to a YAML policy, then benchmark it with three repetitions:

```bash
cd /home/carti/ETH/Msc/CCA/risultatiPart3/Matte/automation
python3 cli.py run batch --config experiment.yaml --policy <best-policy.yaml> --runs 3 --precache
```

If the same cluster was already warmed with `run precache`, you can omit `--precache`
from the final batch command. Keeping it is safe; it warms images once before the first
batch repetition.

For submission, collect the OpenEvolve log and checkpoint that produced the benchmarked
program. The course helper is in:

```bash
/home/carti/ETH/Msc/CCA/cloud-comp-arch-project/openevolve/openevolve_collect.py
```

## Shutdown

Delete the cluster when you are done using it so it does not keep spending cloud credits:

```bash
cd /home/carti/ETH/Msc/CCA/risultatiPart3/Matte/automation
python3 cli.py cluster down --config experiment.yaml
../../checkCredits.sh
```

```

`config.yaml`:

```yaml
max_iterations: 15
checkpoint_interval: 1

llm:
  api_base: "https://api.swissai.cscs.ch/v1"
  primary_model: "moonshotai/Kimi-K2.5"
  top_p: 0.7
  timeout: 900
  retries: 0
  max_tokens: 32000

prompt:
  system_message: |
    You are an expert cloud infrastructure scheduler. Your goal is to optimize a Kubernetes scheduling policy for Part 3 of a cloud computing assignment: minimize the total makespan of seven batch jobs while keeping a latency-critical memcached deployment below a strict 1ms p95 latency SLO at 30K QPS.

    You are editing a Python dictionary returned by get_schedule(). Only edit code inside the EVOLVE-BLOCK comments.

    Cluster:
    - node-a-8core is the 8-core 32GB RAM VM with cores 0-7.
    - node-b-4core is the 4-core 4GB RAM VM with cores 0-3.
    - The nodes differ in CPU platform and memory capacity, so a job can behave differently across nodes.
    - The batch applications use the native dataset size.

    Hardware hints:
    - node-a-8core uses Google Cloud machine type e2-standard-8: 8 vCPUs and 32GB RAM.
    - e2-standard machines can be backed by Intel or AMD EPYC processors selected at VM creation time.
    - node-b-4core uses Google Cloud machine type n2d-highcpu-4: 4 vCPUs and 4GB RAM.
    - n2d-highcpu machines have only 1GB RAM per vCPU and are AMD EPYC based.
    - Treat CPU platform and clock speed as approximate and not guaranteed; the automation captures the actual CPU platform for each run.
    - The most important practical difference is that node-b-4core has much less memory, so memory-heavy jobs can fail there.

    Required workload:
    - Schedule exactly these seven batch jobs once each: barnes, blackscholes, canneal, freqmine, radix, streamcluster, vips.
    - Also schedule memcached continuously. Memcached starts first and receives steady 30K QPS load throughout the batch run.

    Policy schema:
    - The returned dictionary must contain policy_name, memcached, and jobs.
    - memcached must contain node, cores, and threads.
    - jobs must contain one entry for each required batch job.
    - Each job entry must contain node, cores, threads, and after.

    Policy rules:
    - Each job and memcached entry must include node, cores, and threads.
    - cores must be a valid Linux taskset string, for example "0-3", "4,5,6", or "0".
    - threads must be a positive integer and cannot exceed the number of pinned cores.
    - Jobs or memcached running on the same node at the same time must not overlap pinned cores.
    - A job's after value may be "start", a single job name, or a list of job names. A list means the job starts after all listed jobs finish.
    - Keep policy_name descriptive and short.

    Hard safety rules:
    - NEVER schedule radix on node-b-4core. It crashes the 4-core 4GB VM.
    - radix.threads must be exactly one of {1, 2, 4, 8}. Radix can crash with non-power-of-two thread counts.

    Memcached SLO guidance:
    - Prefer keeping memcached on one isolated dedicated core.
    - Avoid sharing memcached's core with any batch job.
    - Avoid CPU-heavy and cache-heavy colocations near memcached when possible, because memcached is sensitive to CPU, L1, L2, and LLC interference.

    Observed heuristics from prior experiments:
    - blackscholes is the safest job near memcached and gains little beyond 4 threads.
    - streamcluster, radix, barnes, and vips scale well with more cores.
    - canneal scales poorly and is memory/cache sensitive.
    - freqmine benefits from higher thread counts but is sensitive to CPU, L1 instruction cache, and LLC contention.
    - vips and streamcluster can stress cache and memory resources, so place them carefully around memcached.

    Search strategy:
    - Exploit parallelism on node-a-8core whenever core sets do not overlap.
    - Serialize jobs that reuse the same core set by using after dependencies.
    - Use after lists when a job must wait for multiple predecessor jobs to finish.
    - Prefer schedules that keep both nodes busy while preserving memcached's SLO and avoiding known crash cases.

    Return only valid Python code for the EVOLVE-BLOCK contents. Do not include markdown fences.

database:
  population_size: 200

evaluator:
  cascade_evaluation: false
  parallel_evaluations: 1
  timeout: 5400

diff_based_evolution: false
max_code_length: 50000

```

`evaluator.py`:

```py
from __future__ import annotations

import importlib.util
import json
import subprocess
import sys
import tempfile
import traceback
from pathlib import Path
from types import ModuleType
from typing import Any

import yaml

try:
    from openevolve.evaluation_result import EvaluationResult
except ModuleNotFoundError:
    class EvaluationResult:  # type: ignore[no-redef]
        def __init__(self, *, metrics: dict[str, float], artifacts: dict[str, str] | None = None):
            self.metrics = metrics
            self.artifacts = artifacts or {}


BASE_DIR = Path(__file__).resolve().parent
AUTOMATION_DIR = BASE_DIR.parent / "automation"
TIMES_CSV_PATH = BASE_DIR.parent.parent / "Part2summary_times.csv"
EXPERIMENT_CONFIG = AUTOMATION_DIR / "experiment.yaml"
AUDIT_TIMEOUT_S = 120
RUN_TIMEOUT_S = 5200


def _result(
    *,
    combined_score: float,
    makespan_s: float = 9999.0,
    max_p95_us: float = 0.0,
    slo_violations: float = 0.0,
    status_pass: float = 0.0,
    artifacts: dict[str, str] | None = None,
    extra_metrics: dict[str, float] | None = None,
) -> EvaluationResult:
    metrics = {
        "combined_score": float(combined_score),
        "makespan_s": float(makespan_s),
        "max_p95_us": float(max_p95_us),
        "slo_violations": float(slo_violations),
        "status_pass": float(status_pass),
    }
    if extra_metrics:
        metrics.update({key: float(value) for key, value in extra_metrics.items()})
    return EvaluationResult(metrics=metrics, artifacts=artifacts or {})


def _artifact_text(value: str, *, limit: int = 20000) -> str:
    if len(value) <= limit:
        return value
    return value[:limit] + "\n...[truncated]..."


def _load_program(program_path: str) -> ModuleType:
    path = Path(program_path).resolve()
    module_name = f"evolved_schedule_{abs(hash(path))}"
    spec = importlib.util.spec_from_file_location(module_name, path)
    if spec is None or spec.loader is None:
        raise ImportError(f"Cannot import evolved program from {path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _load_schedule(program_path: str) -> dict[str, Any]:
    program = _load_program(program_path)
    get_schedule = getattr(program, "get_schedule", None)
    if not callable(get_schedule):
        raise AttributeError("Program must define callable get_schedule()")
    schedule = get_schedule()
    if not isinstance(schedule, dict):
        raise TypeError(f"get_schedule() must return a dict, got {type(schedule).__name__}")
    return schedule


def _run_command(args: list[str], *, timeout_s: int) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        args,
        cwd=str(AUTOMATION_DIR),
        capture_output=True,
        text=True,
        timeout=timeout_s,
        check=False,
    )


def _find_summary_path(run_stdout: str) -> Path | None:
    for raw_line in reversed(run_stdout.splitlines()):
        line = raw_line.strip()
        if not line:
            continue
        candidate = Path(line)
        if not candidate.is_absolute():
            candidate = AUTOMATION_DIR / candidate
        summary_path = candidate / "summary.json"
        if summary_path.exists():
            return summary_path
    return None


def _safe_float(value: Any, default: float) -> float:
    if value is None or isinstance(value, bool):
        return default
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _score_summary(summary: dict[str, Any]) -> EvaluationResult:
    status = str(summary.get("overall_status") or "unknown")
    makespan = _safe_float(summary.get("makespan_s"), 9999.0)
    max_p95 = _safe_float(summary.get("max_observed_p95_us"), 0.0)
    slo_violations = _safe_float(summary.get("slo_violations"), 0.0)

    if status == "pass":
        p95_penalty = max(0.0, max_p95 - 850.0) / 10.0
        score = 10000.0 - makespan - p95_penalty
        status_pass = 1.0
    elif status == "slo_fail":
        score = 1000.0 - makespan - (50.0 * slo_violations)
        status_pass = 0.0
    else:
        score = -1000.0
        status_pass = 0.0

    return _result(
        combined_score=score,
        makespan_s=makespan,
        max_p95_us=max_p95,
        slo_violations=slo_violations,
        status_pass=status_pass,
        artifacts={
            "overall_status": status,
            "summary": _artifact_text(json.dumps(summary, indent=2, sort_keys=True)),
        },
        extra_metrics={
            "audit_pass": 1.0,
            "run_completed": 1.0,
        },
    )


def evaluate(program_path: str) -> EvaluationResult:
    try:
        schedule = _load_schedule(program_path)
    except Exception as exc:
        return _result(
            combined_score=-10000.0,
            artifacts={
                "error_type": type(exc).__name__,
                "error_message": str(exc),
                "traceback": _artifact_text(traceback.format_exc()),
            },
            extra_metrics={"audit_pass": 0.0, "run_completed": 0.0},
        )

    with tempfile.TemporaryDirectory(prefix="openevolve-policy-") as temp_dir:
        policy_path = Path(temp_dir) / "policy.yaml"
        try:
            policy_path.write_text(yaml.safe_dump(schedule, sort_keys=False), encoding="utf-8")
        except Exception as exc:
            return _result(
                combined_score=-9000.0,
                artifacts={
                    "error_type": type(exc).__name__,
                    "error_message": f"Could not write generated policy YAML: {exc}",
                    "traceback": _artifact_text(traceback.format_exc()),
                },
                extra_metrics={"audit_pass": 0.0, "run_completed": 0.0},
            )

        audit_cmd = [
            sys.executable,
            "cli.py",
            "audit",
            "--policy",
            str(policy_path),
            "--times-csv",
            str(TIMES_CSV_PATH),
        ]
        try:
            audit_res = _run_command(audit_cmd, timeout_s=AUDIT_TIMEOUT_S)
        except subprocess.TimeoutExpired as exc:
            return _result(
                combined_score=-5000.0,
                artifacts={
                    "error_type": "AuditTimeout",
                    "error_message": str(exc),
                },
                extra_metrics={"audit_pass": 0.0, "run_completed": 0.0},
            )

        if audit_res.returncode != 0:
            return _result(
                combined_score=-5000.0,
                artifacts={
                    "audit_stdout": _artifact_text(audit_res.stdout),
                    "audit_stderr": _artifact_text(audit_res.stderr),
                    "policy_yaml": _artifact_text(policy_path.read_text(encoding="utf-8")),
                },
                extra_metrics={"audit_pass": 0.0, "run_completed": 0.0},
            )

        run_cmd = [
            sys.executable,
            "cli.py",
            "run",
            "once",
            "--config",
            str(EXPERIMENT_CONFIG),
            "--policy",
            str(policy_path),
        ]
        try:
            run_res = _run_command(run_cmd, timeout_s=RUN_TIMEOUT_S)
        except subprocess.TimeoutExpired as exc:
            return _result(
                combined_score=-2000.0,
                artifacts={
                    "error_type": "RunTimeout",
                    "error_message": str(exc),
                    "audit_stdout": _artifact_text(audit_res.stdout),
                },
                extra_metrics={"audit_pass": 1.0, "run_completed": 0.0},
            )

        if run_res.returncode != 0:
            return _result(
                combined_score=-2000.0,
                artifacts={
                    "run_stdout": _artifact_text(run_res.stdout),
                    "run_stderr": _artifact_text(run_res.stderr),
                    "audit_stdout": _artifact_text(audit_res.stdout),
                    "policy_yaml": _artifact_text(policy_path.read_text(encoding="utf-8")),
                },
                extra_metrics={"audit_pass": 1.0, "run_completed": 0.0},
            )

        summary_path = _find_summary_path(run_res.stdout)
        if summary_path is None:
            return _result(
                combined_score=-2000.0,
                artifacts={
                    "error_type": "MissingSummary",
                    "error_message": "No summary.json path could be found from the run output.",
                    "run_stdout": _artifact_text(run_res.stdout),
                    "run_stderr": _artifact_text(run_res.stderr),
                    "audit_stdout": _artifact_text(audit_res.stdout),
                },
                extra_metrics={"audit_pass": 1.0, "run_completed": 0.0},
            )

        try:
            summary = json.loads(summary_path.read_text(encoding="utf-8"))
            if not isinstance(summary, dict):
                raise TypeError("summary.json must contain a JSON object")
        except Exception as exc:
            return _result(
                combined_score=-2000.0,
                artifacts={
                    "error_type": type(exc).__name__,
                    "error_message": f"Could not parse {summary_path}: {exc}",
                    "traceback": _artifact_text(traceback.format_exc()),
                    "run_stdout": _artifact_text(run_res.stdout),
                },
                extra_metrics={"audit_pass": 1.0, "run_completed": 0.0},
            )

        result = _score_summary(summary)
        result.artifacts.update(
            {
                "summary_path": str(summary_path),
                "audit_stdout": _artifact_text(audit_res.stdout),
                "run_stdout_tail": _artifact_text("\n".join(run_res.stdout.splitlines()[-80:])),
            }
        )
        return result


def main(argv: list[str] | None = None) -> int:
    args = sys.argv[1:] if argv is None else argv
    if len(args) != 1:
        print(f"Usage: {Path(__file__).name} <program_path>", file=sys.stderr)
        return 2
    result = evaluate(args[0])
    print(
        json.dumps(
            {
                "metrics": result.metrics,
                "artifacts": result.artifacts,
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

```

`initial_program.py`:

```py
def get_schedule():
    # EVOLVE-BLOCK-START
    schedule = {
        "policy_name": "split-brain-NodeA",
        "memcached": {
            "node": "node-b-4core",
            "cores": "0",
            "threads": 1,
        },
        "jobs": {
            "canneal": {
                "node": "node-b-4core",
                "cores": "1-3",
                "threads": 3,
                "after": "start",
            },
            "barnes": {
                "node": "node-b-4core",
                "cores": "1-3",
                "threads": 3,
                "after": "canneal",
            },
            "streamcluster": {
                "node": "node-a-8core",
                "cores": "0-3",
                "threads": 4,
                "after": "start",
            },
            "radix": {
                "node": "node-a-8core",
                "cores": "0-3",
                "threads": 4,
                "after": "streamcluster",
            },
            "freqmine": {
                "node": "node-a-8core",
                "cores": "4-7",
                "threads": 4,
                "after": "start",
            },
            "vips": {
                "node": "node-a-8core",
                "cores": "4-7",
                "threads": 4,
                "after": "freqmine",
            },
            "blackscholes": {
                "node": "node-a-8core",
                "cores": "4-7",
                "threads": 4,
                "after": "vips",
            },
        },
    }
    # EVOLVE-BLOCK-END
    return schedule

```