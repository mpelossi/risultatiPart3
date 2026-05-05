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
