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
