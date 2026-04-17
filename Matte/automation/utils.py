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
