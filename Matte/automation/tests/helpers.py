from __future__ import annotations

import json
from pathlib import Path
from tempfile import TemporaryDirectory


def write_json_config(path: Path, payload: dict[str, object]) -> None:
    path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")


def temp_workspace() -> TemporaryDirectory[str]:
    return TemporaryDirectory(prefix="part3-tests-")

