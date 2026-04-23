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
