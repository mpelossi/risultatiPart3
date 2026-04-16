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

