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
