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

