from __future__ import annotations

import shutil
import subprocess
import threading
import time
import json
from dataclasses import dataclass
from pathlib import Path

from .catalog import JOB_CATALOG
from .cluster import ClusterController
from .collect import collect_describes, collect_live_pods, summarize_run
from .config import ExperimentConfig, Phase, PolicyConfig
from .manifests import (
    ResolvedBatchJob,
    render_batch_job_manifest,
    render_memcached_manifest,
    resolve_jobs,
    resolve_memcached,
)
from .provision import assert_client_provisioning
from .utils import append_log, ensure_directory, utc_timestamp


@dataclass
class MeasurementHandle:
    process: subprocess.Popen[str]
    reader_thread: threading.Thread
    ready_event: threading.Event


class ExperimentRunner:
    poll_interval_s = 1.0

    def __init__(self, experiment: ExperimentConfig, policy: PolicyConfig):
        self.experiment = experiment
        self.policy = policy
        self.cluster = ClusterController(experiment)

    def _create_run_dir(self) -> tuple[str, Path, Path]:
        run_id = utc_timestamp().lower()
        run_dir = ensure_directory(self.experiment.results_root / self.experiment.experiment_id / run_id)
        manifests_dir = ensure_directory(run_dir / "rendered_manifests")
        return run_id, run_dir, manifests_dir

    def _log(self, log_path: Path, message: str) -> None:
        append_log(log_path, message)
        print(message)

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
        command = (
            "bash -lc 'set -euo pipefail; "
            f"cd {self.experiment.remote_repo_dir}; "
            f"./mcperf -s {memcached_ip} --loadonly; "
            f"./mcperf -s {memcached_ip} "
            f"-a {agent_a_ip} -a {agent_b_ip} "
            "--noload "
            f"-T {measurement.measure_threads} "
            f"-C {measurement.connections} "
            f"-D {measurement.depth} "
            f"-Q {measurement.qps_interval} "
            f"-c {measurement.connections} "
            "-t 10 "
            f"--scan {measurement.scan_start}:{measurement.scan_stop}:{measurement.scan_step}'"
        )
        process = self.cluster.popen_ssh(
            measure_node.name,
            command,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
        )
        ready_event = threading.Event()

        def _reader() -> None:
            assert process.stdout is not None
            with mcperf_path.open("w", encoding="utf-8") as handle:
                for line in process.stdout:
                    handle.write(line)
                    handle.flush()
                    if line.startswith("#type"):
                        ready_event.set()
                        self._log(log_path, "mcperf measurement header observed")

        reader_thread = threading.Thread(target=_reader, daemon=True)
        reader_thread.start()
        return MeasurementHandle(process=process, reader_thread=reader_thread, ready_event=ready_event)

    def _wait_for_measurement_start(self, handle: MeasurementHandle) -> None:
        if not handle.ready_event.wait(timeout=self.experiment.measurement.max_start_wait_s):
            handle.process.terminate()
            raise TimeoutError("mcperf measurement did not become ready in time")

    def _wait_for_measurement_finish(self, handle: MeasurementHandle) -> None:
        return_code = handle.process.wait(timeout=self.experiment.measurement.completion_timeout_s)
        handle.reader_thread.join(timeout=30)
        if return_code != 0:
            raise RuntimeError(f"mcperf measurement exited with code {return_code}")

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
                self._sleep(min(self.poll_interval_s, max(deadline - self._current_time(), 0.0)))

    def run_once(self, *, dry_run: bool = False) -> Path:
        run_id, run_dir, manifests_dir = self._create_run_dir()
        log_path = run_dir / "events.log"
        self._write_policy_snapshot(run_dir)
        memcached_manifest, resolved_jobs = self._render_manifests(run_id=run_id, manifests_dir=manifests_dir)
        plan_path = run_dir / "phase_plan.json"
        plan_path.write_text(json.dumps(self._phase_plan(resolved_jobs), indent=2) + "\n", encoding="utf-8")

        if dry_run:
            self._log(log_path, f"Dry run prepared at {run_dir}")
            return run_dir

        self._log(log_path, "Cleaning previous managed workloads")
        self.cluster.cleanup_managed_workloads()
        self._log(log_path, "Checking client provisioning")
        assert_client_provisioning(self.cluster)

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

        self._log(log_path, f"Starting measurement against memcached IP {memcached_ip}")
        measurement = self._start_measurement(
            run_dir=run_dir,
            run_id=run_id,
            memcached_ip=memcached_ip,
            agent_a_ip=agent_a_ip,
            agent_b_ip=agent_b_ip,
            log_path=log_path,
        )
        self._wait_for_measurement_start(measurement)

        launched_jobs = self._run_phase_scheduler(
            run_id=run_id,
            resolved_jobs=resolved_jobs,
            manifests_dir=manifests_dir,
            log_path=log_path,
        )
        self._wait_for_measurement_finish(measurement)

        collect_live_pods(self.cluster, run_dir)
        summary = summarize_run(
            run_dir,
            experiment_id=self.experiment.experiment_id,
            policy_name=self.policy.policy_name,
            run_id=run_id,
            expected_jobs=set(JOB_CATALOG),
        )
        if summary["overall_status"] != "pass":
            collect_describes(
                self.cluster,
                run_dir,
                job_name_map={job_id: job.kubernetes_name for job_id, job in launched_jobs.items()},
                summary=summary,
            )
        self._log(log_path, f"Run completed with status {summary['overall_status']}")
        return run_dir

    def run_batch(self, runs: int, *, dry_run: bool = False) -> list[Path]:
        run_dirs: list[Path] = []
        for _ in range(runs):
            run_dirs.append(self.run_once(dry_run=dry_run))
        return run_dirs
