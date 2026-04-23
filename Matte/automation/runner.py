from __future__ import annotations

import shlex
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
from .debug import format_debug_command_hint, summarize_provisioning_hints
from .manifests import (
    ResolvedBatchJob,
    ResolvedPrecachePod,
    render_batch_job_manifest,
    render_memcached_manifest,
    render_precache_pod_manifest,
    resolve_jobs,
    resolve_memcached,
    resolve_precache_pods,
)
from .provision import ProvisioningError, assert_client_provisioning
from .utils import append_log, ensure_directory, run_id_timestamp


@dataclass
class MeasurementHandle:
    process: subprocess.Popen[str]
    reader_thread: threading.Thread
    ready_event: threading.Event
    node_name: str
    remote_pid_file: str
    stop_requested: bool = False


class ExperimentRunner:
    poll_interval_s = 1.0
    scheduler_status_interval_s = 15.0
    final_pod_metadata_wait_s = 30.0
    measurement_stop_int_grace_s = 10.0
    measurement_stop_term_grace_s = 5.0
    precache_completion_timeout_s = 900
    precache_cleanup_timeout_s = 120

    def __init__(self, experiment: ExperimentConfig, policy: PolicyConfig):
        self.experiment = experiment
        self.policy = policy
        self.cluster = ClusterController(experiment)

    def _create_run_dir(self) -> tuple[str, Path, Path]:
        experiment_root = ensure_directory(self.experiment.results_root / self.experiment.experiment_id)
        base_run_id = run_id_timestamp()
        run_id = base_run_id
        suffix = 2
        run_dir = experiment_root / run_id
        while run_dir.exists():
            run_id = f"{base_run_id}-{suffix:02d}"
            run_dir = experiment_root / run_id
            suffix += 1
        run_dir = ensure_directory(run_dir)
        manifests_dir = ensure_directory(run_dir / "rendered_manifests")
        return run_id, run_dir, manifests_dir

    def _log(self, log_path: Path, message: str) -> None:
        append_log(log_path, message)
        print(message)

    def _log_run_prefix(self, run_id: str, message: str) -> None:
        print(f"[run {run_id}] {message}")

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

    def _render_precache_manifests(
        self,
        *,
        run_id: str,
        manifests_dir: Path,
    ) -> tuple[tuple[Path, ...], tuple[ResolvedPrecachePod, ...]]:
        precache_pods = resolve_precache_pods(run_id)
        manifest_paths: list[Path] = []
        for pod in precache_pods:
            manifest_path = manifests_dir / f"{pod.kubernetes_name}.yaml"
            manifest_path.write_text(
                render_precache_pod_manifest(
                    pod,
                    experiment_id=self.experiment.experiment_id,
                    run_id=run_id,
                ),
                encoding="utf-8",
            )
            manifest_paths.append(manifest_path)
        return tuple(manifest_paths), precache_pods

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

    def _bash_lc(self, script: str) -> str:
        return f"bash -lc {shlex.quote(script)}"

    def _measurement_pid_file(self, run_id: str) -> str:
        return f"/tmp/cca-mcperf-{run_id}.pid"

    def _precache_selector(self, run_id: str) -> str:
        return f"cca-project-role=precache,cca-project-precache-run={run_id}"

    def _cleanup_precache_manifests(
        self,
        *,
        manifest_paths: tuple[Path, ...],
        selector: str,
        log_path: Path,
    ) -> None:
        for manifest_path in manifest_paths:
            self.cluster.delete_manifest(manifest_path)
        self.cluster.wait_for_pods_deleted(selector, timeout_s=self.precache_cleanup_timeout_s)
        self._log(log_path, "Pre-cache pods deleted")

    def _precache_images(
        self,
        *,
        run_id: str,
        manifests_dir: Path,
        log_path: Path,
    ) -> None:
        manifest_paths, precache_pods = self._render_precache_manifests(run_id=run_id, manifests_dir=manifests_dir)
        selector = self._precache_selector(run_id)
        expected_names = {pod.kubernetes_name for pod in precache_pods}
        image_count = len(precache_pods[0].images) if precache_pods else 0
        self._log(
            log_path,
            f"Pre-caching {image_count} images on benchmark nodes via {len(precache_pods)} transient pod(s)",
        )
        primary_error: Exception | None = None
        try:
            for manifest_path in manifest_paths:
                self.cluster.apply_manifest(manifest_path)
            self.cluster.wait_for_pods_completion(
                selector,
                expected_names=expected_names,
                timeout_s=self.precache_completion_timeout_s,
            )
            self._log(log_path, "Pre-cache pods completed successfully")
        except Exception as exc:
            primary_error = exc
            raise
        finally:
            try:
                self._cleanup_precache_manifests(
                    manifest_paths=manifest_paths,
                    selector=selector,
                    log_path=log_path,
                )
            except Exception as exc:
                self._log(log_path, f"Warning: failed to clean up pre-cache pods: {exc}")
                if primary_error is None:
                    raise

    def _send_measurement_signal(self, handle: MeasurementHandle, signal_name: str) -> None:
        if handle.process.poll() is not None:
            return
        script = "\n".join(
            [
                "set -euo pipefail",
                f"pid_file={shlex.quote(handle.remote_pid_file)}",
                'if [ ! -s "$pid_file" ]; then',
                "  exit 0",
                "fi",
                'pid="$(cat "$pid_file")"',
                'if kill -0 "$pid" >/dev/null 2>&1; then',
                f'  kill -{signal_name} "$pid"',
                "fi",
            ]
        )
        result = self.cluster.ssh(handle.node_name, self._bash_lc(script), check=False)
        if result.returncode != 0:
            raise RuntimeError(
                f"Failed to send SIG{signal_name} to the mcperf measurement wrapper: {result.combined_output}"
            )

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
        remote_pid_file = self._measurement_pid_file(run_id)
        load_command = shlex.join(["./mcperf", "-s", memcached_ip, "--loadonly"])
        scan_command = shlex.join(
            [
                "./mcperf",
                "-s",
                memcached_ip,
                "-a",
                agent_a_ip,
                "-a",
                agent_b_ip,
                "--noload",
                "-T",
                str(measurement.measure_threads),
                "-C",
                str(measurement.connections),
                "-D",
                str(measurement.depth),
                "-Q",
                str(measurement.qps_interval),
                "-c",
                str(measurement.connections),
                "-t",
                "10",
                "--scan",
                f"{measurement.scan_start}:{measurement.scan_stop}:{measurement.scan_step}",
            ]
        )
        script = "\n".join(
            [
                "set -euo pipefail",
                f"cd {shlex.quote(self.experiment.remote_repo_dir)}",
                f"pid_file={shlex.quote(remote_pid_file)}",
                'child_pid=""',
                "stop_requested=0",
                'rm -f "$pid_file"',
                "cleanup() {",
                '  rm -f "$pid_file"',
                "}",
                "forward_stop() {",
                '  stop_requested=1',
                '  signal_name="$1"',
                '  if [ -n "${child_pid:-}" ] && kill -0 "$child_pid" >/dev/null 2>&1; then',
                '    kill "-$signal_name" "$child_pid" >/dev/null 2>&1 || true',
                "  fi",
                "}",
                "trap cleanup EXIT",
                "trap 'forward_stop INT' INT",
                "trap 'forward_stop TERM' TERM",
                load_command,
                'echo "$$" > "$pid_file"',
                "set +e",
                f"{scan_command} &",
                'child_pid="$!"',
                "while true; do",
                '  wait "$child_pid"',
                '  status="$?"',
                '  if [ "$stop_requested" -eq 1 ] && kill -0 "$child_pid" >/dev/null 2>&1; then',
                "    continue",
                "  fi",
                "  break",
                "done",
                "set -e",
                'if [ "$stop_requested" -eq 1 ]; then',
                "  exit 0",
                "fi",
                'if [ "$status" -eq 130 ] || [ "$status" -eq 143 ]; then',
                "  exit 0",
                "fi",
                'exit "$status"',
            ]
        )
        process = self.cluster.popen_ssh(
            measure_node.name,
            self._bash_lc(script),
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
        return MeasurementHandle(
            process=process,
            reader_thread=reader_thread,
            ready_event=ready_event,
            node_name=measure_node.name,
            remote_pid_file=remote_pid_file,
        )

    def _wait_for_measurement_start(self, handle: MeasurementHandle) -> None:
        if not handle.ready_event.wait(timeout=self.experiment.measurement.max_start_wait_s):
            handle.process.terminate()
            raise TimeoutError("mcperf measurement did not become ready in time")

    def _wait_for_measurement_finish(self, handle: MeasurementHandle, *, timeout_s: float | None = None) -> None:
        timeout = self.experiment.measurement.completion_timeout_s if timeout_s is None else timeout_s
        try:
            return_code = handle.process.wait(timeout=timeout)
        except subprocess.TimeoutExpired as exc:
            raise TimeoutError(f"mcperf measurement did not finish within {timeout:.1f}s") from exc
        handle.reader_thread.join(timeout=30)
        if return_code != 0 and not handle.stop_requested:
            raise RuntimeError(f"mcperf measurement exited with code {return_code}")

    def _stop_measurement(self, handle: MeasurementHandle, *, log_path: Path) -> None:
        if handle.process.poll() is not None:
            return
        handle.stop_requested = True
        self._log(log_path, f"Stopping mcperf measurement wrapper on {handle.node_name} with SIGINT")
        self._send_measurement_signal(handle, "INT")
        try:
            self._wait_for_measurement_finish(handle, timeout_s=self.measurement_stop_int_grace_s)
            return
        except TimeoutError:
            self._log(
                log_path,
                f"mcperf is still running on {handle.node_name} after SIGINT; escalating to SIGTERM",
            )
        self._send_measurement_signal(handle, "TERM")
        try:
            self._wait_for_measurement_finish(handle, timeout_s=self.measurement_stop_term_grace_s)
        except TimeoutError as exc:
            raise TimeoutError("mcperf measurement did not stop after SIGINT/SIGTERM") from exc

    def _jobs_missing_termination_metadata(
        self,
        payload: dict[str, object],
        *,
        expected_job_ids: set[str],
    ) -> list[str]:
        terminated_job_ids: set[str] = set()
        for item in payload.get("items", []):
            metadata = item.get("metadata", {})
            labels = metadata.get("labels", {})
            job_id = labels.get("cca-project-job-id")
            if not isinstance(job_id, str) or job_id not in expected_job_ids:
                continue
            container_status = (item.get("status", {}).get("containerStatuses") or [{}])[0]
            terminated = container_status.get("state", {}).get("terminated", {})
            if terminated.get("startedAt") and terminated.get("finishedAt"):
                terminated_job_ids.add(job_id)
        return sorted(expected_job_ids - terminated_job_ids)

    def _wait_for_final_job_pod_metadata(
        self,
        *,
        run_id: str,
        expected_job_ids: set[str],
        log_path: Path,
    ) -> None:
        if not expected_job_ids:
            return
        self._log(log_path, "Waiting briefly for final pod termination metadata")
        deadline = self._current_time() + self.final_pod_metadata_wait_s
        while True:
            payload = self.cluster.get_run_pods_payload(run_id)
            missing_job_ids = self._jobs_missing_termination_metadata(
                payload,
                expected_job_ids=expected_job_ids,
            )
            if not missing_job_ids:
                return
            now = self._current_time()
            if now >= deadline:
                self._log(
                    log_path,
                    "Proceeding with pod snapshot even though termination metadata is still missing for: "
                    + ", ".join(missing_job_ids),
                )
                return
            self._sleep(min(self.poll_interval_s, max(deadline - now, 0.0)))

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
        next_status_log_at = self._current_time()

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
                if now >= next_status_log_at:
                    running_jobs = sorted(
                        job_name
                        for job_name, info in snapshot.items()
                        if info.get("status") == "running"
                    )
                    pending_phases = [
                        phase.phase_id for phase in phases if phase.phase_id not in launched_phase_ids
                    ]
                    self._log(
                        log_path,
                        "Scheduler heartbeat: "
                        f"launched_phases={sorted(launched_phase_ids)} "
                        f"pending_phases={pending_phases} "
                        f"completed_jobs={sorted(completed_jobs)} "
                        f"running_jobs={running_jobs}",
                    )
                    next_status_log_at = now + self.scheduler_status_interval_s
                self._sleep(min(self.poll_interval_s, max(deadline - self._current_time(), 0.0)))

    def run_once(self, *, dry_run: bool = False, precache: bool = False) -> Path:
        if dry_run and precache:
            raise ValueError("--precache cannot be combined with --dry-run")
        run_id, run_dir, manifests_dir = self._create_run_dir()
        log_path = run_dir / "events.log"
        self._log_run_prefix(run_id, f"Preparing run in {run_dir}")
        self._write_policy_snapshot(run_dir)
        memcached_manifest, resolved_jobs = self._render_manifests(run_id=run_id, manifests_dir=manifests_dir)
        plan_path = run_dir / "phase_plan.json"
        plan_path.write_text(json.dumps(self._phase_plan(resolved_jobs), indent=2) + "\n", encoding="utf-8")
        self._log(log_path, f"Run directory prepared: {run_dir}")
        self._log(log_path, f"Rendered {1 + len(resolved_jobs)} manifests into {manifests_dir}")

        if dry_run:
            self._log(log_path, f"Dry run prepared at {run_dir}")
            return run_dir

        self._log(log_path, "Cleaning previous managed workloads")
        self.cluster.cleanup_managed_workloads()
        self._log(log_path, "Ensuring canonical node labels and checking client provisioning")
        try:
            assert_client_provisioning(self.cluster)
        except ProvisioningError as exc:
            for status in exc.statuses.values():
                self._log(log_path, str(status))
            for hint in summarize_provisioning_hints(exc.statuses):
                self._log(log_path, f"Hint: {hint}")
            self._log(
                log_path,
                "Debug commands: "
                + format_debug_command_hint(
                    config_path=self.experiment.config_path,
                    policy_path=self.policy.config_path,
                    run_id=run_id,
                ),
            )
            raise
        except RuntimeError:
            self._log(
                log_path,
                "Debug commands: "
                + format_debug_command_hint(
                    config_path=self.experiment.config_path,
                    policy_path=self.policy.config_path,
                    run_id=run_id,
                ),
            )
            raise

        if precache:
            self._precache_images(run_id=run_id, manifests_dir=manifests_dir, log_path=log_path)

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
        self._log(log_path, "Waiting for mcperf measurement header")
        self._wait_for_measurement_start(measurement)
        self._log(log_path, "mcperf measurement is live")

        self._log(log_path, "Starting phase scheduler")
        launched_jobs = self._run_phase_scheduler(
            run_id=run_id,
            resolved_jobs=resolved_jobs,
            manifests_dir=manifests_dir,
            log_path=log_path,
        )
        self._log(log_path, "All batch jobs completed; capturing results.json and stopping mcperf")
        self._wait_for_final_job_pod_metadata(
            run_id=run_id,
            expected_job_ids=set(launched_jobs),
            log_path=log_path,
        )
        collect_live_pods(self.cluster, run_dir)
        self._stop_measurement(measurement, log_path=log_path)
        self._wait_for_measurement_finish(measurement)

        self._log(log_path, "Summarizing run from captured pod snapshot and mcperf output")
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
        self._log(
            log_path,
            "Run completed with status "
            f"{summary['overall_status']} makespan={summary.get('makespan_s')} "
            f"max_p95_us={summary.get('max_observed_p95_us')}",
        )
        return run_dir

    def run_batch(self, runs: int, *, dry_run: bool = False, precache: bool = False) -> list[Path]:
        if dry_run and precache:
            raise ValueError("--precache cannot be combined with --dry-run")
        run_dirs: list[Path] = []
        print(f"Starting batch of {runs} run(s)")
        for index in range(1, runs + 1):
            print(f"Starting run {index}/{runs}")
            run_dir = self.run_once(dry_run=dry_run, precache=precache and index == 1)
            run_dirs.append(run_dir)
            print(f"Finished run {index}/{runs}: {run_dir}")
        print("Batch complete")
        return run_dirs
