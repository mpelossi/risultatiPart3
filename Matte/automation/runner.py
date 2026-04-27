from __future__ import annotations

import shlex
import shutil
import subprocess
import threading
import time
import json
from dataclasses import dataclass, field
from pathlib import Path

from .catalog import JOB_CATALOG
from .cluster import BENCHMARK_NODETYPES, ClusterController
from .collect import collect_describes, collect_live_pods, summarize_run
from .config import ExperimentConfig, Phase, PolicyConfig, RunQueueConfig, load_policy_config
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
from .runtime_stats import rebuild_runtime_stats_file
from .utils import append_log, ensure_directory, run_id_timestamp, write_json


@dataclass
class MeasurementHandle:
    process: subprocess.Popen[str]
    reader_thread: threading.Thread
    ready_event: threading.Event
    sample_event: threading.Event
    error_event: threading.Event
    node_name: str
    remote_pid_file: str
    error_messages: list[str] = field(default_factory=list)
    stop_requested: bool = False


class ExperimentRunner:
    poll_interval_s = 1.0
    scheduler_status_interval_s = 15.0
    final_pod_metadata_wait_s = 30.0
    measurement_stop_int_grace_s = 10.0
    measurement_stop_term_grace_s = 5.0
    mcperf_agent_start_timeout_s = 20.0
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

    def _capture_node_platforms(
        self,
        *,
        run_dir: Path,
        log_path: Path,
        nodes: dict[str, object],
    ) -> dict[str, object]:
        self._log(log_path, "Capturing benchmark node CPU platforms")
        try:
            node_platforms = self.cluster.capture_benchmark_node_platforms(nodes=nodes)
        except Exception as exc:
            node_platforms = {
                "capture_status": "error",
                "zone": self.experiment.zone,
                "nodes": {},
                "errors": [str(exc)],
            }
            self._log(log_path, f"Warning: failed to capture benchmark node CPU platforms: {exc}")
        else:
            status = node_platforms.get("capture_status")
            if status == "ok":
                platforms_by_node = node_platforms.get("nodes", {})
                details: list[str] = []
                if isinstance(platforms_by_node, dict):
                    for nodetype in BENCHMARK_NODETYPES:
                        raw_info = platforms_by_node.get(nodetype)
                        if not isinstance(raw_info, dict):
                            continue
                        machine_type = raw_info.get("machine_type") or "machine n/a"
                        cpu_platform = raw_info.get("cpu_platform") or "CPU platform n/a"
                        details.append(f"{nodetype}={machine_type}/{cpu_platform}")
                suffix = ": " + ", ".join(details) if details else ""
                self._log(log_path, f"Benchmark node CPU platform capture complete{suffix}")
            else:
                errors = node_platforms.get("errors", [])
                if isinstance(errors, list) and errors:
                    error_text = "; ".join(str(error) for error in errors)
                else:
                    error_text = f"status={status}"
                self._log(log_path, f"Warning: benchmark node CPU platform capture incomplete: {error_text}")
        write_json(run_dir / "node_platforms.json", node_platforms)
        return node_platforms

    def _refresh_runtime_stats(self, *, log_path: Path) -> None:
        try:
            payload = rebuild_runtime_stats_file(self.experiment.results_root)
        except Exception as exc:
            self._log(log_path, f"Warning: failed to refresh runtime stats: {exc}")
            return
        self._log(
            log_path,
            "Runtime stats refreshed: "
            f"{payload.get('output_path')} "
            f"samples={payload.get('sample_count')} "
            f"eligible_runs={payload.get('eligible_run_count')}",
        )

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

    def _abort_measurement_start(self, handle: MeasurementHandle) -> None:
        if handle.process.poll() is not None:
            return
        handle.stop_requested = True
        try:
            self._send_measurement_signal(handle, "TERM")
        except RuntimeError:
            handle.process.terminate()
            return
        try:
            handle.process.wait(timeout=self.measurement_stop_term_grace_s)
        except subprocess.TimeoutExpired:
            handle.process.terminate()
        handle.reader_thread.join(timeout=5)

    def _mcperf_agent_is_active(self, node_name: str) -> bool:
        result = self.cluster.ssh(
            node_name,
            self._bash_lc("sudo systemctl is-active --quiet mcperf-agent.service"),
            check=False,
        )
        return result.returncode == 0

    def _mcperf_agent_diagnostics(self, node_name: str) -> str:
        script = "\n".join(
            [
                "set +e",
                'echo "--- systemctl status mcperf-agent.service ---"',
                "sudo systemctl status mcperf-agent.service --no-pager -l",
                'echo "--- journalctl -u mcperf-agent.service ---"',
                "sudo journalctl -u mcperf-agent.service -n 80 --no-pager",
                'echo "--- pgrep -a mcperf ---"',
                "pgrep -a mcperf",
                "exit 0",
            ]
        )
        result = self.cluster.ssh(node_name, self._bash_lc(script), check=False)
        return result.combined_output

    def _ensure_mcperf_agents_active(
        self,
        *,
        nodes: dict[str, object],
        log_path: Path,
    ) -> None:
        for nodetype in ("client-agent-a", "client-agent-b"):
            node = nodes.get(nodetype)
            if node is None:
                raise RuntimeError(f"Missing node for {nodetype}")
            node_name = getattr(node, "name", None)
            if not isinstance(node_name, str) or not node_name:
                raise RuntimeError(f"Missing Kubernetes node name for {nodetype}")

            self._log(log_path, f"Restarting mcperf-agent.service on {nodetype} ({node_name})")
            script = "\n".join(
                [
                    "set +e",
                    "sudo systemctl reset-failed mcperf-agent.service",
                    'reset_status="$?"',
                    "sudo systemctl restart mcperf-agent.service",
                    'restart_status="$?"',
                    'echo "reset_failed_status=$reset_status restart_status=$restart_status"',
                    'if [ "$reset_status" -ne 0 ] || [ "$restart_status" -ne 0 ]; then',
                    "  exit 1",
                    "fi",
                ]
            )
            result = self.cluster.ssh(node_name, self._bash_lc(script), check=False)
            if result.returncode != 0:
                suffix = f": {result.combined_output}" if result.combined_output else ""
                self._log(
                    log_path,
                    "Warning: mcperf-agent.service restart command returned nonzero on "
                    f"{nodetype} ({node_name}); continuing to poll active state{suffix}",
                )

            deadline = self._current_time() + self.mcperf_agent_start_timeout_s
            while self._current_time() < deadline:
                if self._mcperf_agent_is_active(node_name):
                    self._log(log_path, f"mcperf-agent.service active on {nodetype} ({node_name})")
                    break
                self._sleep(min(self.poll_interval_s, max(deadline - self._current_time(), 0.0)))
            else:
                if self._mcperf_agent_is_active(node_name):
                    self._log(log_path, f"mcperf-agent.service active on {nodetype} ({node_name})")
                    continue
                diagnostics = self._mcperf_agent_diagnostics(node_name)
                suffix = f"\n{diagnostics}" if diagnostics else ""
                raise RuntimeError(
                    f"mcperf-agent.service did not become active on {nodetype} ({node_name})"
                    f"{suffix}"
                )

    def _line_has_mcperf_sync_error(self, line: str) -> bool:
        return "sync_agent" in line or "ERROR during synchronization" in line

    def _line_looks_like_mcperf_sample(self, line: str, p95_index: int | None) -> bool:
        stripped = line.strip()
        if not stripped or stripped.startswith("#") or stripped.startswith("mcperf.cc"):
            return False
        columns = stripped.split()
        index = 12 if p95_index is None else p95_index
        if len(columns) <= index:
            return False
        try:
            float(columns[index])
        except ValueError:
            return False
        return True

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
        sample_event = threading.Event()
        error_event = threading.Event()
        error_messages: list[str] = []

        def _reader() -> None:
            assert process.stdout is not None
            sample_logged = False
            p95_index: int | None = None
            with mcperf_path.open("w", encoding="utf-8") as handle:
                for line in process.stdout:
                    handle.write(line)
                    handle.flush()
                    if self._line_has_mcperf_sync_error(line):
                        error_messages.append(line.strip())
                        error_event.set()
                    if line.startswith("#type"):
                        header = line.split()
                        if "p95" in header:
                            p95_index = header.index("p95")
                        ready_event.set()
                        self._log(log_path, "mcperf measurement header observed")
                    if self._line_looks_like_mcperf_sample(line, p95_index):
                        sample_event.set()
                        if not sample_logged:
                            sample_logged = True
                            self._log(log_path, "mcperf measurement sample observed")

        reader_thread = threading.Thread(target=_reader, daemon=True)
        reader_thread.start()
        return MeasurementHandle(
            process=process,
            reader_thread=reader_thread,
            ready_event=ready_event,
            sample_event=sample_event,
            error_event=error_event,
            error_messages=error_messages,
            node_name=measure_node.name,
            remote_pid_file=remote_pid_file,
        )

    def _wait_for_measurement_start(self, handle: MeasurementHandle) -> None:
        deadline = time.monotonic() + self.experiment.measurement.max_start_wait_s
        while time.monotonic() < deadline:
            if handle.error_event.is_set():
                self._abort_measurement_start(handle)
                details = "; ".join(message for message in handle.error_messages if message)
                suffix = f": {details}" if details else ""
                raise RuntimeError(f"mcperf measurement failed during agent synchronization{suffix}")
            if handle.sample_event.wait(timeout=0.5):
                return
            return_code = handle.process.poll()
            if return_code is not None:
                handle.reader_thread.join(timeout=5)
                if handle.error_event.is_set():
                    details = "; ".join(message for message in handle.error_messages if message)
                    suffix = f": {details}" if details else ""
                    raise RuntimeError(f"mcperf measurement failed during agent synchronization{suffix}")
                raise RuntimeError(f"mcperf measurement exited before the first sample with code {return_code}")
        self._abort_measurement_start(handle)
        raise TimeoutError("mcperf measurement did not produce a latency sample in time")

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
        node_platforms = self._capture_node_platforms(run_dir=run_dir, log_path=log_path, nodes=nodes)
        self._ensure_mcperf_agents_active(nodes=nodes, log_path=log_path)

        self._log(log_path, f"Starting measurement against memcached IP {memcached_ip}")
        measurement = self._start_measurement(
            run_dir=run_dir,
            run_id=run_id,
            memcached_ip=memcached_ip,
            agent_a_ip=agent_a_ip,
            agent_b_ip=agent_b_ip,
            log_path=log_path,
        )
        self._log(log_path, "Waiting for first mcperf measurement sample")
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
            node_platforms=node_platforms,
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
        self._refresh_runtime_stats(log_path=log_path)
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


def run_policy_queue(
    experiment: ExperimentConfig,
    queue: RunQueueConfig,
    *,
    dry_run: bool = False,
    precache: bool = False,
) -> list[Path]:
    if dry_run and precache:
        raise ValueError("--precache cannot be combined with --dry-run")
    run_dirs: list[Path] = []
    entry_label = "entry" if len(queue.entries) == 1 else "entries"
    print(f"Starting queue {queue.queue_name} with {len(queue.entries)} {entry_label}")
    precache_consumed = False
    for index, entry in enumerate(queue.entries, start=1):
        policy = load_policy_config(str(entry.policy_path))
        runner = ExperimentRunner(experiment, policy)
        entry_precache = precache and not precache_consumed
        print(
            "Starting queue entry "
            f"{index}/{len(queue.entries)}: {entry.policy_path} ({entry.runs} run(s))"
        )
        if entry.runs == 1:
            entry_run_dirs = [runner.run_once(dry_run=dry_run, precache=entry_precache)]
        else:
            entry_run_dirs = runner.run_batch(entry.runs, dry_run=dry_run, precache=entry_precache)
        run_dirs.extend(entry_run_dirs)
        if precache and not dry_run:
            precache_consumed = True
        print(
            "Finished queue entry "
            f"{index}/{len(queue.entries)}: {entry.policy_path} ({len(entry_run_dirs)} run(s))"
        )
    print("Queue complete")
    return run_dirs
