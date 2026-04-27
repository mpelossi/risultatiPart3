from __future__ import annotations

import json
import unittest
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch

from Matte.automation.catalog import JOB_CATALOG
from Matte.automation.cluster import NodeInfo
from Matte.automation.config import (
    JobOverride,
    MemcachedConfig,
    Phase,
    PolicyConfig,
    load_experiment_config,
    load_policy_config,
)
from Matte.automation.runner import ExperimentRunner
from Matte.automation.tests.helpers import temp_workspace, write_json_config
from Matte.automation.utils import CommandResult


BASE_POLICY = "/home/carti/ETH/Msc/CCA/risultatiPart3/Matte/automation/schedule.yaml"


@dataclass(frozen=True)
class JobOutcome:
    duration_s: float
    failed: bool = False


@dataclass
class FakeMeasurementHandle:
    started_at_s: float
    stopped: bool = False


class FakeClock:
    def __init__(self) -> None:
        self.now = 0.0

    def advance(self, seconds: float) -> None:
        self.now += seconds


class FakeCluster:
    def __init__(
        self,
        clock: FakeClock,
        outcomes: dict[str, JobOutcome],
        *,
        pod_metadata_delay_s: float = 0.0,
    ):
        self.clock = clock
        self.outcomes = outcomes
        self.pod_metadata_delay_s = pod_metadata_delay_s
        self.applied_job_ids: list[str] = []
        self.job_launch_times: dict[str, float] = {}
        self.job_names: dict[str, str] = {}
        self.job_run_ids: dict[str, str] = {}
        self.memcached_name: str | None = None
        self.memcached_run_id: str | None = None
        self.precache_pod_names: set[str] = set()
        self.precache_wait_calls: list[tuple[str, tuple[str, ...], int]] = []
        self.precache_deleted_selectors: list[tuple[str, int]] = []
        self.precache_wait_error: Exception | None = None
        self.node_platforms_error: Exception | None = None
        self.node_platform_capture_calls: list[dict[str, NodeInfo] | None] = []
        self.node_platforms_payload: dict[str, object] = {
            "capture_status": "ok",
            "zone": "europe-west1-b",
            "nodes": {
                "node-a-8core": {
                    "capture_status": "ok",
                    "node_type": "node-a-8core",
                    "node_name": "node-a-8core-node",
                    "machine_type": "e2-standard-8",
                    "cpu_platform": "Intel Broadwell",
                },
                "node-b-4core": {
                    "capture_status": "ok",
                    "node_type": "node-b-4core",
                    "node_name": "node-b-4core-node",
                    "machine_type": "n2d-highcpu-4",
                    "cpu_platform": "AMD Milan",
                },
            },
            "errors": [],
        }
        self.base_time = datetime(2026, 1, 1, tzinfo=timezone.utc)

    def cleanup_managed_workloads(self) -> None:
        return

    def apply_manifest(self, manifest_path: Path) -> None:
        manifest = self._parse_manifest(manifest_path)
        if manifest["kind"] == "Pod" and manifest["labels"].get("cca-project-role") == "memcached":
            self.memcached_name = manifest["name"]
            self.memcached_run_id = manifest["labels"]["cca-project-run-id"]
            return
        if manifest["kind"] == "Pod" and manifest["labels"].get("cca-project-role") == "precache":
            self.precache_pod_names.add(manifest["name"])
            return
        if manifest["kind"] != "Job":
            return
        job_id = manifest["labels"]["cca-project-job-id"]
        self.applied_job_ids.append(job_id)
        self.job_launch_times[job_id] = self.clock.now
        self.job_names[job_id] = manifest["name"]
        self.job_run_ids[job_id] = manifest["labels"]["cca-project-run-id"]

    def delete_manifest(self, manifest_path: Path) -> None:
        manifest = self._parse_manifest(manifest_path)
        self.precache_pod_names.discard(manifest["name"])

    def wait_for_pods_completion(
        self,
        selector: str,
        *,
        expected_names: set[str],
        timeout_s: int = 600,
    ) -> None:
        self.precache_wait_calls.append((selector, tuple(sorted(expected_names)), timeout_s))
        if self.precache_wait_error is not None:
            raise self.precache_wait_error

    def wait_for_pods_deleted(self, selector: str, *, timeout_s: int = 120) -> None:
        self.precache_deleted_selectors.append((selector, timeout_s))
        return

    def wait_for_pod_ready(self, pod_name: str, timeout_s: int = 300) -> None:
        return

    def get_pod_by_run_role(self, run_id: str, role: str) -> dict[str, object]:
        return {"status": {"podIP": "10.0.0.10"}}

    def discover_nodes(self) -> dict[str, NodeInfo]:
        return {
            "client-agent-a": NodeInfo("client-agent-a-node", "client-agent-a", "10.0.0.11", None),
            "client-agent-b": NodeInfo("client-agent-b-node", "client-agent-b", "10.0.0.12", None),
            "client-measure": NodeInfo("client-measure-node", "client-measure", "10.0.0.13", None),
        }

    def capture_benchmark_node_platforms(
        self,
        *,
        nodes: dict[str, NodeInfo] | None = None,
    ) -> dict[str, object]:
        self.node_platform_capture_calls.append(nodes)
        if self.node_platforms_error is not None:
            raise self.node_platforms_error
        return json.loads(json.dumps(self.node_platforms_payload))

    def get_run_jobs_snapshot(self, run_id: str) -> dict[str, dict[str, object]]:
        snapshots: dict[str, dict[str, object]] = {}
        for job_id, launch_time in self.job_launch_times.items():
            if self.job_run_ids[job_id] != run_id:
                continue
            outcome = self.outcomes[job_id]
            if self.clock.now - launch_time >= outcome.duration_s:
                status = {"failed": 1} if outcome.failed else {"succeeded": 1}
                state = "failed" if outcome.failed else "completed"
            else:
                status = {"active": 1}
                state = "running"
            snapshots[self.job_names[job_id]] = {
                "status": state,
                "payload": {"metadata": {"name": self.job_names[job_id]}, "status": status},
            }
        return snapshots

    def _format_time(self, seconds: float) -> str:
        instant = self.base_time + timedelta(seconds=seconds)
        return instant.strftime("%Y-%m-%dT%H:%M:%SZ")

    def _build_pods_payload(self, run_id: str | None = None) -> dict[str, object]:
        items: list[dict[str, object]] = []
        if self.memcached_name is not None and self.memcached_run_id is not None:
            if run_id is None or self.memcached_run_id == run_id:
                items.append(
                    {
                        "metadata": {
                            "name": self.memcached_name,
                            "labels": {
                                "cca-project-role": "memcached",
                                "cca-project-run-id": self.memcached_run_id,
                            },
                        },
                        "spec": {"nodeName": "node-b-4core-node"},
                        "status": {
                            "phase": "Running",
                            "podIP": "10.0.0.10",
                            "containerStatuses": [
                                {
                                    "name": "memcached",
                                    "state": {"running": {"startedAt": self._format_time(0.0)}},
                                }
                            ],
                        },
                    }
                )

        for index, job_id in enumerate(sorted(self.job_launch_times)):
            if run_id is not None and self.job_run_ids[job_id] != run_id:
                continue
            launch_time = self.job_launch_times[job_id]
            outcome = self.outcomes[job_id]
            finish_time = launch_time + outcome.duration_s
            metadata_visible_at = finish_time + self.pod_metadata_delay_s
            labels = {
                "cca-project-run-id": self.job_run_ids[job_id],
                "cca-project-job-id": job_id,
            }
            container_state: dict[str, object]
            phase: str
            if self.clock.now >= metadata_visible_at:
                container_state = {
                    "terminated": {
                        "startedAt": self._format_time(launch_time),
                        "finishedAt": self._format_time(finish_time),
                        "exitCode": 1 if outcome.failed else 0,
                    }
                }
                phase = "Failed" if outcome.failed else "Succeeded"
            else:
                container_state = {"running": {"startedAt": self._format_time(launch_time)}}
                phase = "Running"
            items.append(
                {
                    "metadata": {
                        "name": f"{self.job_names[job_id]}-pod",
                        "labels": labels,
                    },
                    "spec": {"nodeName": f"node-{index}"},
                    "status": {
                        "phase": phase,
                        "podIP": f"10.0.1.{index + 10}",
                        "containerStatuses": [
                            {
                                "name": f"parsec-{job_id}",
                                "state": container_state,
                            }
                        ],
                    },
                }
            )
        return {"items": items}

    def get_run_pods_payload(self, run_id: str) -> dict[str, object]:
        return self._build_pods_payload(run_id)

    def capture_pods_json(self, destination: Path) -> None:
        destination.write_text(json.dumps(self._build_pods_payload(), indent=2), encoding="utf-8")

    def _parse_manifest(self, manifest_path: Path) -> dict[str, object]:
        kind = ""
        name = manifest_path.stem
        labels: dict[str, str] = {}
        in_metadata = False
        in_labels = False
        for line in manifest_path.read_text(encoding="utf-8").splitlines():
            if line.startswith("kind: "):
                kind = line.split(":", 1)[1].strip()
            if line == "metadata:":
                in_metadata = True
                in_labels = False
                continue
            if in_metadata and line == "spec:":
                in_metadata = False
                in_labels = False
                continue
            if not in_metadata:
                continue
            if line.startswith("  name: "):
                name = line.split(":", 1)[1].strip()
            elif line.startswith("  labels:"):
                in_labels = True
            elif in_labels and line.startswith("    "):
                key, value = line.strip().split(":", 1)
                labels[key] = value.strip().strip('"')
            elif line.startswith("  ") and not line.startswith("    "):
                in_labels = False
        return {"kind": kind, "name": name, "labels": labels}


class FakeAgentGateCluster:
    def __init__(
        self,
        active_sequences: dict[str, list[bool]],
        *,
        restart_returncodes: dict[str, int] | None = None,
    ):
        self.active_sequences = active_sequences
        self.restart_returncodes = restart_returncodes or {}
        self.active_checks: dict[str, int] = {}
        self.restart_calls: list[str] = []
        self.commands: list[tuple[str, str]] = []

    def ssh(self, node_name: str, command: str, *, check: bool = True) -> CommandResult:
        self.commands.append((node_name, command))
        if "systemctl is-active --quiet mcperf-agent.service" in command:
            count = self.active_checks.get(node_name, 0)
            self.active_checks[node_name] = count + 1
            sequence = self.active_sequences.get(node_name, [True])
            is_active = sequence[min(count, len(sequence) - 1)]
            return CommandResult(
                args=[],
                returncode=0 if is_active else 3,
                stdout="active\n" if is_active else "inactive\n",
                stderr="",
            )
        if "systemctl restart mcperf-agent.service" in command:
            self.restart_calls.append(node_name)
            returncode = self.restart_returncodes.get(node_name, 0)
            return CommandResult(
                args=[],
                returncode=returncode,
                stdout=f"restart_returncode={returncode}\n",
                stderr="",
            )
        if "systemctl status mcperf-agent.service" in command and "journalctl" in command:
            return CommandResult(
                args=[],
                returncode=0,
                stdout=(
                    "--- systemctl status mcperf-agent.service ---\n"
                    "status output\n"
                    "--- journalctl -u mcperf-agent.service ---\n"
                    "journal output\n"
                    "--- pgrep -a mcperf ---\n"
                    "pgrep output\n"
                ),
                stderr="",
            )
        return CommandResult(args=[], returncode=0, stdout="", stderr="")


class FakeMeasurementRunner(ExperimentRunner):
    def __init__(
        self,
        *args,
        clock: FakeClock,
        measurement_finish_s: float = 120.0,
        measurement_shutdown_s: float = 0.25,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.clock = clock
        self.measurement_finish_s = measurement_finish_s
        self.measurement_shutdown_s = measurement_shutdown_s
        self.measurement_events: list[tuple[str, float]] = []
        self.agent_gate_events: list[float] = []

    def _ensure_mcperf_agents_active(self, *, nodes: dict[str, object], log_path: Path) -> None:  # type: ignore[override]
        self.agent_gate_events.append(self.clock.now)

    def _start_measurement(self, *, run_dir: Path, **kwargs):  # type: ignore[override]
        (run_dir / "mcperf.txt").write_text("#type p95\nread 500\n", encoding="utf-8")
        self.measurement_events.append(("start", self.clock.now))
        return FakeMeasurementHandle(started_at_s=self.clock.now)

    def _wait_for_measurement_start(self, handle) -> None:  # type: ignore[override]
        return

    def _stop_measurement(self, handle, *, log_path: Path) -> None:  # type: ignore[override]
        self.measurement_events.append(("stop", self.clock.now))
        handle.stopped = True

    def _wait_for_measurement_finish(self, handle, *, timeout_s: float | None = None) -> None:  # type: ignore[override]
        self.measurement_events.append(("finish", self.clock.now))
        if handle.stopped:
            self.clock.advance(self.measurement_shutdown_s)
            return
        remaining = max(self.measurement_finish_s - self.clock.now, 0.0)
        if timeout_s is not None and remaining > timeout_s:
            self.clock.advance(timeout_s)
            raise TimeoutError(f"mcperf measurement did not finish within {timeout_s:.1f}s")
        self.clock.advance(remaining)

    def _current_time(self) -> float:
        return self.clock.now

    def _sleep(self, seconds: float) -> None:
        self.clock.advance(seconds)


class RunnerDryRunTests(unittest.TestCase):
    def test_dry_run_creates_plan_and_manifests(self) -> None:
        with temp_workspace() as workspace:
            root = Path(workspace)
            experiment_path = root / "experiment.yaml"
            write_json_config(
                experiment_path,
                {
                    "experiment_id": "demo",
                    "cluster_name": "part3.k8s.local",
                    "zone": "europe-west1-b",
                    "kops_state_store": "gs://bucket",
                    "ssh_key_path": "~/.ssh/cloud-computing",
                    "cluster_config_path": "/home/carti/ETH/Msc/CCA/part3/part3.yaml",
                    "results_root": str(root / "runs"),
                    "submission_group": "054",
                },
            )
            runner = ExperimentRunner(load_experiment_config(str(experiment_path)), load_policy_config(BASE_POLICY))
            run_dir = runner.run_once(dry_run=True)
            self.assertTrue((run_dir / "phase_plan.json").exists())
            self.assertTrue((run_dir / "rendered_manifests" / "memcached.yaml").exists())
            self.assertTrue((run_dir / "rendered_manifests" / "barnes.yaml").exists())

    def test_dry_run_uses_human_readable_run_id(self) -> None:
        with temp_workspace() as workspace:
            root = Path(workspace)
            experiment_path = root / "experiment.yaml"
            write_json_config(
                experiment_path,
                {
                    "experiment_id": "demo",
                    "cluster_name": "part3.k8s.local",
                    "zone": "europe-west1-b",
                    "kops_state_store": "gs://bucket",
                    "ssh_key_path": "~/.ssh/cloud-computing",
                    "cluster_config_path": "/home/carti/ETH/Msc/CCA/part3/part3.yaml",
                    "results_root": str(root / "runs"),
                    "submission_group": "054",
                },
            )
            runner = ExperimentRunner(load_experiment_config(str(experiment_path)), load_policy_config(BASE_POLICY))

            with patch("Matte.automation.runner.run_id_timestamp", return_value="2026-04-23-04h12m41s"):
                run_dir = runner.run_once(dry_run=True)

            self.assertEqual(run_dir.name, "2026-04-23-04h12m41s")

    def test_dry_run_appends_suffix_when_same_second_run_dir_exists(self) -> None:
        with temp_workspace() as workspace:
            root = Path(workspace)
            experiment_path = root / "experiment.yaml"
            write_json_config(
                experiment_path,
                {
                    "experiment_id": "demo",
                    "cluster_name": "part3.k8s.local",
                    "zone": "europe-west1-b",
                    "kops_state_store": "gs://bucket",
                    "ssh_key_path": "~/.ssh/cloud-computing",
                    "cluster_config_path": "/home/carti/ETH/Msc/CCA/part3/part3.yaml",
                    "results_root": str(root / "runs"),
                    "submission_group": "054",
                },
            )
            existing = root / "runs" / "demo" / "2026-04-23-04h12m41s"
            existing.mkdir(parents=True)
            runner = ExperimentRunner(load_experiment_config(str(experiment_path)), load_policy_config(BASE_POLICY))

            with patch("Matte.automation.runner.run_id_timestamp", return_value="2026-04-23-04h12m41s"):
                run_dir = runner.run_once(dry_run=True)

            self.assertEqual(run_dir.name, "2026-04-23-04h12m41s-02")

    def test_dry_run_rejects_precache(self) -> None:
        with temp_workspace() as workspace:
            root = Path(workspace)
            experiment_path = root / "experiment.yaml"
            write_json_config(
                experiment_path,
                {
                    "experiment_id": "demo",
                    "cluster_name": "part3.k8s.local",
                    "zone": "europe-west1-b",
                    "kops_state_store": "gs://bucket",
                    "ssh_key_path": "~/.ssh/cloud-computing",
                    "cluster_config_path": "/home/carti/ETH/Msc/CCA/part3/part3.yaml",
                    "results_root": str(root / "runs"),
                    "submission_group": "054",
                },
            )
            runner = ExperimentRunner(load_experiment_config(str(experiment_path)), load_policy_config(BASE_POLICY))

            with self.assertRaisesRegex(ValueError, "--precache"):
                runner.run_once(dry_run=True, precache=True)


class RunnerAsyncSchedulerTests(unittest.TestCase):
    def _agent_nodes(self) -> dict[str, NodeInfo]:
        return {
            "client-agent-a": NodeInfo("client-agent-a-node", "client-agent-a", "10.0.0.11", None),
            "client-agent-b": NodeInfo("client-agent-b-node", "client-agent-b", "10.0.0.12", None),
        }

    def _run_real_agent_gate(self, runner: ExperimentRunner, *, log_path: Path) -> None:
        ExperimentRunner._ensure_mcperf_agents_active(runner, nodes=self._agent_nodes(), log_path=log_path)

    def _write_experiment(self, root: Path):
        experiment_path = root / "experiment.yaml"
        write_json_config(
            experiment_path,
            {
                "experiment_id": "demo",
                "cluster_name": "part3.k8s.local",
                "zone": "europe-west1-b",
                "kops_state_store": "gs://bucket",
                "ssh_key_path": "~/.ssh/cloud-computing",
                "cluster_config_path": "/home/carti/ETH/Msc/CCA/part3/part3.yaml",
                "results_root": str(root / "runs"),
                "submission_group": "054",
                "mcperf_measurement": {
                    "completion_timeout_s": 30,
                },
            },
        )
        return load_experiment_config(str(experiment_path))

    def _write_policy_placeholder(self, root: Path) -> Path:
        policy_path = root / "policy.yaml"
        write_json_config(policy_path, {"policy_name": "test-policy"})
        return policy_path

    def _build_runner(
        self,
        root: Path,
        *,
        phases: list[Phase],
        outcomes: dict[str, JobOutcome],
        job_overrides: dict[str, JobOverride] | None = None,
        pod_metadata_delay_s: float = 0.0,
        measurement_finish_s: float = 120.0,
    ) -> tuple[FakeMeasurementRunner, FakeCluster]:
        experiment = self._write_experiment(root)
        policy = PolicyConfig(
            config_path=self._write_policy_placeholder(root),
            policy_name="test-policy",
            memcached=MemcachedConfig(node="node-b-4core", cores="0", threads=1),
            job_overrides=job_overrides or {},
            phases=phases,
        )
        clock = FakeClock()
        cluster = FakeCluster(clock, outcomes, pod_metadata_delay_s=pod_metadata_delay_s)
        runner = FakeMeasurementRunner(
            experiment,
            policy,
            clock=clock,
            measurement_finish_s=measurement_finish_s,
        )
        runner.cluster = cluster
        return runner, cluster

    def test_mcperf_agent_gate_restarts_agents_even_when_active(self) -> None:
        with temp_workspace() as workspace:
            root = Path(workspace)
            runner, _cluster = self._build_runner(root, phases=[], outcomes={})
            agent_cluster = FakeAgentGateCluster(
                {
                    "client-agent-a-node": [True],
                    "client-agent-b-node": [True],
                }
            )
            runner.cluster = agent_cluster  # type: ignore[assignment]
            log_path = root / "events.log"

            self._run_real_agent_gate(runner, log_path=log_path)

            self.assertEqual(agent_cluster.restart_calls, ["client-agent-a-node", "client-agent-b-node"])
            events_log = log_path.read_text(encoding="utf-8")
            self.assertIn("Restarting mcperf-agent.service on client-agent-a", events_log)
            self.assertIn("Restarting mcperf-agent.service on client-agent-b", events_log)
            self.assertIn("mcperf-agent.service active on client-agent-a", events_log)
            self.assertIn("mcperf-agent.service active on client-agent-b", events_log)

    def test_mcperf_agent_gate_restarts_inactive_agent_and_accepts_active(self) -> None:
        with temp_workspace() as workspace:
            root = Path(workspace)
            runner, _cluster = self._build_runner(root, phases=[], outcomes={})
            agent_cluster = FakeAgentGateCluster(
                {
                    "client-agent-a-node": [False, True],
                    "client-agent-b-node": [True],
                }
            )
            runner.cluster = agent_cluster  # type: ignore[assignment]
            log_path = root / "events.log"

            self._run_real_agent_gate(runner, log_path=log_path)

            self.assertEqual(agent_cluster.restart_calls, ["client-agent-a-node", "client-agent-b-node"])
            events_log = log_path.read_text(encoding="utf-8")
            self.assertIn("Restarting mcperf-agent.service on client-agent-a", events_log)
            self.assertIn("mcperf-agent.service active on client-agent-a", events_log)

    def test_mcperf_agent_gate_tolerates_restart_error_if_agent_becomes_active(self) -> None:
        with temp_workspace() as workspace:
            root = Path(workspace)
            runner, _cluster = self._build_runner(root, phases=[], outcomes={})
            agent_cluster = FakeAgentGateCluster(
                {
                    "client-agent-a-node": [False, True],
                    "client-agent-b-node": [True],
                },
                restart_returncodes={"client-agent-a-node": 1},
            )
            runner.cluster = agent_cluster  # type: ignore[assignment]
            log_path = root / "events.log"

            self._run_real_agent_gate(runner, log_path=log_path)

            self.assertEqual(agent_cluster.restart_calls, ["client-agent-a-node", "client-agent-b-node"])
            self.assertIn("Warning: mcperf-agent.service restart command returned nonzero", log_path.read_text())

    def test_mcperf_agent_gate_reports_diagnostics_when_agent_never_becomes_active(self) -> None:
        with temp_workspace() as workspace:
            root = Path(workspace)
            runner, _cluster = self._build_runner(root, phases=[], outcomes={})
            runner.mcperf_agent_start_timeout_s = 2.0
            agent_cluster = FakeAgentGateCluster(
                {
                    "client-agent-a-node": [False],
                    "client-agent-b-node": [True],
                }
            )
            runner.cluster = agent_cluster  # type: ignore[assignment]

            with self.assertRaises(RuntimeError) as raised:
                self._run_real_agent_gate(runner, log_path=root / "events.log")

            message = str(raised.exception)
            self.assertIn("mcperf-agent.service did not become active on client-agent-a", message)
            self.assertIn("systemctl status mcperf-agent.service", message)
            self.assertIn("journalctl -u mcperf-agent.service", message)
            self.assertIn("pgrep -a mcperf", message)
            self.assertEqual(agent_cluster.restart_calls, ["client-agent-a-node"])

    def _run_once(self, runner: ExperimentRunner, *, precache: bool = False) -> Path:
        with patch("Matte.automation.runner.assert_client_provisioning"), patch(
            "Matte.automation.runner.collect_describes"
        ), patch(
            "Matte.automation.runner.summarize_run",
            return_value={"overall_status": "pass"},
        ):
            return runner.run_once(precache=precache)

    def _run_once_with_real_summary(self, runner: ExperimentRunner, *, precache: bool = False) -> Path:
        with patch("Matte.automation.runner.assert_client_provisioning"), patch(
            "Matte.automation.runner.collect_describes"
        ):
            return runner.run_once(precache=precache)

    def test_later_phase_can_launch_before_earlier_blocked_phase(self) -> None:
        with temp_workspace() as workspace:
            root = Path(workspace)
            runner, cluster = self._build_runner(
                root,
                phases=[
                    Phase("p1", "start", (), 0, ("streamcluster", "blackscholes")),
                    Phase("p2", "jobs_complete", ("streamcluster",), 0, ("canneal",)),
                    Phase("p3", "jobs_complete", ("blackscholes",), 0, ("freqmine",)),
                ],
                outcomes={
                    "streamcluster": JobOutcome(10),
                    "blackscholes": JobOutcome(2),
                    "canneal": JobOutcome(1),
                    "freqmine": JobOutcome(1),
                },
            )

            run_dir = self._run_once(runner)

            self.assertEqual(cluster.applied_job_ids, ["streamcluster", "blackscholes", "freqmine", "canneal"])
            events_log = (run_dir / "events.log").read_text(encoding="utf-8")
            self.assertIn("Phase dependency satisfied for p3: blackscholes", events_log)
            self.assertIn("Job completed: parsec-blackscholes", events_log)

    def test_phase_dependency_waits_for_every_job_in_referenced_phase(self) -> None:
        with temp_workspace() as workspace:
            root = Path(workspace)
            runner, cluster = self._build_runner(
                root,
                phases=[
                    Phase("p1", "start", (), 0, ("blackscholes", "freqmine")),
                    Phase("p2", "phase:p1", (), 0, ("barnes",)),
                ],
                outcomes={
                    "blackscholes": JobOutcome(2),
                    "freqmine": JobOutcome(5),
                    "barnes": JobOutcome(1),
                },
            )

            self._run_once(runner)

            self.assertEqual(cluster.job_launch_times["barnes"], 5.0)

    def test_split_core_follow_up_can_start_while_other_half_is_still_busy(self) -> None:
        with temp_workspace() as workspace:
            root = Path(workspace)
            runner, cluster = self._build_runner(
                root,
                phases=[
                    Phase("p1", "start", (), 0, ("blackscholes", "barnes")),
                    Phase("p2", "jobs_complete", ("blackscholes",), 0, ("freqmine",)),
                ],
                outcomes={
                    "blackscholes": JobOutcome(2),
                    "barnes": JobOutcome(10),
                    "freqmine": JobOutcome(1),
                },
                job_overrides={
                    "blackscholes": JobOverride(node="node-a-8core", cores="0-3", threads=4),
                    "barnes": JobOverride(node="node-a-8core", cores="4-7", threads=4),
                    "freqmine": JobOverride(node="node-a-8core", cores="0-3", threads=4),
                },
            )

            self._run_once(runner)

            self.assertEqual(cluster.job_launch_times["blackscholes"], 0.0)
            self.assertEqual(cluster.job_launch_times["barnes"], 0.0)
            self.assertEqual(cluster.job_launch_times["freqmine"], 2.0)
            self.assertLess(cluster.job_launch_times["freqmine"], cluster.job_launch_times["barnes"] + 10.0)

    def test_failed_job_aborts_before_dependent_phase_launches(self) -> None:
        with temp_workspace() as workspace:
            root = Path(workspace)
            runner, cluster = self._build_runner(
                root,
                phases=[
                    Phase("p1", "start", (), 0, ("blackscholes", "streamcluster")),
                    Phase("p2", "jobs_complete", ("blackscholes",), 0, ("freqmine",)),
                ],
                outcomes={
                    "blackscholes": JobOutcome(2, failed=True),
                    "streamcluster": JobOutcome(10),
                    "freqmine": JobOutcome(1),
                },
            )

            with self.assertRaisesRegex(RuntimeError, "blackscholes"):
                self._run_once(runner)

            self.assertEqual(cluster.applied_job_ids, ["blackscholes", "streamcluster"])

    def test_run_once_stops_measurement_after_batch_completion(self) -> None:
        with temp_workspace() as workspace:
            root = Path(workspace)
            runner, _cluster = self._build_runner(
                root,
                phases=[Phase("p1", "start", (), 0, ("blackscholes", "freqmine"))],
                outcomes={
                    "blackscholes": JobOutcome(2),
                    "freqmine": JobOutcome(3),
                },
                measurement_finish_s=120,
            )

            self._run_once(runner)

            self.assertEqual([event for event, _time in runner.measurement_events], ["start", "stop", "finish"])
            self.assertLess(runner.clock.now, 10.0)

    def test_run_once_waits_for_final_pod_metadata_before_capture(self) -> None:
        with temp_workspace() as workspace:
            root = Path(workspace)
            runner, _cluster = self._build_runner(
                root,
                phases=[Phase("p1", "start", (), 0, ("blackscholes",))],
                outcomes={"blackscholes": JobOutcome(2)},
                pod_metadata_delay_s=3.0,
            )
            capture_times: list[float] = []

            def _capture_side_effect(cluster, run_dir):
                capture_times.append(runner.clock.now)
                return None

            with patch("Matte.automation.runner.assert_client_provisioning"), patch(
                "Matte.automation.runner.collect_live_pods",
                side_effect=_capture_side_effect,
            ), patch("Matte.automation.runner.collect_describes"), patch(
                "Matte.automation.runner.summarize_run",
                return_value={"overall_status": "pass"},
            ):
                runner.run_once()

            self.assertEqual(len(capture_times), 1)
            self.assertGreaterEqual(capture_times[0], 5.0)

    def test_run_once_precaches_before_memcached_and_jobs(self) -> None:
        with temp_workspace() as workspace:
            root = Path(workspace)
            runner, cluster = self._build_runner(
                root,
                phases=[Phase("p1", "start", (), 0, ("blackscholes",))],
                outcomes={"blackscholes": JobOutcome(1)},
            )

            self._run_once(runner, precache=True)

            self.assertEqual(len(cluster.precache_wait_calls), 1)
            self.assertTrue(cluster.precache_deleted_selectors)
            self.assertEqual(cluster.memcached_name is not None, True)
            self.assertEqual(cluster.applied_job_ids, ["blackscholes"])
            self.assertEqual(cluster.precache_pod_names, set())

    def test_run_once_precache_failure_aborts_before_memcached(self) -> None:
        with temp_workspace() as workspace:
            root = Path(workspace)
            runner, cluster = self._build_runner(
                root,
                phases=[Phase("p1", "start", (), 0, ("blackscholes",))],
                outcomes={"blackscholes": JobOutcome(1)},
            )
            cluster.precache_wait_error = RuntimeError("Image pull failed for pod/precache")

            with self.assertRaisesRegex(RuntimeError, "Image pull failed"):
                self._run_once(runner, precache=True)

            self.assertIsNone(cluster.memcached_name)
            self.assertEqual(cluster.applied_job_ids, [])
            self.assertTrue(cluster.precache_deleted_selectors)

    def test_run_batch_precaches_only_before_first_run(self) -> None:
        with temp_workspace() as workspace:
            root = Path(workspace)
            runner, cluster = self._build_runner(
                root,
                phases=[Phase("p1", "start", (), 0, ("blackscholes",))],
                outcomes={"blackscholes": JobOutcome(1)},
            )

            with patch("Matte.automation.runner.assert_client_provisioning"), patch(
                "Matte.automation.runner.collect_describes"
            ), patch(
                "Matte.automation.runner.summarize_run",
                return_value={"overall_status": "pass"},
            ):
                run_dirs = runner.run_batch(2, precache=True)

            self.assertEqual(len(run_dirs), 2)
            self.assertEqual(len(cluster.precache_wait_calls), 1)

    def test_run_batch_checks_mcperf_agents_before_every_measurement(self) -> None:
        with temp_workspace() as workspace:
            root = Path(workspace)
            runner, _cluster = self._build_runner(
                root,
                phases=[Phase("p1", "start", (), 0, ("blackscholes",))],
                outcomes={"blackscholes": JobOutcome(1)},
            )

            with patch("Matte.automation.runner.assert_client_provisioning"), patch(
                "Matte.automation.runner.collect_describes"
            ), patch(
                "Matte.automation.runner.summarize_run",
                return_value={"overall_status": "pass"},
            ):
                run_dirs = runner.run_batch(2)

            self.assertEqual(len(run_dirs), 2)
            self.assertEqual(len(runner.agent_gate_events), 2)
            self.assertLessEqual(runner.agent_gate_events[0], runner.measurement_events[0][1])
            self.assertLessEqual(runner.agent_gate_events[1], runner.measurement_events[3][1])

    def test_intentional_measurement_shutdown_still_summarizes_as_pass(self) -> None:
        with temp_workspace() as workspace:
            root = Path(workspace)
            all_jobs = tuple(sorted(JOB_CATALOG))
            runner, _cluster = self._build_runner(
                root,
                phases=[Phase("p1", "start", (), 0, all_jobs)],
                outcomes={job_id: JobOutcome(1) for job_id in all_jobs},
            )

            run_dir = self._run_once_with_real_summary(runner)

            summary = json.loads((run_dir / "summary.json").read_text(encoding="utf-8"))
            self.assertEqual(summary["overall_status"], "pass")
            self.assertEqual(summary["measurement_status"], "ok")

    def test_real_run_writes_results_json(self) -> None:
        with temp_workspace() as workspace:
            root = Path(workspace)
            runner, _cluster = self._build_runner(
                root,
                phases=[Phase("p1", "start", (), 0, ("blackscholes",))],
                outcomes={"blackscholes": JobOutcome(1)},
            )

            run_dir = self._run_once_with_real_summary(runner)

            self.assertTrue((run_dir / "results.json").exists())

    def test_real_run_writes_node_platforms_artifact_and_summary(self) -> None:
        with temp_workspace() as workspace:
            root = Path(workspace)
            runner, cluster = self._build_runner(
                root,
                phases=[Phase("p1", "start", (), 0, ("blackscholes",))],
                outcomes={"blackscholes": JobOutcome(1)},
            )

            run_dir = self._run_once_with_real_summary(runner)

            artifact = json.loads((run_dir / "node_platforms.json").read_text(encoding="utf-8"))
            summary = json.loads((run_dir / "summary.json").read_text(encoding="utf-8"))
            self.assertEqual(artifact["capture_status"], "ok")
            self.assertEqual(artifact["nodes"]["node-b-4core"]["cpu_platform"], "AMD Milan")
            self.assertEqual(summary["node_platforms"], artifact)
            self.assertEqual(len(cluster.node_platform_capture_calls), 1)

    def test_node_platform_capture_failure_is_diagnostic(self) -> None:
        with temp_workspace() as workspace:
            root = Path(workspace)
            runner, cluster = self._build_runner(
                root,
                phases=[Phase("p1", "start", (), 0, ("blackscholes",))],
                outcomes={"blackscholes": JobOutcome(1)},
            )
            cluster.node_platforms_error = RuntimeError("gcloud unavailable")

            run_dir = self._run_once_with_real_summary(runner)

            artifact = json.loads((run_dir / "node_platforms.json").read_text(encoding="utf-8"))
            summary = json.loads((run_dir / "summary.json").read_text(encoding="utf-8"))
            self.assertEqual(artifact["capture_status"], "error")
            self.assertIn("gcloud unavailable", artifact["errors"][0])
            self.assertEqual(summary["node_platforms"], artifact)
            self.assertIn(
                "Warning: failed to capture benchmark node CPU platforms",
                (run_dir / "events.log").read_text(encoding="utf-8"),
            )


if __name__ == "__main__":
    unittest.main()
