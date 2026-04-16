from __future__ import annotations

import unittest
from dataclasses import dataclass
from pathlib import Path
from unittest.mock import patch

from part3.automation.cluster import NodeInfo
from part3.automation.config import (
    JobOverride,
    MemcachedConfig,
    Phase,
    PolicyConfig,
    load_experiment_config,
    load_policy_config,
)
from part3.automation.runner import ExperimentRunner
from part3.automation.tests.helpers import temp_workspace, write_json_config


BASE_POLICY = "/home/carti/ETH/Msc/CCA/part3/automation/policies/baseline.yaml"


@dataclass(frozen=True)
class JobOutcome:
    duration_s: float
    failed: bool = False


class FakeClock:
    def __init__(self) -> None:
        self.now = 0.0

    def advance(self, seconds: float) -> None:
        self.now += seconds


class FakeCluster:
    def __init__(self, clock: FakeClock, outcomes: dict[str, JobOutcome]):
        self.clock = clock
        self.outcomes = outcomes
        self.applied_job_ids: list[str] = []
        self.job_launch_times: dict[str, float] = {}
        self.job_names: dict[str, str] = {}
        self.job_run_ids: dict[str, str] = {}

    def cleanup_managed_workloads(self) -> None:
        return

    def apply_manifest(self, manifest_path: Path) -> None:
        manifest = self._parse_manifest(manifest_path)
        if manifest["kind"] != "Job":
            return
        job_id = manifest["labels"]["cca-project-job-id"]
        self.applied_job_ids.append(job_id)
        self.job_launch_times[job_id] = self.clock.now
        self.job_names[job_id] = manifest["name"]
        self.job_run_ids[job_id] = manifest["labels"]["cca-project-run-id"]

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


class FakeMeasurementRunner(ExperimentRunner):
    def __init__(self, *args, clock: FakeClock, **kwargs):
        super().__init__(*args, **kwargs)
        self.clock = clock

    def _start_measurement(self, **kwargs):  # type: ignore[override]
        return object()

    def _wait_for_measurement_start(self, handle) -> None:  # type: ignore[override]
        return

    def _wait_for_measurement_finish(self, handle) -> None:  # type: ignore[override]
        return

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


class RunnerAsyncSchedulerTests(unittest.TestCase):
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
        cluster = FakeCluster(clock, outcomes)
        runner = FakeMeasurementRunner(experiment, policy, clock=clock)
        runner.cluster = cluster
        return runner, cluster

    def _run_once(self, runner: ExperimentRunner) -> Path:
        with patch("part3.automation.runner.assert_client_provisioning"), patch(
            "part3.automation.runner.collect_live_pods"
        ), patch("part3.automation.runner.collect_describes"), patch(
            "part3.automation.runner.summarize_run",
            return_value={"overall_status": "pass"},
        ):
            return runner.run_once()

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


if __name__ == "__main__":
    unittest.main()
