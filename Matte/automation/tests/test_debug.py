from __future__ import annotations

import copy
import io
import unittest
from contextlib import redirect_stdout
from pathlib import Path
from unittest.mock import patch

from Matte.automation import cli
from Matte.automation.cluster import ClusterController
from Matte.automation.config import ExperimentConfig, MeasurementConfig, MemcachedConfig, PolicyConfig
from Matte.automation.debug import render_debug_commands, summarize_provisioning_hints
from Matte.automation.provision import ProvisionStatus, ProvisioningError
from Matte.automation.runner import ExperimentRunner
from Matte.automation.tests.helpers import temp_workspace, write_json_config


PART3_YAML = Path("/home/carti/ETH/Msc/CCA/risultatiPart3/Matte/automation/part3.yaml")


def _experiment_config() -> ExperimentConfig:
    return ExperimentConfig(
        config_path=Path("/tmp/experiment.yaml"),
        experiment_id="demo",
        cluster_name="part3.k8s.local",
        zone="europe-west1-b",
        kops_state_store="gs://bucket",
        ssh_key_path=Path("/tmp/cloud-computing"),
        ssh_user="ubuntu",
        cluster_config_path=Path("/tmp/part3.yaml"),
        results_root=Path("/tmp/runs"),
        submission_group="054",
        memcached_name="some-memcached",
        remote_repo_dir="/opt/cca/memcache-perf-dynamic",
        measurement=MeasurementConfig(
            agent_a_threads=2,
            agent_b_threads=4,
            measure_threads=6,
            connections=4,
            depth=4,
            qps_interval=1000,
            scan_start=30000,
            scan_stop=30500,
            scan_step=5,
            max_start_wait_s=180,
            completion_timeout_s=3600,
        ),
    )


def _waiting_statuses() -> dict[str, ProvisionStatus]:
    return {
        "client-agent-a": ProvisionStatus(
            nodetype="client-agent-a",
            node_name="client-agent-a-fn6b",
            bootstrap_ready=False,
            mcperf_present=False,
            agent_service_state="not-installed",
        ),
        "client-agent-b": ProvisionStatus(
            nodetype="client-agent-b",
            node_name="client-agent-b-rw1c",
            bootstrap_ready=False,
            mcperf_present=False,
            agent_service_state="not-installed",
        ),
        "client-measure": ProvisionStatus(
            nodetype="client-measure",
            node_name="client-measure-2dll",
            bootstrap_ready=True,
            mcperf_present=True,
            agent_service_state="not-installed",
        ),
    }


def _simple_schedule_jobs() -> dict[str, object]:
    return {
        "barnes": {"after": "start", "delay_s": 0},
        "blackscholes": {"after": "start", "delay_s": 0},
        "canneal": {"after": "start", "delay_s": 0},
        "freqmine": {"after": "start", "delay_s": 0},
        "radix": {"after": "start", "delay_s": 0},
        "streamcluster": {"after": "start", "delay_s": 0},
        "vips": {"after": "start", "delay_s": 0},
    }


class FakeClusterController(ClusterController):
    def __init__(self, payload: dict[str, object]):
        super().__init__(_experiment_config())
        self.payload = copy.deepcopy(payload)

    def kubectl_json(self, *args: str) -> dict[str, object]:
        return copy.deepcopy(self.payload)


class MinimalRunnerCluster:
    def cleanup_managed_workloads(self) -> None:
        return


class DebugCommandRenderingTests(unittest.TestCase):
    def test_render_debug_commands_uses_resolved_vm_names(self) -> None:
        cluster = FakeClusterController(
            {
                "items": [
                    {"metadata": {"name": "client-agent-a-fn6b", "labels": {}}, "status": {"addresses": []}},
                    {"metadata": {"name": "client-agent-b-rw1c", "labels": {}}, "status": {"addresses": []}},
                    {"metadata": {"name": "client-measure-2dll", "labels": {}}, "status": {"addresses": []}},
                ]
            }
        )

        rendered = render_debug_commands(experiment=_experiment_config(), cluster=cluster)

        self.assertIn("client-agent-a:", rendered)
        self.assertIn("ubuntu@client-agent-a-fn6b", rendered)
        self.assertIn("ubuntu@client-agent-b-rw1c", rendered)
        self.assertIn("ubuntu@client-measure-2dll", rendered)
        self.assertIn("gcloud compute instances get-serial-port-output client-agent-a-fn6b", rendered)

    def test_render_debug_commands_includes_exact_memcached_and_mcperf_paths(self) -> None:
        cluster = FakeClusterController({"items": []})
        policy = PolicyConfig(
            config_path=Path("/tmp/policy.yaml"),
            policy_name="test-policy",
            memcached=MemcachedConfig(node="node-b-4core", cores="0", threads=1),
            job_overrides={},
            phases=[],
        )

        rendered = render_debug_commands(
            experiment=_experiment_config(),
            cluster=cluster,
            policy=policy,
            run_id="run-1",
        )

        self.assertIn("kubectl describe pod memcached-server-run-1", rendered)
        self.assertIn("kubectl logs -f memcached-server-run-1", rendered)
        self.assertIn("kubectl exec -it memcached-server-run-1 -- sh", rendered)
        self.assertIn("tail -f /tmp/runs/demo/run-1/mcperf.txt", rendered)

    def test_summarize_provisioning_hints_explains_bootstrap_failure(self) -> None:
        hints = summarize_provisioning_hints(_waiting_statuses())

        self.assertTrue(any("bootstrap appears to have failed before mcperf installation" in hint for hint in hints))
        self.assertTrue(any("No memcached pod is expected yet" in hint for hint in hints))


class FailureSurfaceTests(unittest.TestCase):
    def test_provision_check_prints_hints_and_debug_pointer(self) -> None:
        experiment = _experiment_config()
        output = io.StringIO()

        with patch("Matte.automation.cli.load_experiment_config", return_value=experiment), patch(
            "Matte.automation.cli.ClusterController"
        ), patch(
            "Matte.automation.cli.check_client_provisioning",
            return_value=_waiting_statuses(),
        ):
            with redirect_stdout(output):
                cli.main(["provision", "check", "--config", "experiment.yaml"])

        rendered = output.getvalue()
        self.assertIn("Hint: client-agent-a: bootstrap appears to have failed before mcperf installation", rendered)
        self.assertIn("Debug commands: python3 cli.py debug commands --config /tmp/experiment.yaml", rendered)

    def test_run_once_logs_debug_pointer_when_provisioning_blocks(self) -> None:
        statuses = _waiting_statuses()
        error = ProvisioningError(
            "client-agent-a is not fully bootstrapped: client-agent-a (client-agent-a-fn6b): WAITING",
            statuses=statuses,
        )
        with temp_workspace() as workspace:
            root = Path(workspace)
            experiment_path = root / "experiment.yaml"
            policy_path = root / "policy.yaml"
            write_json_config(
                experiment_path,
                {
                    "experiment_id": "demo",
                    "cluster_name": "part3.k8s.local",
                    "zone": "europe-west1-b",
                    "kops_state_store": "gs://bucket",
                    "ssh_key_path": "~/.ssh/cloud-computing",
                    "cluster_config_path": str(PART3_YAML),
                    "results_root": str(root / "runs"),
                    "submission_group": "054",
                },
            )
            write_json_config(
                policy_path,
                {
                    "policy_name": "test-policy",
                    "memcached": {"node": "node-b-4core", "cores": "0", "threads": 1},
                    "jobs": _simple_schedule_jobs(),
                },
            )
            runner = ExperimentRunner(
                cli.load_experiment_config(str(experiment_path)),
                cli.load_policy_config(str(policy_path)),
            )
            runner.cluster = MinimalRunnerCluster()

            with patch("Matte.automation.runner.utc_timestamp", return_value="20260417T010203Z"), patch(
                "Matte.automation.runner.assert_client_provisioning",
                side_effect=error,
            ), self.assertRaises(ProvisioningError):
                runner.run_once()

            events_log = (
                root / "runs" / "demo" / "20260417t010203z" / "events.log"
            ).read_text(encoding="utf-8")
            self.assertIn("Hint: client-agent-a: bootstrap appears to have failed before mcperf installation", events_log)
            self.assertIn("Debug commands: python3 cli.py debug commands --config", events_log)
            self.assertIn("--policy", events_log)
            self.assertIn("--run-id 20260417t010203z", events_log)


class BootstrapScriptTests(unittest.TestCase):
    def test_all_client_bootstrap_scripts_share_the_new_dependency_helper(self) -> None:
        payload = PART3_YAML.read_text(encoding="utf-8")

        self.assertEqual(payload.count("prepare_memcached_build_dependencies() {"), 3)
        self.assertEqual(payload.count("apt-cache showsrc memcached >/dev/null 2>&1"), 3)
        self.assertEqual(payload.count("memcached source metadata is unavailable after enabling deb-src"), 3)


if __name__ == "__main__":
    unittest.main()
