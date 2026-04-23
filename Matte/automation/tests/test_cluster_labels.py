from __future__ import annotations

import copy
import json
import unittest
from pathlib import Path
from unittest.mock import patch

from Matte.automation.cluster import ClusterController
from Matte.automation.config import ExperimentConfig, MeasurementConfig
from Matte.automation.utils import CommandResult


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


class FakeClusterController(ClusterController):
    def __init__(self, payload: dict[str, object]):
        super().__init__(_experiment_config())
        self.payload = copy.deepcopy(payload)
        self.label_calls: list[tuple[str, ...]] = []

    def kubectl_json(self, *args: str) -> dict[str, object]:
        self.last_kubectl_json_args = args
        return copy.deepcopy(self.payload)

    def kubectl(self, *args: str, check: bool = True) -> CommandResult:
        if args[:2] == ("label", "nodes"):
            node_name = args[2]
            label_assignment = args[3]
            key, value = label_assignment.split("=", 1)
            for item in self.payload["items"]:
                metadata = item.setdefault("metadata", {})
                if metadata.get("name") != node_name:
                    continue
                metadata.setdefault("labels", {})[key] = value
                break
            self.label_calls.append(args)
            return CommandResult(args=list(args), returncode=0, stdout="", stderr="")
        raise AssertionError(f"Unexpected kubectl call: {args}")


class RetryingClusterController(ClusterController):
    def __init__(self, responses: list[CommandResult]):
        super().__init__(_experiment_config())
        self.responses = list(responses)
        self.calls: list[tuple[str, ...]] = []

    def kubectl(self, *args: str, check: bool = True) -> CommandResult:
        self.calls.append(args)
        if not self.responses:
            raise AssertionError("No more fake kubectl responses configured")
        return self.responses.pop(0)


class ClusterLabelRepairTests(unittest.TestCase):
    def test_discover_nodes_infers_canonical_nodetype_from_randomized_name(self) -> None:
        cluster = FakeClusterController(
            {
                "items": [
                    {
                        "metadata": {"name": "client-agent-a-fn6b", "labels": {}},
                        "status": {
                            "addresses": [
                                {"type": "InternalIP", "address": "10.0.16.5"},
                                {"type": "ExternalIP", "address": "35.189.215.31"},
                            ]
                        },
                    },
                    {
                        "metadata": {"name": "node-a-8core-7jrx", "labels": {}},
                        "status": {
                            "addresses": [
                                {"type": "InternalIP", "address": "10.0.16.8"},
                            ]
                        },
                    },
                ]
            }
        )

        nodes = cluster.discover_nodes()

        self.assertEqual(nodes["client-agent-a"].name, "client-agent-a-fn6b")
        self.assertEqual(nodes["client-agent-a"].internal_ip, "10.0.16.5")
        self.assertEqual(nodes["node-a-8core"].name, "node-a-8core-7jrx")
        self.assertEqual(nodes["node-a-8core"].internal_ip, "10.0.16.8")

    def test_ensure_canonical_node_labels_repairs_unlabeled_nodes(self) -> None:
        cluster = FakeClusterController(
            {
                "items": [
                    {
                        "metadata": {"name": "client-agent-a-fn6b", "labels": {}},
                        "status": {"addresses": []},
                    },
                    {
                        "metadata": {"name": "client-agent-b-rw1c", "labels": {}},
                        "status": {"addresses": []},
                    },
                    {
                        "metadata": {
                            "name": "node-b-4core-h3sc",
                            "labels": {"cca-project-nodetype": "node-b-4core"},
                        },
                        "status": {"addresses": []},
                    },
                ]
            }
        )

        nodes = cluster.ensure_canonical_node_labels()

        self.assertEqual(nodes["client-agent-a"].name, "client-agent-a-fn6b")
        self.assertEqual(nodes["client-agent-b"].name, "client-agent-b-rw1c")
        self.assertEqual(
            cluster.label_calls,
            [
                (
                    "label",
                    "nodes",
                    "client-agent-a-fn6b",
                    "cca-project-nodetype=client-agent-a",
                    "--overwrite",
                ),
                (
                    "label",
                    "nodes",
                    "client-agent-b-rw1c",
                    "cca-project-nodetype=client-agent-b",
                    "--overwrite",
                ),
            ],
        )

    def test_kubectl_json_retries_transient_connectivity_failures(self) -> None:
        cluster = RetryingClusterController(
            [
                CommandResult(
                    args=["kubectl", "get", "jobs", "-o", "json"],
                    returncode=1,
                    stdout="",
                    stderr="Unable to connect to the server: dial tcp 34.77.122.98:443: connect: network is unreachable",
                ),
                CommandResult(
                    args=["kubectl", "get", "jobs", "-o", "json"],
                    returncode=0,
                    stdout=json.dumps({"items": []}),
                    stderr="",
                ),
            ]
        )

        with patch("Matte.automation.cluster.time.sleep"):
            payload = cluster.kubectl_json("get", "jobs", "-o", "json")

        self.assertEqual(payload, {"items": []})
        self.assertEqual(cluster.calls, [("get", "jobs", "-o", "json"), ("get", "jobs", "-o", "json")])

    def test_kubectl_json_raises_clear_error_after_retry_budget(self) -> None:
        attempts = ClusterController.kubectl_read_retry_attempts
        cluster = RetryingClusterController(
            [
                CommandResult(
                    args=["kubectl", "get", "jobs", "-o", "json"],
                    returncode=1,
                    stdout="",
                    stderr="Unable to connect to the server: dial tcp 34.77.122.98:443: connect: network is unreachable",
                )
                for _ in range(attempts)
            ]
        )

        with patch("Matte.automation.cluster.time.sleep"):
            with self.assertRaisesRegex(RuntimeError, "Kubernetes API connectivity was lost"):
                cluster.kubectl_json("get", "jobs", "-o", "json")


if __name__ == "__main__":
    unittest.main()
