from __future__ import annotations

import unittest
from pathlib import Path
from unittest.mock import patch

from Matte.automation.catalog import JOB_CATALOG, NODE_A
from Matte.automation.config import load_experiment_config, load_run_queue_config
from Matte.automation.runner import run_policy_queue
from Matte.automation.tests.helpers import temp_workspace, write_json_config


def _write_experiment(root: Path):
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
    return load_experiment_config(str(experiment_path))


def _write_policy(path: Path, policy_name: str) -> None:
    write_json_config(
        path,
        {
            "policy_name": policy_name,
            "memcached": {"node": NODE_A, "cores": "0", "threads": 1},
            "jobs": {
                job_id: {
                    "node": NODE_A,
                    "cores": "0-7",
                    "threads": min(8, entry.default_threads),
                    "after": "start",
                }
                for job_id, entry in JOB_CATALOG.items()
            },
        },
    )


class QueueRunnerTests(unittest.TestCase):
    def test_run_policy_queue_executes_entries_in_order(self) -> None:
        with temp_workspace() as workspace:
            root = Path(workspace)
            schedules_dir = root / "schedules"
            schedules_dir.mkdir()
            _write_policy(schedules_dir / "schedule1.yaml", "candidate-1")
            _write_policy(schedules_dir / "schedule2.yaml", "candidate-2")
            _write_policy(schedules_dir / "schedule3.yaml", "candidate-3")
            queue_path = root / "schedule_queue.yaml"
            write_json_config(
                queue_path,
                {
                    "queue_name": "candidates",
                    "entries": [
                        {"policy": "schedules/schedule1.yaml", "runs": 1},
                        {"policy": "schedules/schedule2.yaml", "runs": 3},
                        {"policy": "schedules/schedule3.yaml", "runs": 1},
                    ],
                },
            )
            experiment = _write_experiment(root)
            queue = load_run_queue_config(str(queue_path))
            calls: list[tuple[str, str, int, bool, bool]] = []

            class FakeRunner:
                def __init__(self, _experiment, policy):
                    self.policy = policy

                def run_once(self, *, dry_run: bool = False, precache: bool = False) -> Path:
                    calls.append((self.policy.policy_name, "once", 1, dry_run, precache))
                    return root / f"{self.policy.policy_name}-once"

                def run_batch(self, runs: int, *, dry_run: bool = False, precache: bool = False) -> list[Path]:
                    calls.append((self.policy.policy_name, "batch", runs, dry_run, precache))
                    return [root / f"{self.policy.policy_name}-{index}" for index in range(runs)]

            with patch("Matte.automation.runner.ExperimentRunner", FakeRunner):
                run_dirs = run_policy_queue(experiment, queue, precache=True)

            self.assertEqual(
                calls,
                [
                    ("candidate-1", "once", 1, False, True),
                    ("candidate-2", "batch", 3, False, False),
                    ("candidate-3", "once", 1, False, False),
                ],
            )
            self.assertEqual(len(run_dirs), 5)

    def test_run_policy_queue_stops_on_runner_exception(self) -> None:
        with temp_workspace() as workspace:
            root = Path(workspace)
            schedules_dir = root / "schedules"
            schedules_dir.mkdir()
            _write_policy(schedules_dir / "schedule1.yaml", "candidate-1")
            _write_policy(schedules_dir / "schedule2.yaml", "candidate-2")
            _write_policy(schedules_dir / "schedule3.yaml", "candidate-3")
            queue_path = root / "schedule_queue.yaml"
            write_json_config(
                queue_path,
                {
                    "entries": [
                        {"policy": "schedules/schedule1.yaml"},
                        {"policy": "schedules/schedule2.yaml"},
                        {"policy": "schedules/schedule3.yaml"},
                    ],
                },
            )
            experiment = _write_experiment(root)
            queue = load_run_queue_config(str(queue_path))
            calls: list[str] = []

            class FakeRunner:
                def __init__(self, _experiment, policy):
                    self.policy = policy

                def run_once(self, *, dry_run: bool = False, precache: bool = False) -> Path:
                    calls.append(self.policy.policy_name)
                    if self.policy.policy_name == "candidate-2":
                        raise RuntimeError("runner failed")
                    return root / self.policy.policy_name

                def run_batch(self, runs: int, *, dry_run: bool = False, precache: bool = False) -> list[Path]:
                    raise AssertionError("unexpected batch call")

            with patch("Matte.automation.runner.ExperimentRunner", FakeRunner):
                with self.assertRaisesRegex(RuntimeError, "runner failed"):
                    run_policy_queue(experiment, queue)

            self.assertEqual(calls, ["candidate-1", "candidate-2"])


if __name__ == "__main__":
    unittest.main()
