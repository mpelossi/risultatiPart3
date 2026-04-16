from __future__ import annotations

import unittest
from pathlib import Path

from part3.automation.config import load_experiment_config, load_policy_config
from part3.automation.tests.helpers import temp_workspace, write_json_config


class ConfigTests(unittest.TestCase):
    def test_experiment_config_resolves_relative_paths(self) -> None:
        with temp_workspace() as workspace:
            root = Path(workspace)
            config_path = root / "experiment.yaml"
            write_json_config(
                config_path,
                {
                    "experiment_id": "demo",
                    "cluster_name": "part3.k8s.local",
                    "zone": "europe-west1-b",
                    "kops_state_store": "gs://bucket",
                    "ssh_key_path": "~/.ssh/cloud-computing",
                    "cluster_config_path": "part3.yaml",
                    "results_root": "runs",
                    "submission_group": "054",
                },
            )
            config = load_experiment_config(str(config_path))
            self.assertEqual(config.experiment_id, "demo")
            self.assertEqual(config.results_root, (root / "runs").resolve())

    def test_policy_rejects_invalid_phase_dependency(self) -> None:
        with temp_workspace() as workspace:
            path = Path(workspace) / "policy.yaml"
            write_json_config(
                path,
                {
                    "policy_name": "bad",
                    "memcached": {"node": "node-b-4core", "cores": "0", "threads": 1},
                    "job_overrides": {},
                    "phases": [
                        {
                            "id": "phase-1",
                            "after": "phase:phase-1",
                            "delay_s": 0,
                            "launch": [
                                "barnes",
                                "blackscholes",
                                "canneal",
                                "freqmine",
                                "radix",
                                "streamcluster",
                                "vips",
                            ],
                        }
                    ],
                },
            )
            with self.assertRaises(ValueError):
                load_policy_config(str(path))

    def test_policy_rejects_invalid_core_set(self) -> None:
        with temp_workspace() as workspace:
            path = Path(workspace) / "policy.yaml"
            write_json_config(
                path,
                {
                    "policy_name": "bad-cores",
                    "memcached": {"node": "node-b-4core", "cores": "0", "threads": 1},
                    "job_overrides": {"blackscholes": {"node": "node-b-4core", "cores": "7-9"}},
                    "phases": [
                        {"id": "p1", "after": "start", "delay_s": 0, "launch": ["barnes", "blackscholes"]},
                        {"id": "p2", "after": "jobs_complete", "jobs_complete": ["barnes"], "delay_s": 0, "launch": ["canneal"]},
                        {"id": "p3", "after": "jobs_complete", "jobs_complete": ["canneal"], "delay_s": 0, "launch": ["freqmine"]},
                        {"id": "p4", "after": "jobs_complete", "jobs_complete": ["freqmine"], "delay_s": 0, "launch": ["radix"]},
                        {"id": "p5", "after": "jobs_complete", "jobs_complete": ["radix"], "delay_s": 0, "launch": ["streamcluster"]},
                        {"id": "p6", "after": "jobs_complete", "jobs_complete": ["streamcluster"], "delay_s": 0, "launch": ["vips"]},
                    ],
                },
            )
            with self.assertRaises(ValueError):
                load_policy_config(str(path))

    def test_simple_schedule_is_compiled_into_policy(self) -> None:
        with temp_workspace() as workspace:
            path = Path(workspace) / "schedule.yaml"
            write_json_config(
                path,
                {
                    "policy_name": "simple-schedule",
                    "memcached": {"node": "node-b-4core", "cores": "0", "threads": 1},
                    "jobs": {
                        "streamcluster": {
                            "node": "node-a-8core",
                            "cores": "0-7",
                            "threads": 8,
                            "after": "start",
                        },
                        "blackscholes": {
                            "node": "node-b-4core",
                            "cores": "1-3",
                            "threads": 3,
                            "after": "start",
                        },
                        "freqmine": {
                            "node": "node-b-4core",
                            "cores": "1-3",
                            "threads": 3,
                            "after": "blackscholes",
                        },
                        "canneal": {
                            "node": "node-a-8core",
                            "cores": "0-7",
                            "threads": 8,
                            "after": "streamcluster",
                        },
                        "barnes": {
                            "node": "node-a-8core",
                            "cores": "0-7",
                            "threads": 8,
                            "after": "canneal",
                        },
                        "vips": {
                            "node": "node-a-8core",
                            "cores": "0-7",
                            "threads": 8,
                            "after": "barnes",
                        },
                        "radix": {
                            "node": "node-a-8core",
                            "cores": "0-7",
                            "threads": 8,
                            "after": "vips",
                        },
                    },
                },
            )
            policy = load_policy_config(str(path))
            self.assertEqual(policy.policy_name, "simple-schedule")
            self.assertEqual(policy.phases[0].launch, ("streamcluster", "blackscholes"))
            self.assertEqual(policy.phases[1].jobs_complete, ("blackscholes",))
