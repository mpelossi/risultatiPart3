from __future__ import annotations

import unittest
from pathlib import Path

from Matte.automation.config import load_experiment_config, load_policy_config, load_run_queue_config
from Matte.automation.manifests import resolve_jobs
from Matte.automation.tests.helpers import temp_workspace, write_json_config


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

    def test_queue_config_resolves_relative_policy_paths(self) -> None:
        with temp_workspace() as workspace:
            root = Path(workspace)
            schedules_dir = root / "schedules"
            schedules_dir.mkdir()
            first_policy = schedules_dir / "schedule1.yaml"
            second_policy = schedules_dir / "schedule2.yaml"
            write_json_config(first_policy, {"policy_name": "candidate-1"})
            write_json_config(second_policy, {"policy_name": "candidate-2"})
            queue_path = root / "schedule_queue.yaml"
            write_json_config(
                queue_path,
                {
                    "queue_name": "candidates",
                    "entries": [
                        {"policy": "schedules/schedule1.yaml"},
                        {"policy": "schedules/schedule2.yaml", "runs": 3},
                    ],
                },
            )

            queue = load_run_queue_config(str(queue_path))

            self.assertEqual(queue.queue_name, "candidates")
            self.assertEqual(queue.entries[0].policy_path, first_policy.resolve())
            self.assertEqual(queue.entries[0].runs, 1)
            self.assertEqual(queue.entries[1].policy_path, second_policy.resolve())
            self.assertEqual(queue.entries[1].runs, 3)

    def test_queue_config_rejects_invalid_entries(self) -> None:
        with temp_workspace() as workspace:
            root = Path(workspace)
            policy_path = root / "schedule.yaml"
            write_json_config(policy_path, {"policy_name": "candidate"})
            queue_path = root / "schedule_queue.yaml"

            write_json_config(queue_path, {"queue_name": "empty", "entries": []})
            with self.assertRaisesRegex(ValueError, "entries must contain"):
                load_run_queue_config(str(queue_path))

            write_json_config(queue_path, {"entries": [{"policy": "schedule.yaml", "runs": 0}]})
            with self.assertRaisesRegex(ValueError, "runs must be at least 1"):
                load_run_queue_config(str(queue_path))

            write_json_config(queue_path, {"entries": [{"policy": "missing.yaml"}]})
            with self.assertRaises(FileNotFoundError):
                load_run_queue_config(str(queue_path))

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

    def test_simple_schedule_preserves_thread_overrides(self) -> None:
        with temp_workspace() as workspace:
            path = Path(workspace) / "schedule.yaml"
            write_json_config(
                path,
                {
                    "policy_name": "threaded-schedule",
                    "memcached": {"node": "node-b-4core", "cores": "0", "threads": 1},
                    "jobs": {
                        "streamcluster": {
                            "node": "node-a-8core",
                            "cores": "0-7",
                            "threads": 6,
                            "after": "start",
                        },
                        "blackscholes": {
                            "node": "node-b-4core",
                            "cores": "1-3",
                            "threads": 2,
                            "after": "start",
                        },
                        "freqmine": {
                            "node": "node-b-4core",
                            "cores": "1-3",
                            "threads": 2,
                            "after": "blackscholes",
                        },
                        "canneal": {
                            "node": "node-a-8core",
                            "cores": "0-7",
                            "threads": 7,
                            "after": "streamcluster",
                        },
                        "barnes": {
                            "node": "node-a-8core",
                            "cores": "0-7",
                            "threads": 5,
                            "after": "canneal",
                        },
                        "vips": {
                            "node": "node-a-8core",
                            "cores": "0-7",
                            "threads": 4,
                            "after": "barnes",
                        },
                        "radix": {
                            "node": "node-a-8core",
                            "cores": "0-7",
                            "threads": 3,
                            "after": "vips",
                        },
                    },
                },
            )
            policy = load_policy_config(str(path))
            jobs = resolve_jobs(policy, "preview")
            self.assertEqual(policy.job_overrides["streamcluster"].threads, 6)
            self.assertEqual(policy.job_overrides["blackscholes"].threads, 2)
            self.assertEqual(policy.job_overrides["radix"].threads, 3)
            self.assertEqual(jobs["streamcluster"].threads, 6)
            self.assertEqual(jobs["blackscholes"].threads, 2)
            self.assertEqual(jobs["radix"].threads, 3)

    def test_simple_schedule_rejects_zero_thread_override(self) -> None:
        with temp_workspace() as workspace:
            path = Path(workspace) / "schedule.yaml"
            write_json_config(
                path,
                {
                    "policy_name": "bad-zero-threads",
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
                            "threads": 0,
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
            with self.assertRaisesRegex(ValueError, "blackscholes must use at least one thread"):
                load_policy_config(str(path))

    def test_policy_accepts_arbitrary_valid_core_specs(self) -> None:
        with temp_workspace() as workspace:
            path = Path(workspace) / "schedule.yaml"
            write_json_config(
                path,
                {
                    "policy_name": "custom-cores",
                    "memcached": {"node": "node-b-4core", "cores": "0", "threads": 1},
                    "jobs": {
                        "streamcluster": {
                            "node": "node-a-8core",
                            "cores": "0-4",
                            "threads": 5,
                            "after": "start",
                        },
                        "blackscholes": {
                            "node": "node-b-4core",
                            "cores": "1-3",
                            "threads": 3,
                            "after": "start",
                        },
                        "freqmine": {
                            "node": "node-a-8core",
                            "cores": "0,2,4",
                            "threads": 3,
                            "after": "streamcluster",
                        },
                        "canneal": {
                            "node": "node-b-4core",
                            "cores": "1-2",
                            "threads": 2,
                            "after": "blackscholes",
                        },
                        "barnes": {
                            "node": "node-a-8core",
                            "cores": "5-7",
                            "threads": 3,
                            "after": "freqmine",
                        },
                        "vips": {
                            "node": "node-a-8core",
                            "cores": "1-5",
                            "threads": 5,
                            "after": "barnes",
                        },
                        "radix": {
                            "node": "node-b-4core",
                            "cores": "0-2",
                            "threads": 3,
                            "after": "canneal",
                        },
                    },
                },
            )

            policy = load_policy_config(str(path))
            jobs = resolve_jobs(policy, "preview")

            self.assertEqual(jobs["streamcluster"].cores, "0-4")
            self.assertEqual(jobs["freqmine"].cores, "0,2,4")
            self.assertEqual(jobs["vips"].cores, "1-5")

    def test_policy_rejects_duplicate_or_overlapping_core_specs(self) -> None:
        with temp_workspace() as workspace:
            path = Path(workspace) / "schedule.yaml"
            write_json_config(
                path,
                {
                    "policy_name": "overlap-cores",
                    "memcached": {"node": "node-b-4core", "cores": "0", "threads": 1},
                    "jobs": {
                        "streamcluster": {
                            "node": "node-a-8core",
                            "cores": "0-3,2-4",
                            "threads": 5,
                            "after": "start",
                        },
                        "blackscholes": {"node": "node-b-4core", "cores": "1-3", "threads": 3, "after": "start"},
                        "freqmine": {"node": "node-a-8core", "cores": "0-4", "threads": 5, "after": "streamcluster"},
                        "canneal": {"node": "node-b-4core", "cores": "1-3", "threads": 3, "after": "blackscholes"},
                        "barnes": {"node": "node-a-8core", "cores": "5-7", "threads": 3, "after": "freqmine"},
                        "vips": {"node": "node-a-8core", "cores": "0-7", "threads": 8, "after": "barnes"},
                        "radix": {"node": "node-b-4core", "cores": "1-3", "threads": 3, "after": "canneal"},
                    },
                },
            )

            with self.assertRaisesRegex(ValueError, "duplicate or overlapping core 2"):
                load_policy_config(str(path))

    def test_policy_rejects_out_of_range_reversed_and_empty_core_specs(self) -> None:
        with temp_workspace() as workspace:
            path = Path(workspace) / "schedule.yaml"
            base_payload = {
                "policy_name": "bad-cores",
                "memcached": {"node": "node-b-4core", "cores": "0", "threads": 1},
                "jobs": {
                    "streamcluster": {"node": "node-a-8core", "cores": "0-7", "threads": 8, "after": "start"},
                    "blackscholes": {"node": "node-b-4core", "cores": "1-3", "threads": 3, "after": "start"},
                    "freqmine": {"node": "node-a-8core", "cores": "0-7", "threads": 8, "after": "streamcluster"},
                    "canneal": {"node": "node-b-4core", "cores": "1-3", "threads": 3, "after": "blackscholes"},
                    "barnes": {"node": "node-a-8core", "cores": "0-7", "threads": 8, "after": "freqmine"},
                    "vips": {"node": "node-a-8core", "cores": "0-7", "threads": 8, "after": "barnes"},
                    "radix": {"node": "node-b-4core", "cores": "1-3", "threads": 3, "after": "canneal"},
                },
            }

            payload = dict(base_payload)
            payload["jobs"] = dict(base_payload["jobs"])
            payload["jobs"]["streamcluster"] = dict(payload["jobs"]["streamcluster"], cores="7-9")
            write_json_config(path, payload)
            with self.assertRaisesRegex(ValueError, "out of range 0-7"):
                load_policy_config(str(path))

            payload["jobs"]["streamcluster"] = dict(payload["jobs"]["streamcluster"], cores="5-3")
            write_json_config(path, payload)
            with self.assertRaisesRegex(ValueError, "end before start"):
                load_policy_config(str(path))

            payload["jobs"]["streamcluster"] = dict(payload["jobs"]["streamcluster"], cores="")
            write_json_config(path, payload)
            with self.assertRaisesRegex(ValueError, "must be a non-empty string"):
                load_policy_config(str(path))
