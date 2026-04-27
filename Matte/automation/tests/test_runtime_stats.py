from __future__ import annotations

import json
import unittest
from pathlib import Path

from Matte.automation.catalog import JOB_CATALOG, NODE_A, NODE_B
from Matte.automation.runtime_stats import (
    build_runtime_stats,
    load_runtime_stats,
    rebuild_runtime_stats_file,
)
from Matte.automation.tests.helpers import temp_workspace, write_json_config


def _policy_payload(name: str, *, memcached_node: str = NODE_B) -> dict[str, object]:
    jobs: dict[str, object] = {}
    previous_job: str | None = None
    for job_id, entry in JOB_CATALOG.items():
        jobs[job_id] = {
            "node": NODE_B if job_id == "blackscholes" else NODE_A,
            "cores": "1-3" if job_id == "blackscholes" else "0-7",
            "threads": 3 if job_id == "blackscholes" else entry.default_threads,
            "after": previous_job or "start",
        }
        previous_job = job_id
    return {
        "policy_name": name,
        "memcached": {"node": memcached_node, "cores": "0", "threads": 1},
        "jobs": jobs,
    }


def _summary_payload(
    *,
    run_id: str,
    policy_name: str,
    memcached_node_name: str,
    blackscholes_runtime_s: float,
) -> dict[str, object]:
    jobs: dict[str, object] = {}
    for index, job_id in enumerate(JOB_CATALOG):
        runtime_s = blackscholes_runtime_s if job_id == "blackscholes" else float(100 + index)
        node_name = "node-b-4core-node" if job_id == "blackscholes" else "node-a-8core-node"
        jobs[job_id] = {
            "pod_name": f"parsec-{job_id}-{run_id}",
            "node_name": node_name,
            "phase": "Succeeded",
            "status": "completed",
            "started_at": "2026-01-01T00:00:00Z",
            "finished_at": "2026-01-01T00:01:00Z",
            "runtime_s": runtime_s,
        }
    return {
        "experiment_id": "demo",
        "run_id": run_id,
        "policy_name": policy_name,
        "overall_status": "pass",
        "measurement_status": "ok",
        "timing_complete": True,
        "completed_job_count": len(JOB_CATALOG),
        "expected_job_count": len(JOB_CATALOG),
        "makespan_s": 200.0,
        "max_observed_p95_us": 500.0,
        "slo_violations": 0,
        "sample_count": 10,
        "memcached": {
            "pod_name": f"memcached-{run_id}",
            "node_name": memcached_node_name,
            "phase": "Running",
        },
        "jobs": jobs,
        "node_platforms": {
            "capture_status": "ok",
            "nodes": {
                NODE_A: {
                    "node_name": "node-a-8core-node",
                    "machine_type": "e2-standard-8",
                    "cpu_platform": "Intel Broadwell",
                },
                NODE_B: {
                    "node_name": "node-b-4core-node",
                    "machine_type": "n2d-highcpu-4",
                    "cpu_platform": "AMD Milan",
                },
            },
            "errors": [],
        },
    }


def _write_run(
    results_root: Path,
    run_id: str,
    *,
    memcached_node: str,
    blackscholes_runtime_s: float,
) -> Path:
    run_dir = results_root / "demo" / run_id
    run_dir.mkdir(parents=True)
    policy_name = f"policy-{run_id}"
    write_json_config(run_dir / "policy.yaml", _policy_payload(policy_name, memcached_node=memcached_node))
    memcached_node_name = "node-a-8core-node" if memcached_node == NODE_A else "node-b-4core-node"
    (run_dir / "summary.json").write_text(
        json.dumps(
            _summary_payload(
                run_id=run_id,
                policy_name=policy_name,
                memcached_node_name=memcached_node_name,
                blackscholes_runtime_s=blackscholes_runtime_s,
            ),
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )
    return run_dir


class RuntimeStatsTests(unittest.TestCase):
    def test_build_runtime_stats_extracts_samples_with_context(self) -> None:
        with temp_workspace() as workspace:
            results_root = Path(workspace) / "runs"
            _write_run(results_root, "run-1", memcached_node=NODE_B, blackscholes_runtime_s=12.0)

            payload = build_runtime_stats(results_root)

            self.assertEqual(payload["sample_count"], len(JOB_CATALOG))
            sample = next(sample for sample in payload["samples"] if sample["job"] == "blackscholes")
            self.assertEqual(sample["job_node"], NODE_B)
            self.assertEqual(sample["threads"], 3)
            self.assertEqual(sample["memcached_node"], NODE_B)
            self.assertTrue(sample["memcached_same_node"])
            self.assertEqual(sample["job_cpu_platform"], "AMD Milan")

    def test_runtime_stats_index_uses_median_and_memcached_placement(self) -> None:
        with temp_workspace() as workspace:
            results_root = Path(workspace) / "runs"
            _write_run(results_root, "run-1", memcached_node=NODE_B, blackscholes_runtime_s=10.0)
            _write_run(results_root, "run-2", memcached_node=NODE_B, blackscholes_runtime_s=30.0)
            _write_run(results_root, "run-3", memcached_node=NODE_B, blackscholes_runtime_s=20.0)
            _write_run(results_root, "run-4", memcached_node=NODE_A, blackscholes_runtime_s=5.0)

            output_path = results_root / "runtime_stats.json"
            rebuild_runtime_stats_file(results_root, output_path=output_path)
            index = load_runtime_stats(output_path)

            mem_b = index.estimate(
                job_id="blackscholes",
                node=NODE_B,
                threads=3,
                memcached_node=NODE_B,
            )
            mem_a = index.estimate(
                job_id="blackscholes",
                node=NODE_B,
                threads=3,
                memcached_node=NODE_A,
            )

            self.assertIsNotNone(mem_b)
            self.assertIsNotNone(mem_a)
            assert mem_b is not None
            assert mem_a is not None
            self.assertEqual(mem_b.duration_s, 20.0)
            self.assertEqual(mem_b.sample_count, 3)
            self.assertEqual(mem_b.match_type, "exact")
            self.assertEqual(mem_a.duration_s, 5.0)
            self.assertEqual(mem_a.match_type, "exact")


if __name__ == "__main__":
    unittest.main()
