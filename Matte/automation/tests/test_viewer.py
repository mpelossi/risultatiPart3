from __future__ import annotations

import json
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

from Matte.automation.catalog import JOB_CATALOG, NODE_A, NODE_B
from Matte.automation.metrics import build_summary
from Matte.automation.tests.helpers import temp_workspace, write_json_config
from Matte.automation.viewer_data import list_run_experiments, load_experiment_view, load_run_view


BASE_TIME = datetime(2026, 4, 23, 3, 0, 0, tzinfo=timezone.utc)


def _format_time(offset_s: int) -> str:
    return (BASE_TIME + timedelta(seconds=offset_s)).strftime("%Y-%m-%dT%H:%M:%SZ")


def _policy_payload(name: str) -> dict[str, object]:
    jobs: dict[str, object] = {}
    for job_id, entry in JOB_CATALOG.items():
        jobs[job_id] = {
            "node": entry.default_node,
            "cores": entry.default_cores,
            "threads": entry.default_threads,
            "after": "start",
        }
    return {
        "policy_name": name,
        "memcached": {"node": NODE_B, "cores": "0", "threads": 1},
        "jobs": jobs,
    }


def _pods_payload(run_id: str, durations_by_job: dict[str, int]) -> dict[str, object]:
    items = [
        {
            "metadata": {
                "name": f"memcached-server-{run_id}",
                "labels": {
                    "cca-project-role": "memcached",
                    "cca-project-run-id": run_id,
                },
            },
            "spec": {"nodeName": "node-b-4core-demo"},
            "status": {
                "phase": "Running",
                "podIP": "10.0.0.10",
                "containerStatuses": [
                    {
                        "name": "memcached",
                        "state": {"running": {"startedAt": _format_time(-5)}},
                    }
                ],
            },
        }
    ]

    for job_id, entry in JOB_CATALOG.items():
        node_name = "node-a-8core-demo" if entry.default_node == NODE_A else "node-b-4core-demo"
        duration_s = durations_by_job[job_id]
        items.append(
            {
                "metadata": {
                    "name": f"parsec-{job_id}-{run_id}",
                    "labels": {
                        "cca-project-run-id": run_id,
                        "cca-project-job-id": job_id,
                    },
                },
                "spec": {"nodeName": node_name},
                "status": {
                    "phase": "Succeeded",
                    "podIP": f"10.0.1.{len(items) + 10}",
                    "containerStatuses": [
                        {
                            "name": f"parsec-{job_id}",
                            "state": {
                                "terminated": {
                                    "startedAt": _format_time(0),
                                    "finishedAt": _format_time(duration_s),
                                    "exitCode": 0,
                                }
                            },
                        }
                    ],
                },
            }
        )

    return {"apiVersion": "v1", "items": items}


def _write_mcperf(path: Path, p95_values: list[float]) -> None:
    lines = ["#type p95"]
    for index, value in enumerate(p95_values, start=1):
        lines.append(f"read-{index} {value}")
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _write_run(
    experiment_root: Path,
    run_id: str,
    *,
    policy_name: str,
    durations_by_job: dict[str, int] | None = None,
    snapshot_filename: str | None = None,
    mcperf_values: list[float] | None = None,
    mcperf_raw: str | None = None,
    write_summary_file: bool = False,
) -> Path:
    run_dir = experiment_root / run_id
    run_dir.mkdir(parents=True)
    write_json_config(run_dir / "policy.yaml", _policy_payload(policy_name))

    if snapshot_filename is not None and durations_by_job is not None:
        payload = _pods_payload(run_id, durations_by_job)
        (run_dir / snapshot_filename).write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")

    if mcperf_values is not None:
        _write_mcperf(run_dir / "mcperf.txt", mcperf_values)
    elif mcperf_raw is not None:
        (run_dir / "mcperf.txt").write_text(mcperf_raw, encoding="utf-8")

    if write_summary_file and snapshot_filename is not None and durations_by_job is not None and mcperf_values is not None:
        summary = build_summary(
            run_dir / snapshot_filename,
            run_dir / "mcperf.txt",
            set(JOB_CATALOG),
            run_id=run_id,
            experiment_id="demo",
            policy_name=policy_name,
        )
        (run_dir / "summary.json").write_text(json.dumps(summary, indent=2) + "\n", encoding="utf-8")

    return run_dir


class ViewerDataTests(unittest.TestCase):
    def test_load_run_view_supports_human_readable_summary_backed_runs(self) -> None:
        with temp_workspace() as workspace:
            root = Path(workspace)
            experiment_root = root / "runs" / "demo"
            durations = {
                "barnes": 60,
                "blackscholes": 50,
                "canneal": 100,
                "freqmine": 180,
                "radix": 25,
                "streamcluster": 170,
                "vips": 30,
            }
            _write_run(
                experiment_root,
                "2026-04-23-16h42m02s",
                policy_name="summary-backed",
                durations_by_job=durations,
                snapshot_filename="results.json",
                mcperf_values=[430.0, 470.0],
                write_summary_file=True,
            )

            run = load_run_view(root / "runs", "demo", "2026-04-23-16h42m02s")

            self.assertFalse(run["is_reconstructed"])
            self.assertEqual(run["measurement_status"], "ok")
            self.assertTrue(run["artifact_flags"]["summary"])
            self.assertTrue(run["timeline"]["has_data"])
            self.assertEqual(run["run_label"], "2026-04-23 16:42:02 CEST")
            self.assertEqual(run["timestamp_iso"], "2026-04-23T16:42:02+02:00")

    def test_load_run_view_reconstructs_legacy_pods_runs(self) -> None:
        with temp_workspace() as workspace:
            root = Path(workspace)
            experiment_root = root / "runs" / "demo"
            durations = {
                "barnes": 61,
                "blackscholes": 53,
                "canneal": 104,
                "freqmine": 190,
                "radix": 11,
                "streamcluster": 121,
                "vips": 30,
            }
            _write_run(
                experiment_root,
                "20260423t030618z",
                policy_name="legacy-run",
                durations_by_job=durations,
                snapshot_filename="pods.json",
                mcperf_values=[833.1, 840.2],
            )

            run = load_run_view(root / "runs", "demo", "20260423t030618z")

            self.assertTrue(run["is_reconstructed"])
            self.assertFalse(run["artifact_flags"]["results"])
            self.assertTrue(run["artifact_flags"]["pods"])
            self.assertEqual(run["overall_status"], "pass")
            self.assertEqual(run["run_label"], "2026-04-23 05:06:18 CEST")
            self.assertEqual(run["timestamp_iso"], "2026-04-23T03:06:18+00:00")

    def test_load_run_view_handles_runs_without_pod_snapshot(self) -> None:
        with temp_workspace() as workspace:
            root = Path(workspace)
            experiment_root = root / "runs" / "demo"
            _write_run(
                experiment_root,
                "20260423t015053z",
                policy_name="missing-pods",
                snapshot_filename=None,
                mcperf_values=[512.0, 560.0],
            )

            run = load_run_view(root / "runs", "demo", "20260423t015053z")

            self.assertFalse(run["timeline"]["has_data"])
            self.assertFalse(run["artifact_flags"]["snapshot"])
            self.assertIn("No results.json or pods.json snapshot found.", run["issues"])

    def test_load_run_view_marks_malformed_mcperf_as_parse_error(self) -> None:
        with temp_workspace() as workspace:
            root = Path(workspace)
            experiment_root = root / "runs" / "demo"
            durations = {
                "barnes": 60,
                "blackscholes": 50,
                "canneal": 100,
                "freqmine": 237,
                "radix": 11,
                "streamcluster": 121,
                "vips": 30,
            }
            _write_run(
                experiment_root,
                "20260423t024853z",
                policy_name="broken-mcperf",
                durations_by_job=durations,
                snapshot_filename="pods.json",
                mcperf_raw="#type p95\nread THANKS\n",
            )

            run = load_run_view(root / "runs", "demo", "20260423t024853z")

            self.assertEqual(run["measurement_status"], "parse_error")
            self.assertEqual(run["overall_status"], "infra_fail")
            self.assertFalse(run["eligible_for_best"])
            self.assertTrue(any("malformed latency data" in issue for issue in run["issues"]))

    def test_load_run_view_overrides_stale_summary_when_mcperf_has_sync_error(self) -> None:
        with temp_workspace() as workspace:
            root = Path(workspace)
            experiment_root = root / "runs" / "demo"
            durations = {
                "barnes": 60,
                "blackscholes": 50,
                "canneal": 100,
                "freqmine": 237,
                "radix": 11,
                "streamcluster": 121,
                "vips": 30,
            }
            run_dir = _write_run(
                experiment_root,
                "2026-04-23-17h07m12s",
                policy_name="stale-summary",
                durations_by_job=durations,
                snapshot_filename="results.json",
                mcperf_values=[430.0, 440.0],
                write_summary_file=True,
            )
            (run_dir / "mcperf.txt").write_text(
                "#type       avg     std     min      p5     p10     p50     p67     p75     p80     p85     p90     p95\n"
                "mcperf.cc(757): sync_agent[M]: out of sync [1] for agent 1 expected sync got \n",
                encoding="utf-8",
            )

            run = load_run_view(root / "runs", "demo", "2026-04-23-17h07m12s")

            self.assertEqual(run["measurement_status"], "parse_error")
            self.assertEqual(run["overall_status"], "infra_fail")
            self.assertFalse(run["eligible_for_best"])
            self.assertTrue(any("synchronization errors" in issue for issue in run["issues"]))

    def test_load_experiment_view_picks_best_across_summary_and_reconstructed_runs(self) -> None:
        with temp_workspace() as workspace:
            root = Path(workspace)
            experiment_root = root / "runs" / "demo"

            _write_run(
                experiment_root,
                "2026-04-23-16h42m02s",
                policy_name="summary-backed",
                durations_by_job={
                    "barnes": 60,
                    "blackscholes": 50,
                    "canneal": 104,
                    "freqmine": 240,
                    "radix": 11,
                    "streamcluster": 121,
                    "vips": 30,
                },
                snapshot_filename="results.json",
                mcperf_values=[470.0, 480.0],
                write_summary_file=True,
            )
            _write_run(
                experiment_root,
                "20260423t023159z",
                policy_name="legacy-best",
                durations_by_job={
                    "barnes": 61,
                    "blackscholes": 53,
                    "canneal": 104,
                    "freqmine": 230,
                    "radix": 11,
                    "streamcluster": 121,
                    "vips": 30,
                },
                snapshot_filename="pods.json",
                mcperf_values=[430.0, 440.0],
            )
            _write_run(
                experiment_root,
                "20260423t030618z",
                policy_name="broken-mcperf",
                durations_by_job={
                    "barnes": 50,
                    "blackscholes": 40,
                    "canneal": 60,
                    "freqmine": 120,
                    "radix": 10,
                    "streamcluster": 70,
                    "vips": 25,
                },
                snapshot_filename="pods.json",
                mcperf_raw="#type p95\nread THANKS\n",
            )

            view = load_experiment_view(root / "runs", "demo")

            self.assertEqual(view["best_run_id"], "20260423t023159z")
            self.assertEqual(view["runs"][0]["run_id"], "2026-04-23-16h42m02s")


class ViewerExperimentListingTests(unittest.TestCase):
    def test_list_run_experiments_reports_available_experiments(self) -> None:
        with temp_workspace() as workspace:
            root = Path(workspace)
            experiment_root = root / "runs" / "demo"
            _write_run(
                experiment_root,
                "20260423t023159z",
                policy_name="http-check",
                durations_by_job={
                    "barnes": 61,
                    "blackscholes": 53,
                    "canneal": 104,
                    "freqmine": 230,
                    "radix": 11,
                    "streamcluster": 121,
                    "vips": 30,
                },
                snapshot_filename="pods.json",
                mcperf_values=[430.0, 440.0],
            )
            _write_run(
                root / "runs" / "demo-two",
                "2026-04-23-16h42m02s",
                policy_name="second-experiment",
                durations_by_job={
                    "barnes": 50,
                    "blackscholes": 40,
                    "canneal": 90,
                    "freqmine": 160,
                    "radix": 10,
                    "streamcluster": 130,
                    "vips": 25,
                },
                snapshot_filename="results.json",
                mcperf_values=[470.0, 480.0],
            )

            experiments = list_run_experiments(root / "runs")

            self.assertEqual([entry["experiment_id"] for entry in experiments], ["demo", "demo-two"])
            self.assertEqual([entry["run_count"] for entry in experiments], [1, 1])


if __name__ == "__main__":
    unittest.main()
