from __future__ import annotations

import unittest
from pathlib import Path

from Matte.automation.catalog import JOB_CATALOG, NODE_A, NODE_B
from Matte.automation.config import load_policy_config
from Matte.automation.schedule_viewer_data import (
    list_schedule_view,
    load_schedule_view,
    preview_schedule_view,
)
from Matte.automation.tests.helpers import temp_workspace, write_json_config


def _write_times_csv(path: Path) -> None:
    lines = ["job,threads,real_time_seconds"]
    for index, job_id in enumerate(JOB_CATALOG, start=1):
        lines.append(f"{job_id},1,{10 * index}.0")
        lines.append(f"{job_id},2,{8 * index}.0")
        lines.append(f"{job_id},4,{5 * index}.0")
        lines.append(f"{job_id},8,{3 * index}.0")
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _policy_payload(name: str, *, memcached_node: str = NODE_B) -> dict[str, object]:
    jobs: dict[str, object] = {}
    previous_job: str | None = None
    for job_id in JOB_CATALOG:
        jobs[job_id] = {
            "node": NODE_A,
            "cores": "1",
            "threads": 1,
            "after": previous_job or "start",
        }
        previous_job = job_id
    return {
        "policy_name": name,
        "memcached": {"node": memcached_node, "cores": "0", "threads": 1},
        "jobs": jobs,
    }


class ScheduleViewerDataTests(unittest.TestCase):
    def test_lists_non_hidden_schedules_and_queue_entries(self) -> None:
        with temp_workspace() as workspace:
            root = Path(workspace)
            schedules_dir = root / "schedules"
            schedules_dir.mkdir()
            times_csv = root / "times.csv"
            _write_times_csv(times_csv)
            write_json_config(schedules_dir / "schedule1.yaml", _policy_payload("candidate-1"))
            write_json_config(schedules_dir / "schedule2.yaml", _policy_payload("candidate-2"))
            write_json_config(schedules_dir / ".hidden.yaml", _policy_payload("hidden"))
            queue_path = root / "schedule_queue.yaml"
            write_json_config(
                queue_path,
                {
                    "queue_name": "candidates",
                    "entries": [
                        {"policy": "schedules/schedule2.yaml", "runs": 3},
                        {"policy": "schedules/schedule1.yaml", "runs": 1},
                    ],
                },
            )

            payload = list_schedule_view(
                schedules_dir=schedules_dir,
                schedule_queue_path=queue_path,
                times_csv_path=times_csv,
            )

            self.assertEqual([item["schedule_id"] for item in payload["schedules"]], ["schedule1.yaml", "schedule2.yaml"])
            self.assertEqual(payload["default_schedule_id"], "schedule2.yaml")
            self.assertEqual(payload["queue"]["queue_name"], "candidates")
            self.assertEqual([entry["schedule_id"] for entry in payload["queue"]["entries"]], ["schedule2.yaml", "schedule1.yaml"])
            schedule2 = next(item for item in payload["schedules"] if item["schedule_id"] == "schedule2.yaml")
            self.assertTrue(schedule2["in_queue"])
            self.assertEqual(schedule2["queued_runs"], 3)

    def test_load_schedule_view_predicts_timeline_and_generates_loadable_simple_yaml(self) -> None:
        with temp_workspace() as workspace:
            root = Path(workspace)
            schedules_dir = root / "schedules"
            schedules_dir.mkdir()
            times_csv = root / "times.csv"
            _write_times_csv(times_csv)
            write_json_config(schedules_dir / "schedule1.yaml", _policy_payload("candidate-1"))

            payload = load_schedule_view(
                schedules_dir=schedules_dir,
                schedule_queue_path=None,
                times_csv_path=times_csv,
                schedule_id="schedule1.yaml",
            )

            self.assertEqual(payload["policy_name"], "candidate-1")
            self.assertEqual(payload["prediction"]["status"], "ok")
            self.assertTrue(payload["prediction"]["timeline"]["has_data"])
            segments = {
                segment["job_id"]: segment
                for lane in payload["prediction"]["timeline"]["lanes"]
                for segment in lane["segments"]
            }
            self.assertEqual(segments["memcached"]["planned_node"], NODE_B)
            self.assertEqual(segments["memcached"]["kind"], "memcached")

            generated_path = root / "generated.yaml"
            generated_path.write_text(str(payload["yaml"]), encoding="utf-8")
            generated_policy = load_policy_config(str(generated_path))
            self.assertEqual(generated_policy.policy_name, "candidate-1")

    def test_preview_timeline_supports_memcached_on_node_a(self) -> None:
        with temp_workspace() as workspace:
            root = Path(workspace)
            schedules_dir = root / "schedules"
            schedules_dir.mkdir()
            times_csv = root / "times.csv"
            _write_times_csv(times_csv)
            write_json_config(schedules_dir / "schedule1.yaml", _policy_payload("candidate-1"))
            payload = load_schedule_view(
                schedules_dir=schedules_dir,
                schedule_queue_path=None,
                times_csv_path=times_csv,
                schedule_id="schedule1.yaml",
            )
            editor = payload["editor"]
            editor["memcached"] = {"node": NODE_A, "cores": "0", "threads": 1}
            for job in editor["jobs"]:
                job["node"] = NODE_B
                job["cores"] = "1"
                job["threads"] = 1

            preview = preview_schedule_view(times_csv_path=times_csv, payload={"editor": editor})

            self.assertEqual(preview["prediction"]["status"], "ok")
            node_a_segments = preview["prediction"]["timeline"]["lanes"][0]["segments"]
            self.assertEqual([segment["job_id"] for segment in node_a_segments], ["memcached"])
            self.assertEqual(node_a_segments[0]["planned_node"], NODE_A)

    def test_preview_reports_core_overlap_validation_errors(self) -> None:
        with temp_workspace() as workspace:
            root = Path(workspace)
            times_csv = root / "times.csv"
            _write_times_csv(times_csv)
            editor = {
                "policy_name": "bad-overlap",
                "memcached": {"node": NODE_B, "cores": "0", "threads": 1},
                "jobs": [],
            }
            for index, job_id in enumerate(JOB_CATALOG):
                editor["jobs"].append(
                    {
                        "job_id": job_id,
                        "order": index + 1,
                        "node": NODE_A,
                        "cores": "1",
                        "threads": 1,
                        "after": "start" if index < 2 else list(JOB_CATALOG)[index - 1],
                        "delay_s": 0,
                    }
                )

            preview = preview_schedule_view(times_csv_path=times_csv, payload={"editor": editor})

            self.assertEqual(preview["prediction"]["status"], "error")
            self.assertTrue(
                any("Core overlap on node-a-8core" in issue["message"] for issue in preview["prediction"]["errors"])
            )


if __name__ == "__main__":
    unittest.main()
