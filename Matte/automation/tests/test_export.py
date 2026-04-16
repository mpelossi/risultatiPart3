from __future__ import annotations

import json
import unittest
from pathlib import Path

from part3.automation.export import export_submission
from part3.automation.tests.helpers import temp_workspace


class ExportTests(unittest.TestCase):
    def test_export_submission_creates_required_filenames(self) -> None:
        with temp_workspace() as workspace:
            root = Path(workspace)
            experiment_root = root / "runs" / "demo"
            experiment_root.mkdir(parents=True)
            for index in range(1, 4):
                run_dir = experiment_root / f"run-{index}"
                run_dir.mkdir()
                (run_dir / "pods.json").write_text("{}", encoding="utf-8")
                (run_dir / "mcperf.txt").write_text("#type p95\n", encoding="utf-8")
                (run_dir / "summary.json").write_text(
                    json.dumps({"run_id": f"run-{index}", "overall_status": "pass"}) + "\n",
                    encoding="utf-8",
                )
            target_dir = export_submission(
                results_root=root / "runs",
                experiment_id="demo",
                group="054",
                task="3_1",
                output_root=root,
            )
            self.assertTrue((target_dir / "pods_1.json").exists())
            self.assertTrue((target_dir / "pods_2.json").exists())
            self.assertTrue((target_dir / "pods_3.json").exists())
            self.assertTrue((target_dir / "mcperf_1.txt").exists())
            self.assertTrue((target_dir / "mcperf_2.txt").exists())
            self.assertTrue((target_dir / "mcperf_3.txt").exists())
