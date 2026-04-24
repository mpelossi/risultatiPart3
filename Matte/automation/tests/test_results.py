from __future__ import annotations

import json
import unittest
from pathlib import Path

from Matte.automation.results import load_run_summaries, sort_best_runs
from Matte.automation.tests.helpers import temp_workspace


class ResultsTests(unittest.TestCase):
    def test_best_runs_sort_passes_first_then_makespan(self) -> None:
        summaries = [
            {"run_id": "c", "overall_status": "slo_fail", "makespan_s": 10, "max_observed_p95_us": 1200},
            {
                "run_id": "b",
                "overall_status": "pass",
                "measurement_status": "ok",
                "sample_count": 3,
                "timing_complete": True,
                "makespan_s": 200,
                "max_observed_p95_us": 800,
            },
            {
                "run_id": "a",
                "overall_status": "pass",
                "measurement_status": "ok",
                "sample_count": 3,
                "timing_complete": True,
                "makespan_s": 150,
                "max_observed_p95_us": 900,
            },
        ]
        ordered = sort_best_runs(summaries)
        self.assertEqual([entry["run_id"] for entry in ordered], ["a", "b", "c"])

    def test_best_runs_does_not_treat_zero_sample_pass_as_candidate(self) -> None:
        summaries = [
            {
                "run_id": "bad",
                "overall_status": "pass",
                "measurement_status": "ok",
                "sample_count": 0,
                "timing_complete": True,
                "makespan_s": 10,
                "max_observed_p95_us": None,
            },
            {
                "run_id": "good",
                "overall_status": "pass",
                "measurement_status": "ok",
                "sample_count": 2,
                "timing_complete": True,
                "makespan_s": 30,
                "max_observed_p95_us": 450,
            },
        ]

        ordered = sort_best_runs(summaries)

        self.assertEqual([entry["run_id"] for entry in ordered], ["good", "bad"])

    def test_load_run_summaries_reclassifies_stale_sync_error_summary(self) -> None:
        with temp_workspace() as workspace:
            root = Path(workspace)
            run_dir = root / "runs" / "demo" / "run-1"
            run_dir.mkdir(parents=True)
            (run_dir / "summary.json").write_text(
                json.dumps(
                    {
                        "run_id": "run-1",
                        "overall_status": "pass",
                        "measurement_status": "ok",
                        "sample_count": 0,
                        "makespan_s": 10,
                        "max_observed_p95_us": None,
                        "timing_complete": True,
                    }
                ),
                encoding="utf-8",
            )
            (run_dir / "mcperf.txt").write_text(
                "#type       avg     std     min      p5     p10     p50     p67     p75     p80     p85     p90     p95\n"
                "mcperf.cc(757): sync_agent[M]: out of sync [1] for agent 1 expected sync got \n",
                encoding="utf-8",
            )

            summaries = load_run_summaries(root / "runs", "demo")

            self.assertEqual(summaries[0]["overall_status"], "infra_fail")
            self.assertEqual(summaries[0]["measurement_status"], "parse_error")
