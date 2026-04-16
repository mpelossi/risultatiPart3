from __future__ import annotations

import unittest

from part3.automation.results import sort_best_runs


class ResultsTests(unittest.TestCase):
    def test_best_runs_sort_passes_first_then_makespan(self) -> None:
        summaries = [
            {"run_id": "c", "overall_status": "slo_fail", "makespan_s": 10, "max_observed_p95_us": 1200},
            {"run_id": "b", "overall_status": "pass", "makespan_s": 200, "max_observed_p95_us": 800},
            {"run_id": "a", "overall_status": "pass", "makespan_s": 150, "max_observed_p95_us": 900},
        ]
        ordered = sort_best_runs(summaries)
        self.assertEqual([entry["run_id"] for entry in ordered], ["a", "b", "c"])
