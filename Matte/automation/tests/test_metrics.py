from __future__ import annotations

import unittest
from pathlib import Path

from Matte.automation.metrics import build_summary, parse_mcperf_output
from Matte.automation.timing import build_get_time_report


ROOT = Path("/home/carti/ETH/Msc/CCA")


class MetricsTests(unittest.TestCase):
    def test_parse_mcperf_output_counts_slo_violations(self) -> None:
        output = parse_mcperf_output(ROOT / "part3/results/firstRun/run1_mcperf.txt")
        self.assertEqual(output["slo_violations"], 0)
        self.assertIsNotNone(output["max_p95_us"])

    def test_build_summary_marks_first_run_as_pass(self) -> None:
        summary = build_summary(
            ROOT / "part3/results/firstRun/results.json",
            ROOT / "part3/results/firstRun/run1_mcperf.txt",
            {"barnes", "blackscholes", "canneal", "freqmine", "radix", "streamcluster", "vips"},
            run_id="sample-run",
            experiment_id="sample-experiment",
            policy_name="sample-policy",
        )
        self.assertEqual(summary["overall_status"], "pass")
        self.assertAlmostEqual(summary["makespan_s"], 259.0, places=1)

    def test_build_summary_uses_shared_get_time_makespan(self) -> None:
        results_path = ROOT / "part3/results/firstRun/results.json"
        report = build_get_time_report(results_path)
        summary = build_summary(
            results_path,
            ROOT / "part3/results/firstRun/run1_mcperf.txt",
            {"barnes", "blackscholes", "canneal", "freqmine", "radix", "streamcluster", "vips"},
            run_id="sample-run",
            experiment_id="sample-experiment",
            policy_name="sample-policy",
        )

        self.assertTrue(report.is_complete)
        self.assertEqual(summary["completed_job_count"], report.completed_job_count)
        self.assertAlmostEqual(summary["makespan_s"], report.total_runtime.total_seconds(), places=1)
