from __future__ import annotations

import json
import unittest
from pathlib import Path

from Matte.automation.audit import (
    AuditJob,
    AuditMemcached,
    audit_schedule,
    build_schedule_model,
    estimate_runtime,
    load_runtime_table,
    serialize_policy_document,
)
from Matte.automation.catalog import NODE_A, NODE_B
from Matte.automation.gui import build_model_from_planner_state, planner_state_from_model


ROOT = Path("/home/carti/ETH/Msc/CCA")
TIMES_CSV = ROOT / "risultatiPart3/Part2summary_times.csv"


def _base_jobs() -> dict[str, AuditJob]:
    return {
        "blackscholes": AuditJob(
            job_id="blackscholes",
            node=NODE_A,
            cores="0-3",
            threads=4,
            dependencies=(),
            delay_s=0,
            order=1,
        ),
        "barnes": AuditJob(
            job_id="barnes",
            node=NODE_A,
            cores="4-7",
            threads=4,
            dependencies=(),
            delay_s=0,
            order=2,
        ),
        "streamcluster": AuditJob(
            job_id="streamcluster",
            node=NODE_A,
            cores="0-7",
            threads=8,
            dependencies=("blackscholes", "barnes"),
            delay_s=0,
            order=3,
        ),
        "canneal": AuditJob(
            job_id="canneal",
            node=NODE_A,
            cores="0-7",
            threads=8,
            dependencies=("streamcluster",),
            delay_s=0,
            order=4,
        ),
        "vips": AuditJob(
            job_id="vips",
            node=NODE_A,
            cores="0-7",
            threads=8,
            dependencies=("canneal",),
            delay_s=0,
            order=5,
        ),
        "radix": AuditJob(
            job_id="radix",
            node=NODE_A,
            cores="0-7",
            threads=8,
            dependencies=("vips",),
            delay_s=0,
            order=6,
        ),
        "freqmine": AuditJob(
            job_id="freqmine",
            node=NODE_B,
            cores="1-3",
            threads=3,
            dependencies=(),
            delay_s=0,
            order=7,
        ),
    }


class AuditTests(unittest.TestCase):
    def setUp(self) -> None:
        self.runtime_table = load_runtime_table(str(TIMES_CSV))

    def _build_model(self, jobs: dict[str, AuditJob]):
        return build_schedule_model(
            policy_name="test-policy",
            memcached=AuditMemcached(node=NODE_B, cores="0", threads=1),
            jobs=jobs,
        )

    def test_interpolates_three_thread_runtime(self) -> None:
        estimate = estimate_runtime("freqmine", 3, self.runtime_table)
        self.assertIsNotNone(estimate)
        assert estimate is not None
        self.assertGreater(estimate, 206.718)
        self.assertLess(estimate, 266.438)

    def test_allows_split_four_core_jobs_on_node_a(self) -> None:
        report = audit_schedule(self._build_model(_base_jobs()), self.runtime_table)
        self.assertEqual(report.errors, [])

    def test_detects_overlap_for_concurrent_jobs(self) -> None:
        jobs = _base_jobs()
        jobs["barnes"] = AuditJob(
            job_id="barnes",
            node=NODE_A,
            cores="2-3",
            threads=2,
            dependencies=(),
            delay_s=0,
            order=2,
        )
        report = audit_schedule(self._build_model(jobs), self.runtime_table)
        self.assertTrue(report.errors)
        self.assertTrue(any("Core overlap on node-a-8core" in issue.message for issue in report.errors))

    def test_rejects_unsupported_core_set_before_overlap(self) -> None:
        jobs = _base_jobs()
        jobs["barnes"] = AuditJob(
            job_id="barnes",
            node=NODE_A,
            cores="0-3,2-4",
            threads=5,
            dependencies=(),
            delay_s=0,
            order=2,
        )
        report = audit_schedule(self._build_model(jobs), self.runtime_table)
        self.assertTrue(any("duplicate or overlapping core 2" in issue.message for issue in report.errors))

    def test_accepts_arbitrary_valid_core_specs(self) -> None:
        jobs = _base_jobs()
        jobs["blackscholes"] = AuditJob(
            job_id="blackscholes",
            node=NODE_A,
            cores="0,2,4",
            threads=3,
            dependencies=(),
            delay_s=0,
            order=1,
        )
        jobs["barnes"] = AuditJob(
            job_id="barnes",
            node=NODE_A,
            cores="5-7",
            threads=3,
            dependencies=(),
            delay_s=0,
            order=2,
        )
        jobs["streamcluster"] = AuditJob(
            job_id="streamcluster",
            node=NODE_A,
            cores="1-5",
            threads=5,
            dependencies=("blackscholes", "barnes"),
            delay_s=0,
            order=3,
        )
        report = audit_schedule(self._build_model(jobs), self.runtime_table)
        self.assertFalse(report.errors)

    def test_reports_idle_gaps_as_warnings_only(self) -> None:
        jobs = _base_jobs()
        jobs["canneal"] = AuditJob(
            job_id="canneal",
            node=NODE_A,
            cores="0-7",
            threads=8,
            dependencies=("streamcluster",),
            delay_s=15,
            order=4,
        )
        report = audit_schedule(self._build_model(jobs), self.runtime_table)
        self.assertEqual(report.errors, [])
        self.assertTrue(any("Idle gap on node-a-8core" in issue.message for issue in report.warnings))


class PlannerRoundTripTests(unittest.TestCase):
    def test_round_trip_serializes_explicit_policy(self) -> None:
        original_model = self._base_model()
        planner_state = planner_state_from_model(original_model)
        rebuilt_model = build_model_from_planner_state(planner_state)
        payload = json.loads(serialize_policy_document(rebuilt_model))
        self.assertIn("job_overrides", payload)
        self.assertIn("phases", payload)
        self.assertEqual(payload["phases"][0]["launch"], ["blackscholes", "barnes", "freqmine"])

    def test_round_trip_preserves_custom_core_specs(self) -> None:
        jobs = _base_jobs()
        jobs["blackscholes"] = AuditJob(
            job_id="blackscholes",
            node=NODE_A,
            cores="0,2,4",
            threads=3,
            dependencies=(),
            delay_s=0,
            order=1,
        )
        model = build_schedule_model(
            policy_name="planner-test",
            memcached=AuditMemcached(node=NODE_B, cores="0", threads=1),
            jobs=jobs,
        )
        planner_state = planner_state_from_model(model)
        rebuilt_model = build_model_from_planner_state(planner_state)

        self.assertEqual(rebuilt_model.jobs["blackscholes"].cores, "0,2,4")

    def _base_model(self):
        return build_schedule_model(
            policy_name="planner-test",
            memcached=AuditMemcached(node=NODE_B, cores="0", threads=1),
            jobs=_base_jobs(),
        )
