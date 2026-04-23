from __future__ import annotations

import unittest

from Matte.automation.config import load_policy_config
from Matte.automation.manifests import render_batch_job_manifest, resolve_jobs


class ManifestTests(unittest.TestCase):
    def test_splash_jobs_render_with_splash_suite(self) -> None:
        policy = load_policy_config("/home/carti/ETH/Msc/CCA/risultatiPart3/Matte/automation/policies/baseline.yaml")
        jobs = resolve_jobs(policy, "testrun")
        barnes_manifest = render_batch_job_manifest(jobs["barnes"], experiment_id="exp", run_id="testrun")
        radix_manifest = render_batch_job_manifest(jobs["radix"], experiment_id="exp", run_id="testrun")
        self.assertIn("anakli/cca:splash2x_barnes", barnes_manifest)
        self.assertIn("-S splash2x -p barnes", barnes_manifest)
        self.assertIn("anakli/cca:splash2x_radix", radix_manifest)
        self.assertIn("-S splash2x -p radix", radix_manifest)

    def test_parsec_jobs_render_with_parsec_suite(self) -> None:
        policy = load_policy_config("/home/carti/ETH/Msc/CCA/risultatiPart3/Matte/automation/policies/baseline.yaml")
        jobs = resolve_jobs(policy, "testrun")
        manifest = render_batch_job_manifest(jobs["blackscholes"], experiment_id="exp", run_id="testrun")
        self.assertIn("anakli/cca:parsec_blackscholes", manifest)
        self.assertIn("-S parsec -p blackscholes", manifest)
