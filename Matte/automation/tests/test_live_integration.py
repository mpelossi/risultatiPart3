from __future__ import annotations

import os
import unittest


@unittest.skipUnless(os.getenv("PART3_LIVE_TESTS") == "1", "live cluster tests are opt-in")
class LiveIntegrationTests(unittest.TestCase):
    def test_placeholder_for_live_cluster_smoke(self) -> None:
        self.assertEqual(os.getenv("PART3_LIVE_TESTS"), "1")

