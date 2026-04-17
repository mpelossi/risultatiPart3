from __future__ import annotations

import unittest
from pathlib import Path

from Matte.automation.provision import (
    ProvisionStatus,
    render_provision_check_note,
    render_provision_expectations,
)


class ProvisionPresentationTests(unittest.TestCase):
    def test_render_provision_check_note_mentions_three_prompts(self) -> None:
        note = render_provision_check_note(Path("/home/carti/.ssh/cloud-computing"))

        self.assertIn("3 client VMs", note)
        self.assertIn("up to 3 passphrase prompts", note)
        self.assertIn("ssh-add /home/carti/.ssh/cloud-computing", note)

    def test_agent_status_string_explains_waiting_state(self) -> None:
        status = ProvisionStatus(
            nodetype="client-agent-a",
            node_name="client-agent-a-fn6b",
            bootstrap_ready=False,
            mcperf_present=False,
            agent_service_state="not-installed",
        )

        rendered = str(status)

        self.assertFalse(status.is_ready)
        self.assertIn("WAITING", rendered)
        self.assertIn("bootstrap not finished", rendered)
        self.assertIn("mcperf missing", rendered)
        self.assertIn("mcperf-agent.service not installed", rendered)

    def test_measure_node_ready_does_not_require_agent_service(self) -> None:
        status = ProvisionStatus(
            nodetype="client-measure",
            node_name="client-measure-2dll",
            bootstrap_ready=True,
            mcperf_present=True,
            agent_service_state="not-installed",
        )

        self.assertTrue(status.is_ready)
        self.assertEqual(
            str(status),
            "client-measure (client-measure-2dll): READY - bootstrap ready; mcperf present",
        )

    def test_expectations_text_mentions_agents_and_measure_node(self) -> None:
        expectations = render_provision_expectations()

        self.assertIn("client-agent-a/client-agent-b", expectations)
        self.assertIn("client-measure", expectations)


if __name__ == "__main__":
    unittest.main()
