from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from .cluster import ClusterController


BOOTSTRAP_SENTINEL = "/opt/cca/bootstrap.done"
REMOTE_MCperf = "/opt/cca/memcache-perf-dynamic/mcperf"
REQUIRED_CLIENT_NODETYPES = ("client-agent-a", "client-agent-b", "client-measure")


@dataclass(frozen=True)
class ProvisionStatus:
    nodetype: str
    node_name: str
    bootstrap_ready: bool
    mcperf_present: bool
    agent_service_state: str | None

    @property
    def is_ready(self) -> bool:
        if not self.bootstrap_ready or not self.mcperf_present:
            return False
        if self.nodetype.startswith("client-agent"):
            return self.agent_service_state == "active"
        return True

    def pending_reasons(self) -> list[str]:
        reasons: list[str] = []
        if not self.bootstrap_ready:
            reasons.append("bootstrap not finished")
        if not self.mcperf_present:
            reasons.append("mcperf missing")
        if self.nodetype.startswith("client-agent") and self.agent_service_state != "active":
            if self.agent_service_state == "not-installed":
                reasons.append("mcperf-agent.service not installed")
            elif self.agent_service_state:
                reasons.append(f"mcperf-agent.service is {self.agent_service_state}")
            else:
                reasons.append("mcperf-agent.service state unknown")
        return reasons

    def __str__(self) -> str:
        state = "READY" if self.is_ready else "WAITING"
        reasons = self.pending_reasons()
        if reasons:
            detail = "; ".join(reasons)
        elif self.nodetype.startswith("client-agent"):
            detail = "bootstrap ready; mcperf present; mcperf-agent.service active"
        else:
            detail = "bootstrap ready; mcperf present"
        return f"{self.nodetype} ({self.node_name}): {state} - {detail}"


class ProvisioningError(RuntimeError):
    def __init__(self, message: str, *, statuses: dict[str, ProvisionStatus]):
        super().__init__(message)
        self.statuses = statuses


def render_provision_check_note(ssh_key_path: Path) -> str:
    prompt_count = len(REQUIRED_CLIENT_NODETYPES)
    return (
        f"Provision check will SSH into {prompt_count} client VMs. "
        f"If {ssh_key_path} is passphrase-protected and not loaded in ssh-agent, "
        f"expect up to {prompt_count} passphrase prompts, roughly one per VM. "
        f"Run `ssh-add {ssh_key_path}` first if you want to avoid repeated prompts."
    )


def render_provision_expectations() -> str:
    return (
        "Expected READY state: client-agent-a/client-agent-b need bootstrap ready, "
        "mcperf present, and mcperf-agent.service active; client-measure only needs "
        "bootstrap ready and mcperf present."
    )


def check_client_provisioning(cluster: ClusterController) -> dict[str, ProvisionStatus]:
    nodes = cluster.ensure_canonical_node_labels()
    statuses: dict[str, ProvisionStatus] = {}
    for nodetype in REQUIRED_CLIENT_NODETYPES:
        if nodetype not in nodes:
            discovered = ", ".join(sorted(nodes)) or "none"
            raise RuntimeError(
                f"Expected node not found after canonical labeling: {nodetype}. "
                f"Discovered canonical nodes: {discovered}"
            )
        node = nodes[nodetype]
        command = (
            "bash -lc '"
            f"if [ -f {BOOTSTRAP_SENTINEL} ]; then echo bootstrap=ready; else echo bootstrap=missing; fi; "
            f"if [ -x {REMOTE_MCperf} ]; then echo mcperf=present; else echo mcperf=missing; fi; "
            "if systemctl list-unit-files mcperf-agent.service >/dev/null 2>&1; "
            "then systemctl is-active mcperf-agent.service || true; "
            "else echo not-installed; fi'"
        )
        result = cluster.ssh(node.name, command)
        lines = [line.strip() for line in result.stdout.splitlines() if line.strip()]
        bootstrap_ready = "bootstrap=ready" in lines
        mcperf_present = "mcperf=present" in lines
        service_state = None
        for line in reversed(lines):
            if line in {"active", "inactive", "failed", "not-installed"}:
                service_state = line
                break
        statuses[nodetype] = ProvisionStatus(
            nodetype=nodetype,
            node_name=node.name,
            bootstrap_ready=bootstrap_ready,
            mcperf_present=mcperf_present,
            agent_service_state=service_state,
        )
    return statuses


def assert_client_provisioning(cluster: ClusterController) -> dict[str, ProvisionStatus]:
    statuses = check_client_provisioning(cluster)
    for nodetype, status in statuses.items():
        if not status.bootstrap_ready or not status.mcperf_present:
            raise ProvisioningError(f"{nodetype} is not fully bootstrapped: {status}", statuses=statuses)
        if nodetype.startswith("client-agent") and status.agent_service_state != "active":
            raise ProvisioningError(
                f"{nodetype} agent service is not active: {status.agent_service_state}",
                statuses=statuses,
            )
    return statuses
