from __future__ import annotations

from dataclasses import dataclass

from .cluster import ClusterController


BOOTSTRAP_SENTINEL = "/opt/cca/bootstrap.done"
REMOTE_MCperf = "/opt/cca/memcache-perf-dynamic/mcperf"


@dataclass(frozen=True)
class ProvisionStatus:
    nodetype: str
    node_name: str
    bootstrap_ready: bool
    mcperf_present: bool
    agent_service_state: str | None


def check_client_provisioning(cluster: ClusterController) -> dict[str, ProvisionStatus]:
    nodes = cluster.discover_nodes()
    required = ("client-agent-a", "client-agent-b", "client-measure")
    statuses: dict[str, ProvisionStatus] = {}
    for nodetype in required:
        if nodetype not in nodes:
            raise RuntimeError(f"Expected node not found: {nodetype}")
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
            raise RuntimeError(f"{nodetype} is not fully bootstrapped: {status}")
        if nodetype.startswith("client-agent") and status.agent_service_state != "active":
            raise RuntimeError(f"{nodetype} agent service is not active: {status.agent_service_state}")
    return statuses

