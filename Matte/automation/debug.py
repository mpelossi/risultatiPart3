from __future__ import annotations

import shlex
from pathlib import Path

from .cluster import ClusterController, NodeInfo
from .config import ExperimentConfig, PolicyConfig
from .manifests import resolve_memcached
from .provision import ProvisionStatus, REQUIRED_CLIENT_NODETYPES


def _shell_join(args: list[str]) -> str:
    return shlex.join(args)


def format_debug_command_hint(
    *,
    config_path: Path,
    policy_path: Path | None = None,
    run_id: str | None = None,
) -> str:
    args = ["python3", "cli.py", "debug", "commands", "--config", str(config_path)]
    if policy_path is not None:
        args.extend(["--policy", str(policy_path)])
    if run_id is not None:
        args.extend(["--run-id", run_id])
    return _shell_join(args)


def summarize_provisioning_hints(statuses: dict[str, ProvisionStatus]) -> list[str]:
    hints: list[str] = []
    for nodetype, status in statuses.items():
        if not status.bootstrap_ready and not status.mcperf_present and status.agent_service_state == "not-installed":
            hints.append(
                f"{nodetype}: bootstrap appears to have failed before mcperf installation; "
                "inspect cloud-init and serial logs."
            )
        elif nodetype.startswith("client-agent") and status.bootstrap_ready and status.mcperf_present:
            if status.agent_service_state == "not-installed":
                hints.append(
                    f"{nodetype}: mcperf is installed, but mcperf-agent.service was not installed; "
                    "inspect cloud-init and serial logs."
                )
            elif status.agent_service_state not in {None, "active"}:
                hints.append(
                    f"{nodetype}: mcperf-agent.service is not active; inspect the service status and logs."
                )
    if any(not status.is_ready for status in statuses.values()):
        hints.append("No memcached pod is expected yet; `run once` only launches memcached after provisioning passes.")
    return hints


def _vm_command_lines(cluster: ClusterController, nodetype: str, node: NodeInfo | None) -> list[str]:
    lines = [f"{nodetype}:"]
    if node is None:
        lines.append("  Node is not currently discoverable via kubectl.")
        lines.append(f"  Expected canonical nodetype: {nodetype}")
        return lines

    ssh_shell = cluster.ssh_command_str(node.name)
    lines.append(f"  SSH shell: {ssh_shell}")
    lines.append(
        "  cloud-final status: "
        + cluster.ssh_command_str(node.name, command="sudo systemctl status cloud-final.service --no-pager -l")
    )
    lines.append(
        "  Bootstrap log tail: "
        + cluster.ssh_command_str(node.name, command="sudo tail -n 200 /var/log/cca-bootstrap.log")
    )
    lines.append(
        "  cloud-final journal: "
        + cluster.ssh_command_str(node.name, command="sudo journalctl -u cloud-final.service -b --no-pager -n 200")
    )
    if nodetype.startswith("client-agent"):
        lines.append(
            "  mcperf-agent status: "
            + cluster.ssh_command_str(node.name, command="sudo systemctl status mcperf-agent.service --no-pager -l")
        )
        lines.append(
            "  mcperf-agent journal: "
            + cluster.ssh_command_str(node.name, command="sudo journalctl -u mcperf-agent.service -n 200 --no-pager")
        )
        lines.append(
            "  Follow mcperf-agent journal: "
            + cluster.ssh_command_str(node.name, command="sudo journalctl -u mcperf-agent.service -f")
        )
        lines.append(
            "  Follow mcperf-agent log file: "
            + cluster.ssh_command_str(node.name, command="sudo tail -f /var/log/mcperf-agent.log")
        )
        lines.append(
            "  Check running mcperf processes: "
            + cluster.ssh_command_str(node.name, command="pgrep -a mcperf")
        )
    lines.append("  Serial console fallback: " + cluster.serial_port_output_command_str(node.name, port=1))
    return lines


def render_debug_commands(
    *,
    experiment: ExperimentConfig,
    cluster: ClusterController,
    policy: PolicyConfig | None = None,
    run_id: str | None = None,
) -> str:
    nodes = cluster.discover_nodes()
    lines = [
        "Debug commands for Part 3 automation",
        "Use SSH for a VM shell. `kubectl exec -it ... -- sh` opens a shell inside a container.",
        "`kubectl logs -f`, `journalctl -f`, and `tail -f` follow live output instead of opening a shell.",
        "",
        "VMs:",
    ]
    for nodetype in REQUIRED_CLIENT_NODETYPES:
        lines.extend(_vm_command_lines(cluster, nodetype, nodes.get(nodetype)))
    lines.extend(
        [
            "",
            "Memcached:",
            "  List memcached pods: " + _shell_join(["kubectl", "get", "pods", "-A", "-l", "cca-project-role=memcached", "-o", "wide"]),
        ]
    )
    if policy is not None and run_id is not None:
        memcached_name = resolve_memcached(policy, run_id).kubernetes_name
        lines.append("  Describe exact pod: " + _shell_join(["kubectl", "describe", "pod", memcached_name]))
        lines.append("  Follow pod logs: " + _shell_join(["kubectl", "logs", "-f", memcached_name]))
        lines.append("  Open a shell in the pod: " + _shell_join(["kubectl", "exec", "-it", memcached_name, "--", "sh"]))
        lines.append(
            "  If the exact pod is missing, that is expected until provisioning passes and the run reaches memcached startup."
        )
    else:
        lines.append("  Describe a pod after listing one: kubectl describe pod <pod>")
        lines.append("  Follow pod logs after listing one: kubectl logs -f <pod>")
        lines.append("  Open a shell in a pod after listing one: kubectl exec -it <pod> -- sh")
        if run_id is not None and policy is None:
            lines.append("  Pass `--policy` as well if you want the exact memcached pod name for that run id.")
        lines.append("  No memcached pod is expected until provisioning passes.")
    lines.extend(["", "Measurement output:"])
    if run_id is not None:
        mcperf_path = experiment.results_root / experiment.experiment_id / run_id / "mcperf.txt"
        lines.append("  Follow mcperf output: " + _shell_join(["tail", "-f", str(mcperf_path)]))
    else:
        experiment_root = experiment.results_root / experiment.experiment_id
        lines.append(f"  Find saved mcperf outputs: find {shlex.quote(str(experiment_root))} -maxdepth 2 -name mcperf.txt | sort")
        lines.append("  Add `--run-id` if you want an exact `tail -f` command for one run.")
    return "\n".join(lines)
