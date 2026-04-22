from __future__ import annotations

import shlex
from pathlib import Path

from .cluster import ClusterController, NodeInfo
from .config import ExperimentConfig, PolicyConfig
from .manifests import resolve_memcached
from .provision import ProvisionStatus, REQUIRED_CLIENT_NODETYPES


def _shell_join(args: list[str]) -> str:
    return shlex.join(args)


def _append_command(lines: list[str], label: str, command: str, *, indent: str = "  ") -> None:
    lines.append(f"{indent}{label}: {command}")


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

    lines[0] = f"{nodetype} ({node.name}):"
    _append_command(lines, "Open an SSH shell", cluster.ssh_command_str(node.name))
    _append_command(
        lines,
        "Check cloud-init status",
        cluster.ssh_command_str(node.name, command="sudo systemctl status cloud-final.service --no-pager -l"),
    )
    _append_command(
        lines,
        "Read bootstrap log",
        cluster.ssh_command_str(node.name, command="sudo tail -n 200 /var/log/cca-bootstrap.log"),
    )
    _append_command(
        lines,
        "Read cloud-init journal",
        cluster.ssh_command_str(node.name, command="sudo journalctl -u cloud-final.service -b --no-pager -n 200"),
    )
    if nodetype.startswith("client-agent"):
        _append_command(
            lines,
            "Check mcperf-agent.service status",
            cluster.ssh_command_str(node.name, command="sudo systemctl status mcperf-agent.service --no-pager -l"),
        )
        _append_command(
            lines,
            "Read mcperf-agent journal",
            cluster.ssh_command_str(node.name, command="sudo journalctl -u mcperf-agent.service -n 200 --no-pager"),
        )
        _append_command(
            lines,
            "Follow live mcperf-agent output",
            cluster.ssh_command_str(node.name, command="sudo journalctl -u mcperf-agent.service -f"),
        )
        _append_command(
            lines,
            "Follow /var/log/mcperf-agent.log",
            cluster.ssh_command_str(node.name, command="sudo tail -f /var/log/mcperf-agent.log"),
        )
    _append_command(
        lines,
        "Check running mcperf processes",
        cluster.ssh_command_str(node.name, command="pgrep -a mcperf"),
    )
    _append_command(lines, "Read serial console output", cluster.serial_port_output_command_str(node.name, port=1))
    return lines


def render_debug_commands(
    *,
    experiment: ExperimentConfig,
    cluster: ClusterController,
    policy: PolicyConfig | None = None,
    run_id: str | None = None,
) -> str:
    nodes = cluster.discover_nodes()
    mcperf_tail_command: str | None = None
    if run_id is not None:
        mcperf_path = experiment.results_root / experiment.experiment_id / run_id / "mcperf.txt"
        mcperf_tail_command = _shell_join(["tail", "-f", str(mcperf_path)])

    lines = [
        "Part 3 debug guide",
        "",
        "Resolved nodes:",
    ]
    for nodetype in REQUIRED_CLIENT_NODETYPES:
        node = nodes.get(nodetype)
        if node is None:
            lines.append(f"- {nodetype}: not currently discoverable")
        else:
            lines.append(f"- {nodetype}: {node.name}")

    lines.extend(
        [
            "",
            "What to run first:",
            "- To watch load generation, follow `mcperf-agent.service` on `client-agent-a` and `client-agent-b`.",
            "- To watch the benchmark output, follow the local `mcperf.txt` file created by `run once`.",
            "- To confirm the measurement process exists on `client-measure`, run `pgrep -a mcperf` there.",
            "- If provisioning is failing, start with `cloud-final.service`, `/var/log/cca-bootstrap.log`, and the serial console.",
            "",
            "How to read the commands:",
            "- `gcloud compute ssh ...` opens a shell on a VM.",
            "- `kubectl exec -it ... -- sh` opens a shell inside a container.",
            "- `kubectl logs -f`, `journalctl -f`, and `tail -f` stream live output.",
            "- `client-agent-a` and `client-agent-b` run the long-lived `mcperf-agent.service`.",
            "- `client-measure` does not run `mcperf-agent.service`; `run once` starts `./mcperf ...` over SSH and saves stdout into your local `mcperf.txt`.",
            "",
            "Live mcperf output:",
        ]
    )
    if nodes.get("client-agent-a") is not None:
        lines.append(
            "- Agent A output: "
            + cluster.ssh_command_str(
                nodes["client-agent-a"].name,
                command="sudo journalctl -u mcperf-agent.service -f",
            )
        )
    if nodes.get("client-agent-b") is not None:
        lines.append(
            "- Agent B output: "
            + cluster.ssh_command_str(
                nodes["client-agent-b"].name,
                command="sudo journalctl -u mcperf-agent.service -f",
            )
        )
    if mcperf_tail_command is not None:
        lines.append(f"- Measurement output on your workstation: {mcperf_tail_command}")
    else:
        lines.append(
            "- Measurement output on your workstation: add `--run-id <run-id>` to get the exact `tail -f .../mcperf.txt` command."
        )
    if nodes.get("client-measure") is not None:
        lines.append(
            "- Check whether the measurement process exists on client-measure: "
            + cluster.ssh_command_str(nodes["client-measure"].name, command="pgrep -a mcperf")
        )

    lines.extend(["", "Exact VM commands:"])
    for nodetype in REQUIRED_CLIENT_NODETYPES:
        lines.extend(_vm_command_lines(cluster, nodetype, nodes.get(nodetype)))

    lines.extend(
        [
            "",
            "Memcached:",
            "  First check whether a memcached pod exists: "
            + _shell_join(["kubectl", "get", "pods", "-A", "-l", "cca-project-role=memcached", "-o", "wide"]),
        ]
    )
    if policy is not None and run_id is not None:
        memcached_name = resolve_memcached(policy, run_id).kubernetes_name
        lines.append("  Describe the exact pod: " + _shell_join(["kubectl", "describe", "pod", memcached_name]))
        lines.append("  Follow the pod logs: " + _shell_join(["kubectl", "logs", "-f", memcached_name]))
        lines.append("  Open a shell in the pod: " + _shell_join(["kubectl", "exec", "-it", memcached_name, "--", "sh"]))
        lines.append(
            "  If the exact pod is missing, that is expected until provisioning passes and the run reaches memcached startup."
        )
    else:
        lines.append("  After you find a pod, describe it: kubectl describe pod <pod>")
        lines.append("  After you find a pod, follow its logs: kubectl logs -f <pod>")
        lines.append("  After you find a pod, open a shell inside it: kubectl exec -it <pod> -- sh")
        if run_id is not None and policy is None:
            lines.append("  Pass `--policy` as well if you want the exact memcached pod name for that run id.")
        lines.append("  No memcached pod is expected until provisioning passes.")

    lines.extend(["", "Measurement output:"])
    if mcperf_tail_command is not None:
        lines.append("  Follow the live measurement output on your workstation: " + mcperf_tail_command)
        lines.append("  This is usually the most useful place to watch the benchmark while `run once` is active.")
    else:
        experiment_root = experiment.results_root / experiment.experiment_id
        lines.append(
            f"  Find saved mcperf outputs on your workstation: find {shlex.quote(str(experiment_root))} -maxdepth 2 -name mcperf.txt | sort"
        )
        lines.append("  Add `--run-id` if you want the exact `tail -f` command for one run.")
    return "\n".join(lines)
