from __future__ import annotations

import json
import shlex
import subprocess
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

from .config import ExperimentConfig
from .utils import CommandResult, run_command


@dataclass(frozen=True)
class NodeInfo:
    name: str
    nodetype: str
    internal_ip: str | None
    external_ip: str | None


CANONICAL_NODETYPES = (
    "client-agent-a",
    "client-agent-b",
    "client-measure",
    "node-a-8core",
    "node-b-4core",
)


class ClusterController:
    def __init__(self, config: ExperimentConfig):
        self.config = config

    @property
    def env(self) -> dict[str, str]:
        return {"KOPS_STATE_STORE": self.config.kops_state_store}

    def _public_key_path(self) -> Path:
        if self.config.ssh_key_path.suffix == ".pub":
            return self.config.ssh_key_path
        return Path(str(self.config.ssh_key_path) + ".pub")

    def kops(self, *args: str, check: bool = True) -> CommandResult:
        return run_command(["kops", *args], env=self.env, check=check)

    def kubectl(self, *args: str, check: bool = True) -> CommandResult:
        return run_command(["kubectl", *args], env=self.env, check=check)

    def _announce(self, message: str) -> None:
        print(f"[cluster] {message}")

    def kubectl_json(self, *args: str) -> dict[str, object]:
        result = self.kubectl(*args)
        return json.loads(result.stdout)

    def cluster_exists(self) -> bool:
        result = self.kops("get", "cluster", "--name", self.config.cluster_name, check=False)
        return result.returncode == 0

    def cluster_up(self) -> None:
        self._announce(f"Preparing cluster {self.config.cluster_name}")
        if not self.cluster_exists():
            self._announce(f"Creating cluster definition from {self.config.cluster_config_path}")
            run_command(
                ["kops", "create", "-f", str(self.config.cluster_config_path)],
                env=self.env,
                live_output=True,
                output_prefix="[kops] ",
            )
        else:
            self._announce(f"Updating existing cluster definition from {self.config.cluster_config_path}")
            run_command(
                ["kops", "replace", "-f", str(self.config.cluster_config_path), "--force"],
                env=self.env,
                live_output=True,
                output_prefix="[kops] ",
            )
        public_key = self._public_key_path()
        if not public_key.exists():
            raise FileNotFoundError(f"SSH public key not found: {public_key}")
        self._announce(f"Ensuring SSH admin key exists: {public_key}")
        create_secret = self.kops(
            "create",
            "secret",
            "--name",
            self.config.cluster_name,
            "sshpublickey",
            "admin",
            "-i",
            str(public_key),
            check=False,
        )
        if create_secret.returncode != 0 and "already exists" not in create_secret.combined_output.lower():
            raise RuntimeError(create_secret.combined_output)
        self._announce("Applying cloud changes with kops update")
        run_command(
            ["kops", "update", "cluster", "--name", self.config.cluster_name, "--yes", "--admin"],
            env=self.env,
            live_output=True,
            output_prefix="[kops] ",
        )
        self._announce("Waiting for cluster validation to succeed")
        run_command(
            ["kops", "validate", "cluster", "--wait", "10m"],
            env=self.env,
            live_output=True,
            output_prefix="[kops] ",
        )
        self._announce("Exporting kubeconfig for kubectl")
        run_command(
            ["kops", "export", "kubecfg", "--admin", "--name", self.config.cluster_name],
            env=self.env,
            live_output=True,
            output_prefix="[kops] ",
        )
        self._announce("Ensuring canonical node labels")
        self.ensure_canonical_node_labels()
        self._announce("Cluster is ready")

    def cluster_down(self) -> None:
        self._announce(f"Deleting cluster {self.config.cluster_name}")
        run_command(
            ["kops", "delete", "cluster", "--name", self.config.cluster_name, "--yes"],
            env=self.env,
            live_output=True,
            output_prefix="[kops] ",
        )

    def _infer_canonical_nodetype(self, node_name: str) -> str | None:
        for nodetype in sorted(CANONICAL_NODETYPES, key=len, reverse=True):
            if node_name == nodetype or node_name.startswith(f"{nodetype}-"):
                return nodetype
        return None

    def _node_info_from_payload(
        self,
        item: dict[str, object],
        *,
        nodetype: str,
    ) -> NodeInfo:
        metadata = item.get("metadata", {})
        status = item.get("status", {})
        addresses = status.get("addresses", [])
        internal_ip = None
        external_ip = None
        for address in addresses:
            address_type = address.get("type")
            if address_type == "InternalIP":
                internal_ip = address.get("address")
            elif address_type == "ExternalIP":
                external_ip = address.get("address")
        return NodeInfo(
            name=metadata.get("name"),
            nodetype=nodetype,
            internal_ip=internal_ip,
            external_ip=external_ip,
        )

    def _discover_nodes_from_payload(
        self,
        payload: dict[str, object],
        *,
        allow_name_inference: bool,
    ) -> dict[str, NodeInfo]:
        nodes: dict[str, NodeInfo] = {}
        for item in payload.get("items", []):
            labels = item.get("metadata", {}).get("labels", {})
            nodetype = labels.get("cca-project-nodetype")
            if not nodetype and allow_name_inference:
                node_name = item.get("metadata", {}).get("name")
                if isinstance(node_name, str) and node_name:
                    nodetype = self._infer_canonical_nodetype(node_name)
            if not nodetype:
                continue
            info = self._node_info_from_payload(item, nodetype=nodetype)
            existing = nodes.get(nodetype)
            if existing is not None and existing.name != info.name:
                raise RuntimeError(
                    "Multiple Kubernetes nodes map to the same canonical nodetype "
                    f"{nodetype}: {existing.name}, {info.name}"
                )
            nodes[nodetype] = info
        return nodes

    def discover_nodes(self) -> dict[str, NodeInfo]:
        payload = self.kubectl_json("get", "nodes", "-o", "json")
        return self._discover_nodes_from_payload(payload, allow_name_inference=True)

    def ensure_canonical_node_labels(self) -> dict[str, NodeInfo]:
        payload = self.kubectl_json("get", "nodes", "-o", "json")
        updated = False
        for item in payload.get("items", []):
            metadata = item.get("metadata", {})
            node_name = metadata.get("name")
            if not isinstance(node_name, str) or not node_name:
                continue
            desired_nodetype = self._infer_canonical_nodetype(node_name)
            if desired_nodetype is None:
                continue
            labels = metadata.get("labels", {})
            current_nodetype = labels.get("cca-project-nodetype")
            if current_nodetype == desired_nodetype:
                continue
            self._announce(
                f"Labeling node {node_name} with cca-project-nodetype={desired_nodetype}"
            )
            self.kubectl(
                "label",
                "nodes",
                node_name,
                f"cca-project-nodetype={desired_nodetype}",
                "--overwrite",
            )
            updated = True
        if updated:
            payload = self.kubectl_json("get", "nodes", "-o", "json")
        return self._discover_nodes_from_payload(payload, allow_name_inference=True)

    def ssh_args(
        self,
        node_name: str,
        *,
        command: str | None = None,
    ) -> list[str]:
        args = [
            "gcloud",
            "compute",
            "ssh",
            f"{self.config.ssh_user}@{node_name}",
            "--zone",
            self.config.zone,
            "--ssh-key-file",
            str(self.config.ssh_key_path),
        ]
        if command is not None:
            args.extend(["--command", command])
        return args

    def ssh_command_str(
        self,
        node_name: str,
        *,
        command: str | None = None,
    ) -> str:
        return shlex.join(self.ssh_args(node_name, command=command))

    def serial_port_output_args(
        self,
        node_name: str,
        *,
        port: int = 1,
    ) -> list[str]:
        return [
            "gcloud",
            "compute",
            "instances",
            "get-serial-port-output",
            node_name,
            "--zone",
            self.config.zone,
            f"--port={port}",
        ]

    def serial_port_output_command_str(
        self,
        node_name: str,
        *,
        port: int = 1,
    ) -> str:
        return shlex.join(self.serial_port_output_args(node_name, port=port))

    def ssh(
        self,
        node_name: str,
        command: str,
        *,
        check: bool = True,
    ) -> CommandResult:
        return run_command(self.ssh_args(node_name, command=command), check=check)

    def popen_ssh(
        self,
        node_name: str,
        command: str,
        *,
        stdout,
        stderr,
    ) -> subprocess.Popen[str]:
        return subprocess.Popen(
            self.ssh_args(node_name, command=command),
            text=True,
            stdout=stdout,
            stderr=stderr,
        )

    def apply_manifest(self, manifest_path: Path) -> None:
        self.kubectl("apply", "-f", str(manifest_path))

    def cleanup_managed_workloads(self) -> None:
        self._announce("Deleting previous managed jobs and pods")
        self.kubectl("delete", "jobs", "-l", "cca-project-managed=true", "--ignore-not-found=true", check=False)
        self.kubectl("delete", "pods", "-l", "cca-project-managed=true", "--ignore-not-found=true", check=False)
        deadline = time.time() + 180
        iteration = 0
        while time.time() < deadline:
            pods = self.kubectl_json("get", "pods", "-l", "cca-project-managed=true", "-o", "json")
            jobs = self.kubectl_json("get", "jobs", "-l", "cca-project-managed=true", "-o", "json")
            if not pods.get("items") and not jobs.get("items"):
                self._announce("Managed workload cleanup finished")
                return
            if iteration == 0 or iteration % 3 == 0:
                self._announce(
                    "Waiting for managed workloads to disappear: "
                    f"{len(pods.get('items', []))} pods, {len(jobs.get('items', []))} jobs remaining"
                )
            iteration += 1
            time.sleep(5)
        raise TimeoutError("Timed out while cleaning managed workloads")

    def wait_for_pod_ready(self, pod_name: str, timeout_s: int = 300) -> None:
        self._announce(f"Waiting for pod/{pod_name} to become Ready (timeout {timeout_s}s)")
        self.kubectl("wait", "--for=condition=Ready", f"pod/{pod_name}", f"--timeout={timeout_s}s")
        self._announce(f"pod/{pod_name} is Ready")

    def get_pod_by_run_role(self, run_id: str, role: str) -> dict[str, object]:
        payload = self.kubectl_json(
            "get",
            "pods",
            "-l",
            f"cca-project-run-id={run_id},cca-project-role={role}",
            "-o",
            "json",
        )
        items = payload.get("items", [])
        if not items:
            raise RuntimeError(f"No pod found for role={role} run_id={run_id}")
        return items[0]

    def _job_snapshot_from_payload(self, payload: dict[str, object]) -> dict[str, object]:
        status = payload.get("status", {})
        succeeded = status.get("succeeded", 0) or 0
        failed = status.get("failed", 0) or 0
        if succeeded >= 1:
            state = "completed"
        elif failed >= 1:
            state = "failed"
        else:
            state = "running"
        return {"status": state, "payload": payload}

    def get_run_jobs_snapshot(self, run_id: str) -> dict[str, dict[str, object]]:
        payload = self.kubectl_json(
            "get",
            "jobs",
            "-l",
            f"cca-project-run-id={run_id}",
            "-o",
            "json",
        )
        snapshots: dict[str, dict[str, object]] = {}
        for item in payload.get("items", []):
            metadata = item.get("metadata", {})
            job_name = metadata.get("name")
            if isinstance(job_name, str) and job_name:
                snapshots[job_name] = self._job_snapshot_from_payload(item)
        return snapshots

    def get_jobs_snapshot(self, job_names: Iterable[str]) -> dict[str, dict[str, object]]:
        snapshots: dict[str, dict[str, object]] = {}
        for job_name in job_names:
            result = self.kubectl("get", "job", job_name, "-o", "json", check=False)
            if result.returncode != 0:
                snapshots[job_name] = {"status": "missing"}
                continue
            payload = json.loads(result.stdout)
            snapshots[job_name] = self._job_snapshot_from_payload(payload)
        return snapshots

    def wait_for_jobs(self, job_names: list[str], timeout_s: int) -> None:
        deadline = time.time() + timeout_s
        while time.time() < deadline:
            snapshot = self.get_jobs_snapshot(job_names)
            statuses = {name: info["status"] for name, info in snapshot.items()}
            if any(status == "failed" for status in statuses.values()):
                raise RuntimeError(f"One or more jobs failed: {statuses}")
            if all(status == "completed" for status in statuses.values()):
                return
            time.sleep(5)
        raise TimeoutError(f"Timed out waiting for jobs: {job_names}")

    def capture_pods_json(self, destination: Path) -> None:
        result = self.kubectl("get", "pods", "-o", "json")
        destination.write_text(result.stdout, encoding="utf-8")

    def describe_job(self, job_name: str, destination: Path) -> None:
        result = self.kubectl("describe", "job", job_name, check=False)
        destination.write_text(result.combined_output + "\n", encoding="utf-8")
