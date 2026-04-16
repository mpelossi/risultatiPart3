from __future__ import annotations

import json
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

    def kubectl_json(self, *args: str) -> dict[str, object]:
        result = self.kubectl(*args)
        return json.loads(result.stdout)

    def cluster_exists(self) -> bool:
        result = self.kops("get", "cluster", "--name", self.config.cluster_name, check=False)
        return result.returncode == 0

    def cluster_up(self) -> None:
        if not self.cluster_exists():
            self.kops("create", "-f", str(self.config.cluster_config_path))
        else:
            self.kops("replace", "-f", str(self.config.cluster_config_path), "--force")
        public_key = self._public_key_path()
        if not public_key.exists():
            raise FileNotFoundError(f"SSH public key not found: {public_key}")
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
        self.kops("update", "cluster", "--name", self.config.cluster_name, "--yes", "--admin")
        self.kops("validate", "cluster", "--wait", "10m")

    def cluster_down(self) -> None:
        self.kops("delete", "cluster", "--name", self.config.cluster_name, "--yes")

    def discover_nodes(self) -> dict[str, NodeInfo]:
        payload = self.kubectl_json("get", "nodes", "-o", "json")
        nodes: dict[str, NodeInfo] = {}
        for item in payload.get("items", []):
            labels = item.get("metadata", {}).get("labels", {})
            nodetype = labels.get("cca-project-nodetype")
            if not nodetype:
                continue
            addresses = item.get("status", {}).get("addresses", [])
            internal_ip = None
            external_ip = None
            for address in addresses:
                address_type = address.get("type")
                if address_type == "InternalIP":
                    internal_ip = address.get("address")
                elif address_type == "ExternalIP":
                    external_ip = address.get("address")
            nodes[nodetype] = NodeInfo(
                name=item.get("metadata", {}).get("name"),
                nodetype=nodetype,
                internal_ip=internal_ip,
                external_ip=external_ip,
            )
        return nodes

    def ssh(
        self,
        node_name: str,
        command: str,
        *,
        check: bool = True,
    ) -> CommandResult:
        return run_command(
            [
                "gcloud",
                "compute",
                "ssh",
                f"{self.config.ssh_user}@{node_name}",
                "--zone",
                self.config.zone,
                "--ssh-key-file",
                str(self.config.ssh_key_path),
                "--command",
                command,
            ],
            check=check,
        )

    def popen_ssh(
        self,
        node_name: str,
        command: str,
        *,
        stdout,
        stderr,
    ) -> subprocess.Popen[str]:
        return subprocess.Popen(
            [
                "gcloud",
                "compute",
                "ssh",
                f"{self.config.ssh_user}@{node_name}",
                "--zone",
                self.config.zone,
                "--ssh-key-file",
                str(self.config.ssh_key_path),
                "--command",
                command,
            ],
            text=True,
            stdout=stdout,
            stderr=stderr,
        )

    def apply_manifest(self, manifest_path: Path) -> None:
        self.kubectl("apply", "-f", str(manifest_path))

    def cleanup_managed_workloads(self) -> None:
        self.kubectl("delete", "jobs", "-l", "cca-project-managed=true", "--ignore-not-found=true", check=False)
        self.kubectl("delete", "pods", "-l", "cca-project-managed=true", "--ignore-not-found=true", check=False)
        deadline = time.time() + 180
        while time.time() < deadline:
            pods = self.kubectl_json("get", "pods", "-l", "cca-project-managed=true", "-o", "json")
            jobs = self.kubectl_json("get", "jobs", "-l", "cca-project-managed=true", "-o", "json")
            if not pods.get("items") and not jobs.get("items"):
                return
            time.sleep(5)
        raise TimeoutError("Timed out while cleaning managed workloads")

    def wait_for_pod_ready(self, pod_name: str, timeout_s: int = 300) -> None:
        self.kubectl("wait", "--for=condition=Ready", f"pod/{pod_name}", f"--timeout={timeout_s}s")

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
