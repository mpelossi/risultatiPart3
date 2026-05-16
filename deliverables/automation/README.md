# Part 3 Automation

## Purpose

This directory contains the Python automation layer used for Part 3 of the Cloud Computing Architecture project. It was built to replace the manual workflow used during early experiments: creating and validating the kOps cluster, discovering randomized VM and Kubernetes node names, finding client IP addresses, entering VMs over SSH, checking whether client bootstrap had finished, building the augmented `mcperf` tooling, keeping the long-lived `mcperf` load-agent services running, launching `memcached`, running batch jobs with `kubectl`, and collecting the required outputs.

The automation is the coordination layer for reproducible measurements. It runs `memcached` together with the seven native PARSEC and SPLASH-2x batch workloads, applies the selected YAML scheduling policy, starts and stops the `mcperf` measurement, stores the raw `kubectl get pods -o json` and `mcperf` outputs, and summarizes each run against the Part 3 objective: minimize batch makespan while keeping `memcached` below the strict 1 ms p95 latency SLO at 30K QPS.

All commands below assume the current working directory is this `automation/` directory. If the submission archive is extracted under any name, enter the automation directory from the extracted submission root:

```bash
cd automation
```

## Directory Contents

| Path | Purpose |
| --- | --- |
| `cli.py` | Main command-line entrypoint for cluster setup, provisioning checks, policy inspection, experiment runs, result ranking, and submission export. |
| `experiment.yaml` | Experiment configuration for group 054: cluster name, zone, kOps state store, SSH key, result root, and `mcperf` parameters. |
| `part3.yaml` | kOps cluster definition used by the automation. It includes the master, two benchmark nodes, and three `mcperf` client nodes. |
| `schedule.yaml` | Default policy used for local experimentation. The final submitted policies are stored under `schedules/`. |
| `schedules/best_handcrafted.yaml` | Final hand-crafted policy used for task 3.1 measurements. |
| `schedules/second_best_handcrafted.yaml` | Additional hand-crafted candidate kept for comparison and reproducibility. |
| `schedules/openevolve_seed.yaml` | Initial policy given to OpenEvolve. |
| `schedules/best_openevolve.yaml` | Final OpenEvolve-generated policy used for task 3.2 measurements. |
| `runner.py` | Live experiment orchestration: cleanup, image pre-cache, `memcached`, `mcperf`, batch phases, and artifact collection. |
| `cluster.py`, `provision.py` | kOps, Kubernetes, GCP, SSH, and client bootstrap checks. |
| `config.py`, `catalog.py`, `manifests.py` | Policy parsing, workload metadata, validation, and Kubernetes manifest rendering. |
| `audit.py`, `schedule_viewer_data.py` | Static schedule inspection and schedule viewer data. |
| `metrics.py`, `timing.py`, `runtime_stats.py`, `results.py`, `viewer.py` | Run summarization, timing extraction, observed runtime statistics, result ranking, and the supported local result viewer. |
| `gui.py` | Deprecated experimental Python/Tkinter schedule GUI from development. It is retained for traceability, but the supported inspection tool is `python3 cli.py results viewer`. |
| `export.py` | Export helper for assignment-formatted task 3.1 result folders. |
| `runs/` | Archived automated runs. Each run contains the selected policy, copied experiment configuration, generated manifests, raw measurement files, and derived summaries. |
| `schedule_queue.yaml` | Experimental queue file from development. It is not the primary reproduction path because the submitted final policies are the named files under `schedules/`. |

## Requirements

The automation expects the same environment as the assignment setup:

- `python3` with `PyYAML` installed.
- `gcloud`, `kops`, and `kubectl` on `PATH`.
- Valid Google Cloud authentication for project `cca-eth-2026-group-54`.
- Valid application-default credentials if required by the local GCP setup.
- SSH key `~/.ssh/cloud-computing` with the public key available to kOps.
- The configured kOps state store from `experiment.yaml`: `gs://cca-eth-2026-group-54-mpelossi`.
- Enough cloud credits to recreate the Part 3 cluster if live reproduction is required.

On Windows, use `python` instead of `python3` if that is how Python is installed locally.

## Quick Reproduction Path

First verify that the CLI loads:

```bash
python3 cli.py --help
```

Inspect the archived submitted runs with the supported result viewer. This does not require recreating the cluster:

```bash
python3 cli.py results viewer
```

For a headless or SSH session, keep the server running without trying to open a browser:

```bash
python3 cli.py results viewer --no-open
```

Create or refresh the cluster only if it does not already exist, if it was deleted, or if `part3.yaml` changed:

```bash
python3 cli.py cluster up --config experiment.yaml
```

Check that the three client nodes are bootstrapped and that the `mcperf-agent.service` units are running on the two agent nodes:

```bash
python3 cli.py provision check --config experiment.yaml
```

Inspect the final submitted policies:

```bash
python3 cli.py show --policy schedules/best_handcrafted.yaml
python3 cli.py show --policy schedules/best_openevolve.yaml
```

Optionally run the static audit if a local Part 2 timing CSV is available. The submitted `latexSubmission/` tree does not include `Part2summary_times.csv`, so pass an explicit path:

```bash
python3 cli.py audit --policy schedules/best_handcrafted.yaml --times-csv /path/to/Part2summary_times.csv
python3 cli.py audit --policy schedules/best_openevolve.yaml --times-csv /path/to/Part2summary_times.csv
```

Before spending time on a live run, render the manifests and phase plan without touching the cluster:

```bash
python3 cli.py run once --config experiment.yaml --policy schedules/best_handcrafted.yaml --dry-run
```

Warm container images on both benchmark nodes without starting a measurement run:

```bash
python3 cli.py run precache --config experiment.yaml --policy schedules/best_handcrafted.yaml
```

Run three repetitions of the hand-crafted policy:

```bash
python3 cli.py run batch --config experiment.yaml --policy schedules/best_handcrafted.yaml --runs 3 --precache
```

Rank the archived hand-crafted and OpenEvolve run groups:

```bash
python3 cli.py results best --experiment part3-PartA
python3 cli.py results best --experiment part3-PartB
```

Export the three most recent successful task 3.1 runs into the assignment filename format:

```bash
python3 cli.py export submission --experiment part3-PartA --group 054 --task 3_1 --output-root ..
```

The export command currently implements only task `3_1`. The task `3_2` OpenEvolve measurement outputs are already supplied in `../part_3_2_results_group_054`.

When finished with live experiments, delete the cluster to stop cloud charges:

```bash
python3 cli.py cluster down --config experiment.yaml
```

## Final Policies

The final policies are plain YAML files. Each policy specifies the dedicated `memcached` placement and one entry for each required batch job: `barnes`, `blackscholes`, `canneal`, `freqmine`, `radix`, `streamcluster`, and `vips`.

| Policy file | Role |
| --- | --- |
| `schedules/best_handcrafted.yaml` | Final hand-crafted policy for task 3.1. This is the primary policy for reproducing `part_3_1_results_group_054`. |
| `schedules/second_best_handcrafted.yaml` | Additional hand-crafted policy retained to document the policy search process. |
| `schedules/openevolve_seed.yaml` | Baseline policy used as the initial OpenEvolve program schedule. |
| `schedules/best_openevolve.yaml` | Best OpenEvolve-generated policy used for task 3.2 measurements. |
| `schedule.yaml` | Working default policy for local command examples and development. |

The accepted policy schema is intentionally small:

```yaml
policy_name: "example"
memcached:
  node: "node-b-4core"
  cores: "0"
  threads: 1
jobs:
  blackscholes:
    node: "node-b-4core"
    cores: "1-3"
    threads: 3
    after: "start"
```

The `after` field may be `"start"`, a single predecessor job, or a list of predecessor jobs. Core assignments use Linux CPU-set syntax such as `"0"`, `"1-3"`, or `"0,2,4"`. The automation validates that each job uses supported nodes, valid cores, and no more threads than pinned cores.

## How The Automation Works

The experiment lifecycle is:

1. `config.py` loads `experiment.yaml` and the selected policy YAML.
2. `cluster.py` creates or refreshes the kOps cluster from `part3.yaml`, exports kubeconfig, and labels randomized Kubernetes nodes with stable labels such as `node-a-8core`, `node-b-4core`, `client-agent-a`, `client-agent-b`, and `client-measure`.
3. `provision.py` checks that the client VMs built the augmented `memcache-perf-dynamic` binary and that the agent VMs are running `mcperf-agent.service`.
4. `manifests.py` renders one `memcached` pod and one Kubernetes Job per batch workload using the selected node, core, and thread assignments.
5. `runner.py` cleans previous managed workloads, optionally pre-caches images, launches `memcached`, starts the `mcperf` measurement from `client-measure`, runs batch phases in dependency order, and stops the measurement after the final batch job completes.
6. `collect.py`, `metrics.py`, and `timing.py` save the raw pod snapshot and derive a convenience summary from `results.json` and `mcperf.txt`.
7. `runtime_stats.py` updates `runs/runtime_stats.json` from completed run history. The result viewer and development tooling use this file for observed runtime estimates when it exists.
8. `results.py`, `viewer.py`, and `export.py` support ranking runs, inspecting run timelines, and producing assignment-formatted result folders.

## CLI Reference

| Command | Purpose |
| --- | --- |
| `python3 cli.py cluster up --config experiment.yaml` | Create or refresh the Part 3 kOps cluster and label nodes. |
| `python3 cli.py cluster down --config experiment.yaml` | Delete the Part 3 cluster. |
| `python3 cli.py provision check --config experiment.yaml` | Check client VM bootstrap state and `mcperf` agent readiness. |
| `python3 cli.py debug commands --config experiment.yaml --policy schedules/best_handcrafted.yaml` | Print exact SSH, Kubernetes, log, and serial-console commands for debugging. |
| `python3 cli.py show --policy schedules/best_handcrafted.yaml` | Print the policy in launch order. |
| `python3 cli.py audit --policy schedules/best_handcrafted.yaml --times-csv /path/to/Part2summary_times.csv` | Run the optional static schedule checker with an explicit local timing CSV. |
| `python3 cli.py run once --config experiment.yaml --policy schedules/best_handcrafted.yaml --dry-run` | Render manifests and the phase plan without contacting the live cluster. |
| `python3 cli.py run precache --config experiment.yaml --policy schedules/best_handcrafted.yaml` | Pull benchmark and `memcached` images onto both benchmark nodes, then exit. |
| `python3 cli.py run once --config experiment.yaml --policy schedules/best_handcrafted.yaml --precache` | Run one live experiment and warm images first. |
| `python3 cli.py run batch --config experiment.yaml --policy schedules/best_handcrafted.yaml --runs 3 --precache` | Run repeated live measurements for one policy. |
| `python3 cli.py stats rebuild --results-root runs` | Rebuild `runs/runtime_stats.json` from saved run artifacts. |
| `python3 cli.py results best --experiment part3-PartA` | Rank archived hand-crafted runs by pass status, makespan, and observed p95 latency. |
| `python3 cli.py results best --experiment part3-PartB` | Rank archived OpenEvolve runs by pass status, makespan, and observed p95 latency. |
| `python3 cli.py results viewer` | Serve the supported local result viewer for archived runs, summaries, timelines, run-local `policy.yaml`, and generated manifests. |
| `python3 cli.py results viewer --experiment part3-PartB --no-open` | Serve the result viewer for one experiment group without opening a browser. |
| `python3 cli.py export submission --experiment part3-PartA --group 054 --task 3_1 --output-root ..` | Write `../part_3_1_results_group_054` with `pods_1.json` to `pods_3.json` and `mcperf_1.txt` to `mcperf_3.txt`. |

`run queue` is available in the CLI, but it is not the recommended reproduction path for this submission. Use the named final policies directly.

## Output And Submission Mapping

Live runs are written under:

```text
runs/<experiment_id>/<run_id>/
```

In this submission, the archived run history contains:

```text
runs/part3-PartA/<timestamp>/
runs/part3-PartB/<timestamp>/
```

`part3-PartA` contains the hand-crafted policy runs. `part3-PartB` contains the OpenEvolve policy runs. Each timestamped directory is a self-contained record of one automated run, including the configuration and YAML files used to launch it.

A successful timestamped run directory contains:

| Artifact | Meaning |
| --- | --- |
| `experiment.yaml` | Copy of the experiment configuration used for that specific run. |
| `policy.yaml` | Copy of the selected scheduling policy YAML used for that specific run. This is the run-local version of the policy, so it remains available even if the top-level schedule files are later edited. |
| `phase_plan.json` | Concrete batch launch plan after policy parsing and dependency resolution. |
| `rendered_manifests/` | Run-local Kubernetes YAML generated from the policy. It contains `memcached.yaml` plus one YAML file per batch job: `barnes.yaml`, `blackscholes.yaml`, `canneal.yaml`, `freqmine.yaml`, `radix.yaml`, `streamcluster.yaml`, and `vips.yaml`. |
| `events.log` | High-level runner event log. |
| `results.json` | Raw `kubectl get pods -o json` output for the run. This is copied to `pods_i.json` for submission. |
| `mcperf.txt` | Raw `mcperf` measurement output. This is copied to `mcperf_i.txt` for submission. |
| `summary.json` | Derived run status, makespan, p95 latency, SLO status, and job timing summary. |
| `node_platforms.json` | GCP machine type and CPU platform observed for the benchmark nodes. |

The submitted assignment result folders are one level above this directory:

```text
../part_3_1_results_group_054/
../part_3_2_results_group_054/
```

Each folder must contain exactly the assignment-required measurement files:

```text
pods_1.json
pods_2.json
pods_3.json
mcperf_1.txt
mcperf_2.txt
mcperf_3.txt
```

`part_3_1_results_group_054` contains the hand-crafted policy evaluation. `part_3_2_results_group_054` contains the OpenEvolve-generated policy evaluation. The OpenEvolve source, evaluator, configuration, best program, log, and checkpoint are supplied separately in `../part_3_openevolve/`.

## Troubleshooting

### Cluster validation times out

If `cluster up` reaches `kops validate cluster` and fails with connection refused, I/O timeout, or TLS handshake timeout, the control-plane VM may exist before the Kubernetes API server is ready. Try a longer validation before deleting the cluster:

```bash
export KOPS_STATE_STORE=gs://cca-eth-2026-group-54-mpelossi
kops validate cluster --name part3.k8s.local --wait 20m
```

If that succeeds, rerun:

```bash
python3 cli.py cluster up --config experiment.yaml
```

If validation still cannot reach the API server, clean up before retrying:

```bash
python3 cli.py cluster down --config experiment.yaml
```

### `kubectl` uses `localhost:8080`

This usually means kubeconfig is missing or stale. If the cluster exists, export kubeconfig again:

```bash
kops export kubecfg --admin --name part3.k8s.local
```

If the cluster does not exist, recreate it:

```bash
python3 cli.py cluster up --config experiment.yaml
```

### Client provisioning stays in `WAITING`

Use the generated debug commands:

```bash
python3 cli.py debug commands --config experiment.yaml --policy schedules/best_handcrafted.yaml
```

Inspect `cloud-final.service`, `/var/log/cca-bootstrap.log`, `/var/log/mcperf-agent.log`, and the VM serial console output. If `part3.yaml` was changed to fix bootstrap behavior, recreate the cluster because cloud-init runs when the VM is created.

### Optional audit cannot find `Part2summary_times.csv`

The submitted `latexSubmission/` tree does not include that CSV. The audit command is optional and needs an explicit path to a local Part 2 timing CSV:

```bash
python3 cli.py audit --policy schedules/best_handcrafted.yaml --times-csv /path/to/Part2summary_times.csv
```

Live runs and submission export do not require this CSV.

### Viewer on a headless machine and deprecated GUI

The supported inspection tool is the result viewer:

```bash
python3 cli.py results viewer --no-open
```

Then open the printed URL from a machine that can reach the chosen host and port.

`gui.py` is deprecated. It was an experimental Python/Tkinter schedule GUI from development and is retained only for traceability. It requires a graphical display and should not be used as the primary way to inspect submitted runs.
