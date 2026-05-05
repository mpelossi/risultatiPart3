# Part 3 Automation

Everything below assumes you are already inside:

```bash
cd risultatiPart3/Matte/automation
```

## TL;DR

### 1. Check local setup and auth

```bash
../../checkCredits.sh
```

### 2. If the cluster does not exist yet, or you deleted it, bring it up

```bash
python3 cli.py cluster up --config experiment.yaml
```

If `cluster up` only fails during the final `kops validate cluster --wait 10m` step with
`connect: connection refused`, `i/o timeout`, or `TLS handshake timeout`, first give the
API server more time:

```bash
export KOPS_STATE_STORE=gs://cca-eth-2026-group-54-mpelossi
kops validate cluster --name part3.k8s.local --wait 20m
```

If that succeeds, finish the automation step once so kubeconfig is exported and the nodes
get the expected labels:

```bash
python3 cli.py cluster up --config experiment.yaml
```

If validation still cannot reach the API server, or `cluster up` failed before validation,
clean up before retrying:

```bash
python3 cli.py cluster down --config experiment.yaml
../../checkCredits.sh
```

Only retry `cluster up` after `../../checkCredits.sh` shows no leftover Part 3 billable
resources or network artifacts.

You do **not** need to do this every time.

You only need `cluster up` when:
- the cluster has never been created
- the cluster was deleted
- you changed the cluster YAML and want to apply those changes

`run once` and `run batch` do **not** call `cluster up` for you. They assume the cluster
already exists and is reachable.

### 3. Check that the client VMs are ready

```bash
python3 cli.py provision check --config experiment.yaml
```

If `~/.ssh/cloud-computing` is passphrase-protected and not loaded in `ssh-agent`, this
command may ask for the passphrase up to **3 times**, roughly once for each client VM that
is checked. To avoid repeated prompts, run:

```bash
ssh-add ~/.ssh/cloud-computing
```

### 4. Inspect and validate the schedule

```bash
python3 cli.py show --policy schedule.yaml
python3 cli.py audit --policy schedule.yaml --times-csv ../../Part2summary_times.csv
```

`audit` still uses the old Part 2 CSV as a simple static checker. The schedule viewer uses
run-derived `runs/runtime_stats.json` when that file exists.

If you want to edit the schedule visually, open the planner GUI from this directory with:

```bash
python3 gui.py
```

That opens `schedule.yaml` and `../../Part2summary_times.csv` by default. If you want to
use different files, run:

```bash
python3 gui.py --policy other-schedule.yaml --times-csv /path/to/Part2summary_times.csv
```

### 5. Do a dry run first

```bash
python3 cli.py run once --config experiment.yaml --policy schedule.yaml --dry-run
```

### 6. Warm images without a measurement run

```bash
python3 cli.py run precache --config experiment.yaml --policy schedule.yaml
```

This creates transient pre-cache pods on both benchmark nodes, waits for all benchmark
and memcached images to be present, deletes those pods, and exits before launching
memcached, `mcperf`, or benchmark jobs. Artifacts are written under
`runs/__precache/<experiment_id>/<timestamp>` so they do not appear as benchmark runs.

### 7. Run one real experiment

```bash
python3 cli.py run once --config experiment.yaml --policy schedule.yaml
```

Use `--precache` here only when you want the real run itself to do the warmup first.
If you already ran `run precache` on an unchanged cluster, omit `--precache` so the
measurement starts sooner.

After the first warmup on an unchanged cluster, repeating `run once --precache` is safe
but usually optional because the images should already be present. For `run batch
--precache`, the automation warms images once before the first repetition only.

### 8. Run three repetitions

```bash
python3 cli.py run batch --config experiment.yaml --policy schedule.yaml --runs 3 --precache
```

### 8b. Run multiple schedules as a queue

Create a queue file such as:

```yaml
queue_name: "part3-candidates"
entries:
  - policy: "schedules/schedule1.yaml"
    runs: 1
  - policy: "schedules/schedule2.yaml"
    runs: 3
  - policy: "schedules/schedule3.yaml"
    runs: 1
```

Then run:

```bash
python3 cli.py run queue --config experiment.yaml --queue schedule_queue.yaml --precache
```

Each `policy` path is resolved relative to the queue file. `runs: 1` behaves like
`run once`; larger values behave like `run batch`. With `--precache`, images are warmed
only before the first real queued run.

### 8c. Rebuild runtime stats from saved runs

Real runs refresh `runs/runtime_stats.json` automatically. If you want to backfill from
old runs, or you edited/copied run artifacts by hand, rebuild it explicitly:

```bash
python3 cli.py stats rebuild --results-root runs
```

The schedule viewer uses this file for theoretical predictions before falling back to
`../../Part2summary_times.csv`.

### 8. See the best run

```bash
python3 cli.py results best --experiment part3-handcrafted
```

Or open the run viewer:

```bash
python3 cli.py results viewer --experiment part3-handcrafted
```

The viewer serves the frontend from this automation directory, reads `runs/` by default,
uses `runs/runtime_stats.json` for schedule predictions when available, and opens your
browser automatically.

### 9. Export submission files

```bash
python3 cli.py export submission --experiment part3-handcrafted --group 054 --task 3_1
```

## What You Edit

Most of the time, you only edit:

```bash
schedule.yaml
```

That file decides:
- which node each job runs on
- which cores it uses
- how many threads it gets
- when it starts relative to other jobs

The `cores` field accepts any valid Linux CPU-set string that fits on the chosen node, for
example `0-4`, `5-7`, `1-5`, or `0,2,4`.

Only edit:

```bash
experiment.yaml
```

if you need to change:
- cluster name
- zone
- state store
- SSH key path
- results folder
- group number

The cluster definition itself lives in:

```bash
part3.yaml
```

Only edit that file if you want to change VM bootstrap behavior or the cluster layout.

## Step By Step

### Step 1. Preflight

Run:

```bash
../../checkCredits.sh
```

This checks:
- that the automation files exist
- that `cli.py` is runnable
- that `gcloud`, `kops`, `kubectl`, and `python3` exist
- that your Google auth is still valid
- whether `kubectl` is currently usable
- whether there are still billable GCP resources running
- whether stale Part 3 VPC/subnet/firewall/route artifacts are still around

This script does **not** create, update, or delete the cluster.

### Step 2. Create or refresh the cluster only when needed

Run:

```bash
python3 cli.py cluster up --config experiment.yaml
```

This does the full bring-up flow:
- creates or replaces the kOps cluster config
- ensures the SSH public key secret exists
- runs `kops update cluster`
- runs `kops validate cluster`
- exports kubeconfig locally
- labels the Kubernetes nodes with canonical `cca-project-nodetype` values such as
  `client-agent-a` and `node-a-8core`

You do **not** need to run this before every experiment.

If `cluster up` fails only at `kops validate cluster --wait 10m`, do **not** delete the
cluster immediately. The control-plane VM can exist before the Kubernetes API server is
ready, which shows up as errors like `connect: connection refused`, `i/o timeout`, or
`TLS handshake timeout`. Try a longer standalone validation first:

```bash
export KOPS_STATE_STORE=gs://cca-eth-2026-group-54-mpelossi
kops validate cluster --name part3.k8s.local --wait 20m
```

If that succeeds, run the automation command again once:

```bash
python3 cli.py cluster up --config experiment.yaml
```

The second run should pass validation quickly, export kubeconfig, and label the nodes.

If validation still cannot reach the API server after the longer wait, or if `cluster up`
failed before the validation phase, clean up before retrying:

```bash
python3 cli.py cluster down --config experiment.yaml
../../checkCredits.sh
```

Use `../../checkCredits.sh` to confirm there are no leftover billable resources or Part 3
network artifacts before bringing the cluster up again.

### Step 3. Make sure the client VMs are bootstrapped

Run:

```bash
python3 cli.py provision check --config experiment.yaml
```

If your SSH key is passphrase-protected and not already loaded in `ssh-agent`, expect up to
**3 passphrase prompts** here, roughly one per checked client VM. If you want to unlock the
key once instead of on each SSH call, run:

```bash
ssh-add ~/.ssh/cloud-computing
```

This checks that:
- `client-agent-a` exists and has `mcperf`
- `client-agent-b` exists and has `mcperf`
- `client-measure` exists and has `mcperf`
- the `mcperf-agent.service` units are active on the agent VMs
- the randomized Kubernetes node names have the expected canonical
  `cca-project-nodetype` labels so jobs can schedule correctly

The CLI prints each node as `READY` or `WAITING`:
- `WAITING` means bootstrap is still in progress or some expected software/service is missing
- `READY` means that node is usable for experiments
- for `client-agent-a` and `client-agent-b`, `READY` requires `mcperf-agent.service active`
- for `client-measure`, `READY` only requires bootstrap and `mcperf`; `mcperf-agent.service`
  is not expected there

If a node stays in `WAITING`, print the ready-made debug commands with:

```bash
python3 cli.py debug commands --config experiment.yaml --policy schedule.yaml
```

### Step 4. Check the schedule before you spend credits

Run:

```bash
python3 cli.py show --policy schedule.yaml
python3 cli.py audit --policy schedule.yaml --times-csv ../../Part2summary_times.csv
```

Use `show` to read the launch order quickly.

Use `audit` to catch:
- overlapping cores
- unsupported core sets
- memcached collisions
- suspicious idle gaps

If you want to edit the schedule in a GUI instead of hand-editing YAML, run:

```bash
python3 gui.py
```

or, equivalently:

```bash
python3 cli.py gui --policy schedule.yaml --times-csv ../../Part2summary_times.csv
```

How to use the planner correctly:
- run it from a graphical desktop session; plain headless SSH will not open a Tk window
- `Reload` re-reads the policy file from disk
- `Save` validates the schedule first and refuses to write if there are errors
- the `Threads` spinboxes are loaded from the `threads:` values in the policy file
- the `Cores` combobox suggests contiguous presets, but you can also type any valid CPU-set
  string manually
- after saving, run `python3 cli.py show --policy schedule.yaml` if you want to confirm the
  exact node/core/thread assignments that will be used

One important detail: the GUI always saves back an explicit policy file with
`job_overrides` and `phases`. That is still supported by all the automation commands, but
the file will no longer be in the shorter `jobs:` format after you save from the GUI.

### Step 5. Dry run

Run:

```bash
python3 cli.py run once --config experiment.yaml --policy schedule.yaml --dry-run
```

This renders manifests and writes the phase plan, but does not touch the live cluster.

### Step 6. Pre-cache images without measuring

Run:

```bash
python3 cli.py run precache --config experiment.yaml --policy schedule.yaml
```

This:
- cleans previous managed jobs and pods
- checks client provisioning
- creates transient pre-cache pods on both benchmark nodes
- waits for all benchmark and memcached images to be present
- deletes the pre-cache pods
- writes `precache.json` and `events.log` under `runs/__precache/<experiment_id>/<timestamp>`

It does not launch memcached, start `mcperf`, run benchmark jobs, capture `results.json`,
or refresh runtime stats.

### Step 7. Real run

Run:

```bash
python3 cli.py run once --config experiment.yaml --policy schedule.yaml
```

This:
- cleans previous managed jobs and pods
- checks client provisioning
- launches memcached
- starts the `mcperf` measurement
- launches the batch phases in schedule order
- stops `mcperf` when the last batch job completes
- captures `results.json`, `mcperf.txt`, and `summary.json`

For serious timing runs, either run the standalone pre-cache command first or pass
`--precache` to this command. The standalone command is useful when you want to warm
images once without spending time or credits on a full measurement run.

`results.json` is the raw `kubectl get pods -o json` snapshot that matches the assignment
workflow. `summary.json` is a derived convenience report built from `results.json` and
`mcperf.txt`.

Every real run also writes `node_platforms.json` and copies that data into `summary.json`.
This records the GCP `cpuPlatform` and machine type for the benchmark nodes
`node-a-8core` and `node-b-4core`, so n2d Rome/Milan placement is visible after the run.

Every real run also refreshes `runs/runtime_stats.json` from the saved run history. This
file stores per-job observed runtimes grouped by job, node, thread count, and memcached
placement. Refresh failure is logged as a warning and does not mark the benchmark run as
failed.

If you already warmed the images on this cluster, omit `--precache` on later single runs.
Re-running pre-cache is not harmful; it just creates short-lived image-warmup pods and
deletes them before memcached starts.

When the run stops measurement, the signal is sent only to the temporary `mcperf` wrapper on
`client-measure`. It does not target the memcached pod or the long-lived
`mcperf-agent.service` processes on `client-agent-a` / `client-agent-b`.

### Step 8. Repeated runs

Run:

```bash
python3 cli.py run batch --config experiment.yaml --policy schedule.yaml --runs 3 --precache
```

Use this when you want the three measurement files needed for submission. With `--precache`,
the warmup happens once before the first run only.

### Step 9. Pick the best run

If this checkout already contains run artifacts, or you copied run folders in manually,
first rebuild the run-derived runtime statistics:

```bash
python3 cli.py stats rebuild --results-root runs
```

The rebuild scans `runs/<experiment>/<run-id>/`, reads `summary.json`, `policy.yaml`, and
`node_platforms.json` when present, and writes `runs/runtime_stats.json`. The schedule
viewer uses median observed runtime from this file, grouped by job, node, thread count,
and memcached node. If no matching sample exists, it falls back to broader run-derived
groups and then to `../../Part2summary_times.csv`.

Run:

```bash
python3 cli.py results best --experiment part3-handcrafted
```

This sorts the runs by:
1. passing runs first
2. lowest makespan
3. lowest observed p95

By default it reads from this automation directory's own `runs/` folder, so you usually do
not need to pass `--results-root`.

### Step 10. Export the submission folder

Run:

```bash
python3 cli.py export submission --experiment part3-handcrafted --group 054 --task 3_1
```

The export step reads each selected run's local `results.json`, but writes the submission
bundle using the assignment filenames `pods_1.json`, `pods_2.json`, `pods_3.json`.

## What Each Command Does

### `python3 cli.py cluster up --config experiment.yaml`

Creates or refreshes the Part 3 cluster. Use it only when the cluster is missing or you
want to apply cluster-definition changes.

### `python3 cli.py provision check --config experiment.yaml`

Checks whether the three client VMs are bootstrapped correctly for Part 3.

### `python3 cli.py debug commands --config experiment.yaml --policy schedule.yaml`

Prints exact `gcloud compute ssh`, `kubectl`, `journalctl`, `tail -f`, and serial-console
commands for debugging the client VMs, memcached pod, and saved `mcperf` output. Add
`--run-id <run-id>` if you want the exact memcached pod name and `mcperf.txt` path for one
run.

### `python3 cli.py show --policy schedule.yaml`

Prints the current schedule in a human-readable format.

### `python3 cli.py audit --policy schedule.yaml --times-csv ../../Part2summary_times.csv`

Runs the static schedule checker using your Part 2 timing data.

### `python3 cli.py gui --policy schedule.yaml --times-csv ../../Part2summary_times.csv`

Opens the Tkinter planner GUI for the current schedule. From inside this directory,
`python3 gui.py` does the same thing with the default `schedule.yaml` and
`../../Part2summary_times.csv` paths.

### `python3 cli.py run once --config experiment.yaml --policy schedule.yaml --dry-run`

Builds the manifests and phase plan without touching the cluster.

### `python3 cli.py run precache --config experiment.yaml --policy schedule.yaml`

Warms benchmark and memcached images on both benchmark nodes, then exits without a
measurement run. Writes artifacts under `runs/__precache/...`.

### `python3 cli.py run once --config experiment.yaml --policy schedule.yaml --precache`

Runs one full live experiment and performs the image warmup before memcached starts.

### `python3 cli.py run batch --config experiment.yaml --policy schedule.yaml --runs 3 --precache`

Runs the same experiment multiple times.

### `python3 cli.py run queue --config experiment.yaml --queue schedule_queue.yaml --precache`

Runs each schedule listed in a queue file, stopping on the first runner exception.

### `python3 cli.py stats rebuild --results-root runs`

Rebuilds `runs/runtime_stats.json` from saved run artifacts. Use it after copying old runs
into `runs/` or whenever you want to force a deterministic full refresh.

### `python3 cli.py results best --experiment part3-handcrafted`

Shows the best completed runs according to the built-in ranking.

### `python3 cli.py results viewer --experiment part3-handcrafted`

Starts a small local file server for the run viewer, using this directory's `runs/` folder
by default. It prints the URL and tries to open the browser automatically.

Useful options:
- `--no-open` keeps the server-only behavior for SSH/headless sessions
- `--host 0.0.0.0` makes the viewer reachable from another machine that can access the port
- `--port 8080` chooses a different port
- `--results-root /path/to/runs` reads a different results directory
- `--runtime-stats /path/to/runtime_stats.json` reads a different run-derived stats file
- `--times-csv /path/to/Part2summary_times.csv` changes the legacy fallback timing file

You can also run the same viewer directly:

```bash
python3 viewer.py --experiment part3-handcrafted
```

### `python3 cli.py export submission --experiment part3-handcrafted --group 054 --task 3_1`

Creates the submission-ready results directory.

## Common Problems

### `kubectl` points to `localhost:8080`

This usually means:
- the cluster is not up yet, or
- kubeconfig was never exported, or
- kubeconfig is stale

First try:

```bash
python3 cli.py cluster up --config experiment.yaml
```

If the cluster already exists and you only need kubeconfig:

```bash
kops export kubecfg --admin --name part3.k8s.local
```

### `cluster up` timed out in `kops validate cluster`

If the failure is only:

```text
error listing nodes: Get "https://.../api/v1/nodes": connect: connection refused
validation failed: wait time exceeded during validation
```

try validation once more before deleting the cluster:

```bash
export KOPS_STATE_STORE=gs://cca-eth-2026-group-54-mpelossi
kops validate cluster --name part3.k8s.local --wait 20m
```

If this succeeds, finish the normal setup path:

```bash
python3 cli.py cluster up --config experiment.yaml
python3 cli.py provision check --config experiment.yaml
```

Use `cluster down` only if the longer validation still cannot reach the API server, if
`../../checkCredits.sh` shows broken leftover network artifacts, or if the bring-up failed
before the validation phase.

### `cluster up` failed before validation or still cannot validate

Clean up first:

```bash
python3 cli.py cluster down --config experiment.yaml
../../checkCredits.sh
```

Only retry `cluster up` after `../../checkCredits.sh` reports that there are no leftover
billable resources or Part 3 network artifacts.

### `run once` hangs at `Cleaning previous managed workloads`

That usually means `kubectl` cannot actually talk to the cluster, even though the
automation started.

Check:

```bash
kubectl get nodes -o wide
```

If that does not work, fix cluster access before running experiments.

### `provision check` or `run once` says a client is `WAITING`

That usually means the VM bootstrap script did not finish. In this project, the most likely
failure mode is:
- cloud-init started the bootstrap script
- `apt-get build-dep memcached --yes` failed because `deb-src` was not enabled correctly
- the script exited before `mcperf`, `mcperf-agent.service`, and `/opt/cca/bootstrap.done`

Run:

```bash
python3 cli.py debug commands --config experiment.yaml --policy schedule.yaml
```

Then inspect, in this order:
- `cloud-final.service`
- `/var/log/cca-bootstrap.log`
- the serial console output from `gcloud compute instances get-serial-port-output ...`

If provisioning is still failing, do **not** expect a memcached pod yet. `run once` only
applies the memcached manifest after provisioning passes.

If you changed `part3.yaml` to fix the startup script, remember that cloud-init only runs
when the VM is created. Existing client VMs will keep the old broken bootstrap state, so
you need to recreate the cluster with `python3 cli.py cluster down --config experiment.yaml`
followed by `python3 cli.py cluster up --config experiment.yaml`.

Use the command types like this:
- `gcloud compute ssh ...` opens a shell on the VM
- `kubectl exec -it <pod> -- sh` opens a shell inside the container
- `kubectl logs -f`, `journalctl -f`, and `tail -f` follow live output instead of opening a shell

### `gui.py` does not open a window

The planner is a Tkinter desktop app. It needs:
- a Python build with Tkinter available
- a graphical display session

If `python3 gui.py` says Tkinter could not be imported, install the Tkinter package for
your Python distribution. If it says no graphical display is available, run it locally in
a desktop session or use X11 forwarding.

## Important Notes

- The main scheduling file is `schedule.yaml`.
- `python3 gui.py` opens `schedule.yaml` and `../../Part2summary_times.csv` by default.
- The schedule viewer uses `runs/runtime_stats.json` first for predictions, then falls
  back to `../../Part2summary_times.csv` when needed.
- `run once` does **not** create the cluster for you.
- `cluster up` is a separate step from `run once`.
- The Part 2 timing reference file is still `../../Part2summary_times.csv` from this
  folder, but it is now the compatibility fallback rather than the primary predictor.
