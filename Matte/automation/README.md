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

If `cluster up` fails or gets interrupted, clean up before retrying:

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

### 5. Do a dry run first

```bash
python3 cli.py run once --config experiment.yaml --policy schedule.yaml --dry-run
```

### 6. Run one real experiment

```bash
python3 cli.py run once --config experiment.yaml --policy schedule.yaml
```

### 7. Run three repetitions

```bash
python3 cli.py run batch --config experiment.yaml --policy schedule.yaml --runs 3
```

### 8. See the best run

```bash
python3 cli.py results best --experiment part3-handcrafted
```

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

If `cluster up` fails or is interrupted, do **not** immediately retry it on top of the
half-finished state. Run:

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

### Step 5. Dry run

Run:

```bash
python3 cli.py run once --config experiment.yaml --policy schedule.yaml --dry-run
```

This renders manifests and writes the phase plan, but does not touch the live cluster.

### Step 6. Real run

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
- collects logs and results

### Step 7. Repeated runs

Run:

```bash
python3 cli.py run batch --config experiment.yaml --policy schedule.yaml --runs 3
```

Use this when you want the three measurement files needed for submission.

### Step 8. Pick the best run

Run:

```bash
python3 cli.py results best --experiment part3-handcrafted
```

This sorts the runs by:
1. passing runs first
2. lowest makespan
3. lowest observed p95

### Step 9. Export the submission folder

Run:

```bash
python3 cli.py export submission --experiment part3-handcrafted --group 054 --task 3_1
```

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

### `python3 cli.py run once --config experiment.yaml --policy schedule.yaml --dry-run`

Builds the manifests and phase plan without touching the cluster.

### `python3 cli.py run once --config experiment.yaml --policy schedule.yaml`

Runs one full live experiment.

### `python3 cli.py run batch --config experiment.yaml --policy schedule.yaml --runs 3`

Runs the same experiment multiple times.

### `python3 cli.py results best --experiment part3-handcrafted`

Shows the best completed runs according to the built-in ranking.

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

### `cluster up` failed halfway through

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

## Important Notes

- The main scheduling file is `schedule.yaml`.
- `run once` does **not** create the cluster for you.
- `cluster up` is a separate step from `run once`.
- The Part 2 timing reference file is `../../Part2summary_times.csv` from this folder.
