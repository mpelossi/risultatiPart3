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

You do **not** need to run this before every experiment.

### Step 3. Make sure the client VMs are bootstrapped

Run:

```bash
python3 cli.py provision check --config experiment.yaml
```

This checks that:
- `client-agent-a` exists and has `mcperf`
- `client-agent-b` exists and has `mcperf`
- `client-measure` exists and has `mcperf`
- the `mcperf-agent.service` units are active on the agent VMs

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

### `run once` hangs at `Cleaning previous managed workloads`

That usually means `kubectl` cannot actually talk to the cluster, even though the
automation started.

Check:

```bash
kubectl get nodes -o wide
```

If that does not work, fix cluster access before running experiments.

## Important Notes

- The main scheduling file is `schedule.yaml`.
- `run once` does **not** create the cluster for you.
- `cluster up` is a separate step from `run once`.
- The Part 2 timing reference file is `../../Part2summary_times.csv` from this folder.
