# Part 3, Simplified

The main file you should edit to plan your schedule is:

`part3/automation/schedule.yaml`

## Start Every Session

Run this from the repo root:

```bash
source ./checkCredits.sh
```

This is now the main Part 3 bootstrap and doctor script. It does the safe setup work
you keep needing at the start of a session:

- loads `part3/automation/experiment.yaml`
- exports `KOPS_STATE_STORE` and `PROJECT`
- checks that `gcloud`, `kops`, `kubectl`, and `python3` exist
- checks whether `gcloud auth login` has expired
- checks whether `gcloud auth application-default login` has expired
- refreshes kubeconfig with `kops export kubecfg` when the cluster exists
- warns if `kubectl` is falling back to `localhost:8080`
- shows the active billable GCP resources still consuming credits

Useful modes:

```bash
./checkCredits.sh help
./checkCredits.sh kubeconfig
./checkCredits.sh resources
```

If the script says something expired, the fix is usually one of these:

```bash
gcloud auth login
gcloud auth application-default login
./checkCredits.sh kubeconfig
python3 -m part3.automation.cli cluster up --config part3/automation/experiment.yaml
```

Important:

- `source ./checkCredits.sh` is better than `./checkCredits.sh` because the exported
  `KOPS_STATE_STORE` and `PROJECT` stay in your current shell.
- The script can tell you what is still running and whether auth looks expired, but
  Google Cloud does not expose the exact remaining education coupon balance cleanly in
  the CLI. For remaining credits, still use the Billing report in the GCP console.

That file answers exactly these questions for each program:
- which VM it runs on
- which cores it uses
- how many threads it gets
- what job must finish before it starts

You do **not** need to edit the individual files in `part3/yaml/` anymore for normal scheduling work.

## What You Edit

### 1. Schedule
Edit `part3/automation/schedule.yaml`.

Example:

```json
"blackscholes": {
  "node": "node-b-4core",
  "cores": "1-3",
  "threads": 3,
  "after": "start"
}
```

That means:
- run `blackscholes` on `node-b-4core`
- pin it to cores `1-3`
- run it with `3` threads
- start it immediately after memcached/load are ready

If you want one job to start after another finishes:

```json
"freqmine": {
  "node": "node-b-4core",
  "cores": "1-3",
  "threads": 3,
  "after": "blackscholes"
}
```

### 2. Cluster/paths
Only touch `part3/automation/experiment.yaml` if you need to change:
- cluster name
- zone
- state store
- SSH key path
- results folder
- group number

### 3. Startup scripts
Only touch `part3/part3.yaml` if you want to change VM bootstrap behavior.

## The Basic Workflow

### Make sure the shell and cluster context are healthy

```bash
source ./checkCredits.sh
```

If the cluster has been deleted and you want to recreate it:

```bash
python3 -m part3.automation.cli cluster up --config part3/automation/experiment.yaml
```

### Preview the schedule

```bash
python3 -m part3.automation.cli show --policy part3/automation/schedule.yaml
```

This prints the order, VM, cores, and threads in a human-readable way.

### Audit the schedule

```bash
python3 -m part3.automation.cli audit \
  --policy part3/automation/schedule.yaml \
  --times-csv risultatiPart3/Part2summary_times.csv
```

This checks for:
- overlapping cores on concurrent jobs
- unsupported core sets
- memcached collisions
- idle gaps on each node timeline

### Open the planner GUI

```bash
python3 -m part3.automation.cli gui \
  --policy part3/automation/schedule.yaml \
  --times-csv risultatiPart3/Part2summary_times.csv
```

The GUI edits the real policy file, validates in real time, and saves deterministic
explicit `phases` plus `job_overrides`.

### Dry-run the schedule

```bash
python3 -m part3.automation.cli run once --config part3/automation/experiment.yaml --policy part3/automation/schedule.yaml --dry-run
```

This creates rendered manifests and a phase plan without touching the cluster.

### Run one real experiment

```bash
python3 -m part3.automation.cli run once --config part3/automation/experiment.yaml --policy part3/automation/schedule.yaml
```

### Run three repetitions

```bash
python3 -m part3.automation.cli run batch --config part3/automation/experiment.yaml --policy part3/automation/schedule.yaml --runs 3
```

### See which run is best

```bash
python3 -m part3.automation.cli results best --experiment part3-handcrafted
```

This sorts runs so the best `pass` results are first, using:
1. `overall_status == pass`
2. lowest makespan
3. lowest max p95

### Export submission files

```bash
python3 -m part3.automation.cli export submission --experiment part3-handcrafted --group 054 --task 3_1
```

## How To Search For A Better Policy

Use this loop:

1. Edit `part3/automation/schedule.yaml`
2. Preview it with `show`
3. Run it 3 times with `run batch`
4. Check `results best`
5. Keep the version with the best `pass` makespan

In practice, the knobs you should change are:
- move a job from `node-a-8core` to `node-b-4core` or the opposite
- change the `cores`
- change the `threads`
- change the `after` dependency to reorder jobs
- start two jobs together by giving both `after: "start"` or the same dependency

## Cluster Bootstrap Notes

### SSH public key secret

For Part 3, the SSH key secret is attached to the cluster with:

```bash
kops create secret --name part3.k8s.local sshpublickey admin -i ~/.ssh/cloud-computing.pub
```

You do not need to use `part1.k8s.local` here. The automation’s cluster bring-up
already runs the equivalent Part 3 command through `ClusterController.cluster_up()`.

### Canonical client identities

The real kOps node names include random suffixes such as `client-agent-a-s8mr`, but
the automation resolves the stable logical roles from the node label
`cca-project-nodetype`:
- `client-agent-a`
- `client-agent-b`
- `client-measure`

This is why the policy and provisioning code do not depend on the random suffixes.

## Live Verification Checklist

### Preflight

```bash
python3 -m part3.automation.cli provision check --config part3/automation/experiment.yaml
python3 -m part3.automation.cli show --policy part3/automation/schedule.yaml
python3 -m part3.automation.cli audit --policy part3/automation/schedule.yaml --times-csv risultatiPart3/Part2summary_times.csv
python3 -m part3.automation.cli run once --config part3/automation/experiment.yaml --policy part3/automation/schedule.yaml --dry-run
```

### Cluster and VM checks

```bash
kubectl get nodes -o wide
gcloud compute ssh --ssh-key-file ~/.ssh/cloud-computing ubuntu@<actual-node-name> --zone europe-west1-b
```

On `client-agent-a` and `client-agent-b`:

```bash
systemctl status mcperf-agent.service
journalctl -u mcperf-agent.service -n 50 --no-pager
pgrep -af mcperf
ls -l /opt/cca/memcache-perf-dynamic/mcperf
```

On `client-measure`:

```bash
ls -l /opt/cca/memcache-perf-dynamic/mcperf
```

### Job checks

```bash
kubectl get pods -o wide
kubectl get jobs
kubectl describe job <job-name>
kubectl logs job/<job-name>
```

## Important Notes

- Memcached should usually keep core `0` on `node-b-4core`, so batch jobs there should normally use `1-3`.
- `barnes` and `radix` are handled correctly as `splash2x`; you do not need to remember that manually.
- If you change startup scripts in `part3/part3.yaml`, recreate or roll the client nodes so fresh instances execute the new `additionalUserData`.
- The config files use JSON-compatible YAML because this repo does not currently have `PyYAML` installed.

## Folder Layout

- `part3/automation/`:
  the Python automation framework, schedule file, config, tests, and generated automation runs
- `part3/part3.yaml`:
  the kOps cluster definition
- `part3/yaml/`:
  reference workload manifests
- `part3/context/`:
  assignment/reference docs
