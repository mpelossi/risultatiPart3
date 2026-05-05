# Part 3 OpenEvolve

This directory contains the OpenEvolve setup for Part 3 task 3.2. It evolves a Python
schedule dictionary in `initial_program.py`, evaluates candidates through
`evaluator.py`, and uses the existing sibling automation framework in `../automation`.

## Files

- `initial_program.py`: baseline schedule. OpenEvolve edits only the code between
  `# EVOLVE-BLOCK-START` and `# EVOLVE-BLOCK-END`.
- `evaluator.py`: imports an evolved program, writes its schedule to a temporary YAML
  policy, runs `cli.py audit`, then runs one real Kubernetes experiment for valid
  schedules.
- `config.yaml`: OpenEvolve configuration, Swiss AI endpoint, model, prompt, and
  evaluator timeout.
- `.env`: local API key environment file. Do not commit or print this file.

## Do I Need Kubernetes Running?

Yes. The evaluator runs:

```bash
python3 cli.py run once --config experiment.yaml --policy <generated-policy>
```

That command expects the Part 3 Kubernetes cluster to already exist, be reachable, and
have the client VMs provisioned. OpenEvolve does not bring the cluster up for you.

You only need to create the cluster when it does not exist yet or after you deleted it.
Once it is running, you can reuse it for OpenEvolve iterations.

## Setup

From this directory:

```bash
cd /home/carti/ETH/Msc/CCA/risultatiPart3/Matte/part3_openEvolve
```

Use the local virtual environment that contains the pinned course version:

```bash
source .venv/bin/activate
python -c "import openevolve; print(openevolve.__version__)"
```

If the venv is missing, recreate it:

```bash
python3 -m venv .venv
.venv/bin/pip install openevolve==0.2.26
```

Load the Swiss AI API key:

```bash
set -a
source .env
set +a
```

## Cluster Preflight

From the automation directory:

```bash
cd /home/carti/ETH/Msc/CCA/risultatiPart3/Matte/automation
../../checkCredits.sh
python3 cli.py cluster up --config experiment.yaml
python3 cli.py provision check --config experiment.yaml
```

If the cluster is already running, you usually only need:

```bash
cd /home/carti/ETH/Msc/CCA/risultatiPart3/Matte/automation
python3 cli.py provision check --config experiment.yaml
```

Before a serious OpenEvolve run, warm the Kubernetes images once without doing a full
benchmark run:

```bash
python3 cli.py run precache --config experiment.yaml --policy schedules/schedule7bis.yaml
```

This creates transient pre-cache pods on both benchmark nodes, waits for the benchmark
and memcached images to be present, deletes the pods, and exits before launching
memcached, `mcperf`, or benchmark jobs. Its artifacts live under
`../automation/runs/__precache/...`, separate from real benchmark results.

The OpenEvolve evaluator intentionally does not use `--precache` or run this pre-cache
step on every iteration, because doing that repeatedly wastes time once images are warm.

## Run OpenEvolve

Use a fresh output directory each time. Do not reuse old output directories, because
OpenEvolve may overwrite checkpoints and logs needed for submission.
The `-o/--output` flag controls where OpenEvolve saves its checkpoints, logs, and
best program. The evaluator controls the separate Kubernetes benchmark run
artifacts.

```bash
cd /home/carti/ETH/Msc/CCA/risultatiPart3/Matte/part3_openEvolve
set -a
source .env
set +a
.venv/bin/openevolve-run --config config.yaml -o runs/run_$(date +%Y%m%d_%H%M%S) initial_program.py evaluator.py
```

For a one-iteration smoke test:

```bash
.venv/bin/openevolve-run --config config.yaml --iterations 1 -o runs/smoke_$(date +%Y%m%d_%H%M%S) initial_program.py evaluator.py
```

## Visualizer

Yes, you can run the OpenEvolve visualizer while the evolution is running. Use a
separate terminal and point it at the OpenEvolve output directory. It reads logs and
checkpoints; it does not run Kubernetes jobs itself.

It is normal for the visualizer to show little or nothing before the first checkpoint.
This config has `checkpoint_interval: 1`, so it should update after each completed
iteration.

Set it up outside this submission/work directory:

```bash
cd /tmp
git clone https://github.com/algorithmicsuperintelligence/openevolve.git
cd openevolve
python3 -m venv .venv
source .venv/bin/activate
pip install -r scripts/requirements.txt
```

Then, while OpenEvolve is running, start the visualizer with the output directory:

```bash
python3 scripts/visualizer.py --path /home/carti/ETH/Msc/CCA/risultatiPart3/Matte/part3_openEvolve/<run-output-dir>
```

You can also point it at a specific checkpoint directory:

```bash
python3 scripts/visualizer.py --path /home/carti/ETH/Msc/CCA/risultatiPart3/Matte/part3_openEvolve/<run-output-dir>/checkpoints/<checkpoint-dir>
```

## After Evolution

The best evolved program is saved under the OpenEvolve output directory, usually in
`best/` and in the latest checkpoint. Convert the best program's `get_schedule()` output
to a YAML policy, then benchmark it with three repetitions:

```bash
cd /home/carti/ETH/Msc/CCA/risultatiPart3/Matte/automation
python3 cli.py run batch --config experiment.yaml --policy <best-policy.yaml> --runs 3 --precache
```

If the same cluster was already warmed with `run precache`, you can omit `--precache`
from the final batch command. Keeping it is safe; it warms images once before the first
batch repetition.

For submission, collect the OpenEvolve log and checkpoint that produced the benchmarked
program. The course helper is in:

```bash
/home/carti/ETH/Msc/CCA/cloud-comp-arch-project/openevolve/openevolve_collect.py
```

## Shutdown

Delete the cluster when you are done using it so it does not keep spending cloud credits:

```bash
cd /home/carti/ETH/Msc/CCA/risultatiPart3/Matte/automation
python3 cli.py cluster down --config experiment.yaml
../../checkCredits.sh
```
