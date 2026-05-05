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

Before a serious OpenEvolve run, do one manual warm run with pre-cache:

```bash
python3 cli.py run once --config experiment.yaml --policy schedules/schedule7bis.yaml --precache
```

The OpenEvolve evaluator intentionally does not use `--precache` on every iteration,
because doing that repeatedly wastes time once images are warm.

## Run OpenEvolve

Use a fresh output directory each time. Do not reuse old output directories, because
OpenEvolve may overwrite checkpoints and logs needed for submission.

```bash
cd /home/carti/ETH/Msc/CCA/risultatiPart3/Matte/part3_openEvolve
set -a
source .env
set +a
.venv/bin/openevolve-run --config config.yaml -o run_$(date +%Y%m%d_%H%M%S) initial_program.py evaluator.py
```

For a one-iteration smoke test:

```bash
.venv/bin/openevolve-run --config config.yaml --iterations 1 -o smoke_$(date +%Y%m%d_%H%M%S) initial_program.py evaluator.py
```

## After Evolution

The best evolved program is saved under the OpenEvolve output directory, usually in
`best/` and in the latest checkpoint. Convert the best program's `get_schedule()` output
to a YAML policy, then benchmark it with three repetitions:

```bash
cd /home/carti/ETH/Msc/CCA/risultatiPart3/Matte/automation
python3 cli.py run batch --config experiment.yaml --policy <best-policy.yaml> --runs 3 --precache
```

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
