# Part 3 Q1(b) Automation Handoff

## Goal

Continue the answer to:

> Which files did you modify or add and in what way? Which Kubernetes features did you use?

The current answer in `latexSubmission/Project Template/part3.tex` is intentionally short. Expand it only after carefully analyzing the automation code under:

`risultatiPart3 WIN/Matte/automation`

Use `latexSubmission/writingNotes/part3q1subB.md` as the main narrative source. Preserve the student's wording and ideas, but turn them into polished report prose.

## Current Report Constraints

- Do not mention internal run IDs in `part3.tex`; refer to them as Run 1, Run 2, and Run 3.
- Do not mention the internal final policy name in the report prose. Use "the final hand-crafted policy" or "the selected final policy".
- The internal final policy name is `testFinal2`, and its source of truth is the selected run-local `policy.yaml` files and `schedules/aFFinalscheduleParta bis.yaml`.
- The final policy keeps memcached on `node-b-4core`, core `0`, with one thread.
- `radix` must be treated as a stability-constrained job:
  - Never schedule `radix` on `node-b-4core`; it can crash the 4-core, 4 GB VM.
  - `radix.threads` must be exactly one of `{1, 2, 4, 8}`; non-power-of-two thread counts can crash.

## Automation Areas To Inspect

Analyze the full automation directory before rewriting the files/Kubernetes paragraph. Important likely files:

- `README.md`: high-level description of the automation workflow.
- `part3.yaml`: kOps cluster definition, instance groups, node labels, and bootstrap user-data for client agents.
- `schedule.yaml` and `schedules/*.yaml`: policy YAML format and policy variants.
- `config.py`, `catalog.py`, `manifests.py`: policy parsing, workload catalog, node/core/thread rendering, `taskset`, node selectors.
- `runner.py`, `cli.py`, `collect.py`, `results.py`, `metrics.py`, `timing.py`: benchmark orchestration, mcperf execution, logs, timing, summaries, SLO computation.
- `provision.py`, `cluster.py`: cluster deployment, node discovery, SSH/IP handling, node aliasing or labeling.
- `audit.py`, `runtime_stats.py`: validation, thread/core constraints, runtime aggregation, resource-contention checks.
- `viewer.py`, `viewer_data.py`, `schedule_viewer_data.py`, `viewer_static/*`: web interface and timeline visualization for designing policies and reviewing past runs.

Use these observations from `part3q1subB.md`:

- The automation handles SSH and IP discovery for client nodes.
- It renames/aliases nodes and validates the Kubernetes cluster deployment.
- It supports image precaching before the benchmark suite, because container startup time was empirically important.
- It has queue functions / dependency handling for policies, effectively a small sequential DAG controller.
- It collects logs, results, timings, and summaries.
- It validates policies before running benchmarks, especially that thread counts do not exceed pinned core counts.
- It contains a rudimentary resource-contention checker based on Part 2 speedup/runtime measurements and running statistics from previous runs.
- Runtime statistics are also used by a web interface for creating new scheduling policies and for visualizing past runs as horizontal timelines.

## Kubernetes Features To Cover

Mention concrete Kubernetes mechanisms rather than only saying "scheduler":

- kOps instance groups and machine types in `part3.yaml`.
- Node labels such as `cca-project-nodetype`.
- `nodeSelector` to bind memcached and each batch job to a selected node type.
- Kubernetes Pods/Jobs for memcached and PARSEC/SPLASH-2x jobs.
- `taskset` inside container commands for CPU affinity.
- Per-run rendered manifests saved under each run directory.
- Bootstrap `additionalUserData` scripts for client machines, especially mcperf build/install and `mcperf-agent.service`.

## Second-Best Policy Exploration
and improving the last question answer.

"Describe how your policy (and its
performance) compares to another policy that you experimented with (in particular
to the second-best policy that you designed)."


The next agent should also analyze the second-best policy, internally named `split-brain-NodeA`.

i ran some runs named
2026-05-09-16h12m47s
2026-05-09-14h52m35s
2026-05-10-02h03m48s
2026-05-10-02h15m53s

but they're all high makespan.

the reason i want you to mention this second best is 
that on the directory /part3-handcrafted we had run 
2026-04-27-07h02m33s

which had our best makespan of 222s

but the day after at another time of day the makespan was completely different, with stream cluster running concurrently with 3 other jobs freqmine barnes and radix.
This colocation might make the run times highly unpredictable because of share cached states.
BUT ON ONE OCCASION IT GAVE US THE BEST OVERALL score of 222s.

Primary path:

`risultatiPart3 WIN/Matte/automation/schedules/aFFinalscheduleParta.yaml`

Observed behavior to investigate:

- It behaved somewhat unpredictably.
- Time of day affected runtimes, probably because the VM platform / cloud sharing conditions changed.
- This policy had substantial variance in job runtimes and makespan across runs.
- It still often kept SLO violations at zero, but its runtime stability was worse than the final selected policy.

Compare it with the final selected policy:

- `split-brain-NodeA` uses more split execution on node A: several 4-thread jobs on different core ranges.
- The final selected policy gives full node A to `streamcluster` and `freqmine`, then overlaps only `vips` and `radix` at the tail.
- The final selected policy moves `barnes` to node B and keeps `radix` safely on node A with a power-of-two thread count.
- The final selected policy is more conservative, easier to reason about, and was chosen because it was stable across the three selected report runs.

## Expected Output

Produce a replacement paragraph or short subsection for the "Which files did you modify or add and in what way? Which Kubernetes features did you use?" answer in `part3.tex`.

Keep it report-ready:

- No internal policy names.
- No raw run IDs.
- No long script-by-script dump.
- Mention enough implementation detail to show that the automation was real and reproducible.
- Make clear that Kubernetes did placement through labels/selectors, while core isolation was done through `taskset`.
