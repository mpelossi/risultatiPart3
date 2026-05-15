# Part 3 Q2 OpenEvolve Handoff

## Immediate Next Task

The next task is to write the answer for Part 3, Part 2(b), "Analysis of the Evolutionary Process", in:

`latexSubmission/Project Template/part3.tex`

The answer slot is currently empty after:

```tex
\textbf{Answer:}
```

This answer must explain how the OpenEvolve LLM evolutionary process went. It should not focus on making more plots; the current plotting work for Part 2(a) is already in place.

Most importantly, the writer must use the wording and ideas from:

`latexSubmission/writingNotes/part3q2.md`

That note contains the intended story: Kimi K2.5, token/max-token issues, the three log files, the automation harness helping because the LLM only had to emit a structured scheduling dictionary, and the system prompt constraints.

## Current Report State

In `latexSubmission/Project Template/part3.tex`, Part 2(a) currently includes the updated OpenEvolve iteration plots:

- `figures/part3/cx_part3_q2a_openevolve_p95_makespan.png`
- `figures/part3/cx_part3_q2a_openevolve_slo_score.png`

The current text says OpenEvolve ran for 20 iterations, with iteration 0 as the baseline policy. The first plot shows mean p95 latency with standard-deviation error bars plus makespan. The second plot shows SLO violations plus `combined_score`.

The report currently states:

- all saved evaluations stayed below the `1 ms` p95 SLO;
- SLO violations stayed at zero;
- for passing policies, the score is:

```tex
10000 - \mathrm{makespan} - \max(0,\mathrm{max\_p95\_us}-850)/10
```

The final AI-generated policy benchmark table currently reports:

- hand-crafted total mean time: `232.33 s`
- AI-generated total mean time: `197.33 s`
- SLO violation ratio: `0/63 = 0`

## Current Plot Script State

The plotting script is:

`latexSubmission/Project Template/figures/cx_make_part3_plots.py`

For the OpenEvolve plots it currently reads:

- checkpoint metadata from `risultatiPart3 WIN/Matte/part3_openEvolve/runs/run_20260505_154001/checkpoints`;
- raw evaluation runs from `risultatiPart3 WIN/Matte/automation/runs/part3-OPENEVOLVE`;
- `mcperf.txt` from each matched evaluation run;
- `summary.json` job start/end times to restrict p95 statistics to the batch-job window.

It computes:

- `p95_mean_us`
- `p95_std_us` using population standard deviation;
- `p95_sample_count`

It writes:

- `cx_part3_q2a_openevolve_p95_makespan.png`
- `cx_part3_q2a_openevolve_slo_score.png`

The figure directory currently contains only these two OpenEvolve Q2a plot files, so the old abandoned plot names are no longer present there.

## OpenEvolve Run Facts

The benchmarked OpenEvolve run is:

`risultatiPart3 WIN/Matte/part3_openEvolve/runs/run_20260505_154001`

It contains:

- `best/`
- `checkpoints/`
- `logs/`

The relevant logs are:

- `logs/openevolve_20260505_154001.log`
- `logs/openevolve_20260505_174021.log`
- `logs/openevolve_20260505_182140.log`

The logs show that this run used:

```text
moonshotai/Kimi-K2.5
```

The best program was generated in:

`logs/openevolve_20260505_182140.log`

Best program facts:

- program id: `86b89acd-c366-43c4-8fa5-061f5c187143`
- found at iteration: `14`
- latest checkpoint containing it: `checkpoint_20`
- `combined_score = 9802.0`
- `makespan_s = 198.0`
- `max_p95_us = 398.1`
- `slo_violations = 0.0`
- `status_pass = 1.0`
- `audit_pass = 1.0`
- `run_completed = 1.0`

Important score progression from checkpoint metadata:

| Iteration | Score | Makespan | Max p95 us | SLO violations | Note |
|---:|---:|---:|---:|---:|---|
| 0 | 9777 | 223 | 380.2 | 0 | initial saved policy |
| 1 | 9714 | 286 | 390.8 | 0 | worse rewrite |
| 2 | 9786 | 214 | 403.0 | 0 | improvement |
| 7 | 9791 | 209 | 400.1 | 0 | improvement |
| 9 | 9799 | 201 | 375.4 | 0 | new best |
| 11 | 9801 | 199 | 515.7 | 0 | new best |
| 14 | 9802 | 198 | 398.1 | 0 | final best |
| 20 | 9779 | 221 | 383.2 | 0 | later candidate, not best |

The run continued after finding the best at iteration 14, but later candidates did not beat it.

## Initial Policy Correction

Do not trust the current root file:

`risultatiPart3 WIN/Matte/part3_openEvolve/initial_program.py`

As of the last check, that file matched the final `optimized-parallel-radix` policy, not the true iteration-0 policy. For the report and submission, reconstruct the true initial program from:

`risultatiPart3 WIN/Matte/part3_openEvolve/runs/run_20260505_154001/checkpoints/checkpoint_1/programs/b57c5815-291c-44ac-ac35-03878949c74b.json`

The true iteration-0 policy is named:

```text
split-brain-NodeA
```

Its structure:

- memcached on `node-b-4core`, core `0`;
- `canneal` then `barnes` on `node-b-4core`, cores `1-3`;
- `streamcluster` then `radix` on `node-a-8core`, cores `0-3`;
- `freqmine` then `vips` then `blackscholes` on `node-a-8core`, cores `4-7`.

Its metrics:

- `combined_score = 9777.0`
- `makespan_s = 223.0`
- `max_p95_us = 380.2`
- `slo_violations = 0.0`

### Provenance clarification

The run summary:

`risultatiPart3 WIN/Matte/automation/runs/part3-OPENEVOLVE/2026-05-05-15h40m02sBEST2 not openevolve/summary.json`

maps to the true OpenEvolve iteration-0 policy, not to the current root `initial_program.py`. Its summary says:

- `policy_name = split-brain-NodeA`
- `run_id = 2026-05-05-15h40m02s`
- `makespan_s = 223.0`
- `max_observed_p95_us = 380.2`
- `slo_violations = 0`

That exactly matches checkpoint program:

`run_20260505_154001/checkpoints/checkpoint_1/programs/b57c5815-291c-44ac-ac35-03878949c74b.json`

This is the real initial policy for the Kimi OpenEvolve run.

An exact text-style comparison of that `policy.yaml` found matches only in these `automation/runs` folders:

- `part3-OPENEVOLVE/2026-05-05-14h23m09s`
- `part3-OPENEVOLVE/2026-05-05-14h29m49s`
- `part3-OPENEVOLVE/2026-05-05-14h41m11s`
- `part3-OPENEVOLVE/2026-05-05-15h05m12s`
- `part3-OPENEVOLVE/2026-05-05-15h35m08s`
- `part3-OPENEVOLVE/2026-05-05-15h40m02sBEST2 not openevolve`

However, semantic policy matching does map the OpenEvolve seed back to the handcrafted work. The exact schedule, ignoring comments, quote style, and YAML ordering, is:

- `risultatiPart3 WIN/Matte/automation/schedules/schedule7bis.yaml`
- `risultatiPart3 WIN/Matte/automation/runs/part3-handcrafted/2026-04-27-06h39m55s/policy.yaml`
- `risultatiPart3 WIN/Matte/automation/runs/part3-handcrafted/2026-04-27-06h45m29s/policy.yaml`

Those two handcrafted summaries report:

- `2026-04-27-06h39m55s`: `makespan_s = 232.0`, `max_observed_p95_us = 429.0`, `slo_violations = 0`
- `2026-04-27-06h45m29s`: `makespan_s = 233.0`, `max_observed_p95_us = 398.4`, `slo_violations = 0`

So the right provenance statement is: the Kimi OpenEvolve initial program was seeded from our handcrafted `schedule7bis` / `split-brain-NodeA` policy. It was not the later best Part A/final policy, which is why it looked disconnected when only checking newer runs. Also, do not match by `policy_name` alone: some other `part3-handcrafted` runs use `split-brain-NodeA` for different schedules. For example, `part3-handcrafted/2026-04-27-08h19m29s/policy.yaml` also says `split-brain-NodeA`, but it places `vips` on `node-b-4core` and uses a different radix dependency, so it is not the OpenEvolve seed.

Additional correction: the policy later named `optimized-parallel` in
`automation/runs/part3-OPENEVOLVE/2026-05-05-17h21m01sBEST1not openvolve/policy.yaml`
is not iteration 0. It is semantically the same topology as several OpenEvolve-generated candidates with different policy names:

- iteration 2, program `7d0df73a`, name `balanced-3track`, parent `b57c5815`, metrics `makespan_s = 214.0`, `max_p95_us = 403.0`
- iteration 8, program `dc4291a2`, name `balanced-3track`, metrics `makespan_s = 209.0`, `max_p95_us = 417.0`
- iteration 12, program `4a2d83ba`, name `overlap-radix-vips`, metrics `makespan_s = 214.0`, `max_p95_us = 396.0`
- iteration 13, program `c458e9b1`, name `overlap-radix-vips-optimized`, metrics `makespan_s = 210.0`, `max_p95_us = 399.7`

The later `optimized-parallel` folder is best treated as a manual re-run / relabel of that evolved topology, with observed metrics `makespan_s = 205.0`, `max_p95_us = 384.0`, `slo_violations = 0`. It should not replace `b57c5815` as the initial OpenEvolve seed in the report.

The current root `initial_program.py` is byte-for-byte identical to:

- `run_20260505_154001/best/best_program.py`
- `run_20260505_154001/checkpoints/checkpoint_20/best_program.py`
- `run_20260505_211836/best/best_program.py`

All four files have the same SHA256 hash:

```text
F73CAF135FC6138D64D76B917C0C880ED9816A6AEF74FF185293FF3A1000A4FD
```

So the current root `initial_program.py` is not the original Kimi seed. It appears to have been overwritten or repurposed after the Kimi run, likely to seed the later GLM 5.1 attempt from the already-evolved `optimized-parallel-radix` best policy.

## Config Correction

Do not blindly trust the current root file:

`risultatiPart3 WIN/Matte/part3_openEvolve/config.yaml`

As of the last check, it says:

```yaml
primary_model: "zai-org/GLM-5.1-FP8-pOah"
```

But the benchmarked OpenEvolve run `run_20260505_154001` logs show Kimi K2.5:

```text
Initialized OpenAI LLM with model: moonshotai/Kimi-K2.5
```

For the submission archive, the config inside `part_3_openevolve/` should correspond to the run that produced the benchmarked scheduler. That means the staged config should be corrected/restored to the Kimi K2.5 run configuration, or at minimum clearly documented if the original Kimi config snapshot is not available.

There is also another run:

`risultatiPart3 WIN/Matte/part3_openEvolve/runs/run_20260505_211836`

That run used GLM 5.1 and then hit repeated LLM timeout/provider failures. It should not be presented as the main benchmarked OpenEvolve run unless the report explicitly discusses it as a failed/secondary attempt.

## System Prompt Points To Mention

The system prompt told the LLM:

- it was an expert cloud infrastructure scheduler;
- the goal was to minimize total makespan for seven batch jobs while keeping memcached below a strict `1 ms` p95 SLO at `30K QPS`;
- it could only edit the Python dictionary inside `EVOLVE-BLOCK`;
- it had to return a policy with `policy_name`, `memcached`, and `jobs`;
- each job needed `node`, `cores`, `threads`, and `after`;
- same-node concurrent jobs could not overlap pinned cores;
- `after` could be `start`, one job name, or a list of job names;
- radix must never run on `node-b-4core`;
- radix threads must be one of `{1, 2, 4, 8}`;
- memcached should get an isolated dedicated core;
- blackscholes was described as safest near memcached;
- canneal was described as poorly scaling and memory/cache sensitive;
- streamcluster, radix, barnes, and vips were described as scaling well with cores;
- the model should exploit parallelism on `node-a-8core` while preserving the SLO.

This is important for the Part 2(b) answer because it explains why the LLM search space was constrained enough to be useful.

## Suggested Part 2(b) Answer Structure

Use this order in `part3.tex`:

1. Explain that OpenEvolve was run as saved policy evaluations, with iteration 0 as the initial policy and 20 saved iterations in the plotted Kimi run.
2. Say the benchmarked run used Kimi K2.5. Mention GLM 5.1 only as a tried model that was operationally problematic.
3. Explain the token/max-token issue using the wording from `part3q2.md`: Kimi used many thinking tokens, and OpenEvolve counted those thinking tokens, not only final response tokens.
4. Explain that there are three Kimi log files because of the restart/configuration process.
5. Explain the automation harness: it handled validation, Kubernetes execution, mcperf collection, and scoring, so the LLM only had to emit a structured policy dictionary.
6. Explain the success metric:

```text
if pass:
    score = 10000 - makespan_s - max(0, max_p95_us - 850) / 10
elif slo_fail:
    score = 1000 - makespan_s - 50 * slo_violations
else:
    score = -1000
```

7. Describe the true initial policy `split-brain-NodeA`.
8. Describe the evolution: the model moved from a two-track baseline to the final `optimized-parallel-radix` layout, keeping memcached isolated on node-b core 0 while using node-a for the larger parallel jobs.
9. Explain why the final policy was better: it kept both nodes useful, isolated memcached, moved blackscholes after canneal on node-b, and let node-a run barnes/streamcluster in parallel, then freqmine/radix, then vips on all eight node-a cores.
10. For a flaw/hallucination example, use a concrete generated policy:
    - iteration 1 `parallel-8core-optimized` looked plausible but made performance much worse: score dropped from `9777` to `9714`, makespan increased from `223 s` to `286 s`, while SLO was still zero;
    - this is a good logical-flaw example because it over-serialized/poorly ordered the node-a work even though it obeyed the schema.

## Final AI Policy Used In Report

The final AI-generated policy benchmark in the report uses these three runs:

- `risultatiPart3 WIN/Matte/automation/runs/part3-PartB/2026-05-09-15h15m46s`
- `risultatiPart3 WIN/Matte/automation/runs/part3-PartB/2026-05-09-15h21m20s`
- `risultatiPart3 WIN/Matte/automation/runs/part3-PartB/2026-05-09-15h26m52s`

Each contains:

- `events.log`
- `experiment.yaml`
- `mcperf.txt`
- `node_platforms.json`
- `phase_plan.json`
- `policy.yaml`
- `results.json`
- `summary.json`
- `rendered_manifests/`

The report table uses these to compute the AI column.

## Why The Best OpenEvolve Policy Improved Makespan

Use this analysis for the question:

```text
\item Explain why did this AI-generated policy perform better (or worse) than the baseline?
```

The answer should focus on critical path, not p95/SLO. In these runs p95 was always far below the `1 ms` SLO and every compared run had zero SLO violations, so latency is basically an inactive constraint. The real difference is whether the schedule shortens the batch critical path.

### Policies To Compare

Best OpenEvolve run:

- folder: `risultatiPart3 WIN/Matte/automation/runs/part3-OPENEVOLVE/2026-05-05-18h31m30s`
- policy: `optimized-parallel-radix`
- makespan: `198.0 s`
- max p95: `398.1 us`
- SLO violations: `0`

Final hand-crafted baseline:

- schedule: `risultatiPart3 WIN/Matte/automation/schedules/aFFinalscheduleParta bis.yaml`
- policy name: `testFinal2`
- repeated Part A folders:
  - `part3-PartA/2026-05-10-02h28m11s`
  - `part3-PartA/2026-05-10-02h34m13s`
  - `part3-PartA/2026-05-10-02h40m25s`
- makespans: `233.0 s`, `235.0 s`, `229.0 s`
- max p95: `378.6 us`, `384.8 us`, `375.2 us`
- SLO violations: `0` for all three

Older repeated runs of the same hand-crafted idea:

- `part3-handcrafted/2026-04-27-02h29m00s`: `241.0 s`, p95 `431.5 us`
- `part3-handcrafted/2026-04-27-02h34m49s`: `229.0 s`, p95 `431.9 us`
- `part3-handcrafted/2026-04-27-02h40m22s`: `225.0 s`, p95 `424.9 us`

### Critical Path Argument

The final hand-crafted schedule is roughly:

```text
node A: streamcluster -> freqmine -> max(vips, radix)
node B: blackscholes -> canneal -> barnes
```

The node-B chain is long. In the repeated final Part A runs:

- `blackscholes`: `45-51 s`
- `canneal`: `106-109 s`
- `barnes`: `62-65 s`

So node B contributes roughly:

```text
45-51 + 106-109 + 62-65 = about 213-225 s
```

before startup/scheduling gaps. This explains the observed `229-235 s` makespan. The hand-crafted policy is conservative and latency-safe, but it leaves a long serial chain next to memcached.

The best OpenEvolve policy changes the dependency graph to:

```text
node A lane 1: barnes -> freqmine
node A lane 2: streamcluster -> radix
node B: canneal -> blackscholes
then: vips after freqmine and radix
```

The key improvement is that it removes `barnes` from the node-B serial chain and runs it on node A instead. In the best OpenEvolve run:

- `canneal`: `117 s`
- `blackscholes`: `43 s`
- node-B chain: about `160 s`
- `barnes`: `55 s`, moved to node A
- `streamcluster`: `160 s`
- `freqmine`: `117 s`
- `radix`: `11 s`
- `vips`: `16 s`

Some individual node-A jobs become slower than in our hand-crafted schedule because the model often gives them only four cores instead of eight. That is not the important part. The dependency graph is better balanced: node B is no longer the long tail, and the final `vips` stage adds only a short tail after both node-A lanes complete. That is why makespan drops to `198 s`.

### Report-Ready Wording

Use or adapt:

```latex
The improvement came from shortening the critical path rather than from better
latency behavior. In our handcrafted policy, node B executes
\texttt{blackscholes -> canneal -> barnes} serially next to memcached. Since
those jobs take roughly \(45\)--\(51\), \(106\)--\(109\), and \(62\)--\(65\)
seconds, this creates a long node-B chain of about \(213\)--\(225\) seconds
before scheduling overhead. The AI policy removes \texttt{barnes} from that
chain and runs it on node A instead.

This turns the schedule into two concurrent node-A lanes,
\texttt{barnes -> freqmine} and \texttt{streamcluster -> radix}, while node B
only runs \texttt{canneal -> blackscholes}. Although some node-A jobs receive
fewer cores than in our handcrafted schedule, the dependency graph is better
balanced: node B is no longer the long tail, and the final \texttt{vips} stage
adds only a short tail after both node-A lanes finish. Therefore the makespan
drops from \(229\)--\(235\,\mathrm{s}\) for our repeated handcrafted runs to
\(198\,\mathrm{s}\) for the best OpenEvolve run.

The p95/SLO constraint did not drive the difference here, since both policies
had zero SLO violations and remained well below \(1\,\mathrm{ms}\).
```

## Final Hand-Crafted Policy Used In Report

The hand-crafted comparison uses:

- `risultatiPart3 WIN/Matte/automation/runs/part3-PartA/2026-05-10-02h28m11s`
- `risultatiPart3 WIN/Matte/automation/runs/part3-PartA/2026-05-10-02h34m13s`
- `risultatiPart3 WIN/Matte/automation/runs/part3-PartA/2026-05-10-02h40m25s`

Each contains the same artifact shape:

- `events.log`
- `experiment.yaml`
- `mcperf.txt`
- `node_platforms.json`
- `phase_plan.json`
- `policy.yaml`
- `results.json`
- `summary.json`
- `rendered_manifests/`

## Submission Rules

Submission rules are in:

`latexSubmission/part3SubmissionRules.md`

Required for Part 3:

- filled PDF report;
- all modified/new YAML files;
- all automation scripts used;
- all useful scripts/files needed to understand the scheduling policy;
- root directory `part_3_openevolve/`;
- root directory `part_3_1_results_group_XXX/`;
- root directory `part_3_2_results_group_XXX/`.

The group number appears to be `054` from the GCP project paths in run summaries (`cca-eth-2026-group-54`), but confirm before final packaging.

## Submission Artifact Plan

Create a clean staging directory. Do not delete or mutate the original run directories.

Suggested staging root:

```text
latexSubmission/part3_submission_stage/
```

or:

```text
C:/tmp/part3_submission_stage/
```

### Measurement Folder For Task 1

Create:

```text
part_3_1_results_group_054/
```

Copy exactly six files:

From `part3-PartA/2026-05-10-02h28m11s`:

- `results.json` -> `pods_1.json`
- `mcperf.txt` -> `mcperf_1.txt`

From `part3-PartA/2026-05-10-02h34m13s`:

- `results.json` -> `pods_2.json`
- `mcperf.txt` -> `mcperf_2.txt`

From `part3-PartA/2026-05-10-02h40m25s`:

- `results.json` -> `pods_3.json`
- `mcperf.txt` -> `mcperf_3.txt`

### Measurement Folder For Task 2

Create:

```text
part_3_2_results_group_054/
```

Copy exactly six files:

From `part3-PartB/2026-05-09-15h15m46s`:

- `results.json` -> `pods_1.json`
- `mcperf.txt` -> `mcperf_1.txt`

From `part3-PartB/2026-05-09-15h21m20s`:

- `results.json` -> `pods_2.json`
- `mcperf.txt` -> `mcperf_2.txt`

From `part3-PartB/2026-05-09-15h26m52s`:

- `results.json` -> `pods_3.json`
- `mcperf.txt` -> `mcperf_3.txt`

### OpenEvolve Folder

Create:

```text
part_3_openevolve/
```

Include:

- corrected/restored Kimi config for `run_20260505_154001`;
- corrected true initial program reconstructed from `b57c5815-291c-44ac-ac35-03878949c74b.json`;
- `risultatiPart3 WIN/Matte/part3_openEvolve/evaluator.py`;
- final best program from `run_20260505_154001/best/best_program.py`;
- final best info from `run_20260505_154001/best/best_program_info.json` if present, otherwise from `checkpoint_20/best_program_info.json`;
- all three Kimi logs from `run_20260505_154001/logs/`;
- full latest checkpoint containing best:

```text
run_20260505_154001/checkpoints/checkpoint_20/
```

The submission rules require the log of the run that generated the best program and the latest checkpoint containing that best program. Including all three Kimi logs is useful because the report will mention the three-log/restart history.

### Automation Files To Include

Include the useful automation source, but not all raw runs:

- `risultatiPart3 WIN/Matte/automation/*.py`
- `risultatiPart3 WIN/Matte/automation/*.yaml`
- `risultatiPart3 WIN/Matte/automation/README.md`
- `risultatiPart3 WIN/Matte/automation/schedules/`

Also include:

- `latexSubmission/Project Template/figures/cx_make_part3_plots.py`
- the generated Part 3 figures under `latexSubmission/Project Template/figures/part3/`
- final report PDF: `latexSubmission/Project Template/main.pdf`

## Cleanup / Exclude From Submission

Do not include bulky or irrelevant run data unless deliberately needed:

- `__pycache__/`
- `tmp/`
- `automation/runs/__precache/`
- all historical `part3-PartA` and `part3-PartB` runs except the three selected runs if preserving extra context;
- most raw `part3-OPENEVOLVE` evaluation runs;
- `part3-OPENEVOLVE_GLM5.1/` unless explicitly used as supporting evidence;
- `part3_openEvolve/runs/run_20260505_211836/` unless explicitly used to discuss GLM failures;
- LaTeX aux files such as `.aux`, `.log`, `.out`, `.synctex.gz`.

Do not delete the original directories. Stage a clean archive copy instead.

## Verification Before Zipping

Run/verify:

1. Regenerate plots:

```powershell
python "latexSubmission/Project Template/figures/cx_make_part3_plots.py"
```

2. Compile report from `latexSubmission/Project Template`:

```powershell
latexmk -pdf -interaction=nonstopmode -halt-on-error main.tex
```

3. Check measurement folders:

- `part_3_1_results_group_054` has exactly six files;
- `part_3_2_results_group_054` has exactly six files;
- files are named exactly `pods_1.json`, `pods_2.json`, `pods_3.json`, `mcperf_1.txt`, `mcperf_2.txt`, `mcperf_3.txt`;
- each `pods_*.json` parses as JSON and contains pod output;
- each `mcperf_*.txt` contains raw mcperf output.

4. Check OpenEvolve artifacts:

- config corresponds to Kimi K2.5 benchmarked run, not the later GLM attempt;
- initial program corresponds to true iteration 0 `split-brain-NodeA`;
- best program corresponds to id `86b89acd-c366-43c4-8fa5-061f5c187143`;
- checkpoint is `checkpoint_20`;
- best metrics match the report.

5. Check final report:

- Part 2(b) is filled;
- wording from `part3q2.md` is represented;
- the report does not claim the root `config.yaml`/`initial_program.py` were correct without fixing/staging them.
