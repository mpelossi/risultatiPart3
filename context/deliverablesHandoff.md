# Part 3 Deliverables Handoff

This file is the packaging handoff for the next agent. It is based on
`latexSubmission/deliverables.md`, `latexSubmission/submissionRules.md`, and the
tail of `latexSubmission/writingNotes/part3q2_openevolve_handoff.md`.

## Current State

- The report source is `latexSubmission/Project Template/main.tex`, with Part 3
  content in `latexSubmission/Project Template/part3.tex`.
- Part 3.2(b) has been filled and includes labels/captions for the OpenEvolve
  figures.
- The Part 3 plotting script is
  `latexSubmission/Project Template/figures/cx_make_part3_plots.py`.
- The generated Part 3 figures are in
  `latexSubmission/Project Template/figures/part3/`.
- `latexSubmission/Project Template/main.pdf` exists, but do not assume it is
  current unless the report compiles again. Recent `latexmk` attempts stopped
  before compilation because MiKTeX reports a fresh installation that needs
  setup.
- `latexSubmission/part3SubmissionRules.md` does not exist in this workspace.
  Use `latexSubmission/deliverables.md` and `latexSubmission/submissionRules.md`
  as the local rules sources.

## Required Submission Root Layout

Create a clean staging directory and zip that staging directory. Do not delete or
move the original run directories.

Recommended staging root:

```text
latexSubmission/part3_submission_stage/
```

The root of the zip should contain at least:

```text
main.pdf
part_3_openevolve/
part_3_1_results_group_054/
part_3_2_results_group_054/
```

Also include the useful YAML/scripts/source files described below. The group
number appears to be `054` from run summaries and GCP paths such as
`cca-eth-2026-group-54`. Confirm if possible before zipping.

## Report and Figures

Before packaging, regenerate figures and compile the report:

```powershell
python "latexSubmission/Project Template/figures/cx_make_part3_plots.py"
cd "latexSubmission/Project Template"
latexmk -pdf -interaction=nonstopmode -halt-on-error main.tex
```

Known blocker: `latexmk` currently exits before reading the document with:

```text
It seems that this is a fresh TeX installation.
Please finish the setup before proceeding.
```

If this still happens, fix MiKTeX setup first or compile on a working TeX
installation. Do not package an old PDF without verifying it reflects the latest
`part3.tex`.

Generated figures currently expected under `figures/part3/`:

- `cx_part3_q1a_run1.png`
- `cx_part3_q1a_run2.png`
- `cx_part3_q1a_run3.png`
- `cx_part3_q1b_aggressive_candidate.png`
- `cx_part3_q2a_ai_run1.png`
- `cx_part3_q2a_ai_run2.png`
- `cx_part3_q2a_ai_run3.png`
- `cx_part3_q2a_openevolve_p95_makespan.png`
- `cx_part3_q2a_openevolve_slo_score.png`
- `cx_part3_q2b_openevolve_descendants.png`
- `cx_part3_q2b_openevolve_generations.png`
- `cx_part3_q2b_openevolve_iteration0_seed.png`
- `cx_part3_q2b_openevolve_iteration1_flaw.png`

Include `latexSubmission/Project Template/figures/cx_make_part3_plots.py` in the
submission because it explains how the report plots were generated.

## Measurement Output Folders

Each measurement folder must contain exactly six files:

```text
pods_1.json
pods_2.json
pods_3.json
mcperf_1.txt
mcperf_2.txt
mcperf_3.txt
```

The `pods_*.json` files should be copied from each run's `results.json`. The
`mcperf_*.txt` files should be copied from each run's `mcperf.txt`.

### Task 1: Handcrafted Policy

Create:

```text
part_3_1_results_group_054/
```

Copy:

| Source run | Source file | Destination |
|---|---|---|
| `risultatiPart3 WIN/Matte/automation/runs/part3-PartA/2026-05-10-02h28m11s` | `results.json` | `pods_1.json` |
| `risultatiPart3 WIN/Matte/automation/runs/part3-PartA/2026-05-10-02h28m11s` | `mcperf.txt` | `mcperf_1.txt` |
| `risultatiPart3 WIN/Matte/automation/runs/part3-PartA/2026-05-10-02h34m13s` | `results.json` | `pods_2.json` |
| `risultatiPart3 WIN/Matte/automation/runs/part3-PartA/2026-05-10-02h34m13s` | `mcperf.txt` | `mcperf_2.txt` |
| `risultatiPart3 WIN/Matte/automation/runs/part3-PartA/2026-05-10-02h40m25s` | `results.json` | `pods_3.json` |
| `risultatiPart3 WIN/Matte/automation/runs/part3-PartA/2026-05-10-02h40m25s` | `mcperf.txt` | `mcperf_3.txt` |

These are the three handcrafted runs used in the report table:

- makespans: `233.0 s`, `235.0 s`, `229.0 s`
- max p95 values: `378.6 us`, `384.8 us`, `375.2 us`
- SLO violations: `0` for all three

### Task 2: AI/OpenEvolve Policy

Create:

```text
part_3_2_results_group_054/
```

Copy:

| Source run | Source file | Destination |
|---|---|---|
| `risultatiPart3 WIN/Matte/automation/runs/part3-PartB/2026-05-09-15h15m46s` | `results.json` | `pods_1.json` |
| `risultatiPart3 WIN/Matte/automation/runs/part3-PartB/2026-05-09-15h15m46s` | `mcperf.txt` | `mcperf_1.txt` |
| `risultatiPart3 WIN/Matte/automation/runs/part3-PartB/2026-05-09-15h21m20s` | `results.json` | `pods_2.json` |
| `risultatiPart3 WIN/Matte/automation/runs/part3-PartB/2026-05-09-15h21m20s` | `mcperf.txt` | `mcperf_2.txt` |
| `risultatiPart3 WIN/Matte/automation/runs/part3-PartB/2026-05-09-15h26m52s` | `results.json` | `pods_3.json` |
| `risultatiPart3 WIN/Matte/automation/runs/part3-PartB/2026-05-09-15h26m52s` | `mcperf.txt` | `mcperf_3.txt` |

These are the three AI policy runs used in the report table. The AI column mean
makespan is `197.33 s`, and the SLO violation ratio is `0/63 = 0`.

## OpenEvolve Folder

Create:

```text
part_3_openevolve/
```

The benchmarked OpenEvolve run is:

```text
risultatiPart3 WIN/Matte/part3_openEvolve/runs/run_20260505_154001
```

The successful model in the logs is:

```text
moonshotai/Kimi-K2.5
```

Include these artifacts:

| Artifact | Source |
|---|---|
| evaluator | `risultatiPart3 WIN/Matte/part3_openEvolve/evaluator.py` |
| final best program | `risultatiPart3 WIN/Matte/part3_openEvolve/runs/run_20260505_154001/best/best_program.py` |
| final best info | `risultatiPart3 WIN/Matte/part3_openEvolve/runs/run_20260505_154001/best/best_program_info.json` |
| all Kimi logs | `risultatiPart3 WIN/Matte/part3_openEvolve/runs/run_20260505_154001/logs/*.log` |
| latest checkpoint containing best | `risultatiPart3 WIN/Matte/part3_openEvolve/runs/run_20260505_154001/checkpoints/checkpoint_20/` |
| true initial program source JSON | `risultatiPart3 WIN/Matte/part3_openEvolve/runs/run_20260505_154001/checkpoints/checkpoint_1/programs/b57c5815-291c-44ac-ac35-03878949c74b.json` |

Best program facts to verify:

- id: `86b89acd-c366-43c4-8fa5-061f5c187143`
- found at iteration: `14`
- latest checkpoint containing best: `checkpoint_20`
- `combined_score = 9802.0`
- `makespan_s = 198.0`
- `max_p95_us = 398.1`
- `slo_violations = 0.0`
- `status_pass = 1.0`
- `audit_pass = 1.0`
- `run_completed = 1.0`

### OpenEvolve Caveats

Do not blindly use root files without checking:

- `risultatiPart3 WIN/Matte/part3_openEvolve/initial_program.py` has been
  overwritten/reused and matches the final best program, not the true
  iteration-0 seed.
- The true initial program is the `code` field inside
  `checkpoint_1/programs/b57c5815-291c-44ac-ac35-03878949c74b.json`.
- `risultatiPart3 WIN/Matte/part3_openEvolve/config.yaml` currently reports
  `primary_model: "moonshotai/Kmi-K2.5"` with a typo. The benchmarked run logs
  show `moonshotai/Kimi-K2.5`. For submission, stage a corrected/restored Kimi
  config or document the correction clearly inside `part_3_openevolve/`.
- The assignment instructions mention an `openevolve_collect.py` helper, but no
  file with that name was found in this workspace. Collect artifacts manually.

Recommended staged names inside `part_3_openevolve/`:

```text
config.yaml
initial_program.py
evaluator.py
best_program.py
best_program_info.json
logs/
checkpoint_20/
```

If reconstructing `initial_program.py`, extract the JSON `code` value from:

```text
run_20260505_154001/checkpoints/checkpoint_1/programs/b57c5815-291c-44ac-ac35-03878949c74b.json
```

and write that exact code to the staged `part_3_openevolve/initial_program.py`.

## Automation and Policy Files To Include

Include useful automation source, but not all raw run history:

- `risultatiPart3 WIN/Matte/automation/*.py`
- `risultatiPart3 WIN/Matte/automation/*.yaml`
- `risultatiPart3 WIN/Matte/automation/README.md`
- `risultatiPart3 WIN/Matte/automation/schedules/`
- `latexSubmission/Project Template/figures/cx_make_part3_plots.py`
- `latexSubmission/Project Template/figures/part3/`

Important automation files currently present include:

- `audit.py`
- `catalog.py`
- `cli.py`
- `cluster.py`
- `config.py`
- `cpu_sets.py`
- `experiment.yaml`
- `gui.py`
- `manifests.py`
- `metrics.py`
- `part3.yaml`
- `runner.py`
- `runtime_stats.py`
- `schedule_queue.yaml`
- `viewer.py`
- `viewer_data.py`

It is acceptable to include the whole `automation/schedules/` directory because
the report discusses both final and competing handcrafted schedules.

## Exclusions

Do not include bulky or irrelevant data unless deliberately needed:

- `__pycache__/`
- `tmp/`
- `automation/runs/__precache/`
- most historical `automation/runs/` folders
- most raw `part3-OPENEVOLVE` evaluation folders
- `part3-OPENEVOLVE_GLM5.1/`, unless intentionally using it as supporting
  evidence
- `part3_openEvolve/runs/run_20260505_211836/`, unless intentionally packaging
  GLM failure evidence
- LaTeX build intermediates such as `.aux`, `.log`, `.out`, `.synctex.gz`

Do not delete originals. Stage clean copies.

## Verification Before Zipping

Run or verify all of the following before creating the final zip:

1. Regenerate plots:

   ```powershell
   python "latexSubmission/Project Template/figures/cx_make_part3_plots.py"
   ```

2. Compile report from `latexSubmission/Project Template`:

   ```powershell
   latexmk -pdf -interaction=nonstopmode -halt-on-error main.tex
   ```

3. Confirm measurement folder file counts and names:

   - `part_3_1_results_group_054` has exactly six files.
   - `part_3_2_results_group_054` has exactly six files.
   - File names are exactly `pods_1.json`, `pods_2.json`, `pods_3.json`,
     `mcperf_1.txt`, `mcperf_2.txt`, `mcperf_3.txt`.

4. Validate measurement file content:

   - every `pods_*.json` parses as JSON and contains the pod output from the
     corresponding `results.json`;
   - every `mcperf_*.txt` contains raw mcperf output and has the expected header.

5. Validate OpenEvolve artifacts:

   - staged config corresponds to Kimi K2.5, not a GLM attempt;
   - staged initial program is the true iteration-0 program from checkpoint 1,
     not the overwritten root `initial_program.py`;
   - staged best program/info correspond to id
     `86b89acd-c366-43c4-8fa5-061f5c187143`;
   - staged checkpoint is `checkpoint_20`;
   - logs include all three Kimi logs:
     `openevolve_20260505_154001.log`,
     `openevolve_20260505_174021.log`,
     `openevolve_20260505_182140.log`.

6. Validate report consistency:

   - Part 3.2(b) is filled.
   - The report mentions Kimi K2.5 as the benchmarked successful run.
   - The report does not expose private seed names in the prose.
   - OpenEvolve score facts match the staged best info.
   - Figure references resolve after a successful LaTeX compile.

## Suggested Packaging Sequence

1. Fix TeX/MiKTeX setup and compile a fresh `main.pdf`.
2. Create `latexSubmission/part3_submission_stage/`.
3. Copy fresh `main.pdf` to the staging root.
4. Create and populate `part_3_1_results_group_054/`.
5. Create and populate `part_3_2_results_group_054/`.
6. Create and populate `part_3_openevolve/`, applying the OpenEvolve caveats
   above.
7. Copy useful automation, YAML policies, report plotting script, and generated
   figures.
8. Run the verification checklist.
9. Zip the contents of the staging root, not the parent directory unless the
   submission platform explicitly wants a wrapper folder.
