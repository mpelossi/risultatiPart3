# CCA Course Project - Part 4: Analysis & Strategy Guide

## 1. Executive Summary: Part 3 vs. Part 4
Part 4 represents a fundamental shift in architecture from Part 3. 

*   **Part 3 (Static Global Scheduling):** We built a static Directed Acyclic Graph (DAG) scheduler that dispatched workloads across a heterogeneous Kubernetes cluster (4-core + 8-core) against a constant `memcached` load, maintaining a 1.0ms SLO.
*   **Part 4 (Dynamic Local Control Loop):** We are building a continuous, reactive controller on a **single 4-core VM**. `memcached` runs natively alongside Dockerized PARSEC batch jobs. The load fluctuates randomly between 5K and 125K QPS. We must dynamically scale `memcached`'s core allocation using `taskset` and manage batch jobs using Docker commands, all while maintaining a stricter **0.8ms** tail-latency SLO.

## 2. Codebase Reusability Assessment
Our current Part 3 codebase (`Matte/automation`) is highly optimized for Kubernetes but requires significant stripping down for Part 4.

*   **DO NOT REUSE:** `manifests.py` (K8s YAML generation), `audit.py` (static DAG validation), and the core `run_phase_scheduler` loop in `runner.py`. Kubernetes does not natively support sub-second dynamic CPU-set reassignment, so we must interface directly with Docker and `systemd`.
*   **REUSE & ADAPT:**
    *   `cluster.py`: Adapt to use `part4.yaml` for bringing up the 4-node cluster (Master, 8-core Agent, 2-core Measure, 4-core Server).
    *   `metrics.py`: The `parse_mcperf_output_tolerant` function is still perfectly valid for reading latency data.
    *   **SSH / Subprocess logic:** The mechanisms used to remotely execute `mcperf` from the `client-measure` node remain identical, though the flags change (using `--qps_interval`, `--qps_min`, `--qps_max`).
    *   `schedule_viewer_data.py`: The timeline plotting logic will be highly valuable for generating the complex 3-pane "mega-plots" required for the final report.

---

## 3. Step-by-Step Task Breakdown

### Question 1: Static Profiling (19 points)
Before writing the dynamic controller, we must establish our baseline thresholds.
*   **Action:** Manually benchmark `memcached` running alone on the 4-core VM.
*   **Parameters:** Sweep QPS from 5K to 125K using configurations of Threads ($T$) $\in \{1, 2, 3\}$ and Cores ($C$) $\in \{1, 2, 3\}$.
*   **Goal:** Identify the optimal fixed number of threads ($T$) required to survive 125K QPS at < 0.8ms latency. We will then profile the CPU utilization mapping for $C \in \{1, 2, 3\}$ to define the exact CPU% thresholds that should trigger our controller to add or remove cores.

### Question 2: Controller Design (17 points)
We must write a Python script (`controller.py`) that runs **locally on the 4-core VM**.
*   **Action:** Implement a control loop (e.g., polling every 0.5s via `psutil`).
*   **Logic:** Dictate exactly when to scale up `memcached` (steal cores from batch jobs) and when to scale down (return cores to batch jobs). Justify the order and core allocations of PARSEC jobs.

### Question 3: 15-Second Interval Evaluation (23 points)
Evaluate the controller against a 30-minute trace where load changes every 15 seconds.
*   **Action:** Run the controller 3 times.
*   **Deliverables:**
    1.  **Custom Execution Logs:** We *must* use the strictly defined logging format (e.g., `2026-05-07T14:30:00.123456 start memcached [0, 1] 4`). We cannot use our old K8s `results.json`. Any divergence in formatting results in point deductions.
    2.  **Mega-Plots:** A 3-pane vertically stacked plot sharing the X-axis (Time). Top: Core allocation Gantt chart. Middle: Target QPS vs. Achieved p95 Latency (with a 0.8ms threshold line). Bottom: CPU utilization per core.

### Question 4: 5-Second Interval Stress Test (16 points)
Stress test the controller by dropping the load interval to 5 seconds.
*   **Action:** Determine the absolute minimum `qps_interval` our controller can successfully handle while keeping the SLO violation ratio under 3%.
*   **Deliverables:** Explain the system bottlenecks (e.g., Docker update latency, polling intervals, context switching overhead) that dictate this theoretical limit.

---

## 4. Proposed "Optimal" Controller Architecture (SOTA Inspired)

Drawing inspiration from the course lectures and State-of-the-Art (SOTA) systems like Google Borg (Autopilot) and Quasar, here is the proposed design for our controller.

### A. Threshold-Based State Machine (inspired by *Quasar*)
Instead of guessing, we use the data from Q1 to build a deterministic state machine.
*   *Example State Transition:* If our Q1 data shows that `memcached` on 1 core hits 0.8ms latency when CPU reaches 75%, our controller sets a hard scale-up threshold at 70% CPU to preempt the violation.

### B. Asymmetric Scaling: Fast-Up, Slow-Down (inspired by *Google Autopilot*)
Network loads exhibit micro-bursts. 
*   **Scale-Up:** Must be instantaneous. If CPU > Threshold, immediately grant an additional core.
*   **Scale-Down:** Must incorporate an AIMD-style (Additive Increase, Multiplicative Decrease) cooldown. If CPU utilization drops, the controller must observe low utilization for $N$ consecutive polling intervals (e.g., 2 seconds) before revoking a core. This prevents "core thrashing" and protects against rapid subsequent spikes.

### C. Predictive/Derivative Monitoring
Standard controllers only look at absolute CPU usage. We should look at the **first derivative (slope)** of CPU utilization.
*   If `memcached` CPU usage is currently at 45% but was 15% just 0.5 seconds ago ($\Delta = +30\%$), a massive load spike is underway. The controller should proactively assign a core *before* it hits the 70% threshold. This is critical for surviving the 5-second interval stress test in Q4.

### D. Interference-Aware Batch Queuing
From Part 2, we know exactly how PARSEC jobs behave under cache and memory bandwidth contention. We should statically sort our batch queue based on interference.
*   **High QPS State (Memcached needs 2-3 cores):** Schedule CPU-bound, cache-friendly jobs (e.g., `blackscholes`, `freqmine`). They will execute on the remaining core(s) without thrashing `memcached`'s memory access.
*   **Low QPS State (Memcached needs 1 core):** Schedule noisy, memory-heavy jobs (e.g., `canneal`, `streamcluster`). Since `memcached` is under low load, it can safely absorb the memory bus contention.

### E. Fast-Path Actuation via `SIGSTOP`
Re-pinning cores via `docker update --cpuset-cpus` requires the Linux scheduler to migrate memory pages and process trees, which introduces latency.
*   **Emergency Fast-Path:** When a massive spike hits, instead of repinning the batch container, we issue a `docker pause <container>` (which sends an instant `SIGSTOP` to the processes). The kernel immediately halts the batch job, instantly freeing up the core for `memcached`.
*   **Recovery:** Once the burst subsides, we use `docker unpause` (`SIGCONT`) to resume batch processing. This sub-millisecond actuation is what will allow us to push the minimum `qps_interval` in Q4 to its absolute limits.

---

## 5. Next Steps / Action Plan
1.  **Isolate Part 4 Code:** Create a new directory. Copy over `cluster.py`, modify it to point to `part4.yaml`.
2.  **Script Q1 Profiling:** Write a quick script to automate the execution of the 9 configurations ($T \in \{1,2,3\}, C \in \{1,2,3\}$) to generate the baseline data.
3.  **Draft `controller.py`:** Build the local Python daemon using `psutil` (for `/proc/stat` monitoring) and the `subprocess` module to issue `taskset` and `docker` commands. Ensure the custom `jobs_1.txt` logger is implemented exactly to spec.
4.  **Build Plotting Utilities:** Adapt the frontend/plotting logic from `schedule_viewer_data.py` to ingest the new `.txt` logs and `mcperf` data to generate the shared-X-axis mega-plots.