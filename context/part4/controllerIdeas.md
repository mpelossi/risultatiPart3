# Architectural Brainstorming: Dynamic Resource Controller (Part 4)

To design the optimal dynamic resource controller for the Part 4 mixed-criticality environment (collocating latency-critical `memcached` with best-effort PARSEC batch jobs), we can adapt several State-of-the-Art (SOTA) cluster management paradigms. 

Below is a detailed breakdown of advanced control strategies and OS-level actuation mechanisms to ensure we meet the strict **0.8ms tail-latency SLO** while minimizing total batch makespan under highly volatile load (5K–125K QPS).

---

## 1. Empirical State Machine (Inspired by *Quasar*)
The *Quasar (ASPLOS '14)* paper demonstrates that systems should allocate resources based on empirical performance mappings rather than static user requests. 

*   **The Concept:** We will use our static profiling data from Part 4, Question 1 ($T \in \{1,2,3\}, C \in \{1,2,3\}$ vs. QPS) to build a deterministic mapping of safely sustainable CPU utilization.
*   **Implementation:** 
    *   Instead of guessing when `memcached` needs a new core, we define hard thresholds. For example, if profiling shows that a 1-core `memcached` breaches the 0.8ms SLO when core utilization exceeds 75%, we set our **Scale-Up Threshold** strictly at 70% to preempt the violation.
    *   We similarly define a **Scale-Down Threshold** (e.g., 30%) to reclaim "slack" resources for the batch jobs as soon as it is mathematically safe to do so.

## 2. Asymmetric Hysteresis / Slack Reclamation (Inspired by *Google Autopilot*)
Google's *Autopilot (EuroSys '20)* dynamically tunes CPU limits to minimize wasted resources ("slack") while avoiding CPU throttling. Network workloads are uniquely vulnerable to micro-bursts, meaning symmetric scaling will cause continuous SLO violations.

*   **The Concept:** Implement an AIMD-like (Additive Increase, Multiplicative Decrease) cooling-off period to prevent "core thrashing."
*   **Implementation:**
    *   **Fast-Path Scale-Up:** When CPU usage breaches the upper threshold, the controller must instantly grant an additional core to `memcached`. 
    *   **Slow-Path Scale-Down:** When load drops, the controller **must not** instantly revoke the core. It must require $N$ consecutive polling intervals (e.g., 3 consecutive checks over 1.5 seconds) of low utilization before reassigning the core to a batch job. This acts as a buffer against high-frequency QPS oscillations.

## 3. Predictive Derivative-Based Actuation (PID-inspired)
Standard threshold-based controllers are purely reactive—they only act once the CPU is already saturated, which guarantees transient SLO violations during extreme load spikes (like the 5-second interval trace in Q4).

*   **The Concept:** Monitor the **first derivative (slope)** of CPU utilization over time ($d(\text{CPU})/dt$) rather than just the absolute value.
*   **Implementation:**
    *   The controller tracks the CPU utilization $\Delta$ between the current and previous polling ticks (e.g., every 0.5s).
    *   If current utilization is only 50%, but the $\Delta$ is $+30\%$ over the last 0.5 seconds, a massive load spike is actively occurring.
    *   The controller preemptively triggers a Scale-Up event *before* the 70% upper threshold is hit, ensuring the core is ready by the time the load crests.

## 4. Interference-Aware Batch Queuing (Inspired by *Paragon/Quasar*)
Max-Min fairness fails when collocated applications share microarchitectural resources (LLC, memory bandwidth). From our Part 2 iBench profiling, we know exactly how PARSEC jobs degrade under specific contention vectors.

*   **The Concept:** We must actively sort our batch job queue to pair the safest batch jobs with the most vulnerable `memcached` states.
*   **Implementation:**
    *   **High QPS State (`memcached` needs 2-3 cores):** Schedule CPU-bound, cache-friendly jobs (e.g., `blackscholes`, `freqmine`). They can execute on the remaining 1-2 cores and will not thrash the Last Level Cache (LLC) or saturate the memory bus, allowing `memcached`'s network packet processing to proceed unimpeded.
    *   **Low QPS State (`memcached` needs 1 core):** Schedule the noisy, memory-heavy jobs (e.g., `canneal`, `streamcluster`). Because `memcached` is handling very few packets, it is temporarily resilient to memory bus contention.

## 5. Sub-Millisecond Actuation via `SIGSTOP`/`SIGCONT` (OS-Level SOTA)
Updating Docker CPU sets (`docker update --cpuset-cpus`) requires the Linux kernel's Completely Fair Scheduler (CFS) to migrate memory pages and process trees across cores. Under extreme stress, this overhead takes too long.

*   **The Concept:** Utilize process-level signaling for emergency latency control.
*   **Implementation:**
    *   Allow batch containers to share cores with `memcached` at the CFS level, but use `docker pause <container>` as an **Emergency Fast-Path**.
    *   `docker pause` sends a `SIGSTOP` signal to the batch cgroups. The kernel instantaneously deschedules the batch processes, immediately freeing 100% of the core's cycles for `memcached` in sub-milliseconds.
    *   Once the load spike subsides, issue `docker unpause` (`SIGCONT`) to resume batch processing. This trick is what will allow our controller to survive the brutal 5-second `qps_interval` trace with a $<3\%$ SLO violation ratio.

## 6. Token Bucket Scheduling for Batch Progress (Fairness)
If we aggressively pause or throttle batch jobs to protect `memcached`, we risk indefinitely starving them, which ruins our secondary objective: minimizing the total batch makespan.

*   **The Concept:** Treat batch job CPU time like a network token bucket. 
*   **Implementation:**
    *   Maintain an internal counter tracking how long each batch job has been paused/throttled. 
    *   If a job accumulates too many "starvation tokens", the controller prioritizes allocating the next available free core to that specific job. 
    *   To globally optimize makespan, we can weight this bucket using a Shortest Job First (SJF) heuristic (e.g., prioritizing `radix` to clear it out of the system quickly, freeing up memory overhead).