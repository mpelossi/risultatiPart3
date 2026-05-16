# PDF Extraction: How to get correct measurements? (ETH Zürich, D-INFK)

*Note: The following content is extracted page-by-page, preserving the chronological order. The presentation flows logically from explaining why performance evaluation matters, to outlining common pitfalls (Slides 5-11), giving advice on avoiding them (Slides 12-18), conducting a visual quiz on bad plotting practices (Slides 19-30), distinguishing between "description" and "explanation" in scientific writing (Slides 31-38), and finally concluding with plotting best practices and technical recommendations (Slides 39-41).*

---

## Page 1
### Title Slide
*   **Header:** ETH Zürich
*   **Title:** How to get correct measurements?
*   **Visual:** Solid blue background with white text.

---

## Page 2
### Slide: Always measure one level deeper
*   **Header:** ETH Zürich | D-INFK
*   **Concepts & Text:**
    *   Thorough performance evaluation is important in:
        *   **Academic research:** Essential for many publications to prove the value of a new idea (prove my system is working).
        *   **Industry:** Maintain a high level of performance (across the lifetime) of a product (e.g., clients care about SLAs - Service Level Agreements).
    *   **Good performance evaluation requires:**
        *   Deep understanding of a system's behavior - *why does the system behave like that?*
        *   Understanding of internal mechanisms and components.
*   **Reference/Citation:** *Always Measure One Level Deeper*, John Ousterhout, 2018.

---

## Page 3
### Slide: What is wrong with common performance evaluation practices?
*   **Header:** ETH Zürich | D-INFK
*   **Concepts & Text:** 
    *   This is a transition/question slide posing the central problem of the lecture. (Left blank below the title).

---

## Page 4
### Slide: What is wrong with common performance evaluation practices?
*   **Header:** ETH Zürich | D-INFK
*   **Concepts & Text:**
    *   *Relationship: This slide provides the answers to the question posed on Page 3.*
    *   Performance measurement is often done superficially, e.g., just before the paper deadline.
    *   People stop measuring as soon as they find an experiment that gave positive results, omitting the less favorable results.
    *   **Conclusion (Highlighted with a red arrow):** This leads to incomplete, misleading, and erroneous measurements.

---

## Page 5
### Slide: Common mistakes in performance evaluation?
*   **Header:** ETH Zürich | D-INFK
*   **Concepts & Text:**
    *   Transition/question slide. (Left blank below the title).

---

## Page 6
### Slide: Common mistakes in performance evaluation?
*   **Header:** ETH Zürich | D-INFK
*   **Concepts & Text:**
    *   *Relationship: This outlines the 5 main mistakes that will be individually detailed in Pages 7 through 11.*
    1.  Trusting the numbers
    2.  Guessing instead of measuring
    3.  Superficial measurements
    4.  Confirmation Bias
    5.  Not enough time

---

## Page 7
### Slide: Mistake 1: trusting the numbers
*   **Header:** ETH Zürich | D-INFK
*   **Concepts & Text:**
    *   Performance-measurement code is likely to have bugs, but the bugs are less obvious. These bugs could appear in:
        *   The benchmarks themselves.
        *   The scripts that process/plot the measurements.
    *   **Common misconception:** *If the system is not crashing, the measurements are probably correct.*
    *   **Reality:** Most measurement bugs don't cause crashes; instead, they simply produce wrong numbers.

---

## Page 8
### Slide: Mistake 2: guessing instead of measuring
*   **Header:** ETH Zürich | D-INFK
*   **Concepts & Text:**
    *   It is unsafe to draw conclusions based on intuition without measurements to back them up.
    *   **"What else could it possibly be?"**: not a valid justification.
    *   It is unsafe to present an explanation as fact unless measurements confirm the specific behavior.
        *   *Example:* If you think that component A is the bottleneck, measure this and verify!
    *   **Core Rule:** It is important to measure before fixing!

---

## Page 9
### Slide: Mistake 3: superficial measurements
*   **Header:** ETH Zürich | D-INFK
*   **Concepts & Text:**
    *   The **all-to-all** performance of a system is essential (e.g., overall runtime, average latency of a request to the server).
    *   **However, this is not enough!**
    *   We have to measure the **"internal"** behavior of a system, i.e., per-component:
        *   Break the performance down (e.g., what components are responsible for which portion of the latency?).
        *   What improvement had the greatest impact on performance?

---

## Page 10
### Slide: Mistake 4: Confirmation Bias
*   **Header:** ETH Zürich | D-INFK
*   **Concepts & Text:**
    *   Confirmation bias => select and interpret data in a way that supports their hypothesis (you hope/expect the system will work well).
    *   When you see a result that supports your hypothesis (e.g., "the system has high throughput"), you are more likely to accept the result without question.
    *   In contrast, if a measurement suggests it is not performing well, you are more likely to dig deeper to explain what is happening and fix the problem.
    *   **Conclusion (Highlighted with a red arrow):** Results can be inaccurate or misleading.

---

## Page 11
### Slide: Mistake 5: Not enough time
*   **Header:** ETH Zürich | D-INFK
*   **Concepts & Text:**
    *   **Allow enough time to do performance evaluation!!!**
        *   (Almost certainly) there will be bugs.
        *   Many iterations of evaluation/performance improvements are required.
        *   Need to evaluate in multiple scenarios/use cases for a thorough analysis.

---

## Page 12
### Slide: Advice for high-quality measurements
*   **Header:** ETH Zürich | D-INFK
*   **Concepts & Text:**
    *   *Relationship: This slide shifts the presentation from diagnosing problems to offering solutions.*
    *   **Allow lots of time.**
    *   **Don't trust the numbers:**
        *   Measurements should be considered *guilty until proven innocent*.
        *   Take different measurements at the same level.
        *   Validate with back-of-the-envelope calculations/simulations.
    *   **Use intuition to ask questions, not answer them:**
        *   Can help identifying directions for further exploration.
        *   *Always validate with data before making decisions or claims* (Text highlighted in red).
    *   **Always measure one level deeper:**
        *   Measure performance of different components.
        *   Look at the distribution, not only median values.

---

## Page 13
### Slide: Measurement infrastructure
*   **Header:** ETH Zürich | D-INFK
*   **Concepts & Text:**
    *   **Automate** measurements as much as possible.
    *   Create a **dashboard**: a display to show all performance measurements from a particular run.
    *   **Do not remove the instrumentation.**
    *   **Presentation matters:**
        *   Should expose the correct level of details.
        *   Easy to understand.
        *   High-quality plotting!!

---

## Page 14
### Slide: Measurement infrastructure
*   **Header:** ETH Zürich | D-INFK
*   **Concepts & Text:**
    *   *Relationship: Identical to Page 13, but adds a critical grading warning for the students.*
    *   Text is identical to Page 13 with the addition of:
        *   A callout box: "Please look at the slides in Moodle for guidelines!"
        *   Bold red text at the bottom: **Readability of your plot will be part of your grade!**

---

## Page 15
### Slide: Plotting: common errors and guidelines
*   **Header:** ETH Zürich
*   **Concepts & Text:**
    *   Transition slide with a solid blue background. Marks the beginning of the "Plotting" section.

---

## Page 16
### Slide: Recap: system under test (part 1)
*   **Header:** ETH Zürich | D-INFK
*   **Diagram/Visual:**
    *   A block diagram showing a measurement setup.
    *   **Left Block:** "Mcperf Load generator (clients)" [Pink box].
    *   **Right Block:** "Memcached (server) (2) Process" [Blue box]. Next to it is a light blue cylinder representing a database/storage.
    *   **Bottom Block:** "Mcperf Measurements" [Pink box].
    *   **Connections:** 
        *   Blue arrow from clients to server labeled "(1) Requests".
        *   Red arrow from server to clients labeled "(3) Responses".
        *   Black arrows pointing from both the load generator and the server to the "Mcperf Measurements" block, indicating data collection.

---

## Page 17
### Slide: Testing methodology (part 1)
*   **Header:** ETH Zürich | D-INFK
*   **Concepts & Text:**
    *   Repeat at least 3 times.
    *   **X-axis:** the **actual** achieved **QPS** (not the target). *("actual" is highlighted in red).*
    *   **Y-axis:** 95th percentile latency.
    *   **Plot error bars for each point on both axes.**
    *   Report QPS from the measurement machine.

---

## Page 18
### Slide: Collect and process results
*   **Header:** ETH Zürich | D-INFK
*   **Concepts & Text:**
    *   **Structure** your measurement data and **store the results of each repetition:**
        *   Collect p95 response times of each repetition.
        *   Collect queries per second (**achieved not target!**) of each repetition. *(Text in red).*
    *   Compute average, sample standard deviation.
    *   Use a scripting language that works for you: bash, Python, ...
    *   Other statistics (e.g., min, max, percentiles, ...) might also be interesting to calculate.
    *   **Automate** as much as possible.

---

## Page 19
### Slide: Plot 1: What is wrong with this plot?
*   **Header:** ETH Zürich | D-INFK
*   **Diagram/Visual:**
    *   Line chart titled internally but acting as a bad example.
    *   **Y-axis:** Throughput (requests/s) from 0 to 30000.
    *   **X-axis:** Number of clients from 0 to 400.
    *   Two lines are plotted ("1 thread" and "2 threads"), but they are exactly the same color (blue).
    *   Error bars are present, but there are no point markers indicating exactly where the data points fall.

---

## Page 20
### Slide: Plot 1: What is wrong with this plot?
*   **Header:** ETH Zürich | D-INFK
*   **Concepts & Text:**
    *   *Relationship: This slide provides the answers to the visual quiz on Page 19.*
    *   **Identified Errors:**
        *   Misses over-saturated part (X-axis stops too early to show where the system plateaus fully).
        *   Lines are not distinguishable (bad choice of colors).
        *   Missing line points.

---

## Page 21
### Slide: Plot 2: What is wrong with this plot?
*   **Header:** ETH Zürich | D-INFK
*   **Diagram/Visual:**
    *   Line chart.
    *   **Y-axis:** Response time (µsec) ranging from 0 up to 600000 (very large numbers).
    *   **X-axis:** Number of clients from 0 to 400.
    *   Three lines (1 thread, 2 threads, 4 threads).
    *   The lines have erratic, deep, sharp dips at random client intervals (e.g., line 1 jumps from 450000 down to 300000 then back up to 500000).

---

## Page 22
### Slide: Plot 2: What is wrong with this plot?
*   **Header:** ETH Zürich | D-INFK
*   **Concepts & Text:**
    *   *Relationship: Begins answering the quiz from Page 21.*
    *   **Visual Update:** Red squares are drawn over the erratic dips in the graph.
    *   **Identified Errors/Questions:**
        *   Why these **unexpected dips** in response time? *(Text in red)*
            *   Are requests being lost?
            *   Is there a benchmarking problem?
            *   Is the same setup used in each experiment?

---

## Page 23
### Slide: Plot 2: What is wrong with this plot?
*   **Header:** ETH Zürich | D-INFK
*   **Concepts & Text:**
    *   *Relationship: Completes the answers for Plot 2 (adding to Page 22).*
    *   **Additional Identified Errors:**
        *   Y-axis would be better in ms-scale (to avoid showing 600000 µsec).
        *   Error bars absent.
        *   The same symbol (an open circle) is used everywhere for the line points, making it harder to read if printed in black and white.

---

## Page 24
### Slide: Plot 3: What is wrong with this plot?
*   **Header:** ETH Zürich | D-INFK
*   **Diagram/Visual:**
    *   Line chart.
    *   **Y-axis:** Throughput (requests/s) starting at 10000 and ending at 30000.
    *   **X-axis:** Number of clients from 0 to 1000.
    *   Shows 2 lines (1 thread, 2 threads). The lines are highly jagged, fluctuating wildly up and down across the entire X-axis.

---

## Page 25
### Slide: Plot 3: What is wrong with this plot?
*   **Header:** ETH Zürich | D-INFK
*   **Concepts & Text:**
    *   *Relationship: Answers the quiz from Page 24.*
    *   **Visual Update:** A red box highlights a massive, sharp drop in the orange line around 200 clients.
    *   **Identified Errors:**
        *   Y-axis should start from 0.
        *   What is going on **here**? (Pointing to the red box drop).
        *   Data fluctuations? Were the measurements repeated?
        *   Error bars and line points are absent.

---

## Page 26
### Slide: Plot 4: What is wrong with this plot?
*   **Header:** ETH Zürich | D-INFK
*   **Diagram/Visual:**
    *   Line chart.
    *   **Y-axis:** Throughput (requests/s) starting at 20000 up to 30000.
    *   **X-axis:** Number of clients from 0 to 1000.
    *   Lines for "System A" and "System B". The data points only exist at 200, 400, 600, 800, and 1000. No data between 0 and 200.

---

## Page 27
### Slide: Plot 4: What is wrong with this plot?
*   **Header:** ETH Zürich | D-INFK
*   **Concepts & Text:**
    *   *Relationship: Answers the quiz from Page 26.*
    *   **Identified Errors:**
        *   Y-axis should start from 0.
        *   What happens when:
            *   Num_clients < 200? (There is a massive gap in data where the system behavior is unknown).
            *   Num_clients > 1000?
        *   Y-axis would be better in Krequests/s.

---

## Page 28
### Slide: Find the improvements
*   **Header:** ETH Zürich | D-INFK
*   **Diagram/Visual:**
    *   A very poorly formatted, bare-bones plot.
    *   **Y-axis:** Numbers 10000 to 50000, but no title/label.
    *   **X-axis:** Titled "Number of clients", but ticks are spaced terribly (20, 60, 100... then a massive gap to 300).
    *   Two blue lines without points, without error bars, and without a legend. Font is extremely small.

---

## Page 29
### Slide: Find the improvements
*   **Header:** ETH Zürich | D-INFK
*   **Concepts & Text:**
    *   *Relationship: Annotates the bad plot from Page 28 with red text indicating all the flaws.*
    *   **Y-axis:** Missing label, Does not start at 0, Upper limit too high (data peaks around 25000, but axis goes to 50000).
    *   **X-axis:** Inconsistent ticks at X axis.
    *   **General:** Font size too small, No legend, Line colors not distinguishable, Missing line points, Missing error bars.

---

## Page 30
### Slide: After the improvements
*   **Header:** ETH Zürich | D-INFK
*   **Concepts & Text:**
    *   *Relationship: Shows the transformation of the bad plot from Page 28/29 into a standard, high-quality plot.*
    *   **Visual:** A large red arrow points from the bad plot to a new, improved plot.
    *   **Improved Plot Features:**
        *   Clear title: "Throughput of different systems" and subtitle "Error bars: 1s. (10 repetitions per point)".
        *   Y-axis properly labeled and scaled: "Throughput (requests/s)" from 0 to 30K (using K notation).
        *   X-axis properly scaled and labeled: 0 to 400 evenly spaced.
        *   Legend clearly placed: System A (Blue circle), System B (Orange X).
        *   Line points clearly visible with distinct shapes and colors.
        *   Error bars correctly plotted for every point.

---

## Page 31
### Slide: Description vs Explanation
*   **Header:** ETH Zürich | D-INFK
*   **Concepts & Text:**
    *   *Relationship: This slide begins an 8-part sequence clarifying the semantic difference between describing a graph and explaining system behavior in scientific writing.*
    *   **Example sentence 1:** We observe in Figure 1 that the throughput increases linearly with the number of clients, until X number of clients is reached.

---

## Page 32
### Slide: Description vs Explanation
*   **Header:** ETH Zürich | D-INFK
*   **Concepts & Text:**
    *   **Example sentence 1** is labeled in blue text as **DESCRIPTION**.

---

## Page 33
### Slide: Description vs Explanation
*   **Header:** ETH Zürich | D-INFK
*   **Concepts & Text:**
    *   **Example sentence 2:** Since the rate of increase in response time changes suddenly at X number of clients, we deduce the system is saturated at...

---

## Page 34
### Slide: Description vs Explanation
*   **Header:** ETH Zürich | D-INFK
*   **Concepts & Text:**
    *   **Example sentence 2** is labeled in blue text as **DESCRIPTION**.

---

## Page 35
### Slide: Description vs Explanation
*   **Header:** ETH Zürich | D-INFK
*   **Concepts & Text:**
    *   **Example sentence 3:** The throughput saturates at X number of clients, because...

---

## Page 36
### Slide: Description vs Explanation
*   **Header:** ETH Zürich | D-INFK
*   **Concepts & Text:**
    *   **Example sentence 3** is labeled in red text as **EXPLANATION**.
    *   **Visual:** A thought bubble points to "EXPLANATION" asking: *"What happens from a systems' perspective?"* (This is the core difference: explanations address the 'why' under the hood).

---

## Page 37
### Slide: Description vs Explanation
*   **Header:** ETH Zürich | D-INFK
*   **Concepts & Text:**
    *   **Example sentence 4:** The interactive law holds as expected, as shown in Figure 1.

---

## Page 38
### Slide: Description vs Explanation
*   **Header:** ETH Zürich | D-INFK
*   **Concepts & Text:**
    *   **Example sentence 4** is labeled in blue text as **DESCRIPTION**. 
    *   *(Note: Simply verifying a mathematical law visually is a description of the data, not a systems-level explanation of mechanisms).*

---

## Page 39
### Slide: Plotting - best practices
*   **Header:** ETH Zürich | D-INFK
*   **Concepts & Text:**
    *   **Start axis at zero**, and keep same range for related graphs.
    *   **Label both axes, state units clearly:**
        *   Use understandable units: e.g., requests/s, not requests/minute.
        *   Instead of 1200000 use 1.2M or 1.2 million.
        *   **Caution with logarithmic scales on axis.**
    *   **Include error bars! AND: explain** what the error bars represent!
    *   Make sure system configuration is easily found: what was measured?

---

## Page 40
### Slide: Benchmarking - recommendations
*   **Header:** ETH Zürich | D-INFK
*   **Concepts & Text:**
    *   As you progress and rerun experiments save results in different files:
        *   You can regenerate graphs for different versions of the result.
    *   Keep style consistent over graphs.
    *   **Automate and use scripts for exporting graphs.**
    *   **Plotting tools:**
        *   gnuplot (shell)
        *   matplotlib (Python)
        *   pgfplots (LaTeX)
        *   Excel (Windows) (though, not (easily) automated)
    *   **No hand-drawn plots!**

---

## Page 41
### Slide: Example: Matplotlib
*   **Header:** ETH Zürich
*   **Concepts & Text:**
    *   *Relationship: Provides concrete code examples for the Python plotting tool recommended on Page 40.*
    *   **Create a plot** (Grey box):
        *   `import matplotlib.pyplot as plt`
        *   `fig = plt.figure()`
        *   `fig_ax = fig.gca()`
        *   `...`
        *   `plt.tight_layout()`
        *   `plt.savefig("example.pdf")`
    *   **Plot data** (Green box):
        *   `fig_ax.plot(x, y, ...)`
        *   `fig_ax.errorbar(x, y, xerr=..., yerr=..., ...)`
    *   **Fix layout** (Pink box):
        *   `fig_ax.set_xlabel(...)`, `fig_ax.set_ylabel(...)`
        *   `fig_ax.grid(...)`
        *   `fig_ax.set_xlim(...)`, `fig_ax.set_ylim(...)`
        *   `fig_ax.tick_params(...)`
        *   `fig_ax.set_xticks(...)`, `fig_ax.set_yticks(...)`
        *   `fig_ax.set_xticklabels(...)`, `fig_ax.set_yticklabels(...)`
        *   `fig_ax.legend(...)`