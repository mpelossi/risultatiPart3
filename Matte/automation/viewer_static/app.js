const state = {
  experiments: [],
  experimentId: null,
  runs: [],
  filteredRuns: [],
  bestRunId: null,
  selectedRunIds: [],
  statusFilter: "all",
  searchText: "",
  hasPrimedSelection: false,
};

const statusFilter = document.getElementById("status-filter");
const experimentSelect = document.getElementById("experiment-select");
const searchInput = document.getElementById("search-input");
const selectionCount = document.getElementById("selection-count");
const statsGrid = document.getElementById("stats-grid");
const bestRunCard = document.getElementById("best-run-card");
const compareGrid = document.getElementById("compare-grid");
const compareNote = document.getElementById("compare-note");
const historyGrid = document.getElementById("history-grid");
const historyCount = document.getElementById("history-count");
const refreshButton = document.getElementById("refresh-button");
const clearSelectionButton = document.getElementById("clear-selection-button");
const statCardTemplate = document.getElementById("stat-card-template");
const REFRESH_INTERVAL_MS = 5000;
const MAX_SELECTED_RUNS = 2;

const JOB_LABELS = {
  memcached: "memcached",
  barnes: "barnes",
  blackscholes: "blackscholes",
  canneal: "canneal",
  freqmine: "freqmine",
  radix: "radix",
  streamcluster: "streamcluster",
  vips: "vips",
};

const JOB_SHORT_LABELS = {
  memcached: "mc",
  barnes: "barnes",
  blackscholes: "black",
  canneal: "canneal",
  freqmine: "freq",
  radix: "radix",
  streamcluster: "stream",
  vips: "vips",
};

function fmtSeconds(value) {
  if (value === null || value === undefined || Number.isNaN(Number(value))) {
    return "n/a";
  }
  return `${Number(value).toFixed(1)} s`;
}

function fmtSecondsCompact(value) {
  if (value === null || value === undefined || Number.isNaN(Number(value))) {
    return "n/a";
  }
  return `${Number(value).toFixed(0)}s`;
}

function fmtP95(value) {
  if (value === null || value === undefined || Number.isNaN(Number(value))) {
    return "n/a";
  }
  return `${Number(value).toFixed(1)} us`;
}

function fmtTimestamp(value) {
  if (!value) {
    return "n/a";
  }
  return String(value).replace("T", " ").replace("Z", " UTC");
}

function text(value, fallback = "n/a") {
  if (value === null || value === undefined || value === "") {
    return fallback;
  }
  return String(value);
}

function coreLabel(value) {
  return value === null || value === undefined || value === "" ? "cores n/a" : `cores ${value}`;
}

function compactCoreLabel(value) {
  return value === null || value === undefined || value === "" ? "c n/a" : `c ${value}`;
}

function threadLabel(value) {
  if (value === null || value === undefined || value === "") {
    return "threads n/a";
  }
  const count = Number(value);
  return `${value} ${count === 1 ? "thread" : "threads"}`;
}

function coreCount(segment) {
  return Array.isArray(segment.core_ids) && segment.core_ids.length ? segment.core_ids.length : null;
}

function coreCountLabel(segment) {
  const count = coreCount(segment);
  if (count === null) {
    return "core count n/a";
  }
  return `${count} ${count === 1 ? "core" : "cores"}`;
}

function compactResourceLabel(segment) {
  const count = coreCount(segment);
  const coreText = count === null ? "c?" : `${count}c`;
  const threadText = segment.threads === null || segment.threads === undefined || segment.threads === "" ? "t?" : `${segment.threads}t`;
  return `${coreText}/${threadText}`;
}

function statusClass(status) {
  const normalized = text(status, "unknown").toLowerCase();
  return `status-${normalized.replace(/[^a-z0-9]+/g, "_")}`;
}

function uniqueStatuses(runs) {
  const values = new Set();
  runs.forEach((run) => values.add(text(run.overall_status, "unknown")));
  return Array.from(values).sort();
}

function resetStatusFilterOptions(runs) {
  const currentValue = state.statusFilter;
  statusFilter.innerHTML = "";
  const allOption = document.createElement("option");
  allOption.value = "all";
  allOption.textContent = "All statuses";
  statusFilter.appendChild(allOption);

  uniqueStatuses(runs).forEach((status) => {
    const option = document.createElement("option");
    option.value = status;
    option.textContent = status;
    statusFilter.appendChild(option);
  });
  statusFilter.value = uniqueStatuses(runs).includes(currentValue) ? currentValue : "all";
  state.statusFilter = statusFilter.value;
}

async function fetchJson(url) {
  const response = await fetch(url, { cache: "no-store" });
  if (!response.ok) {
    const payload = await response.json().catch(() => ({}));
    throw new Error(payload.error || `Request failed: ${response.status}`);
  }
  return response.json();
}

async function loadExperiments() {
  const payload = await fetchJson("/api/experiments");
  state.experiments = payload.experiments || [];

  experimentSelect.innerHTML = "";
  state.experiments.forEach((experiment) => {
    const option = document.createElement("option");
    option.value = experiment.experiment_id;
    option.textContent = `${experiment.experiment_id} (${experiment.run_count} runs)`;
    experimentSelect.appendChild(option);
  });

  if (!state.experimentId) {
    state.experimentId = payload.default_experiment_id || (state.experiments[0] && state.experiments[0].experiment_id) || null;
  }
  if (state.experimentId) {
    experimentSelect.value = state.experimentId;
  }
}

async function loadRuns() {
  if (!state.experimentId) {
    state.runs = [];
    state.filteredRuns = [];
    render();
    return;
  }
  const payload = await fetchJson(`/api/runs?experiment=${encodeURIComponent(state.experimentId)}`);
  state.runs = payload.runs || [];
  state.bestRunId = payload.best_run_id || null;
  resetStatusFilterOptions(state.runs);
  applyFilters();
  primeSelection();
  render();
}

function applyFilters() {
  const needle = state.searchText.trim().toLowerCase();
  state.filteredRuns = state.runs.filter((run) => {
    if (state.statusFilter !== "all" && text(run.overall_status, "unknown") !== state.statusFilter) {
      return false;
    }
    if (!needle) {
      return true;
    }
    const haystack = [
      text(run.run_id),
      text(run.run_label),
      text(run.policy_name),
    ].join(" ").toLowerCase();
    return haystack.includes(needle);
  });
}

function primeSelection() {
  if (state.hasPrimedSelection) {
    state.selectedRunIds = state.selectedRunIds.filter((runId) => state.runs.some((run) => run.run_id === runId));
    return;
  }

  const firstRun = state.runs.find((run) => run.run_id === state.bestRunId) || state.runs[0];
  state.selectedRunIds = firstRun ? [firstRun.run_id] : [];
  state.hasPrimedSelection = true;
}

function focusRun(runId) {
  state.selectedRunIds = [runId];
  render();
}

function toggleComparisonRun(runId) {
  const selected = state.selectedRunIds.filter((selectedRunId) => state.runs.some((run) => run.run_id === selectedRunId));
  const index = selected.indexOf(runId);
  if (index === 0) {
    state.selectedRunIds = selected;
    render();
    return;
  }
  if (index > 0) {
    selected.splice(index, 1);
    state.selectedRunIds = selected;
    render();
    return;
  }

  if (!selected.length) {
    state.selectedRunIds = [runId];
  } else {
    state.selectedRunIds = [selected[0], runId].slice(0, MAX_SELECTED_RUNS);
  }
  render();
}

function compareWithBest(runId) {
  const selected = [];
  if (state.bestRunId && state.bestRunId !== runId) {
    selected.push(state.bestRunId);
  }
  selected.push(runId);
  state.selectedRunIds = selected.slice(0, MAX_SELECTED_RUNS);
  render();
}

function removeSelectedRun(runId) {
  state.selectedRunIds = state.selectedRunIds.filter((selectedRunId) => selectedRunId !== runId);
  render();
}

function render() {
  renderStats();
  renderBestRun();
  renderCompare();
  renderHistory();
  selectionCount.textContent = `${state.selectedRunIds.length} / ${MAX_SELECTED_RUNS}`;
  historyCount.textContent = `${state.filteredRuns.length} shown out of ${state.runs.length}`;
}

function renderStats() {
  const eligible = state.runs.filter((run) => run.eligible_for_best).length;
  const summaryBacked = state.runs.filter((run) => run.artifact_flags && run.artifact_flags.summary).length;
  const partial = state.runs.filter((run) => !run.timeline || !run.timeline.has_data).length;
  const best = state.runs.find((run) => run.run_id === state.bestRunId) || null;

  statsGrid.innerHTML = "";
  [
    {
      label: "Total Runs",
      value: state.runs.length,
      detail: state.experimentId ? text(state.experimentId) : "No experiment selected",
    },
    {
      label: "Best Makespan",
      value: best ? fmtSeconds(best.makespan_s) : "n/a",
      detail: best ? `${text(best.run_id)} | ${text(best.policy_name)}` : "No eligible best run",
    },
    {
      label: "Eligible Runs",
      value: eligible,
      detail: "Pass + complete timing + usable mcperf",
    },
    {
      label: "Reconstruction Split",
      value: `${summaryBacked} / ${state.runs.length}`,
      detail: `${partial} metadata-only or chart-limited runs`,
    },
  ].forEach((item) => {
    const fragment = statCardTemplate.content.cloneNode(true);
    fragment.querySelector(".stat-label").textContent = item.label;
    fragment.querySelector(".stat-value").textContent = item.value;
    fragment.querySelector(".stat-detail").textContent = item.detail;
    statsGrid.appendChild(fragment);
  });
}

function createRunBadges(run) {
  const badges = [];
  if (run.artifact_flags && run.artifact_flags.summary) {
    badges.push("summary");
  }
  if (run.is_reconstructed) {
    badges.push("reconstructed");
  }
  if (run.artifact_flags && run.artifact_flags.results) {
    badges.push("results.json");
  }
  if (run.artifact_flags && run.artifact_flags.pods) {
    badges.push("pods.json");
  }
  if (run.artifact_flags && !run.artifact_flags.snapshot) {
    badges.push("missing pods");
  }
  if (run.artifact_flags && !run.artifact_flags.mcperf) {
    badges.push("missing mcperf");
  }
  if (run.measurement_status === "parse_error") {
    badges.push("parse error");
  }
  if (run.measurement_status === "no_samples") {
    badges.push("no samples");
  }
  return badges;
}

function renderBadges(target, run) {
  const badgeRow = document.createElement("div");
  badgeRow.className = "badge-row";
  createRunBadges(run).forEach((badgeText) => {
    const badge = document.createElement("span");
    badge.className = "badge";
    badge.textContent = badgeText;
    badgeRow.appendChild(badge);
  });
  target.appendChild(badgeRow);
}

function renderBestRun() {
  const run = state.runs.find((item) => item.run_id === state.bestRunId);
  bestRunCard.innerHTML = "";

  if (!run) {
    bestRunCard.className = "empty-state";
    bestRunCard.textContent = "No run currently qualifies as best.";
    return;
  }

  bestRunCard.className = "best-card best-highlight";
  bestRunCard.appendChild(createRunSummaryBlock(run, { compact: false, includeTimeline: true }));
  bestRunCard.appendChild(createRunMetaBlock(run));
}

function createRunSummaryBlock(run, options) {
  const wrapper = document.createElement("div");

  const head = document.createElement("div");
  head.className = "run-title-row";

  const titleGroup = document.createElement("div");
  const title = document.createElement("h3");
  title.className = "run-title";
  title.textContent = text(run.run_label, run.run_id);
  titleGroup.appendChild(title);

  const subtitle = document.createElement("p");
  subtitle.className = "run-subtitle";
  subtitle.textContent = `${text(run.run_id)} | ${text(run.policy_name)} | ${text(run.timestamp_iso, "timestamp unavailable")}`;
  titleGroup.appendChild(subtitle);
  head.appendChild(titleGroup);

  const status = document.createElement("span");
  status.className = `status-pill ${statusClass(run.overall_status)}`;
  status.textContent = text(run.overall_status, "unknown");
  head.appendChild(status);
  wrapper.appendChild(head);

  const metrics = document.createElement("div");
  metrics.className = "metrics-grid";
  [
    { label: "Makespan", value: fmtSeconds(run.makespan_s) },
    { label: "P95", value: fmtP95(run.max_observed_p95_us) },
    { label: "Measurement", value: text(run.measurement_status, "missing") },
  ].forEach((item) => {
    const metric = document.createElement("div");
    metric.className = "metric";
    const label = document.createElement("span");
    label.className = "metric-label";
    label.textContent = item.label;
    const value = document.createElement("span");
    value.className = "metric-value";
    value.textContent = item.value;
    metric.appendChild(label);
    metric.appendChild(value);
    metrics.appendChild(metric);
  });
  wrapper.appendChild(metrics);

  renderBadges(wrapper, run);

  if (run.issues && run.issues.length) {
    const issue = document.createElement("div");
    issue.className = "issue";
    issue.textContent = run.issues[0];
    wrapper.appendChild(issue);
  }

  const actions = document.createElement("div");
  actions.className = "card-actions";

  const compareButton = document.createElement("button");
  compareButton.type = "button";
  compareButton.className = `card-button primary ${state.selectedRunIds[0] === run.run_id ? "selected" : ""}`.trim();
  compareButton.textContent = state.selectedRunIds[0] === run.run_id ? "Viewing Details" : "View Details";
  compareButton.addEventListener("click", () => focusRun(run.run_id));
  actions.appendChild(compareButton);

  if (state.selectedRunIds[0] !== run.run_id) {
    const duoButton = document.createElement("button");
    duoButton.type = "button";
    duoButton.className = `card-button ${state.selectedRunIds.includes(run.run_id) ? "selected" : ""}`.trim();
    duoButton.textContent = state.selectedRunIds.includes(run.run_id) ? "Remove Compare" : "Compare";
    duoButton.addEventListener("click", () => toggleComparisonRun(run.run_id));
    actions.appendChild(duoButton);
  }

  wrapper.appendChild(actions);

  if (options.includeTimeline) {
    wrapper.appendChild(createTimelineCard(run, options.scaleMax));
  }

  return wrapper;
}

function createRunMetaBlock(run) {
  const wrapper = document.createElement("div");
  const title = document.createElement("p");
  title.className = "section-note";
  title.textContent = "Quick notes";
  wrapper.appendChild(title);

  const metrics = document.createElement("div");
  metrics.className = "metrics-grid";
  [
    { label: "Completed jobs", value: `${text(run.completed_job_count, 0)} / ${text(run.expected_job_count, 0)}` },
    { label: "Samples", value: text(run.sample_count, "0") },
    { label: "Best eligible", value: run.eligible_for_best ? "yes" : "no" },
  ].forEach((item) => {
    const metric = document.createElement("div");
    metric.className = "metric";
    const label = document.createElement("span");
    label.className = "metric-label";
    label.textContent = item.label;
    const value = document.createElement("span");
    value.className = "metric-value";
    value.textContent = item.value;
    metric.appendChild(label);
    metric.appendChild(value);
    metrics.appendChild(metric);
  });
  wrapper.appendChild(metrics);

  if (run.issues && run.issues.length) {
    const issueRow = document.createElement("div");
    issueRow.className = "issue-row";
    run.issues.slice(0, 4).forEach((issueText) => {
      const issue = document.createElement("div");
      issue.className = "issue";
      issue.textContent = issueText;
      issueRow.appendChild(issue);
    });
    wrapper.appendChild(issueRow);
  }

  const finePrint = document.createElement("p");
  finePrint.className = "fine-print";
  finePrint.textContent = "Best-run ranking follows the existing CLI rule: pass first, then lowest makespan, then lowest p95.";
  wrapper.appendChild(finePrint);
  return wrapper;
}

function selectedRuns() {
  return state.selectedRunIds
    .map((runId) => state.runs.find((run) => run.run_id === runId))
    .filter(Boolean);
}

function renderCompare() {
  const runs = selectedRuns();
  compareGrid.innerHTML = "";
  compareGrid.className = "compare-grid";

  if (!runs.length) {
    compareNote.textContent = "Pick one run to inspect; add a second run to compare.";
    compareGrid.innerHTML = '<div class="empty-state">No run selected yet.</div>';
    return;
  }

  const scaleMax = runs.reduce((max, run) => {
    const runMax = run.timeline && run.timeline.max_end_s ? Number(run.timeline.max_end_s) : 0;
    return Math.max(max, runMax);
  }, 0);

  compareGrid.classList.add(runs.length === 1 ? "single-detail" : "comparison-mode");
  compareNote.textContent = runs.length === 1
    ? "Single-run detail view. Use Compare on another run to place it beside this one."
    : `Shared axis uses ${fmtSeconds(scaleMax)} as the comparison ceiling.`;
  runs.forEach((run, index) => {
    const card = document.createElement("article");
    card.className = [
      "compare-card",
      index === 0 ? "primary-detail" : "",
      run.run_id === state.bestRunId ? "best-highlight" : "",
    ].filter(Boolean).join(" ");

    const head = document.createElement("div");
    head.className = "compare-head";
    head.innerHTML = `
      <div>
        <h3 class="run-title">${text(run.run_label, run.run_id)}</h3>
        <p class="run-subtitle">${text(run.policy_name)} | ${text(run.run_id)}</p>
      </div>
      <span class="status-pill ${statusClass(run.overall_status)}">${text(run.overall_status, "unknown")}</span>
    `;
    card.appendChild(head);

    card.appendChild(createMetricsGrid([
      { label: "Makespan", value: fmtSeconds(run.makespan_s) },
      { label: "P95", value: fmtP95(run.max_observed_p95_us) },
      { label: "Measurement", value: text(run.measurement_status, "missing") },
    ]));
    renderBadges(card, run);
    card.appendChild(createTimelineCard(run, scaleMax, { includeDetails: runs.length === 1 }));

    const actions = document.createElement("div");
    actions.className = "card-actions";
    const focusButton = document.createElement("button");
    focusButton.type = "button";
    focusButton.className = `card-button primary ${index === 0 ? "selected" : ""}`.trim();
    focusButton.textContent = index === 0 ? "Viewing Details" : "View Details";
    focusButton.addEventListener("click", () => focusRun(run.run_id));
    actions.appendChild(focusButton);

    const removeButton = document.createElement("button");
    removeButton.type = "button";
    removeButton.className = "card-button";
    removeButton.textContent = runs.length === 1 ? "Clear" : "Remove";
    removeButton.addEventListener("click", () => removeSelectedRun(run.run_id));
    if (runs.length > 1 || index > 0) {
      actions.appendChild(removeButton);
    }
    card.appendChild(actions);

    compareGrid.appendChild(card);
  });
}

function createMetricsGrid(items) {
  const metrics = document.createElement("div");
  metrics.className = "metrics-grid";
  items.forEach((item) => {
    const metric = document.createElement("div");
    metric.className = "metric";
    const label = document.createElement("span");
    label.className = "metric-label";
    label.textContent = item.label;
    const value = document.createElement("span");
    value.className = "metric-value";
    value.textContent = item.value;
    metric.appendChild(label);
    metric.appendChild(value);
    metrics.appendChild(metric);
  });
  return metrics;
}

function createTimelineCard(run, explicitScaleMax, options = {}) {
  const includeDetails = options.includeDetails !== false;
  const container = document.createElement("div");
  container.className = "timeline-card";

  const timeline = run.timeline || {};
  if (!timeline.has_data) {
    container.innerHTML = '<div class="empty-state">This run does not have enough timing data to render a chart.</div>';
    return container;
  }

  const scaleMax = Math.max(Number(explicitScaleMax || 0), Number(timeline.max_end_s || 0), 1);
  const scroller = document.createElement("div");
  scroller.className = "timeline-scroll";
  const chart = document.createElement("div");
  chart.className = "timeline-chart";
  chart.appendChild(createAxis(scaleMax));

  (timeline.lanes || []).forEach((lane) => {
    chart.appendChild(createLane(lane, scaleMax));
  });
  scroller.appendChild(chart);
  container.appendChild(scroller);
  if (includeDetails) {
    container.appendChild(createTimelineDetails(timeline));
  }

  return container;
}

function createAxis(scaleMax) {
  const axis = document.createElement("div");
  axis.className = "axis";
  const axisLine = document.createElement("div");
  axisLine.className = "axis-line";
  axis.appendChild(axisLine);

  [0, 0.25, 0.5, 0.75, 1].forEach((ratio) => {
    const tick = document.createElement("span");
    tick.className = [
      "tick",
      ratio === 0 ? "tick-start" : "",
      ratio === 1 ? "tick-end" : "",
    ].filter(Boolean).join(" ");
    tick.style.left = `${ratio * 100}%`;
    tick.textContent = fmtSeconds(scaleMax * ratio);
    axis.appendChild(tick);
  });
  return axis;
}

function createLane(lane, scaleMax) {
  const laneWrap = document.createElement("div");
  laneWrap.className = "lane";

  const label = document.createElement("div");
  label.className = "lane-label";
  const labelName = document.createElement("span");
  labelName.className = "lane-label-name";
  labelName.textContent = text(lane.label || lane.short_label, "lane");
  const labelType = document.createElement("span");
  labelType.className = "lane-label-type";
  labelType.textContent = text(lane.lane_id, "");
  const labelNode = document.createElement("span");
  labelNode.className = "lane-label-node";
  const rawNames = Array.isArray(lane.node_names) ? lane.node_names.filter(Boolean) : [];
  labelNode.textContent = rawNames.length ? rawNames.join(", ") : "no pod node";
  label.appendChild(labelName);
  label.appendChild(labelType);
  label.appendChild(labelNode);
  laneWrap.appendChild(label);

  const track = document.createElement("div");
  track.className = "lane-track";

  const segments = lane.segments || [];
  if (!segments.length) {
    track.style.minHeight = "54px";
    const empty = document.createElement("div");
    empty.className = "lane-empty";
    empty.textContent = "No timed work recorded.";
    track.appendChild(empty);
  } else {
    const layout = layoutLaneSegments(segments);
    const trackHeight = Math.max(78, layout.rowCount * 58 + (layout.memcached.length ? 52 : 16));
    track.style.minHeight = `${trackHeight}px`;
    layout.jobs.forEach((item) => {
      track.appendChild(createSegment(item.segment, scaleMax, { rowIndex: item.rowIndex }));
    });
    layout.memcached.forEach((segment) => {
      track.appendChild(createSegment(segment, scaleMax));
    });
  }

  laneWrap.appendChild(track);
  return laneWrap;
}

function layoutLaneSegments(segments) {
  const memcached = segments.filter((segment) => text(segment.kind, "job") === "memcached");
  const jobs = segments
    .filter((segment) => text(segment.kind, "job") !== "memcached")
    .sort((left, right) => {
      const startDiff = Number(left.start_s || 0) - Number(right.start_s || 0);
      if (startDiff !== 0) {
        return startDiff;
      }
      return Number(right.duration_s || 0) - Number(left.duration_s || 0);
    });

  const rowEnds = [];
  const laidOutJobs = jobs.map((segment) => {
    const start = Number(segment.start_s || 0);
    const end = Number(segment.end_s || start);
    let rowIndex = rowEnds.findIndex((rowEnd) => start >= rowEnd);
    if (rowIndex === -1) {
      rowIndex = rowEnds.length;
      rowEnds.push(end);
    } else {
      rowEnds[rowIndex] = end;
    }
    return { segment, rowIndex };
  });

  return {
    jobs: laidOutJobs,
    memcached,
    rowCount: Math.max(rowEnds.length, 1),
  };
}

function createSegment(segment, scaleMax, options = {}) {
  const bar = document.createElement("div");
  const kind = text(segment.kind, "job");
  const jobId = text(segment.job_id, "unknown");
  const rawWidth = (Number(segment.duration_s || 0) / scaleMax) * 100;
  const width = Math.max(rawWidth, kind === "memcached" ? 0 : 12);
  const left = (Number(segment.start_s || 0) / scaleMax) * 100;
  const boundedWidth = Math.max(0, Math.min(width, 100 - left));
  const isTight = boundedWidth < 18 && kind !== "memcached";
  bar.className = [
    "segment",
    kind === "memcached" ? "memcached" : "",
    isTight ? "segment-tight" : "",
    `job-${jobId}`,
  ].filter(Boolean).join(" ");
  bar.style.left = `${left}%`;
  bar.style.width = `${boundedWidth}%`;
  if (options.rowIndex !== undefined) {
    bar.style.setProperty("--segment-row", String(options.rowIndex));
  }
  bar.title = [
    text(segment.label, jobId),
    `start: ${fmtSeconds(segment.start_s)}`,
    `end: ${fmtSeconds(segment.end_s)}`,
    `duration: ${fmtSeconds(segment.duration_s)}`,
    coreCountLabel(segment),
    threadLabel(segment.threads),
    coreLabel(segment.cores),
    `status: ${text(segment.status, "unknown")}`,
    segment.planned_node ? `planned node: ${segment.planned_node}` : null,
    segment.raw_node_name ? `node: ${segment.raw_node_name}` : null,
  ].filter(Boolean).join("\n");

  if (kind !== "memcached") {
    const name = document.createElement("span");
    name.className = "segment-label";
    name.textContent = isTight ? text(JOB_SHORT_LABELS[jobId], segment.label) : text(segment.label, jobId);
    bar.appendChild(name);

    const meta = document.createElement("span");
    meta.className = "segment-meta";
    meta.textContent = isTight
      ? `${fmtSecondsCompact(segment.duration_s)} | ${compactResourceLabel(segment)} | ${text(segment.cores, "n/a")}`
      : `${fmtSeconds(segment.duration_s)} | ${coreCountLabel(segment)} / ${threadLabel(segment.threads)} | ${coreLabel(segment.cores)}`;
    bar.appendChild(meta);

    const cores = document.createElement("span");
    cores.className = "segment-cores";
    cores.textContent = `${compactResourceLabel(segment)} | ${compactCoreLabel(segment.cores)}`;
    bar.appendChild(cores);
  } else {
    const name = document.createElement("span");
    name.className = "segment-label";
    name.textContent = "memcached";
    bar.appendChild(name);

    const meta = document.createElement("span");
    meta.className = "segment-meta";
    meta.textContent = `${fmtSeconds(segment.duration_s)} | ${coreCountLabel(segment)} / ${threadLabel(segment.threads)} | ${coreLabel(segment.cores)}`;
    bar.appendChild(meta);

    bar.setAttribute(
      "aria-label",
      `memcached ${fmtSeconds(segment.duration_s)} ${coreCountLabel(segment)} ${threadLabel(segment.threads)} ${coreLabel(segment.cores)}`
    );
  }
  return bar;
}

function timelineSegments(timeline) {
  return (timeline.lanes || [])
    .flatMap((lane) => (lane.segments || []).map((segment) => ({ ...segment, lane_label: lane.label || lane.lane_id })))
    .sort((left, right) => {
      const startDiff = Number(left.start_s || 0) - Number(right.start_s || 0);
      if (startDiff !== 0) {
        return startDiff;
      }
      return text(left.job_id).localeCompare(text(right.job_id));
    });
}

function createTimelineDetails(timeline) {
  const details = document.createElement("div");
  details.className = "timeline-details";
  details.setAttribute("aria-label", "Job timing details");

  const header = document.createElement("div");
  header.className = "timeline-detail-row timeline-detail-head";
  ["Job", "Duration", "Core Count", "Cores", "Threads", "Start", "End", "Node", "Status"].forEach((labelText) => {
    const cell = document.createElement("span");
    cell.textContent = labelText;
    header.appendChild(cell);
  });
  details.appendChild(header);

  let rowCount = 0;
  timelineSegments(timeline).forEach((segment) => {
    const row = document.createElement("div");
    row.className = "timeline-detail-row";

    [
      { className: "detail-job", value: text(JOB_LABELS[segment.job_id] || segment.label, segment.job_id) },
      { className: "detail-duration", value: fmtSeconds(segment.duration_s) },
      { className: "detail-core-count", value: text(coreCount(segment), "n/a") },
      { className: "detail-cores", value: text(segment.cores, "n/a") },
      { className: "detail-threads", value: text(segment.threads, "n/a") },
      { className: "detail-time", value: fmtTimestamp(segment.started_at) },
      { className: "detail-time", value: fmtTimestamp(segment.finished_at) },
      { className: "detail-node", value: text(segment.raw_node_name || segment.lane_label) },
      { className: "detail-status", value: text(segment.status, "unknown") },
    ].forEach((item) => {
      const cell = document.createElement("span");
      cell.className = item.className;
      cell.textContent = item.value;
      row.appendChild(cell);
    });

    details.appendChild(row);
    rowCount += 1;
  });

  if (!rowCount) {
    const empty = document.createElement("p");
    empty.className = "timeline-detail-empty";
    empty.textContent = "No completed job durations recorded.";
    details.appendChild(empty);
  }

  return details;
}

function renderHistory() {
  historyGrid.innerHTML = "";

  if (!state.filteredRuns.length) {
    historyGrid.innerHTML = '<div class="empty-state">No runs match the current filters.</div>';
    return;
  }

  state.filteredRuns.forEach((run) => {
    const card = document.createElement("article");
    card.className = [
      "history-card",
      run.run_id === state.bestRunId ? "best-highlight" : "",
      state.selectedRunIds.includes(run.run_id) ? "selected-highlight" : "",
    ].filter(Boolean).join(" ");

    const head = document.createElement("div");
    head.className = "history-head";
    head.innerHTML = `
      <div>
        <h3 class="run-title">${text(run.run_label, run.run_id)}</h3>
        <p class="run-subtitle">${text(run.policy_name)} | ${text(run.run_id)}</p>
      </div>
      <span class="status-pill ${statusClass(run.overall_status)}">${text(run.overall_status, "unknown")}</span>
    `;
    card.appendChild(head);

    const metrics = document.createElement("div");
    metrics.className = "metrics-grid";
    [
      { label: "Makespan", value: fmtSeconds(run.makespan_s) },
      { label: "P95", value: fmtP95(run.max_observed_p95_us) },
      { label: "Measurement", value: text(run.measurement_status, "missing") },
    ].forEach((item) => {
      const metric = document.createElement("div");
      metric.className = "metric";
      metric.innerHTML = `<span class="metric-label">${item.label}</span><span class="metric-value">${item.value}</span>`;
      metrics.appendChild(metric);
    });
    card.appendChild(metrics);

    renderBadges(card, run);

    if (run.issues && run.issues.length) {
      const issue = document.createElement("div");
      issue.className = "issue";
      issue.textContent = run.issues[0];
      card.appendChild(issue);
    }

    const actions = document.createElement("div");
    actions.className = "card-actions";

    const compareButton = document.createElement("button");
    compareButton.type = "button";
    compareButton.className = `card-button primary ${state.selectedRunIds[0] === run.run_id ? "selected" : ""}`.trim();
    compareButton.textContent = state.selectedRunIds[0] === run.run_id ? "Viewing" : "View Details";
    compareButton.addEventListener("click", () => focusRun(run.run_id));
    actions.appendChild(compareButton);

    if (state.selectedRunIds[0] !== run.run_id) {
      const comparisonButton = document.createElement("button");
      comparisonButton.type = "button";
      comparisonButton.className = `card-button ${state.selectedRunIds.includes(run.run_id) ? "selected" : ""}`.trim();
      comparisonButton.textContent = state.selectedRunIds.includes(run.run_id) ? "Remove Compare" : "Compare";
      comparisonButton.addEventListener("click", () => toggleComparisonRun(run.run_id));
      actions.appendChild(comparisonButton);
    }

    if (run.run_id !== state.bestRunId && state.bestRunId) {
      const bestButton = document.createElement("button");
      bestButton.type = "button";
      bestButton.className = "card-button";
      bestButton.textContent = "With Best";
      bestButton.addEventListener("click", () => compareWithBest(run.run_id));
      actions.appendChild(bestButton);
    }

    card.appendChild(actions);
    historyGrid.appendChild(card);
  });
}

async function refreshAll(options = {}) {
  const silent = Boolean(options.silent);
  try {
    if (!silent) {
      refreshButton.disabled = true;
      refreshButton.textContent = "Refreshing...";
    }
    await loadExperiments();
    await loadRuns();
  } catch (error) {
    bestRunCard.className = "empty-state";
    bestRunCard.textContent = error.message;
    compareGrid.innerHTML = '<div class="empty-state">Failed to load run data.</div>';
    historyGrid.innerHTML = '<div class="empty-state">Failed to load run data.</div>';
  } finally {
    if (!silent) {
      refreshButton.disabled = false;
      refreshButton.textContent = "Refresh Data";
    }
  }
}

experimentSelect.addEventListener("change", async (event) => {
  state.experimentId = event.target.value;
  state.selectedRunIds = [];
  state.hasPrimedSelection = false;
  await loadRuns();
});

statusFilter.addEventListener("change", () => {
  state.statusFilter = statusFilter.value;
  applyFilters();
  render();
});

searchInput.addEventListener("input", () => {
  state.searchText = searchInput.value;
  applyFilters();
  render();
});

refreshButton.addEventListener("click", refreshAll);

clearSelectionButton.addEventListener("click", () => {
  state.selectedRunIds = [];
  render();
});

refreshAll();
setInterval(() => refreshAll({ silent: true }), REFRESH_INTERVAL_MS);
