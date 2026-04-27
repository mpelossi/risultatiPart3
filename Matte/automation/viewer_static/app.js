const state = {
  experiments: [],
  experimentId: null,
  runs: [],
  filteredRuns: [],
  bestRunId: null,
  selectedRunIds: [],
  previewRunIds: [],
  statusFilter: "all",
  searchText: "",
  historySortMode: "newest",
  policyNotices: new Map(),
  hasPrimedSelection: false,
  experimentsPayloadSignature: "",
  runsPayloadSignature: "",
};

const statusFilter = document.getElementById("status-filter");
const experimentSelect = document.getElementById("experiment-select");
const searchInput = document.getElementById("search-input");
const selectionCount = document.getElementById("selection-count");
const statsGrid = document.getElementById("stats-grid");
const bestRunCard = document.getElementById("best-run-card");
const compareGrid = document.getElementById("compare-grid");
const compareNote = document.getElementById("compare-note");
const runDetailSection = document.getElementById("run-detail-section");
const historyGrid = document.getElementById("history-grid");
const historyCount = document.getElementById("history-count");
const historySortButton = document.getElementById("history-sort-button");
const refreshButton = document.getElementById("refresh-button");
const clearSelectionButton = document.getElementById("clear-selection-button");
const statCardTemplate = document.getElementById("stat-card-template");
const REFRESH_INTERVAL_MS = 5000;
const MAX_SELECTED_RUNS = 2;
const TIMELINE_SEGMENT_TOP_PX = 10;
const TIMELINE_SEGMENT_ROW_HEIGHT_PX = 76;
const TIMELINE_SEGMENT_MIN_HEIGHT_PX = 45;
const TIMELINE_SEGMENT_MAX_HEIGHT_PX = 60;
const TIMELINE_TRACK_MIN_HEIGHT_PX = 94;
const TIMELINE_TRACK_BOTTOM_GAP_PX = 16;
const TIMELINE_TRACK_MEMCACHED_GAP_PX = 52;
const TIMELINE_MEMCACHED_HEIGHT_PX = 34;
const NODE_CORE_CAPACITY = {
  "node-a-8core": 8,
  "node-b-4core": 4,
};
const timelineScrollPositions = new Map();
const CLASSES = {
  emptyState: "empty-state rounded-2xl bg-white/60 p-5 text-[var(--muted)]",
  bestCard: "best-card best-highlight grid min-w-0 grid-cols-1 gap-5 rounded-[20px] border border-emerald-700/30 bg-white/75 p-5 shadow-[0_18px_40px_rgba(45,125,87,0.14)]",
  historyCard: "history-card flex min-w-0 flex-col rounded-[20px] border border-slate-900/10 bg-white/75 p-5 transition hover:-translate-y-0.5",
  compareCard: "compare-card flex min-w-0 flex-col gap-4 rounded-[20px] border border-slate-900/10 bg-white/75 p-5",
  bestHighlight: "best-highlight border-emerald-700/30 shadow-[0_18px_40px_rgba(45,125,87,0.14)]",
  historyBestHighlight: "history-best-highlight border-emerald-700/60 bg-emerald-50/55 shadow-[0_18px_36px_rgba(21,128,61,0.16)]",
  selectedHighlight: "selected-highlight border-emerald-700/70 bg-emerald-50/70 shadow-[0_0_0_5px_var(--selected-ring),0_22px_44px_rgba(21,128,61,0.18)]",
  primaryDetail: "primary-detail border-emerald-700/45 bg-emerald-50/45",
  runHead: "flex items-start justify-between gap-3 max-[720px]:flex-col max-[720px]:items-start",
  runTitle: "m-0 text-[1.2rem] font-bold leading-tight",
  runSubtitle: "mt-1.5 mb-0 text-[var(--muted)]",
  sectionNote: "mt-2.5 max-w-[720px] text-[var(--muted)] leading-6",
  metricsGrid: "metrics-grid my-4 grid grid-cols-3 gap-3 max-[720px]:grid-cols-1",
  metricsGridFlush: "metrics-grid grid grid-cols-3 gap-3 max-[720px]:grid-cols-1",
  metric: "rounded-[14px] border border-slate-900/5 bg-slate-100/75 px-3.5 py-3",
  metricLabel: "block text-[0.78rem] uppercase tracking-[0.08em] text-[var(--muted)]",
  metricValue: "mt-1.5 block text-[1.05rem] font-bold",
  badgeRow: "badge-row flex flex-wrap gap-2.5",
  badge: "badge inline-flex min-h-[30px] items-center rounded-full bg-slate-900/10 px-2.5 py-1.5 text-[0.82rem] text-[var(--page-ink)]",
  bestPill: "best-pill inline-flex min-h-[30px] items-center rounded-full bg-emerald-700 px-2.5 py-1.5 text-[0.82rem] font-bold uppercase tracking-[0.06em] text-white shadow-[0_8px_20px_rgba(21,128,61,0.2)]",
  issueRow: "issue-row flex flex-wrap gap-2.5",
  issue: "issue mt-2.5 rounded-[14px] bg-[rgba(196,77,43,0.08)] px-3 py-2.5 text-[var(--accent-deep)]",
  cardActions: "card-actions mt-4 flex flex-wrap items-center gap-3",
  cardActionsSeparated: "card-actions mt-auto flex flex-wrap items-center gap-3 border-t border-slate-900/10 pt-4",
  cardButton: "card-button inline-flex min-h-[42px] items-center justify-center rounded-full border px-4 py-2.5 text-sm font-bold shadow-sm transition hover:-translate-y-px focus-visible:outline focus-visible:outline-3 focus-visible:outline-offset-2 focus-visible:outline-[var(--selected-ring)]",
  primaryButton: "border-emerald-800/80 bg-emerald-700 text-white hover:bg-emerald-800",
  compareButton: "border-amber-500/45 bg-amber-100/85 text-amber-950 hover:bg-amber-200/90",
  neutralButton: "border-slate-900/15 bg-white/80 text-[var(--page-ink)] hover:bg-white",
  selectedButton: "selected border-emerald-800 bg-emerald-700 text-white shadow-[0_0_0_3px_var(--selected-ring)] hover:bg-emerald-800",
  policyNotice: "policy-notice mt-3 rounded-[14px] bg-slate-100/80 px-3 py-2.5 text-[var(--page-ink)]",
  policyNoticeError: "policy-notice mt-3 rounded-[14px] bg-[rgba(196,77,43,0.08)] px-3 py-2.5 text-[var(--accent-deep)]",
  finePrint: "mt-3 text-[0.84rem] text-[var(--muted)]",
};

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

const BENCHMARK_NODE_IDS = ["node-a-8core", "node-b-4core"];
const policyResultCache = new Map();

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

function clampNumber(value, min, max) {
  return Math.min(Math.max(value, min), max);
}

function laneCoreCapacity(laneId) {
  return NODE_CORE_CAPACITY[laneId] || null;
}

function segmentHeightPx(segment, laneId) {
  if (text(segment.kind, "job") === "memcached") {
    return TIMELINE_MEMCACHED_HEIGHT_PX;
  }

  const count = coreCount(segment);
  const capacity = laneCoreCapacity(laneId);
  if (count === null || capacity === null) {
    return Math.round((TIMELINE_SEGMENT_MIN_HEIGHT_PX + TIMELINE_SEGMENT_MAX_HEIGHT_PX) / 2);
  }

  const ratio = clampNumber(count / capacity, 0, 1);
  return Math.round(
    TIMELINE_SEGMENT_MIN_HEIGHT_PX
      + ratio * (TIMELINE_SEGMENT_MAX_HEIGHT_PX - TIMELINE_SEGMENT_MIN_HEIGHT_PX),
  );
}

function segmentTopPx(rowIndex, height) {
  const centeredOffset = Math.max(0, Math.round((TIMELINE_SEGMENT_MAX_HEIGHT_PX - height) / 2));
  return TIMELINE_SEGMENT_TOP_PX + rowIndex * TIMELINE_SEGMENT_ROW_HEIGHT_PX + centeredOffset;
}

function statusClass(status) {
  const normalized = text(status, "unknown").toLowerCase();
  return `status-${normalized.replace(/[^a-z0-9]+/g, "_")}`;
}

function cx(...values) {
  return values.filter(Boolean).join(" ");
}

function stableSignature(value) {
  return JSON.stringify(value);
}

function numericOrNull(value) {
  if (value === null || value === undefined || value === "") {
    return null;
  }
  const number = Number(value);
  return Number.isFinite(number) ? number : null;
}

function runTimestampMs(run) {
  const parsed = Date.parse(text(run.timestamp_iso, ""));
  return Number.isNaN(parsed) ? Number.NEGATIVE_INFINITY : parsed;
}

function compareRunsNewestFirst(left, right) {
  const leftTimestamp = runTimestampMs(left);
  const rightTimestamp = runTimestampMs(right);
  if (leftTimestamp !== rightTimestamp) {
    return rightTimestamp - leftTimestamp;
  }
  return text(right.run_id).localeCompare(text(left.run_id));
}

function compareRunsFastestFirst(left, right) {
  const leftMakespan = numericOrNull(left.makespan_s);
  const rightMakespan = numericOrNull(right.makespan_s);
  if (leftMakespan !== null && rightMakespan !== null) {
    const makespanDiff = leftMakespan - rightMakespan;
    if (makespanDiff !== 0) {
      return makespanDiff;
    }
    return compareRunsNewestFirst(left, right);
  }
  if (leftMakespan !== null) {
    return -1;
  }
  if (rightMakespan !== null) {
    return 1;
  }
  return compareRunsNewestFirst(left, right);
}

function sortedHistoryRuns(runs) {
  const sorted = [...runs];
  sorted.sort(state.historySortMode === "duration" ? compareRunsFastestFirst : compareRunsNewestFirst);
  return sorted;
}

function historySortDescription() {
  return state.historySortMode === "duration" ? "fastest first" : "newest first";
}

function policyCacheKey(runId) {
  return `${state.experimentId || ""}:${runId}`;
}

function policyEndpoint(runId) {
  return `/api/runs/${encodeURIComponent(runId)}/policy?experiment=${encodeURIComponent(state.experimentId || "")}`;
}

async function loadRunPolicy(run) {
  const key = policyCacheKey(run.run_id);
  if (policyResultCache.has(key)) {
    return policyResultCache.get(key);
  }
  const payload = await fetchJson(policyEndpoint(run.run_id));
  policyResultCache.set(key, payload);
  return payload;
}

function policyMatchSummary(payload) {
  if (payload.match_status === "matched") {
    const labels = (payload.matches || []).map((match) => text(match.label || match.schedule_id)).join(", ");
    return `already in schedules: ${labels || "matching file found"}`;
  }
  if (payload.match_status === "unmatched") {
    return "not found in schedules";
  }
  if (payload.match_status === "missing_policy") {
    return "policy.yaml is missing";
  }
  return "policy could not be checked";
}

function policyNotice(runId, message, options = {}) {
  state.policyNotices.set(policyCacheKey(runId), {
    message,
    error: Boolean(options.error),
  });
  render();
}

function errorMessage(error) {
  return error && error.message ? error.message : String(error);
}

function renderPolicyNotice(target, run) {
  const notice = state.policyNotices.get(policyCacheKey(run.run_id));
  if (!notice) {
    return;
  }
  const element = document.createElement("div");
  element.className = notice.error ? CLASSES.policyNoticeError : CLASSES.policyNotice;
  element.textContent = notice.message;
  target.appendChild(element);
}

function fallbackCopyText(value) {
  const textarea = document.createElement("textarea");
  textarea.value = value;
  textarea.setAttribute("readonly", "");
  textarea.style.position = "fixed";
  textarea.style.top = "-1000px";
  textarea.style.left = "-1000px";
  document.body.appendChild(textarea);
  textarea.select();
  try {
    if (!document.execCommand("copy")) {
      throw new Error("copy command was rejected");
    }
  } finally {
    document.body.removeChild(textarea);
  }
}

async function copyText(value) {
  if (navigator.clipboard && navigator.clipboard.writeText) {
    try {
      await navigator.clipboard.writeText(value);
      return;
    } catch (error) {
      // Fall through to the legacy path for browsers that block clipboard writes.
    }
  }
  fallbackCopyText(value);
}

async function copyRunPolicy(run) {
  policyNotice(run.run_id, "Copying policy.yaml...");
  try {
    const payload = await loadRunPolicy(run);
    if (!payload.policy_yaml) {
      throw new Error((payload.errors && payload.errors[0]) || "policy.yaml is unavailable");
    }
    await copyText(payload.policy_yaml);
    policyNotice(run.run_id, `Copied policy.yaml; ${policyMatchSummary(payload)}.`);
  } catch (error) {
    policyNotice(run.run_id, `Could not copy policy.yaml: ${errorMessage(error)}`, { error: true });
  }
}

async function checkRunPolicy(run) {
  policyNotice(run.run_id, "Checking schedules for this policy...");
  try {
    const payload = await loadRunPolicy(run);
    const errors = payload.errors && payload.errors.length ? ` (${payload.errors[0]})` : "";
    policyNotice(run.run_id, `Policy ${policyMatchSummary(payload)}.${errors}`, {
      error: payload.match_status === "parse_error" || payload.match_status === "missing_policy",
    });
  } catch (error) {
    policyNotice(run.run_id, `Could not check policy.yaml: ${errorMessage(error)}`, { error: true });
  }
}

function appendPolicyActions(actions, run) {
  const copyButton = document.createElement("button");
  copyButton.type = "button";
  copyButton.className = cardButtonClass({ variant: "neutral" });
  copyButton.textContent = "Copy Policy";
  copyButton.addEventListener("click", () => copyRunPolicy(run));
  actions.appendChild(copyButton);

  const checkButton = document.createElement("button");
  checkButton.type = "button";
  checkButton.className = cardButtonClass({ variant: "neutral" });
  checkButton.textContent = "Check Policy";
  checkButton.addEventListener("click", () => checkRunPolicy(run));
  actions.appendChild(checkButton);
}

function rememberTimelineScrollPositions() {
  document.querySelectorAll(".timeline-scroll[data-scroll-key]").forEach((scroller) => {
    timelineScrollPositions.set(scroller.dataset.scrollKey, scroller.scrollLeft);
  });
}

function restoreTimelineScrollPositions() {
  document.querySelectorAll(".timeline-scroll[data-scroll-key]").forEach((scroller) => {
    const savedScrollLeft = timelineScrollPositions.get(scroller.dataset.scrollKey);
    if (savedScrollLeft === undefined) {
      return;
    }
    const maxScrollLeft = Math.max(0, scroller.scrollWidth - scroller.clientWidth);
    scroller.scrollLeft = Math.min(savedScrollLeft, maxScrollLeft);
  });
}

function afterLayoutSettles(callback) {
  requestAnimationFrame(() => {
    requestAnimationFrame(callback);
  });
}

function rememberTimelineScroll(event) {
  const scroller = event.currentTarget;
  if (scroller.dataset.scrollKey) {
    timelineScrollPositions.set(scroller.dataset.scrollKey, scroller.scrollLeft);
  }
}

function scrollToRunDetail() {
  if (!runDetailSection) {
    return;
  }
  const top = runDetailSection.getBoundingClientRect().top + window.scrollY - 18;
  const reduceMotion = window.matchMedia("(prefers-reduced-motion: reduce)").matches;
  window.scrollTo({
    top: Math.max(0, top),
    behavior: reduceMotion ? "auto" : "smooth",
  });
}

function statusPillClass(status) {
  return cx(
    "status-pill inline-flex min-h-[30px] items-center rounded-full px-2.5 py-1.5 text-[0.82rem] font-bold uppercase tracking-[0.06em] text-white",
    statusClass(status),
  );
}

function createBestPill() {
  const pill = document.createElement("span");
  pill.className = CLASSES.bestPill;
  pill.textContent = "best";
  return pill;
}

function cardButtonClass(options = {}) {
  const variant = options.variant || (options.primary ? "primary" : "neutral");
  const variantClass = {
    primary: CLASSES.primaryButton,
    compare: CLASSES.compareButton,
    neutral: CLASSES.neutralButton,
  }[variant] || CLASSES.neutralButton;

  return cx(
    CLASSES.cardButton,
    variantClass,
    options.selected ? CLASSES.selectedButton : "",
  );
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

async function loadExperiments(options = {}) {
  const payload = await fetchJson("/api/experiments");
  const payloadSignature = stableSignature(payload);
  if (options.silent && payloadSignature === state.experimentsPayloadSignature) {
    return false;
  }

  state.experimentsPayloadSignature = payloadSignature;
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
  return true;
}

async function loadRuns(options = {}) {
  if (!state.experimentId) {
    state.runs = [];
    state.filteredRuns = [];
    render();
    return true;
  }
  const payload = await fetchJson(`/api/runs?experiment=${encodeURIComponent(state.experimentId)}`);
  const payloadSignature = stableSignature(payload);
  if (options.silent && payloadSignature === state.runsPayloadSignature) {
    return false;
  }

  state.runsPayloadSignature = payloadSignature;
  state.runs = payload.runs || [];
  state.bestRunId = payload.best_run_id || null;
  state.previewRunIds = state.previewRunIds.filter((runId) => state.runs.some((run) => run.run_id === runId));
  resetStatusFilterOptions(state.runs);
  applyFilters();
  primeSelection();
  render();
  return true;
}

function applyFilters() {
  const needle = state.searchText.trim().toLowerCase();
  state.filteredRuns = sortedHistoryRuns(state.runs.filter((run) => {
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
  }));
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
  render({ scrollToDetail: true });
}

function toggleComparisonRun(runId) {
  const selected = state.selectedRunIds.filter((selectedRunId) => state.runs.some((run) => run.run_id === selectedRunId));
  const index = selected.indexOf(runId);
  if (index === 0) {
    state.selectedRunIds = selected;
    render({ scrollToDetail: true });
    return;
  }
  if (index > 0) {
    selected.splice(index, 1);
    state.selectedRunIds = selected;
    render({ scrollToDetail: true });
    return;
  }

  if (!selected.length) {
    state.selectedRunIds = [runId];
  } else {
    state.selectedRunIds = [selected[0], runId].slice(0, MAX_SELECTED_RUNS);
  }
  render({ scrollToDetail: true });
}

function compareWithBest(runId) {
  const selected = [];
  if (state.bestRunId && state.bestRunId !== runId) {
    selected.push(state.bestRunId);
  }
  selected.push(runId);
  state.selectedRunIds = selected.slice(0, MAX_SELECTED_RUNS);
  render({ scrollToDetail: true });
}

function removeSelectedRun(runId) {
  state.selectedRunIds = state.selectedRunIds.filter((selectedRunId) => selectedRunId !== runId);
  render({ scrollToDetail: true });
}

function togglePreviewRun(runId) {
  if (state.previewRunIds.includes(runId)) {
    state.previewRunIds = state.previewRunIds.filter((previewRunId) => previewRunId !== runId);
  } else {
    state.previewRunIds = [...state.previewRunIds, runId];
  }
  render();
}

function render(options = {}) {
  const preserveTimelineScroll = options.preserveTimelineScroll !== false;
  if (preserveTimelineScroll) {
    rememberTimelineScrollPositions();
  }
  renderStats();
  renderBestRun();
  renderCompare();
  renderHistory();
  selectionCount.textContent = `${state.selectedRunIds.length} / ${MAX_SELECTED_RUNS}`;
  historyCount.textContent = `${state.filteredRuns.length} shown out of ${state.runs.length} · ${historySortDescription()}`;
  historySortButton.textContent = state.historySortMode === "duration" ? "Fastest First" : "Newest First";
  historySortButton.setAttribute("aria-pressed", state.historySortMode === "duration" ? "true" : "false");
  historySortButton.className = cardButtonClass({
    variant: state.historySortMode === "duration" ? "primary" : "neutral",
    selected: state.historySortMode === "duration",
  });
  afterLayoutSettles(() => {
    if (preserveTimelineScroll) {
      restoreTimelineScrollPositions();
    }
    if (options.scrollToDetail) {
      scrollToRunDetail();
    }
  });
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
  if (run.artifact_flags && run.artifact_flags.node_platforms) {
    badges.push("node platforms");
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
  badgeRow.className = CLASSES.badgeRow;
  createRunBadges(run).forEach((badgeText) => {
    const badge = document.createElement("span");
    badge.className = CLASSES.badge;
    badge.textContent = badgeText;
    badgeRow.appendChild(badge);
  });
  target.appendChild(badgeRow);
}

function renderBestRun() {
  const run = state.runs.find((item) => item.run_id === state.bestRunId);
  bestRunCard.innerHTML = "";

  if (!run) {
    bestRunCard.className = CLASSES.emptyState;
    bestRunCard.textContent = "No run currently qualifies as best.";
    return;
  }

  bestRunCard.className = cx(
    CLASSES.bestCard,
    state.selectedRunIds.includes(run.run_id) ? CLASSES.selectedHighlight : "",
  );
  bestRunCard.appendChild(createRunSummaryBlock(run, {
    compact: false,
    includeTimeline: true,
    scrollKey: `best:${run.run_id}`,
  }));
  bestRunCard.appendChild(createRunMetaBlock(run));
}

function createRunSummaryBlock(run, options) {
  const wrapper = document.createElement("div");

  const head = document.createElement("div");
  head.className = CLASSES.runHead;

  const titleGroup = document.createElement("div");
  const title = document.createElement("h3");
  title.className = CLASSES.runTitle;
  title.textContent = text(run.run_label, run.run_id);
  titleGroup.appendChild(title);

  const subtitle = document.createElement("p");
  subtitle.className = CLASSES.runSubtitle;
  subtitle.textContent = `${text(run.run_id)} | ${text(run.policy_name)} | ${text(run.timestamp_iso, "timestamp unavailable")}`;
  titleGroup.appendChild(subtitle);
  head.appendChild(titleGroup);

  const status = document.createElement("span");
  status.className = statusPillClass(run.overall_status);
  status.textContent = text(run.overall_status, "unknown");
  head.appendChild(status);
  wrapper.appendChild(head);

  const metrics = document.createElement("div");
  metrics.className = CLASSES.metricsGrid;
  [
    { label: "Makespan", value: fmtSeconds(run.makespan_s) },
    { label: "P95", value: fmtP95(run.max_observed_p95_us) },
    { label: "Measurement", value: text(run.measurement_status, "missing") },
  ].forEach((item) => {
    const metric = document.createElement("div");
    metric.className = CLASSES.metric;
    const label = document.createElement("span");
    label.className = CLASSES.metricLabel;
    label.textContent = item.label;
    const value = document.createElement("span");
    value.className = CLASSES.metricValue;
    value.textContent = item.value;
    metric.appendChild(label);
    metric.appendChild(value);
    metrics.appendChild(metric);
  });
  wrapper.appendChild(metrics);

  renderBadges(wrapper, run);

  if (run.issues && run.issues.length) {
    const issue = document.createElement("div");
    issue.className = CLASSES.issue;
    issue.textContent = run.issues[0];
    wrapper.appendChild(issue);
  }

  const actions = document.createElement("div");
  actions.className = CLASSES.cardActions;

  const compareButton = document.createElement("button");
  compareButton.type = "button";
  compareButton.className = cardButtonClass({ variant: "primary", selected: state.selectedRunIds[0] === run.run_id });
  compareButton.textContent = state.selectedRunIds[0] === run.run_id ? "Viewing Details" : "View Details";
  compareButton.addEventListener("click", () => focusRun(run.run_id));
  actions.appendChild(compareButton);

  if (state.selectedRunIds[0] !== run.run_id) {
    const duoButton = document.createElement("button");
    duoButton.type = "button";
    duoButton.className = cardButtonClass({ variant: "compare", selected: state.selectedRunIds.includes(run.run_id) });
    duoButton.textContent = state.selectedRunIds.includes(run.run_id) ? "Remove Compare" : "Compare";
    duoButton.addEventListener("click", () => toggleComparisonRun(run.run_id));
    actions.appendChild(duoButton);
  }
  appendPolicyActions(actions, run);

  wrapper.appendChild(actions);
  renderPolicyNotice(wrapper, run);

  if (options.includeTimeline) {
    wrapper.appendChild(createTimelineCard(run, options.scaleMax, { scrollKey: options.scrollKey }));
  }

  return wrapper;
}

function createRunMetaBlock(run) {
  const wrapper = document.createElement("div");
  const title = document.createElement("p");
  title.className = CLASSES.sectionNote;
  title.textContent = "Quick notes";
  wrapper.appendChild(title);

  const metrics = document.createElement("div");
  metrics.className = CLASSES.metricsGrid;
  [
    { label: "Completed jobs", value: `${text(run.completed_job_count, 0)} / ${text(run.expected_job_count, 0)}` },
    { label: "Samples", value: text(run.sample_count, "0") },
    { label: "Best eligible", value: run.eligible_for_best ? "yes" : "no" },
  ].forEach((item) => {
    const metric = document.createElement("div");
    metric.className = CLASSES.metric;
    const label = document.createElement("span");
    label.className = CLASSES.metricLabel;
    label.textContent = item.label;
    const value = document.createElement("span");
    value.className = CLASSES.metricValue;
    value.textContent = item.value;
    metric.appendChild(label);
    metric.appendChild(value);
    metrics.appendChild(metric);
  });
  wrapper.appendChild(metrics);

  const nodePlatformBlock = createNodePlatformBlock(run);
  if (nodePlatformBlock) {
    wrapper.appendChild(nodePlatformBlock);
  }

  if (run.issues && run.issues.length) {
    const issueRow = document.createElement("div");
    issueRow.className = CLASSES.issueRow;
    run.issues.slice(0, 4).forEach((issueText) => {
      const issue = document.createElement("div");
      issue.className = CLASSES.issue;
      issue.textContent = issueText;
      issueRow.appendChild(issue);
    });
    wrapper.appendChild(issueRow);
  }

  const finePrint = document.createElement("p");
  finePrint.className = CLASSES.finePrint;
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
  compareGrid.className = "grid gap-6";

  if (!runs.length) {
    compareNote.textContent = "Pick one run to inspect; add a second run to compare.";
    const empty = document.createElement("div");
    empty.className = CLASSES.emptyState;
    empty.textContent = "No run selected yet.";
    compareGrid.appendChild(empty);
    return;
  }

  const scaleMax = runs.reduce((max, run) => {
    const runMax = run.timeline && run.timeline.max_end_s ? Number(run.timeline.max_end_s) : 0;
    return Math.max(max, runMax);
  }, 0);

  compareGrid.className = runs.length === 1
    ? "grid grid-cols-1 gap-6"
    : "grid grid-cols-2 gap-6 max-[1080px]:grid-cols-1";
  compareNote.textContent = runs.length === 1
    ? "Single-run detail view. Use Compare on another run to place it beside this one."
    : `Shared axis uses ${fmtSeconds(scaleMax)} as the comparison ceiling.`;
  runs.forEach((run, index) => {
    const card = document.createElement("article");
    card.className = cx(
      CLASSES.compareCard,
      index === 0 ? CLASSES.primaryDetail : "",
      run.run_id === state.bestRunId ? CLASSES.bestHighlight : "",
      state.selectedRunIds.includes(run.run_id) ? CLASSES.selectedHighlight : "",
    );

    const head = document.createElement("div");
    head.className = CLASSES.runHead;
    head.innerHTML = `
      <div>
        <h3 class="${CLASSES.runTitle}">${text(run.run_label, run.run_id)}</h3>
        <p class="${CLASSES.runSubtitle}">${text(run.policy_name)} | ${text(run.run_id)}</p>
      </div>
      <span class="${statusPillClass(run.overall_status)}">${text(run.overall_status, "unknown")}</span>
    `;
    card.appendChild(head);

    card.appendChild(createMetricsGrid([
      { label: "Makespan", value: fmtSeconds(run.makespan_s) },
      { label: "P95", value: fmtP95(run.max_observed_p95_us) },
      { label: "Measurement", value: text(run.measurement_status, "missing") },
    ]));
    renderBadges(card, run);
    const nodePlatformBlock = createNodePlatformBlock(run);
    if (nodePlatformBlock) {
      card.appendChild(nodePlatformBlock);
    }
    card.appendChild(createTimelineCard(run, scaleMax, {
      includeDetails: runs.length === 1,
      scrollKey: `compare:${run.run_id}`,
    }));

    const actions = document.createElement("div");
    actions.className = CLASSES.cardActionsSeparated;
    const focusButton = document.createElement("button");
    focusButton.type = "button";
    focusButton.className = cardButtonClass({ variant: "primary", selected: index === 0 });
    focusButton.textContent = index === 0 ? "Viewing Details" : "View Details";
    focusButton.addEventListener("click", () => focusRun(run.run_id));
    actions.appendChild(focusButton);

    const removeButton = document.createElement("button");
    removeButton.type = "button";
    removeButton.className = cardButtonClass();
    removeButton.textContent = runs.length === 1 ? "Clear" : "Remove";
    removeButton.addEventListener("click", () => removeSelectedRun(run.run_id));
    if (runs.length > 1 || index > 0) {
      actions.appendChild(removeButton);
    }
    appendPolicyActions(actions, run);
    card.appendChild(actions);
    renderPolicyNotice(card, run);

    compareGrid.appendChild(card);
  });
}

function createMetricsGrid(items) {
  const metrics = document.createElement("div");
  metrics.className = CLASSES.metricsGridFlush;
  items.forEach((item) => {
    const metric = document.createElement("div");
    metric.className = CLASSES.metric;
    const label = document.createElement("span");
    label.className = CLASSES.metricLabel;
    label.textContent = item.label;
    const value = document.createElement("span");
    value.className = CLASSES.metricValue;
    value.textContent = item.value;
    metric.appendChild(label);
    metric.appendChild(value);
    metrics.appendChild(metric);
  });
  return metrics;
}

function nodePlatformEntries(run) {
  const payload = run.node_platforms || {};
  const nodes = payload.nodes && typeof payload.nodes === "object" ? payload.nodes : {};
  return BENCHMARK_NODE_IDS
    .map((nodeId) => ({ nodeId, data: nodes[nodeId] }))
    .filter((entry) => entry.data && typeof entry.data === "object");
}

function createNodePlatformBlock(run) {
  const entries = nodePlatformEntries(run);
  if (!entries.length) {
    return null;
  }

  const wrapper = document.createElement("div");
  const title = document.createElement("p");
  title.className = CLASSES.sectionNote;
  title.textContent = "Benchmark nodes";
  wrapper.appendChild(title);

  const metrics = document.createElement("div");
  metrics.className = CLASSES.metricsGridFlush;
  entries.forEach(({ nodeId, data }) => {
    const metric = document.createElement("div");
    metric.className = CLASSES.metric;

    const label = document.createElement("span");
    label.className = CLASSES.metricLabel;
    label.textContent = nodeId;
    metric.appendChild(label);

    const value = document.createElement("span");
    value.className = CLASSES.metricValue;
    value.textContent = data.capture_status === "error"
      ? "capture error"
      : text(data.cpu_platform, "CPU platform n/a");
    metric.appendChild(value);

    const detail = document.createElement("span");
    detail.className = CLASSES.metricLabel;
    detail.textContent = data.capture_status === "error"
      ? text(data.error, "metadata unavailable")
      : `${text(data.machine_type, "machine n/a")} | ${text(data.node_name, "node n/a")}`;
    metric.appendChild(detail);

    metrics.appendChild(metric);
  });
  wrapper.appendChild(metrics);
  return wrapper;
}

function runHasTimelineData(run) {
  return Boolean(run.timeline && run.timeline.has_data);
}

function createTimelineCard(run, explicitScaleMax, options = {}) {
  if (window.CcaTimeline && window.CcaTimeline.createTimelineCard) {
    return window.CcaTimeline.createTimelineCard(run, explicitScaleMax, {
      ...options,
      emptyClassName: CLASSES.emptyState,
      emptyText: "This run does not have enough timing data to render a chart.",
    });
  }

  const includeDetails = options.includeDetails !== false;
  const container = document.createElement("div");
  container.className = "timeline-card";

  const timeline = run.timeline || {};
  if (!timeline.has_data) {
    const empty = document.createElement("div");
    empty.className = CLASSES.emptyState;
    empty.textContent = "This run does not have enough timing data to render a chart.";
    container.appendChild(empty);
    return container;
  }

  const scaleMax = Math.max(Number(explicitScaleMax || 0), Number(timeline.max_end_s || 0), 1);
  const scroller = document.createElement("div");
  scroller.className = "timeline-scroll";
  scroller.dataset.scrollKey = options.scrollKey || `timeline:${run.run_id}`;
  scroller.addEventListener("scroll", rememberTimelineScroll, { passive: true });
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
    track.style.minHeight = "64px";
    const empty = document.createElement("div");
    empty.className = "lane-empty";
    empty.textContent = "No timed work recorded.";
    track.appendChild(empty);
  } else {
    const layout = layoutLaneSegments(segments);
    const trackHeight = Math.max(
      TIMELINE_TRACK_MIN_HEIGHT_PX,
      layout.rowCount * TIMELINE_SEGMENT_ROW_HEIGHT_PX
        + (layout.memcached.length ? TIMELINE_TRACK_MEMCACHED_GAP_PX : TIMELINE_TRACK_BOTTOM_GAP_PX),
    );
    track.style.minHeight = `${trackHeight}px`;
    layout.jobs.forEach((item) => {
      track.appendChild(createSegment(item.segment, scaleMax, {
        laneId: lane.lane_id,
        rowIndex: item.rowIndex,
      }));
    });
    layout.memcached.forEach((segment) => {
      track.appendChild(createSegment(segment, scaleMax, { laneId: lane.lane_id }));
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

  const knownCoreKeys = Array.from(new Set(jobs.map(coreTrackKey).filter(Boolean))).sort(compareCoreTrackKeys);
  const coreRows = new Map(knownCoreKeys.map((key, index) => [key, index]));
  const rowEnds = knownCoreKeys.map(() => Number.NEGATIVE_INFINITY);
  const overflowRows = new Map();
  const laidOutJobs = [];
  jobs.filter((segment) => coreTrackKey(segment)).forEach((segment) => {
    const rowIndex = placeKnownCoreSegment(segment, coreRows, overflowRows, rowEnds);
    laidOutJobs.push({ segment, rowIndex });
  });

  const unknownRowOffset = rowEnds.length;
  const unknownRowEnds = [];
  jobs.filter((segment) => !coreTrackKey(segment)).forEach((segment) => {
    const start = Number(segment.start_s || 0);
    const end = Number(segment.end_s || start);
    let rowIndex = unknownRowEnds.findIndex((rowEnd) => start >= rowEnd);
    if (rowIndex === -1) {
      rowIndex = unknownRowEnds.length;
      unknownRowEnds.push(end);
    } else {
      unknownRowEnds[rowIndex] = end;
    }
    laidOutJobs.push({ segment, rowIndex: unknownRowOffset + rowIndex });
  });

  return {
    jobs: laidOutJobs,
    memcached,
    rowCount: Math.max(rowEnds.length + unknownRowEnds.length, 1),
  };
}

function coreTrackKey(segment) {
  if (Array.isArray(segment.core_ids) && segment.core_ids.length) {
    return segment.core_ids.map((coreId) => Number(coreId)).sort((left, right) => left - right).join(",");
  }
  const cores = text(segment.cores, "");
  return cores && cores !== "n/a" ? cores : null;
}

function compareCoreTrackKeys(left, right) {
  const leftBounds = coreTrackBounds(left);
  const rightBounds = coreTrackBounds(right);
  if (leftBounds.min !== rightBounds.min) {
    return leftBounds.min - rightBounds.min;
  }
  if (leftBounds.max !== rightBounds.max) {
    return leftBounds.max - rightBounds.max;
  }
  return left.localeCompare(right);
}

function coreTrackBounds(key) {
  const values = String(key).match(/\d+/g) || [];
  const numbers = values.map((value) => Number(value)).filter((value) => Number.isFinite(value));
  if (!numbers.length) {
    return { min: Number.MAX_SAFE_INTEGER, max: Number.MAX_SAFE_INTEGER };
  }
  return {
    min: Math.min(...numbers),
    max: Math.max(...numbers),
  };
}

function placeKnownCoreSegment(segment, coreRows, overflowRows, rowEnds) {
  const key = coreTrackKey(segment);
  const start = Number(segment.start_s || 0);
  const end = Number(segment.end_s || start);
  const preferredRow = coreRows.get(key);
  if (preferredRow !== undefined && start >= rowEnds[preferredRow]) {
    rowEnds[preferredRow] = end;
    return preferredRow;
  }

  const rows = overflowRows.get(key) || [];
  let rowIndex = rows.find((candidateRow) => start >= rowEnds[candidateRow]);
  if (rowIndex === undefined) {
    rowIndex = rowEnds.length;
    rowEnds.push(Number.NEGATIVE_INFINITY);
    rows.push(rowIndex);
    overflowRows.set(key, rows);
  }
  rowEnds[rowIndex] = end;
  return rowIndex;
}

function createSegment(segment, scaleMax, options = {}) {
  const bar = document.createElement("div");
  const kind = text(segment.kind, "job");
  const jobId = text(segment.job_id, "unknown");
  const barHeight = segmentHeightPx(segment, options.laneId);
  const rawWidth = (Number(segment.duration_s || 0) / scaleMax) * 100;
  const width = Math.max(rawWidth, kind === "memcached" ? 0 : 12);
  const left = (Number(segment.start_s || 0) / scaleMax) * 100;
  const boundedWidth = Math.max(0, Math.min(width, 100 - left));
  const isTight = boundedWidth < 18 && kind !== "memcached";
  const isShort = barHeight < 36 && kind !== "memcached";
  bar.className = [
    "segment",
    kind === "memcached" ? "memcached" : "",
    isTight ? "segment-tight" : "",
    isShort ? "segment-short" : "",
    `job-${jobId}`,
  ].filter(Boolean).join(" ");
  bar.style.left = `${left}%`;
  bar.style.width = `${boundedWidth}%`;
  bar.style.height = `${barHeight}px`;
  if (options.rowIndex !== undefined) {
    bar.style.setProperty("--segment-row", String(options.rowIndex));
    bar.style.setProperty(
      "--segment-top",
      `${segmentTopPx(options.rowIndex, barHeight)}px`,
    );
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
    cores.textContent = isTight
      ? `${fmtSecondsCompact(segment.duration_s)} | ${compactResourceLabel(segment)} | ${compactCoreLabel(segment.cores)}`
      : `${compactResourceLabel(segment)} | ${compactCoreLabel(segment.cores)}`;
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
    const empty = document.createElement("div");
    empty.className = CLASSES.emptyState;
    empty.textContent = "No runs match the current filters.";
    historyGrid.appendChild(empty);
    return;
  }

  state.filteredRuns.forEach((run) => {
    const card = document.createElement("article");
    card.className = cx(
      CLASSES.historyCard,
      run.run_id === state.bestRunId ? CLASSES.historyBestHighlight : "",
      state.selectedRunIds.includes(run.run_id) ? CLASSES.selectedHighlight : "",
    );

    const head = document.createElement("div");
    head.className = CLASSES.runHead;
    const titleGroup = document.createElement("div");
    const title = document.createElement("h3");
    title.className = CLASSES.runTitle;
    title.textContent = text(run.run_label, run.run_id);
    titleGroup.appendChild(title);

    const subtitle = document.createElement("p");
    subtitle.className = CLASSES.runSubtitle;
    subtitle.textContent = `${text(run.policy_name)} | ${text(run.run_id)}`;
    titleGroup.appendChild(subtitle);
    head.appendChild(titleGroup);

    const pillGroup = document.createElement("div");
    pillGroup.className = "run-pill-group flex flex-wrap justify-end gap-2 max-[720px]:justify-start";
    const status = document.createElement("span");
    status.className = statusPillClass(run.overall_status);
    status.textContent = text(run.overall_status, "unknown");
    pillGroup.appendChild(status);
    if (run.run_id === state.bestRunId) {
      pillGroup.appendChild(createBestPill());
    }
    head.appendChild(pillGroup);
    card.appendChild(head);

    const metrics = document.createElement("div");
    metrics.className = CLASSES.metricsGrid;
    [
      { label: "Makespan", value: fmtSeconds(run.makespan_s) },
      { label: "P95", value: fmtP95(run.max_observed_p95_us) },
      { label: "Measurement", value: text(run.measurement_status, "missing") },
    ].forEach((item) => {
      const metric = document.createElement("div");
      metric.className = CLASSES.metric;
      metric.innerHTML = `<span class="${CLASSES.metricLabel}">${item.label}</span><span class="${CLASSES.metricValue}">${item.value}</span>`;
      metrics.appendChild(metric);
    });
    card.appendChild(metrics);

    renderBadges(card, run);

    if (run.issues && run.issues.length) {
      const issue = document.createElement("div");
      issue.className = CLASSES.issue;
      issue.textContent = run.issues[0];
      card.appendChild(issue);
    }

    const actions = document.createElement("div");
    actions.className = CLASSES.cardActionsSeparated;

    const compareButton = document.createElement("button");
    compareButton.type = "button";
    compareButton.className = cardButtonClass({ variant: "primary", selected: state.selectedRunIds[0] === run.run_id });
    compareButton.textContent = state.selectedRunIds[0] === run.run_id ? "Viewing" : "View Details";
    compareButton.addEventListener("click", () => focusRun(run.run_id));
    actions.appendChild(compareButton);

    if (state.selectedRunIds[0] !== run.run_id) {
      const comparisonButton = document.createElement("button");
      comparisonButton.type = "button";
      comparisonButton.className = cardButtonClass({ variant: "compare", selected: state.selectedRunIds.includes(run.run_id) });
      comparisonButton.textContent = state.selectedRunIds.includes(run.run_id) ? "Remove Compare" : "Compare";
      comparisonButton.addEventListener("click", () => toggleComparisonRun(run.run_id));
      actions.appendChild(comparisonButton);
    }

    if (run.run_id !== state.bestRunId && state.bestRunId) {
      const bestButton = document.createElement("button");
      bestButton.type = "button";
      bestButton.className = cardButtonClass({ variant: "compare" });
      bestButton.textContent = "With Best";
      bestButton.addEventListener("click", () => compareWithBest(run.run_id));
      actions.appendChild(bestButton);
    }
    appendPolicyActions(actions, run);

    card.appendChild(actions);
    renderPolicyNotice(card, run);

    if (runHasTimelineData(run)) {
      const previewing = state.previewRunIds.includes(run.run_id);
      const previewButton = document.createElement("button");
      previewButton.type = "button";
      previewButton.className = cardButtonClass({ variant: "neutral", selected: previewing });
      previewButton.textContent = previewing ? "Hide Preview" : "Preview";
      previewButton.addEventListener("click", () => togglePreviewRun(run.run_id));
      actions.appendChild(previewButton);

      if (previewing) {
        const preview = document.createElement("div");
        preview.className = "history-preview";
        preview.appendChild(createTimelineCard(run, run.timeline.max_end_s, {
          includeDetails: false,
          scrollKey: `preview:${run.run_id}`,
        }));
        card.appendChild(preview);
      }
    }

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
    await loadExperiments({ silent });
    await loadRuns({ silent });
  } catch (error) {
    bestRunCard.className = CLASSES.emptyState;
    bestRunCard.textContent = error.message;
    compareGrid.innerHTML = "";
    historyGrid.innerHTML = "";
    [compareGrid, historyGrid].forEach((target) => {
      const empty = document.createElement("div");
      empty.className = CLASSES.emptyState;
      empty.textContent = "Failed to load run data.";
      target.appendChild(empty);
    });
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
  state.previewRunIds = [];
  state.policyNotices.clear();
  policyResultCache.clear();
  state.hasPrimedSelection = false;
  state.runsPayloadSignature = "";
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

historySortButton.addEventListener("click", () => {
  if (state.historySortMode === "duration") {
    state.historySortMode = "newest";
  } else {
    state.historySortMode = "duration";
    state.statusFilter = "all";
    statusFilter.value = "all";
  }
  applyFilters();
  render();
});

clearSelectionButton.addEventListener("click", () => {
  state.selectedRunIds = [];
  render({ scrollToDetail: true });
});

refreshAll();
setInterval(() => refreshAll({ silent: true }), REFRESH_INTERVAL_MS);
