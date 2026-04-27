const state = {
  schedules: [],
  queue: null,
  catalog: null,
  selectedScheduleId: null,
  editor: null,
  prediction: null,
  yaml: "",
  metricsSource: "",
  dirty: false,
  metadataSignature: "",
  previewTimer: null,
};

const REFRESH_INTERVAL_MS = 5000;
const PREVIEW_DEBOUNCE_MS = 220;
const NODE_IDS = ["node-a-8core", "node-b-4core"];

const scheduleSelect = document.getElementById("schedule-select");
const queueList = document.getElementById("queue-list");
const statsGrid = document.getElementById("stats-grid");
const editorPanel = document.getElementById("editor-panel");
const timelinePanel = document.getElementById("timeline-panel");
const issuesPanel = document.getElementById("issues-panel");
const yamlOutput = document.getElementById("yaml-output");
const refreshButton = document.getElementById("refresh-button");
const copyButton = document.getElementById("copy-button");
const copyStatus = document.getElementById("copy-status");
const dirtyNote = document.getElementById("dirty-note");
const statCardTemplate = document.getElementById("stat-card-template");

const CLASSES = {
  emptyState: "empty-state rounded-2xl bg-white/60 p-5 text-[var(--muted)]",
  metric: "rounded-[14px] border border-slate-900/5 bg-slate-100/75 px-3.5 py-3",
  metricLabel: "block text-[0.78rem] uppercase tracking-[0.08em] text-[var(--muted)]",
  metricValue: "mt-1.5 block text-[1.05rem] font-bold",
  input: "min-h-[42px] w-full rounded-xl border border-slate-900/15 bg-white/85 px-3 py-2 text-[var(--page-ink)]",
  smallInput: "min-h-[42px] w-full rounded-xl border border-slate-900/15 bg-white/85 px-2.5 py-2 text-[var(--page-ink)]",
  issue: "rounded-[14px] bg-[rgba(196,77,43,0.08)] px-3 py-2.5 text-[var(--accent-deep)]",
  warning: "rounded-[14px] bg-amber-100/80 px-3 py-2.5 text-amber-950",
  badge: "inline-flex min-h-[30px] items-center rounded-full bg-slate-900/10 px-2.5 py-1.5 text-[0.82rem] text-[var(--page-ink)]",
};

function text(value, fallback = "n/a") {
  if (value === null || value === undefined || value === "") {
    return fallback;
  }
  return String(value);
}

function fmtSeconds(value) {
  if (value === null || value === undefined || Number.isNaN(Number(value))) {
    return "n/a";
  }
  return `${Number(value).toFixed(1)} s`;
}

function statusClass(status) {
  const normalized = text(status, "unknown").toLowerCase();
  return `status-${normalized.replace(/[^a-z0-9]+/g, "_")}`;
}

function stableSignature(value) {
  return JSON.stringify(value);
}

function afterLayoutSettles(callback) {
  requestAnimationFrame(() => {
    requestAnimationFrame(callback);
  });
}

async function fetchJson(url, options = {}) {
  const response = await fetch(url, {
    cache: "no-store",
    headers: options.body ? { "Content-Type": "application/json" } : undefined,
    ...options,
  });
  if (!response.ok) {
    const payload = await response.json().catch(() => ({}));
    throw new Error(payload.error || `Request failed: ${response.status}`);
  }
  return response.json();
}

function nodeCatalog(nodeId) {
  return (state.catalog?.nodes || []).find((node) => node.node_id === nodeId) || null;
}

function jobCatalog(jobId) {
  return (state.catalog?.jobs || []).find((job) => job.job_id === jobId) || null;
}

function coreSuggestionsFor(nodeId, jobId = null) {
  const job = jobId ? jobCatalog(jobId) : null;
  if (job?.core_suggestions?.[nodeId]) {
    return job.core_suggestions[nodeId];
  }
  return nodeCatalog(nodeId)?.core_suggestions || [];
}

async function loadScheduleList(options = {}) {
  const payload = await fetchJson("/api/schedules");
  const signature = stableSignature(payload);
  const changed = signature !== state.metadataSignature;
  if (options.silent && !changed) {
    return;
  }

  state.metadataSignature = signature;
  state.schedules = payload.schedules || [];
  state.queue = payload.queue || null;
  state.catalog = payload.catalog || null;
  state.metricsSource = payload.metrics_source || "";

  renderScheduleSelect();
  renderQueue();

  if (!state.selectedScheduleId) {
    state.selectedScheduleId = payload.default_schedule_id || (state.schedules[0] && state.schedules[0].schedule_id) || null;
  }
  if (state.selectedScheduleId) {
    scheduleSelect.value = state.selectedScheduleId;
  }
  if (state.selectedScheduleId && (!options.silent || (changed && !state.dirty && options.reloadActive))) {
    await loadSchedule(state.selectedScheduleId);
  }
}

async function loadSchedule(scheduleId) {
  if (!scheduleId) {
    return;
  }
  const payload = await fetchJson(`/api/schedules/${encodeURIComponent(scheduleId)}`);
  state.selectedScheduleId = payload.schedule_id;
  state.editor = payload.editor;
  state.prediction = payload.prediction;
  state.yaml = payload.yaml || "";
  state.metricsSource = payload.metrics_source || state.metricsSource;
  state.catalog = payload.catalog || state.catalog;
  state.dirty = false;
  renderAll({ renderEditorPanel: true });
}

function renderAll(options = {}) {
  if (options.renderEditorPanel) {
    renderEditor();
  }
  renderStats();
  renderTimeline();
  renderIssues();
  renderYaml();
  renderDirtyNote();
}

function renderScheduleSelect() {
  const previousValue = scheduleSelect.value || state.selectedScheduleId;
  scheduleSelect.innerHTML = "";
  state.schedules.forEach((schedule) => {
    const option = document.createElement("option");
    option.value = schedule.schedule_id;
    const queueText = schedule.in_queue ? ` | queue x${schedule.queued_runs}` : "";
    option.textContent = `${schedule.label}${schedule.policy_name ? ` | ${schedule.policy_name}` : ""}${queueText}`;
    scheduleSelect.appendChild(option);
  });
  if (previousValue && state.schedules.some((schedule) => schedule.schedule_id === previousValue)) {
    scheduleSelect.value = previousValue;
  }
}

function renderQueue() {
  queueList.innerHTML = "";
  if (state.queue?.error) {
    const error = document.createElement("span");
    error.className = CLASSES.issue;
    error.textContent = state.queue.error;
    queueList.appendChild(error);
    return;
  }
  const entries = state.queue?.entries || [];
  if (!entries.length) {
    queueList.textContent = "No queue entries found.";
    return;
  }
  entries.forEach((entry) => {
    const button = document.createElement("button");
    button.type = "button";
    button.className = `${CLASSES.badge} border border-transparent transition hover:-translate-y-px hover:bg-white`;
    button.textContent = `${entry.label} x${entry.runs}`;
    button.addEventListener("click", () => {
      if (state.selectedScheduleId !== entry.schedule_id) {
        loadSchedule(entry.schedule_id).catch(showError);
      }
    });
    queueList.appendChild(button);
  });
}

function renderStats() {
  statsGrid.innerHTML = "";
  const errors = state.prediction?.errors || [];
  const warnings = state.prediction?.warnings || [];
  const activeSchedule = state.schedules.find((schedule) => schedule.schedule_id === state.selectedScheduleId);
  [
    {
      label: "Status",
      value: state.prediction?.status || "n/a",
      detail: `${errors.length} errors | ${warnings.length} warnings`,
    },
    {
      label: "Estimated Makespan",
      value: fmtSeconds(state.prediction?.makespan_s),
      detail: state.editor?.policy_name || "No schedule selected",
    },
    {
      label: "Schedule File",
      value: activeSchedule?.label || "n/a",
      detail: activeSchedule?.in_queue ? `Queued for ${activeSchedule.queued_runs} run(s)` : "Not in queue",
    },
    {
      label: "Metric Source",
      value: state.metricsSource ? state.metricsSource.split("/").pop() : "n/a",
      detail: "Part 2 runtime estimates",
    },
  ].forEach((item) => {
    const fragment = statCardTemplate.content.cloneNode(true);
    fragment.querySelector(".stat-label").textContent = item.label;
    fragment.querySelector(".stat-value").textContent = item.value;
    fragment.querySelector(".stat-detail").textContent = item.detail;
    statsGrid.appendChild(fragment);
  });
}

function renderEditor() {
  editorPanel.innerHTML = "";
  if (!state.editor) {
    const empty = document.createElement("div");
    empty.className = CLASSES.emptyState;
    empty.textContent = "No schedule selected.";
    editorPanel.appendChild(empty);
    return;
  }

  editorPanel.appendChild(createGeneralEditor());
  editorPanel.appendChild(createJobEditor());
}

function createGeneralEditor() {
  const wrapper = document.createElement("div");
  wrapper.className = "grid grid-cols-[minmax(220px,1fr)_repeat(3,minmax(150px,0.7fr))] gap-4 max-[1080px]:grid-cols-2 max-[720px]:grid-cols-1";
  wrapper.appendChild(createTextField("Policy name", state.editor.policy_name, (value) => {
    state.editor.policy_name = value;
    markDirtyAndPreview();
  }));
  wrapper.appendChild(createNodeSelect("Memcached node", state.editor.memcached.node, (value) => {
    state.editor.memcached.node = value;
    if (!coreSuggestionsFor(value).includes(state.editor.memcached.cores)) {
      state.editor.memcached.cores = coreSuggestionsFor(value)[0] || state.editor.memcached.cores;
      renderEditor();
    }
    markDirtyAndPreview();
  }));
  wrapper.appendChild(createCoreField("Memcached cores", "memcached-cores", state.editor.memcached.node, null, state.editor.memcached.cores, (value) => {
    state.editor.memcached.cores = value;
    markDirtyAndPreview();
  }));
  wrapper.appendChild(createNumberField("Memcached threads", state.editor.memcached.threads, 1, 8, (value) => {
    state.editor.memcached.threads = value;
    markDirtyAndPreview();
  }));
  return wrapper;
}

function createJobEditor() {
  const wrapper = document.createElement("div");
  wrapper.className = "schedule-editor-table overflow-x-auto rounded-2xl border border-slate-900/10 bg-white/70";
  const table = document.createElement("table");
  table.className = "min-w-[1120px] w-full border-collapse text-sm";
  table.innerHTML = `
    <thead>
      <tr>
        <th>Job</th>
        <th>Order</th>
        <th>Node</th>
        <th>Cores</th>
        <th>Threads</th>
        <th>After</th>
        <th>Delay</th>
        <th>Runtime</th>
      </tr>
    </thead>
  `;
  const body = document.createElement("tbody");
  state.editor.jobs.forEach((job) => {
    body.appendChild(createJobRow(job));
  });
  table.appendChild(body);
  wrapper.appendChild(table);
  return wrapper;
}

function createJobRow(job) {
  const row = document.createElement("tr");
  row.dataset.jobId = job.job_id;
  row.appendChild(cellWithText(job.job_id, "font-bold"));
  row.appendChild(cellWithElement(createNumberInput(job.order, 1, 99, (value) => updateJob(job.job_id, "order", value))));
  row.appendChild(cellWithElement(createNodeInput(job.node, (value) => {
    updateJob(job.job_id, "node", value);
    const currentJob = state.editor.jobs.find((item) => item.job_id === job.job_id);
    if (currentJob && !coreSuggestionsFor(value, job.job_id).includes(currentJob.cores)) {
      currentJob.cores = coreSuggestionsFor(value, job.job_id)[0] || currentJob.cores;
      renderEditor();
    }
    schedulePreview();
  })));
  row.appendChild(cellWithElement(createCoreInput(`cores-${job.job_id}`, job.node, job.job_id, job.cores, (value) => updateJob(job.job_id, "cores", value))));
  row.appendChild(cellWithElement(createNumberInput(job.threads, 1, 8, (value) => updateJob(job.job_id, "threads", value))));
  row.appendChild(cellWithElement(createTextInput(job.after, (value) => updateJob(job.job_id, "after", value), "start or job1,job2")));
  row.appendChild(cellWithElement(createNumberInput(job.delay_s, 0, 3600, (value) => updateJob(job.job_id, "delay_s", value))));
  const runtime = document.createElement("span");
  runtime.className = "runtime-value font-bold";
  runtime.dataset.jobId = job.job_id;
  runtime.textContent = fmtSeconds(job.runtime_s);
  row.appendChild(cellWithElement(runtime));
  return row;
}

function createTextField(labelText, value, onInput) {
  const label = document.createElement("label");
  label.className = "flex flex-col gap-2";
  label.innerHTML = `<span class="text-[0.82rem] uppercase tracking-[0.08em] text-[var(--muted)]">${labelText}</span>`;
  label.appendChild(createTextInput(value, onInput));
  return label;
}

function createNumberField(labelText, value, min, max, onInput) {
  const label = document.createElement("label");
  label.className = "flex flex-col gap-2";
  label.innerHTML = `<span class="text-[0.82rem] uppercase tracking-[0.08em] text-[var(--muted)]">${labelText}</span>`;
  label.appendChild(createNumberInput(value, min, max, onInput));
  return label;
}

function createNodeSelect(labelText, value, onInput) {
  const label = document.createElement("label");
  label.className = "flex flex-col gap-2";
  label.innerHTML = `<span class="text-[0.82rem] uppercase tracking-[0.08em] text-[var(--muted)]">${labelText}</span>`;
  label.appendChild(createNodeInput(value, onInput));
  return label;
}

function createCoreField(labelText, listId, nodeId, jobId, value, onInput) {
  const label = document.createElement("label");
  label.className = "flex flex-col gap-2";
  label.innerHTML = `<span class="text-[0.82rem] uppercase tracking-[0.08em] text-[var(--muted)]">${labelText}</span>`;
  label.appendChild(createCoreInput(listId, nodeId, jobId, value, onInput));
  return label;
}

function createTextInput(value, onInput, placeholder = "") {
  const input = document.createElement("input");
  input.className = CLASSES.input;
  input.type = "text";
  input.value = text(value, "");
  input.placeholder = placeholder;
  input.addEventListener("input", (event) => onInput(event.target.value));
  return input;
}

function createNumberInput(value, min, max, onInput) {
  const input = document.createElement("input");
  input.className = CLASSES.smallInput;
  input.type = "number";
  input.min = String(min);
  input.max = String(max);
  input.step = "1";
  input.value = text(value, "");
  input.addEventListener("input", (event) => onInput(event.target.value));
  return input;
}

function createNodeInput(value, onInput) {
  const select = document.createElement("select");
  select.className = CLASSES.input;
  NODE_IDS.forEach((nodeId) => {
    const option = document.createElement("option");
    option.value = nodeId;
    option.textContent = nodeId;
    select.appendChild(option);
  });
  select.value = value;
  select.addEventListener("change", (event) => onInput(event.target.value));
  return select;
}

function createCoreInput(listId, nodeId, jobId, value, onInput) {
  const wrapper = document.createElement("div");
  const input = document.createElement("input");
  input.className = CLASSES.smallInput;
  input.type = "text";
  input.value = text(value, "");
  input.setAttribute("list", listId);
  input.addEventListener("input", (event) => onInput(event.target.value));
  const datalist = document.createElement("datalist");
  datalist.id = listId;
  coreSuggestionsFor(nodeId, jobId).forEach((coreSpec) => {
    const option = document.createElement("option");
    option.value = coreSpec;
    datalist.appendChild(option);
  });
  wrapper.appendChild(input);
  wrapper.appendChild(datalist);
  return wrapper;
}

function cellWithText(value, className = "") {
  const cell = document.createElement("td");
  cell.className = className;
  cell.textContent = value;
  return cell;
}

function cellWithElement(element) {
  const cell = document.createElement("td");
  cell.appendChild(element);
  return cell;
}

function updateJob(jobId, field, value) {
  const job = state.editor.jobs.find((item) => item.job_id === jobId);
  if (!job) {
    return;
  }
  job[field] = value;
  markDirtyAndPreview();
}

function markDirtyAndPreview() {
  state.dirty = true;
  renderDirtyNote();
  schedulePreview();
}

function schedulePreview() {
  if (state.previewTimer) {
    window.clearTimeout(state.previewTimer);
  }
  state.previewTimer = window.setTimeout(previewSchedule, PREVIEW_DEBOUNCE_MS);
}

async function previewSchedule() {
  state.previewTimer = null;
  if (!state.editor) {
    return;
  }
  try {
    const payload = await fetchJson("/api/schedules/preview", {
      method: "POST",
      body: JSON.stringify({
        schedule_id: state.selectedScheduleId || "preview",
        editor: state.editor,
      }),
    });
    state.prediction = payload.prediction;
    state.yaml = payload.yaml || "";
    updateRuntimeCells(payload.editor?.jobs || []);
    renderStats();
    renderTimeline();
    renderIssues();
    renderYaml();
  } catch (error) {
    showError(error);
  }
}

function updateRuntimeCells(jobs) {
  jobs.forEach((job) => {
    const target = document.querySelector(`.runtime-value[data-job-id="${job.job_id}"]`);
    if (target) {
      target.textContent = fmtSeconds(job.runtime_s);
    }
  });
}

function renderTimeline() {
  timelinePanel.innerHTML = "";
  const timeline = state.prediction?.timeline;
  if (!timeline || !timeline.has_data) {
    const empty = document.createElement("div");
    empty.className = CLASSES.emptyState;
    empty.textContent = "No prediction timeline available yet.";
    timelinePanel.appendChild(empty);
    return;
  }
  if (window.CcaTimeline?.rememberScrollPositions) {
    window.CcaTimeline.rememberScrollPositions();
  }
  timelinePanel.appendChild(window.CcaTimeline.createTimelineCard(
    {
      run_id: state.selectedScheduleId || "preview",
      timeline,
    },
    timeline.max_end_s,
    {
      includeDetails: true,
      scrollKey: `schedule:${state.selectedScheduleId || "preview"}`,
      emptyClassName: CLASSES.emptyState,
    },
  ));
  afterLayoutSettles(() => {
    if (window.CcaTimeline?.restoreScrollPositions) {
      window.CcaTimeline.restoreScrollPositions();
    }
  });
}

function renderIssues() {
  issuesPanel.innerHTML = "";
  const errors = state.prediction?.errors || [];
  const warnings = state.prediction?.warnings || [];
  if (!errors.length && !warnings.length) {
    const ok = document.createElement("div");
    ok.className = "rounded-[14px] bg-emerald-50 px-3 py-2.5 text-emerald-900";
    ok.textContent = "No validation issues.";
    issuesPanel.appendChild(ok);
    return;
  }
  errors.forEach((issue) => issuesPanel.appendChild(createIssue(issue, "error")));
  warnings.forEach((issue) => issuesPanel.appendChild(createIssue(issue, "warning")));
}

function createIssue(issue, level) {
  const item = document.createElement("div");
  item.className = level === "error" ? CLASSES.issue : CLASSES.warning;
  item.textContent = issue.message;
  return item;
}

function renderYaml() {
  yamlOutput.value = state.yaml || "";
}

function renderDirtyNote() {
  dirtyNote.textContent = state.dirty
    ? "Browser edits are not written to disk. Polling will not overwrite this active draft."
    : "Loaded from disk. Edits update the prediction only in the browser.";
}

async function copyYaml() {
  copyStatus.textContent = "";
  try {
    if (navigator.clipboard?.writeText) {
      await navigator.clipboard.writeText(state.yaml || "");
    } else {
      yamlOutput.focus();
      yamlOutput.select();
      document.execCommand("copy");
    }
    copyStatus.textContent = "Copied";
  } catch (error) {
    copyStatus.textContent = `Copy failed: ${error.message}`;
  }
}

function showError(error) {
  issuesPanel.innerHTML = "";
  const item = document.createElement("div");
  item.className = CLASSES.issue;
  item.textContent = error.message;
  issuesPanel.appendChild(item);
}

scheduleSelect.addEventListener("change", (event) => {
  loadSchedule(event.target.value).catch(showError);
});

refreshButton.addEventListener("click", async () => {
  refreshButton.disabled = true;
  try {
    await loadScheduleList({ reloadActive: !state.dirty });
  } catch (error) {
    showError(error);
  } finally {
    refreshButton.disabled = false;
  }
});

copyButton.addEventListener("click", copyYaml);

loadScheduleList({ reloadActive: true })
  .catch(showError)
  .finally(() => {
    window.setInterval(() => {
      loadScheduleList({ silent: true, reloadActive: !state.dirty }).catch(showError);
    }, REFRESH_INTERVAL_MS);
  });
