(function () {
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
  const timelineScrollPositions = new Map();

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

  function rememberScrollPositions() {
    document.querySelectorAll(".timeline-scroll[data-scroll-key]").forEach((scroller) => {
      timelineScrollPositions.set(scroller.dataset.scrollKey, scroller.scrollLeft);
    });
  }

  function restoreScrollPositions() {
    document.querySelectorAll(".timeline-scroll[data-scroll-key]").forEach((scroller) => {
      const savedScrollLeft = timelineScrollPositions.get(scroller.dataset.scrollKey);
      if (savedScrollLeft === undefined) {
        return;
      }
      const maxScrollLeft = Math.max(0, scroller.scrollWidth - scroller.clientWidth);
      scroller.scrollLeft = Math.min(savedScrollLeft, maxScrollLeft);
    });
  }

  function rememberTimelineScroll(event) {
    const scroller = event.currentTarget;
    if (scroller.dataset.scrollKey) {
      timelineScrollPositions.set(scroller.dataset.scrollKey, scroller.scrollLeft);
    }
  }

  function createTimelineCard(run, explicitScaleMax, options = {}) {
    const includeDetails = options.includeDetails !== false;
    const container = document.createElement("div");
    container.className = "timeline-card";

    const timeline = run.timeline || run || {};
    if (!timeline.has_data) {
      const empty = document.createElement("div");
      empty.className = options.emptyClassName || "empty-state";
      empty.textContent = options.emptyText || "This schedule does not have enough timing data to render a chart.";
      container.appendChild(empty);
      return container;
    }

    const scaleMax = Math.max(Number(explicitScaleMax || 0), Number(timeline.max_end_s || 0), 1);
    const scroller = document.createElement("div");
    scroller.className = "timeline-scroll";
    scroller.dataset.scrollKey = options.scrollKey || `timeline:${run.run_id || run.schedule_id || "preview"}`;
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
    labelNode.textContent = rawNames.length ? rawNames.join(", ") : "planned node";
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
        `memcached ${fmtSeconds(segment.duration_s)} ${coreCountLabel(segment)} ${threadLabel(segment.threads)} ${coreLabel(segment.cores)}`,
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

  window.CcaTimeline = {
    createTimelineCard,
    rememberScrollPositions,
    restoreScrollPositions,
    timelineSegments,
  };
}());
