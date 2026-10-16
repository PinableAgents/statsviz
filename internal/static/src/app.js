import StatsManager from "./StatsManager.js";
import PlotManager from "./PlotManager.js";
import { gcEnabled, initNav, updateVisibility } from "./nav.js";
import { buildWebsocketURI } from "./utils.js";
import WebSocketClient from "./socket.js";
import HistoryClient, {
  HistoryUnavailableError,
  StaleRequestError,
  buildHTTPBase,
} from "./historyClient.js";
import {
  buildChartData,
  liveRunFromConfig,
  makeSchemaEntries,
  mergeHistoryAndLive,
  runIdForPointRequest,
  visiblePlotNames,
} from "./historyData.js";
import {
  DebouncedRangeQuery,
  canAutoRefreshRange,
  clearPresetSelection,
  isFollowingLive,
  pausedWindowBounds,
  sliderBounds,
} from "./historyState.js";

const LIVE_CACHE_MS = 15 * 60 * 1000;
const LONG_RANGE_REFRESH_MS = 10000;

export let statsMgr;
export let plotMgr;

let liveConfig = null;
let managerSignature = "";
let renderHandle = null;
let historyClient = null;
let historyMeta = null;
let currentRangeResponse = null;
let lastRangeQueryAt = 0;
let lastRangeAttemptAt = 0;
let lastMetaRefreshAt = 0;
let historyAvailable = false;
let initialized = false;
let rangeQueryInFlight = null;
let rangeQueryDebouncer = null;
let renderedPoints = [];
let frozenView = null;
let liveDurationMs = 60 * 1000;

const view = {
  mode: "follow",
  paused: false,
  durationMs: 60 * 1000,
  from: 0,
  to: 0,
};

const elements = {};

const collectElements = () => {
  for (const id of [
    "history-panel",
    "history-toggle",
    "history-mode",
    "history-status",
    "history-granularity",
    "history-prev",
    "history-next",
    "history-from",
    "history-to",
    "history-apply",
    "history-slider",
    "history-point-at",
    "history-point-query",
    "point-details",
    "point-detail-content",
  ]) {
    elements[id] = document.getElementById(id);
  }
};

const formatLocalInput = (timestamp) => {
  if (!Number.isFinite(timestamp)) return "";
  const date = new Date(timestamp - new Date(timestamp).getTimezoneOffset() * 60000);
  return date.toISOString().slice(0, 23);
};

const parseLocalInput = (value) => {
  const timestamp = new Date(value).getTime();
  return Number.isFinite(timestamp) ? timestamp : null;
};

const setStatus = (message, error = false) => {
  elements["history-status"].textContent = message;
  elements["history-status"].classList.toggle("text-danger", error);
};

const setMode = (mode) => {
  view.mode = mode;
  const fixed = mode === "history";
  elements["history-mode"].textContent = fixed ? "固定历史" : "跟随实时";
  elements["history-mode"].className = `badge ${fixed ? "text-bg-warning" : "text-bg-success"}`;
  elements["history-prev"].disabled = !historyAvailable || !fixed;
  elements["history-next"].disabled = !historyAvailable || !fixed;
  elements["history-panel"].hidden = !fixed;
  elements["history-toggle"].textContent = fixed ? "返回实时" : "查看历史";
  elements["history-toggle"].setAttribute("aria-expanded", String(fixed));
  elements["history-toggle"].classList.toggle("active", fixed);
};

const updateRangeControls = () => {
  elements["history-from"].value = formatLocalInput(view.from);
  elements["history-to"].value = formatLocalInput(view.to);
  if (historyMeta) {
    const bounds = sliderBounds(historyMeta, view.durationMs, view.to);
    elements["history-slider"].min = String(bounds.min);
    elements["history-slider"].max = String(bounds.max);
    elements["history-slider"].step = String(bounds.step);
    elements["history-slider"].value = String(bounds.value);
  }
};

const setHistoryControlsEnabled = (enabled) => {
  elements["history-toggle"].hidden = !enabled;
  for (const id of [
    "history-from",
    "history-to",
    "history-apply",
    "history-slider",
    "history-point-at",
    "history-point-query",
  ]) {
    elements[id].disabled = !enabled;
  }
  for (const input of document.querySelectorAll('input[name="range"]')) {
    if (Number(input.value) > 600) input.disabled = !enabled;
  }
  setMode(view.mode);
};

const openHistory = () => {
  if (!historyAvailable || view.mode === "history") return;
  currentBounds();
  setMode("history");
  render(true);
  scheduleRangeQuery(true);
};

const returnToLive = () => {
  historyClient?.cancelPoint();
  rangeQueryDebouncer?.cancel();
  view.durationMs = liveDurationMs;
  setMode("follow");
  frozenView = null;
  elements["point-details"].open = false;
  plotMgr?.setCursor(null);
  if (view.paused) {
    const playButton = document.getElementById("btn-play");
    playButton.checked = true;
    playButton.dispatchEvent(new Event("change"));
  }
  for (const input of document.querySelectorAll('input[name="range"]')) {
    input.checked = Number(input.value) * 1000 === liveDurationMs;
  }
  currentBounds();
  updateRangeControls();
  render(true);
  if (historyAvailable) queryRange({ interactive: true });
};

const schemaEntriesFor = (points) => {
  const runs = currentRangeResponse?.runs || [];
  const pointRunIds = new Set(points.map((point) => String(point.run_id)));
  const relevantRuns = runs.filter((run) => pointRunIds.has(String(run.id)));
  const liveRunId = liveConfig?.history?.run_id;
  const entries = makeSchemaEntries(relevantRuns, liveConfig, liveRunId);
  if (
    liveRunId !== undefined &&
    points.some((point) => String(point.run_id) === String(liveRunId)) &&
    !entries.some((entry) => entry.runIds.has(String(liveRunId)))
  ) {
    entries.push({
      key: `live-${liveRunId}`,
      config: liveConfig,
      runIds: new Set([String(liveRunId)]),
      startedAt: null,
    });
  }
  return entries;
};

const ensurePlotManager = (entries) => {
  const signature = JSON.stringify(
    entries.map((entry) => [entry.key, entry.config, [...entry.runIds]]),
  );
  if (signature === managerSignature && plotMgr) return;
  plotMgr?.dispose();
  plotMgr = new PlotManager(entries, {
    onPoint: requestPoint,
    onRange: onPlotRange,
  });
  managerSignature = signature;
  updateVisibility(plotMgr);
};

const currentBounds = () => {
  if (view.mode === "history") return [view.from, view.to];
  if (view.paused && frozenView) return [frozenView.from, frozenView.to];
  const latest = statsMgr?.latest()?.timestamp || Date.now();
  view.to = latest + 1;
  view.from = view.to - view.durationMs;
  return [view.from, view.to];
};

const render = (force = false) => {
  if (!liveConfig || !statsMgr) return;
  const [from, to] = currentBounds();
  const responseMatches =
    currentRangeResponse &&
    Number(currentRangeResponse.from) <= to &&
    Number(currentRangeResponse.to) >= from;
  const response = responseMatches ? currentRangeResponse : { points: [], gaps: [], runs: [] };
  const live = statsMgr.between(from, to);
  renderedPoints = mergeHistoryAndLive(
    response,
    live,
    from,
    to,
    liveRunFromConfig(liveConfig),
  );
  const entries = schemaEntriesFor(renderedPoints);
  if (!entries.length) return;
  ensurePlotManager(entries);
  const data = buildChartData(renderedPoints, entries, response.gaps || []);
  plotMgr.update(data, gcEnabled, [from, to], force);
  if (view.paused && view.mode === "follow") {
    frozenView = { from, to, points: renderedPoints, gaps: response.gaps || [], entries };
  }
  updateRangeControls();
};

export const drawPlots = (force = false) => {
  if (renderHandle !== null) return;
  renderHandle = requestAnimationFrame(() => {
    renderHandle = null;
    render(force);
  });
};

const rangeStatus = (response) => {
  const step = Number(response.step_ms) || 0;
  const pointCount = response.points?.length || 0;
  const gaps = response.gaps?.length || 0;
  elements["history-granularity"].textContent = step > 0
    ? `服务端桶宽 ${step >= 60000 ? `${step / 60000} 分钟` : `${step / 1000} 秒`} · ${pointCount} 点 · ${gaps} 个缺口 · GC 线仅表示采样时可见的 lastgc（最近一次 GC），最多 120 条`
    : `原始采样 · ${pointCount} 点 · ${gaps} 个缺口 · GC 线仅表示采样时可见的 lastgc（最近一次 GC），最多 120 条`;
};

const queryRange = async ({ interactive = false } = {}) => {
  if (!historyAvailable || !historyClient || view.to <= view.from) return;
  if (!interactive && rangeQueryInFlight) return rangeQueryInFlight;
  const plots = visiblePlotNames(plotMgr);
  if (plotMgr && !plots.length) return;
  const requestedRange = { from: view.from, to: view.to };
  setStatus("正在查询历史…");
  lastRangeAttemptAt = Date.now();
  const pending = historyClient.range({
    ...requestedRange,
    plots,
    maxPoints: 1500,
  });
  rangeQueryInFlight = pending;
  try {
    const response = await pending;
    const aggregate = Number(response.step_ms) > 0;
    response.points = (response.points || []).map((point) => ({ ...point, aggregate }));
    currentRangeResponse = response;
    lastRangeQueryAt = Date.now();
    rangeStatus(response);
    const status = historyMeta?.status || {};
    const dropped = Number(status.dropped) || 0;
    setStatus(dropped ? `历史已加载，累计丢弃 ${dropped} 个样本` : "历史已加载");
    if (interactive || !view.paused) render(true);
  } catch (error) {
    if (error instanceof StaleRequestError) return;
    setStatus(error.message || "历史查询失败，保留上一幅图", true);
  } finally {
    if (rangeQueryInFlight === pending) rangeQueryInFlight = null;
  }
};

const scheduleRangeQuery = (interactive = true) => {
  rangeQueryDebouncer?.schedule(interactive);
};

const updateMetaStatus = (meta) => {
  const status = meta.status || {};
  const dbSize = Number(status.db_bytes || 0);
  const walSize = Number(status.wal_bytes || 0);
  const storage = dbSize || walSize
    ? ` · DB ${(dbSize / 1048576).toFixed(1)} MiB / WAL ${(walSize / 1048576).toFixed(1)} MiB`
    : "";
  const storageError = status.last_error ? ` · ${status.last_error}` : "";
  const committed = Number(status.last_commit_ms)
    ? ` · 最近提交 ${new Date(Number(status.last_commit_ms)).toLocaleTimeString()}`
    : "";
  const dropped = Number(status.dropped) ? ` · 丢弃 ${status.dropped}` : "";
  setStatus(
    `历史可用，队列 ${status.queue_depth || 0}${committed}${dropped}${storage}${storageError}`,
    Boolean(status.last_error),
  );
};

const initializeHistory = async () => {
  if (!liveConfig?.history?.enabled) {
    rangeQueryDebouncer?.cancel();
    historyAvailable = false;
    if (view.mode === "history") returnToLive();
    setHistoryControlsEnabled(false);
    setStatus("服务未启用持久历史，实时监控仍可使用");
    return;
  }
  const wasAvailable = historyAvailable;
  try {
    const meta = await historyClient.meta();
    if (!meta.enabled) throw new HistoryUnavailableError("服务未启用持久历史");
    historyMeta = meta;
    lastMetaRefreshAt = Date.now();
    historyAvailable = true;
    setHistoryControlsEnabled(true);
    updateMetaStatus(meta);
    currentBounds();
    updateRangeControls();
    if (isFollowingLive(view)) await queryRange();
  } catch (error) {
    if (error instanceof StaleRequestError) return;
    if (wasAvailable && !(error instanceof HistoryUnavailableError)) {
      setStatus(error.message || "历史状态暂时不可读，保留上一幅图", true);
      return;
    }
    historyAvailable = false;
    if (view.mode === "history") returnToLive();
    setHistoryControlsEnabled(false);
    setStatus("历史接口不可用，已保留实时监控", true);
  }
};

const refreshHistoryStatus = async () => {
  if (!historyAvailable || Date.now() - lastMetaRefreshAt < 30000) return;
  lastMetaRefreshAt = Date.now();
  try {
    const meta = await historyClient.meta();
    if (!meta.enabled) return;
    historyMeta = meta;
    updateRangeControls();
    updateMetaStatus(meta);
  } catch (error) {
    if (!(error instanceof StaleRequestError)) {
      setStatus(error.message || "历史状态暂时不可读", true);
    }
  }
};

const setFixedRange = (from, to) => {
  if (!Number.isFinite(from) || !Number.isFinite(to) || to <= from) return;
  historyClient?.cancelPoint();
  clearPresetSelection(document.querySelectorAll('input[name="range"]'));
  const requestedDuration = to - from;
  if (historyMeta && Number(historyMeta.to) > Number(historyMeta.from)) {
    const availableDuration = Number(historyMeta.to) - Number(historyMeta.from);
    const duration = Math.min(requestedDuration, availableDuration);
    to = Math.min(Number(historyMeta.to), Math.max(Number(historyMeta.from) + duration, to));
    from = to - duration;
  }
  view.from = from;
  view.to = to;
  view.durationMs = to - from;
  setMode("history");
  updateRangeControls();
  scheduleRangeQuery(true);
};

const onPlotRange = ([from, to]) => {
  if (!historyAvailable) return;
  setFixedRange(from, to);
};

const appendDetailRow = (table, label, value) => {
  const row = table.insertRow();
  row.insertCell().textContent = label;
  row.insertCell().textContent = value;
};

const showPointDetails = (response) => {
  const container = elements["point-detail-content"];
  container.replaceChildren();
  if (!response.found || !response.sample) {
    container.textContent = "容差范围内没有真实采样点";
    elements["point-details"].open = true;
    return;
  }

  const sample = response.sample;
  const table = document.createElement("table");
  table.className = "table table-sm point-table";
  const actualDate = new Date(sample.timestamp);
  appendDetailRow(
    table,
    "实际时间",
    `${actualDate.toLocaleString(undefined, { timeZoneName: "short" })} (${actualDate.toISOString()})`,
  );
  appendDetailRow(table, "请求偏差", `${Number(response.offset_ms) || 0} ms`);
  appendDetailRow(table, "运行 / 序号", `${sample.run_id ?? "旧服务"} / ${sample.seq ?? "无"}`);
  const configPlots = new Map((response.run?.config?.series || []).map((plot) => [plot.name, plot]));
  for (const [name, values] of Object.entries(sample.series || {})) {
    const config = configPlots.get(name);
    const list = Array.isArray(values) ? values : [values];
    list.forEach((value, index) => {
      const subName = config?.subplots?.[index]?.name;
      const unit = config?.layout?.yaxis?.title;
      appendDetailRow(
        table,
        subName ? `${name} / ${subName}` : `${name}[${index}]`,
        value === null ? "缺失" : `${value}${unit ? ` ${unit}` : ""}`,
      );
    });
  }
  container.appendChild(table);
  elements["point-details"].open = true;
  elements["history-point-at"].value = formatLocalInput(sample.timestamp);
  plotMgr?.setCursor(sample.timestamp);
  render(true);
};

async function requestPoint(timestamp, schemaKey = null) {
  if (!historyAvailable) {
    setStatus("真实单点查询需要启用持久历史", true);
    return;
  }
  openHistory();
  const runId = runIdForPointRequest(
    renderedPoints,
    currentRangeResponse?.runs,
    timestamp,
    schemaKey,
  );
  setStatus("正在查询真实采样点…");
  try {
    const response = await historyClient.point({
      at: timestamp,
      runId,
    });
    showPointDetails(response);
    setStatus(response.found ? "已选中真实采样点" : "附近没有真实采样点");
  } catch (error) {
    if (error instanceof StaleRequestError) return;
    setStatus(error.message || "单点查询失败", true);
  }
}

const bindHistoryControls = () => {
  elements["history-apply"].addEventListener("click", () => {
    const from = parseLocalInput(elements["history-from"].value);
    const to = parseLocalInput(elements["history-to"].value);
    setFixedRange(from, to);
  });
  elements["history-prev"].addEventListener("click", () => {
    setFixedRange(view.from - view.durationMs, view.to - view.durationMs);
  });
  elements["history-next"].addEventListener("click", () => {
    const nextTo = Math.min(Number(historyMeta?.to || view.to + view.durationMs), view.to + view.durationMs);
    setFixedRange(nextTo - view.durationMs, nextTo);
  });
  elements["history-toggle"].addEventListener("click", () => {
    if (view.mode === "history") returnToLive();
    else openHistory();
  });
  elements["history-slider"].addEventListener("input", () => {
    const to = Number(elements["history-slider"].value);
    setFixedRange(to - view.durationMs, to);
  });
  elements["history-point-query"].addEventListener("click", () => {
    const timestamp = parseLocalInput(elements["history-point-at"].value);
    if (timestamp !== null) requestPoint(timestamp);
  });
};

const initializeUI = () => {
  if (initialized) return;
  initialized = true;
  collectElements();
  bindHistoryControls();
  initNav({
    getPlotManager: () => plotMgr,
    onRedraw: (force) => render(force),
    onPause: (paused) => {
      view.paused = paused;
      if (paused) {
        frozenView = {
          from: view.from,
          to: view.to,
          points: renderedPoints,
          gaps: currentRangeResponse?.gaps || [],
          entries: schemaEntriesFor(renderedPoints),
        };
      } else {
        frozenView = null;
      }
      setStatus(paused ? "画面已暂停；历史交互仍可使用" : historyAvailable ? "画面已恢复" : "实时画面已恢复");
      if (!paused) render(true);
    },
    onPreset: (seconds) => {
      historyClient?.cancelPoint();
      view.durationMs = seconds * 1000;
      if (view.mode === "follow") liveDurationMs = view.durationMs;
      const pausedFollow = view.mode === "follow" && view.paused;
      if (pausedFollow) {
        const bounds = pausedWindowBounds(frozenView?.to ?? view.to, view.durationMs);
        view.from = bounds.from;
        view.to = bounds.to;
        if (frozenView) frozenView = { ...frozenView, ...bounds };
        render(true);
      } else if (view.mode === "follow") {
        currentBounds();
      } else {
        view.from = view.to - view.durationMs;
      }
      updateRangeControls();
      if (historyAvailable) scheduleRangeQuery(true);
      else if (!pausedFollow) render(true);
    },
    onVisibility: () => {
      render(true);
      if (historyAvailable) scheduleRangeQuery(true);
    },
  });
};

const onConfig = (config) => {
  liveConfig = config;
  if (!statsMgr) statsMgr = new StatsManager(LIVE_CACHE_MS, config);
  else statsMgr.reset(config);
  initializeUI();
  if (isFollowingLive(view)) {
    render(true);
  }
  if (!historyClient) {
    const websocketURL = buildWebsocketURI();
    historyClient = new HistoryClient(buildHTTPBase(websocketURL, window.location.href));
    rangeQueryDebouncer = new DebouncedRangeQuery(
      () => {
        historyClient.cancelRange();
        rangeQueryInFlight = null;
      },
      (interactive) => queryRange({ interactive }),
    );
  }
  initializeHistory();
};

const onData = (message) => {
  if (!statsMgr) return;
  statsMgr.pushData(message);
  refreshHistoryStatus();
  if (!isFollowingLive(view)) return;
  drawPlots(true);
  if (historyAvailable && Date.now() - lastRangeQueryAt >= LONG_RANGE_REFRESH_MS) {
    currentBounds();
    if (canAutoRefreshRange({
      inFlight: rangeQueryInFlight,
      lastAttemptAt: lastRangeAttemptAt,
      now: Date.now(),
      refreshMs: LONG_RANGE_REFRESH_MS,
    })) {
      queryRange();
    }
  }
};

export const connect = () => {
  const uri = buildWebsocketURI();
  new WebSocketClient(uri, onConfig, onData, undefined, () => {
    if (historyAvailable && isFollowingLive(view)) {
      view.to = Date.now() + 1;
      view.from = view.to - view.durationMs;
      queryRange();
    }
  });
};
