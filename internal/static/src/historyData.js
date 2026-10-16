const finiteNumber = (value) => {
  const number = Number(value);
  return Number.isFinite(number) ? number : null;
};

const cloneValues = (values) => {
  if (!Array.isArray(values)) return [];
  return values.map((value) => (value === null ? null : finiteNumber(value)));
};

export const sampleKey = (sample) => {
  if (sample?.run_id !== undefined && sample?.run_id !== null && sample?.seq !== undefined && sample?.seq !== null) {
    return `run:${sample.run_id}:${sample.seq}`;
  }
  return `time:${sample?.timestamp}`;
};

export const normalizeSample = (sample, source = "history") => {
  const series = {};
  for (const [name, values] of Object.entries(sample?.series || {})) {
    series[name] = cloneValues(values);
  }

  const normalized = {
    run_id: sample?.run_id ?? null,
    seq: sample?.seq ?? null,
    timestamp: finiteNumber(sample?.timestamp),
    duration_ns: finiteNumber(sample?.duration_ns),
    series,
    source,
    aggregate: Boolean(sample?.aggregate),
  };

  for (const field of ["min", "max"]) {
    if (!sample?.[field]) continue;
    normalized[field] = {};
    for (const [name, values] of Object.entries(sample[field])) {
      normalized[field][name] = cloneValues(values);
    }
  }
  return normalized;
};

export class TimeWindowCache {
  #windowMs;
  #maxSamples;
  #samples = [];
  #start = 0;
  #keys = new Map();
  #sizes = new WeakMap();
  #bytes = 0;
  #maxBytes;

  constructor(windowMs, maxSamples = 20000, maxBytes = 16 * 1024 * 1024) {
    if (!(windowMs > 0) || !(maxSamples > 0) || !(maxBytes > 0)) {
      throw new Error("windowMs, maxSamples and maxBytes must be positive");
    }
    this.#windowMs = windowMs;
    this.#maxSamples = maxSamples;
    this.#maxBytes = maxBytes;
  }

  add(sample, source = "live") {
    const normalized = normalizeSample(sample, source);
    if (normalized.timestamp === null) return false;

    const key = sampleKey(normalized);
    const previous = this.#keys.get(key);
    if (previous) {
      if (previous.source === "live" && source !== "live") return false;
      const index = this.#samples.indexOf(previous);
      if (index >= this.#start) {
        this.#bytes -= this.#sizes.get(previous) || 0;
        this.#samples.splice(index, 1);
      }
      this.#rememberSize(normalized);
      this.#keys.set(key, normalized);
      this.#insert(normalized);
      this.#trim();
      return false;
    }

    this.#insert(normalized);
    this.#rememberSize(normalized);
    this.#keys.set(key, normalized);
    this.#trim();
    return true;
  }

  addMany(samples, source = "history") {
    for (const sample of samples || []) this.add(sample, source);
  }

  between(from, to) {
    return this.#samples.slice(this.#start).filter(
      (sample) => sample.timestamp >= from && sample.timestamp < to,
    );
  }

  latest() {
    return this.#samples.length > this.#start
      ? this.#samples[this.#samples.length - 1]
      : undefined;
  }

  clear() {
    this.#samples = [];
    this.#start = 0;
    this.#keys.clear();
    this.#sizes = new WeakMap();
    this.#bytes = 0;
  }

  #trim() {
    if (this.#samples.length <= this.#start) return;
    const newest = this.#samples[this.#samples.length - 1].timestamp;
    const cutoff = newest - this.#windowMs;
    while (
      this.#start < this.#samples.length &&
      (this.#samples.length - this.#start > this.#maxSamples ||
        this.#bytes > this.#maxBytes ||
        this.#samples[this.#start].timestamp < cutoff)
    ) {
      const index = this.#start++;
      const removed = this.#samples[index];
      this.#samples[index] = null;
      this.#keys.delete(sampleKey(removed));
      this.#bytes -= this.#sizes.get(removed) || 0;
    }
    if (this.#start === this.#samples.length) {
      this.#samples = [];
      this.#start = 0;
    } else if (this.#start > 1024 && this.#start * 2 > this.#samples.length) {
      this.#samples.splice(0, this.#start);
      this.#start = 0;
    }
  }

  #rememberSize(sample) {
    const bytes = JSON.stringify(sample).length * 2;
    this.#sizes.set(sample, bytes);
    this.#bytes += bytes;
  }

  #insert(sample) {
    const last = this.#samples.at(-1);
    if (!last || compareSamples(last, sample) <= 0) {
      this.#samples.push(sample);
      return;
    }
    let low = this.#start;
    let high = this.#samples.length;
    while (low < high) {
      const middle = (low + high) >>> 1;
      if (compareSamples(this.#samples[middle], sample) <= 0) low = middle + 1;
      else high = middle;
    }
    this.#samples.splice(low, 0, sample);
  }
}

const compareSamples = (a, b) =>
  a.timestamp - b.timestamp ||
  String(a.run_id ?? "").localeCompare(String(b.run_id ?? "")) ||
  Number(a.seq ?? 0) - Number(b.seq ?? 0);

const watermarksMap = (watermarks) =>
  new Map((watermarks || []).map((item) => [String(item.run_id), Number(item.seq)]));

const kindFor = (run, plotName, index) => {
  const kinds = run?.aggregation?.[plotName]?.kinds || [];
  return kinds.length === 1 ? kinds[0] : kinds[index] || "gauge";
};

export const liveRunFromConfig = (config) => {
  const runID = config?.history?.run_id;
  if (runID === undefined || runID === null) return null;

  const aggregation = {};
  for (const plot of config.series || []) {
    if (!plot?.name) continue;
    if (plot.type === "heatmap") {
      aggregation[plot.name] = {
        kinds: [plot.aggregation || "histogram"],
        heatmap: true,
      };
    } else {
      aggregation[plot.name] = {
        kinds: (plot.subplots || []).map((subplot) => subplot.aggregation || "gauge"),
      };
    }
  }
  for (const eventName of config.events || []) {
    aggregation[eventName] = { kinds: ["event"] };
  }
  return { id: runID, aggregation };
};

const runsForResponse = (response, liveRun) => {
  const runs = new Map((response?.runs || []).map((run) => [String(run.id), run]));
  if (liveRun && !runs.has(String(liveRun.id))) {
    runs.set(String(liveRun.id), liveRun);
  }
  return runs;
};

const bucketKey = (runID, timestamp, step) =>
  `${runID}:${Math.floor(timestamp / step) * step}`;

const liveRollups = (response, liveSamples, from, to, occupiedBuckets, liveRun) => {
  const step = Number(response?.step_ms) || 0;
  const runs = runsForResponse(response, liveRun);
  const watermarks = watermarksMap(response?.watermarks);
  const grouped = new Map();
  const ordered = (liveSamples || [])
    .map((sample) => normalizeSample(sample, "live"))
    .filter((sample) => sample.timestamp >= from && sample.timestamp < to)
    .sort(compareSamples);
  const previousByRun = new Map();

  for (const sample of ordered) {
    const key = bucketKey(sample.run_id, sample.timestamp, step);
    const previous = previousByRun.get(String(sample.run_id));
    previousByRun.set(String(sample.run_id), sample);
    const extendsHistory = occupiedBuckets.has(key);
    const committed = watermarks.get(String(sample.run_id));
    if (extendsHistory && (committed === undefined || Number(sample.seq) <= committed)) {
      continue;
    }
    if (!grouped.has(key)) {
      grouped.set(key, {
        run_id: sample.run_id,
        seq: sample.seq,
        timestamp: Math.floor(sample.timestamp / step) * step,
        duration_ns: 0,
        source: "live-rollup",
        aggregate: true,
        extendsHistory,
        series: {},
        min: {},
        max: {},
        weights: {},
        coverage_ns: {},
        incomplete: {},
        cells: new Map(),
      });
    }
    const rollup = grouped.get(key);
    if (Number(sample.seq) > Number(rollup.seq)) rollup.seq = sample.seq;
    const weight = sample.duration_ns > 0 ? sample.duration_ns : 1;
    rollup.duration_ns += weight;
    const run = runs.get(String(sample.run_id));
    for (const [plotName, values] of Object.entries(sample.series)) {
      if (!rollup.cells.has(plotName)) rollup.cells.set(plotName, []);
      if (!rollup.incomplete[plotName]) rollup.incomplete[plotName] = [];
      const cells = rollup.cells.get(plotName);
      values.forEach((rawValue, index) => {
        let value = rawValue;
        const kind = kindFor(run, plotName, index);
        if (kind === "histogram") {
          const prior = previous?.series?.[plotName]?.[index];
          const sequential =
            previous &&
            String(previous.run_id) === String(sample.run_id) &&
            Number(previous.seq) + 1 === Number(sample.seq);
          value = sequential && prior !== null && value !== null && value >= prior
            ? value - prior
            : null;
          if (value === null) rollup.incomplete[plotName][index] = true;
        }
        if (value === null || !Number.isFinite(value)) return;
        const cell = cells[index] || {
          kind,
          weightedSum: 0,
          weight: 0,
          coverage: 0,
          sum: 0,
          event: 0,
          min: value,
          max: value,
        };
        cell.coverage += weight;
        cell.min = Math.min(cell.min, value);
        cell.max = Math.max(cell.max, value);
        if (kind === "gauge" || kind === "rate") {
          cell.weightedSum += value * weight;
          cell.weight += weight;
        } else if (kind === "delta" || kind === "histogram") {
          cell.sum += value;
        } else if (kind === "event") {
          if (value > 0) cell.event = value;
        }
        cells[index] = cell;
      });
    }
  }

  for (const rollup of grouped.values()) {
    for (const [plotName, cells] of rollup.cells) {
      const incomplete = rollup.incomplete[plotName] || [];
      const length = Math.max(cells.length, incomplete.length);
      rollup.series[plotName] = Array.from({ length }, (_, index) => {
        const cell = cells[index];
        if (!cell) return null;
        if (cell.kind === "gauge" || cell.kind === "rate") {
          return cell.weight ? cell.weightedSum / cell.weight : null;
        }
        if (cell.kind === "event") return cell.event;
        return cell.sum;
      });
      rollup.min[plotName] = Array.from({ length }, (_, index) => cells[index]?.min ?? null);
      rollup.max[plotName] = Array.from({ length }, (_, index) => cells[index]?.max ?? null);
      rollup.weights[plotName] = Array.from({ length }, (_, index) => cells[index]?.weight ?? 0);
      rollup.coverage_ns[plotName] = Array.from(
        { length },
        (_, index) => cells[index]?.coverage ?? 0,
      );
      if (!incomplete.some(Boolean)) delete rollup.incomplete[plotName];
    }
    if (!Object.keys(rollup.incomplete).length) delete rollup.incomplete;
    delete rollup.cells;
  }
  return [...grouped.values()];
};

const mergeAggregateTail = (historyPoint, tail, run) => {
  const historyDuration = historyPoint.duration_ns || 0;
  historyPoint.seq = Number(tail.seq) > Number(historyPoint.seq) ? tail.seq : historyPoint.seq;
  historyPoint.duration_ns = historyDuration + (tail.duration_ns || 0);
  historyPoint.source = "history+live-rollup";
  for (const [plotName, tailValues] of Object.entries(tail.series)) {
    if (!historyPoint.series[plotName]) historyPoint.series[plotName] = [];
    if (!historyPoint.min) historyPoint.min = {};
    if (!historyPoint.max) historyPoint.max = {};
    if (!historyPoint.min[plotName]) historyPoint.min[plotName] = [];
    if (!historyPoint.max[plotName]) historyPoint.max[plotName] = [];
    tailValues.forEach((tailValue, index) => {
      if (!historyPoint.coverage_ns) historyPoint.coverage_ns = {};
      if (!historyPoint.coverage_ns[plotName]) historyPoint.coverage_ns[plotName] = [];
      const current = historyPoint.series[plotName][index];
      const historyCoverage = historyPoint.coverage_ns[plotName][index]
        ?? (current === null || current === undefined ? 0 : historyDuration);
      historyPoint.coverage_ns[plotName][index] =
        historyCoverage + (tail.coverage_ns?.[plotName]?.[index] || 0);
      if (tailValue === null || tailValue === undefined) return;
      const kind = kindFor(run, plotName, index);
      if (current === null || current === undefined) {
        historyPoint.series[plotName][index] = tailValue;
      } else if (kind === "delta" || kind === "histogram") {
        historyPoint.series[plotName][index] = current + tailValue;
      } else if (kind === "event") {
        historyPoint.series[plotName][index] = tailValue > 0 ? tailValue : current;
      } else {
        const historyWeight = Math.max(0, historyDuration);
        const tailWeight = tail.weights?.[plotName]?.[index] || 0;
        const totalWeight = historyWeight + tailWeight;
        historyPoint.series[plotName][index] = totalWeight
          ? (current * historyWeight + tailValue * tailWeight) / totalWeight
          : tailValue;
      }
      const tailMin = tail.min?.[plotName]?.[index];
      const tailMax = tail.max?.[plotName]?.[index];
      const currentMin = historyPoint.min[plotName][index];
      const currentMax = historyPoint.max[plotName][index];
      if (tailMin !== null && tailMin !== undefined) {
        historyPoint.min[plotName][index] = currentMin === null || currentMin === undefined
          ? tailMin
          : Math.min(currentMin, tailMin);
      }
      if (tailMax !== null && tailMax !== undefined) {
        historyPoint.max[plotName][index] = currentMax === null || currentMax === undefined
          ? tailMax
          : Math.max(currentMax, tailMax);
      }
    });
  }
  for (const [plotName, flags] of Object.entries(tail.incomplete || {})) {
    if (!historyPoint.incomplete) historyPoint.incomplete = {};
    if (!historyPoint.incomplete[plotName]) historyPoint.incomplete[plotName] = [];
    flags.forEach((flag, index) => {
      if (flag) historyPoint.incomplete[plotName][index] = true;
    });
  }
  return historyPoint;
};

export const mergeHistoryAndLive = (response, liveSamples, from, to, liveRun = null) => {
  const step = Number(response?.step_ms) || 0;
  const samples = new Map();
  const occupiedBuckets = new Set();
  const historyByBucket = new Map();
  for (const point of response?.points || []) {
    const normalized = normalizeSample(point, "history");
    if (normalized.timestamp < from || normalized.timestamp >= to) continue;
    normalized.aggregate ||= step > 0;
    samples.set(sampleKey(normalized), normalized);
    if (step > 0) {
      const key = bucketKey(normalized.run_id, normalized.timestamp, step);
      occupiedBuckets.add(key);
      historyByBucket.set(key, normalized);
    }
  }

  if (step > 0) {
    const runs = runsForResponse(response, liveRun);
    for (const point of liveRollups(response, liveSamples, from, to, occupiedBuckets, liveRun)) {
      if (point.extendsHistory) {
        const key = bucketKey(point.run_id, point.timestamp, step);
        const historyPoint = historyByBucket.get(key);
        if (historyPoint) {
          mergeAggregateTail(historyPoint, point, runs.get(String(point.run_id)));
          continue;
        }
      }
      delete point.extendsHistory;
      delete point.weights;
      samples.set(sampleKey(point), point);
    }
    return [...samples.values()].sort(compareSamples);
  }

  const watermarks = watermarksMap(response?.watermarks);
  for (const point of liveSamples || []) {
    const normalized = normalizeSample(point, "live");
    if (normalized.timestamp < from || normalized.timestamp >= to) continue;
    const key = sampleKey(normalized);
    const committed = watermarks.get(String(normalized.run_id));
    if (samples.has(key)) continue;
    if (committed !== undefined && Number(normalized.seq) <= committed) continue;
    samples.set(key, normalized);
  }

  return [...samples.values()].sort(compareSamples);
};

const configKeyForRun = (run) =>
  String(run?.config_hash || run?.id || "unknown");

export const makeSchemaEntries = (runs, fallbackConfig, fallbackRunId = fallbackConfig?.history?.run_id) => {
  const grouped = new Map();
  for (const run of runs || []) {
    if (!run?.config?.series) continue;
    const key = configKeyForRun(run);
    if (!grouped.has(key)) {
      grouped.set(key, {
        key,
        config: run.config,
        runIds: new Set(),
        startedAt: run.started_at_ms ?? null,
      });
    }
    grouped.get(key).runIds.add(String(run.id));
  }

  if (!grouped.size && fallbackConfig?.series) {
    grouped.set("live", {
      key: "live",
      config: fallbackConfig,
      runIds: fallbackRunId === undefined || fallbackRunId === null
        ? new Set()
        : new Set([String(fallbackRunId)]),
      startedAt: null,
    });
  }
  return [...grouped.values()];
};

const gapBetween = (previous, current, gaps) => {
  if (!previous) return false;
  if (String(previous.run_id) !== String(current.run_id)) return true;
  if (
    !previous.aggregate &&
    !current.aggregate &&
    previous.seq !== null &&
    current.seq !== null &&
    Number(current.seq) > Number(previous.seq) + 1
  ) {
    return true;
  }
  return (gaps || []).some(
    (gap) =>
      String(gap.run_id) === String(current.run_id) &&
      Number(gap.from) < current.timestamp &&
      Number(gap.to) > previous.timestamp,
  );
};

const dimensionsFor = (plot) =>
  plot.type === "heatmap" ? plot.buckets?.length || 0 : plot.subplots?.length || 0;

export const buildChartData = (points, entries, gaps = []) => {
  const times = new Map();
  const metadata = new Map();
  const series = new Map();
  const minimums = new Map();
  const maximums = new Map();
  const events = new Map();

  for (const entry of entries) {
    const selected = points.filter(
      (point) => !entry.runIds.size || entry.runIds.has(String(point.run_id)),
    );
    const entryTimes = [];
    const entryMeta = [];
    const valueArrays = new Map();
    const minArrays = new Map();
    const maxArrays = new Map();

    for (const plot of entry.config.series || []) {
      const dimensions = dimensionsFor(plot);
      valueArrays.set(plot.name, Array.from({ length: dimensions }, () => []));
      minArrays.set(plot.name, Array.from({ length: dimensions }, () => []));
      maxArrays.set(plot.name, Array.from({ length: dimensions }, () => []));
    }
    for (const eventName of entry.config.events || []) {
      if (!events.has(eventName)) events.set(eventName, []);
    }

    let previous = null;
    for (const point of selected) {
      if (gapBetween(previous, point, gaps)) {
        const breakAt = previous.timestamp + (point.timestamp - previous.timestamp) / 2;
        entryTimes.push(breakAt);
        entryMeta.push(null);
        for (const plot of entry.config.series || []) {
          for (const values of valueArrays.get(plot.name)) values.push(null);
          for (const values of minArrays.get(plot.name)) values.push(null);
          for (const values of maxArrays.get(plot.name)) values.push(null);
        }
      }

      entryTimes.push(point.timestamp);
      entryMeta.push({
        run_id: point.run_id,
        seq: point.seq,
        aggregate: Boolean(point.aggregate || (point.source === "history" && (point.min || point.max))),
        incomplete: point.incomplete || null,
        summary: point.aggregate || (point.source === "history" && (point.min || point.max))
          ? "聚合桶摘要"
          : "原始采样",
      });
      for (const plot of entry.config.series || []) {
        const values = point.series?.[plot.name] || [];
        const mins = point.min?.[plot.name] || [];
        const maxs = point.max?.[plot.name] || [];
        valueArrays.get(plot.name).forEach((dimension, index) => {
          dimension.push(values[index] ?? null);
          minArrays.get(plot.name)[index].push(mins[index] ?? null);
          maxArrays.get(plot.name)[index].push(maxs[index] ?? null);
        });
      }
      for (const eventName of entry.config.events || []) {
        const eventAt = point.series?.[eventName]?.[0];
        if (eventAt !== null && Number.isFinite(Number(eventAt))) {
          const numericEvent = Number(eventAt);
          if (numericEvent <= 0) continue;
          const eventDate = new Date(numericEvent);
          if (!Number.isFinite(eventDate.getTime()) || eventDate.getUTCFullYear() < 2000) continue;
          const list = events.get(eventName);
          if (
            !list.length ||
            list[list.length - 1].getTime() !== eventDate.getTime()
          ) {
            list.push(eventDate);
          }
        }
      }
      previous = point;
    }

    times.set(entry.key, entryTimes);
    metadata.set(entry.key, entryMeta);
    for (const plot of entry.config.series || []) {
      const key = `${entry.key}:${plot.name}`;
      series.set(key, valueArrays.get(plot.name));
      minimums.set(key, minArrays.get(plot.name));
      maximums.set(key, maxArrays.get(plot.name));
    }
  }

  return { times, metadata, series, minimums, maximums, events };
};

export const visiblePlotNames = (plotManager) => [
  ...new Set(
    (plotManager?.plots || [])
      .filter((plot) => plot.isVisible())
      .map((plot) => plot.sourceName()),
  ),
];

export const runIdForPointRequest = (points, runs, timestamp, schemaKey) => {
  if (!schemaKey) return undefined;
  let nearest = null;
  for (const point of points || []) {
    if (nearest && Math.abs(nearest.timestamp - timestamp) <= Math.abs(point.timestamp - timestamp)) {
      continue;
    }
    const run = (runs || []).find((item) => String(item.id) === String(point.run_id));
    const key = String(run?.config_hash || run?.id || "live");
    if (key !== schemaKey && `live-${point.run_id}` !== schemaKey) continue;
    nearest = point;
  }
  return nearest?.run_id;
};

export const envelopeSummary = (metadata, minimum, maximum, incomplete = false) => {
  if (!metadata) return "缺口";
  if (!metadata.aggregate) return metadata.summary;
  const min = minimum === null || minimum === undefined ? "缺失" : minimum;
  const max = maximum === null || maximum === undefined ? "缺失" : maximum;
  const suffix = incomplete
    ? "<br><b>实时尾部</b>: 缺少累计基线，仅显示已提交部分"
    : "";
  return `${metadata.summary}<br><b>最小值</b>: ${min}<br><b>最大值</b>: ${max}${suffix}`;
};

export const timeTickFormat = ([from, to]) => {
  const duration = Number(to) - Number(from);
  const crossesDay = new Date(from).toDateString() !== new Date(to).toDateString();
  return duration >= 24 * 60 * 60 * 1000 || crossesDay
    ? "%Y-%m-%d\n%H:%M"
    : "%H:%M:%S";
};
