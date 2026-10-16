import test from "node:test";
import assert from "node:assert/strict";
import {
  TimeWindowCache,
  buildChartData,
  envelopeSummary,
  liveRunFromConfig,
  makeSchemaEntries,
  mergeHistoryAndLive,
  runIdForPointRequest,
  timeTickFormat,
} from "./historyData.js";

const config = {
  events: [],
  series: [
    {
      name: "cpu",
      type: "scatter",
      tags: ["cpu"],
      subplots: [{ name: "cpu" }],
    },
  ],
};

test("时间窗口按时间而不是样本数裁剪，并保留 null", () => {
  const cache = new TimeWindowCache(3000, 10);
  cache.add({ timestamp: 1000, series: { cpu: [1] } });
  cache.add({ timestamp: 2000, series: { cpu: [null] } });
  cache.add({ timestamp: 5001, series: { cpu: [3] } });

  assert.deepEqual(
    cache.between(0, 6000).map((sample) => sample.timestamp),
    [5001],
  );
  assert.equal(cache.latest().series.cpu[0], 3);

  cache.add({ timestamp: 6000, series: { cpu: [null] } });
  assert.equal(cache.latest().series.cpu[0], null);
});

test("有身份的样本按 run_id 和 seq 去重，旧样本按时间去重", () => {
  const cache = new TimeWindowCache(10000, 10);
  cache.add({ run_id: "a", seq: 1, timestamp: 1000, series: { cpu: [1] } });
  cache.add({ run_id: "a", seq: 1, timestamp: 1001, series: { cpu: [2] } });
  cache.add({ timestamp: 2000, series: { cpu: [3] } });
  cache.add({ timestamp: 2000, series: { cpu: [4] } });

  const samples = cache.between(0, 3000);
  assert.equal(samples.length, 2);
  assert.equal(samples[0].series.cpu[0], 2);
  assert.equal(samples[1].series.cpu[0], 4);
});

test("实时缓存顺序追加且能在少量乱序到达时保持时间顺序", () => {
  const cache = new TimeWindowCache(10000, 10);
  cache.add({ run_id: "a", seq: 1, timestamp: 1000, series: { cpu: [1] } });
  cache.add({ run_id: "a", seq: 3, timestamp: 3000, series: { cpu: [3] } });
  cache.add({ run_id: "a", seq: 2, timestamp: 2000, series: { cpu: [2] } });

  assert.deepEqual(
    cache.between(0, 4000).map((sample) => sample.seq),
    [1, 2, 3],
  );
});

test("实时缓存同时受样本数和估算字节数上限约束", () => {
  const cache = new TimeWindowCache(10000, 100, 500);
  cache.add({ timestamp: 1000, series: { cpu: Array(40).fill(1) } });
  cache.add({ timestamp: 2000, series: { cpu: Array(40).fill(2) } });
  assert.deepEqual(cache.between(0, 3000).map((sample) => sample.timestamp), [2000]);
});

test("单个超预算样本会释放，后续小样本仍可追加", () => {
  const cache = new TimeWindowCache(10000, 100, 300);
  cache.add({ timestamp: 1000, series: { cpu: Array(1000).fill(1) } });
  assert.equal(cache.latest(), undefined);
  cache.add({ timestamp: 2000, series: { cpu: [2] } });
  assert.equal(cache.latest().timestamp, 2000);
});

test("历史与实时按水位和样本身份合并", () => {
  const response = {
    points: [
      { run_id: "a", seq: 0, timestamp: -1, series: { cpu: [0] } },
      { run_id: "a", seq: 1, timestamp: 1000, series: { cpu: [1] } },
      { run_id: "a", seq: 2, timestamp: 2000, series: { cpu: [2] } },
      { run_id: "a", seq: 4, timestamp: 4000, series: { cpu: [4] } },
    ],
    watermarks: [{ run_id: "a", seq: 2 }],
  };
  const live = [
    { run_id: "a", seq: 2, timestamp: 2000, series: { cpu: [20] } },
    { run_id: "a", seq: 3, timestamp: 3000, series: { cpu: [3] } },
  ];

  const merged = mergeHistoryAndLive(response, live, 0, 4000);
  assert.deepEqual(merged.map((sample) => sample.seq), [1, 2, 3]);
  assert.equal(merged[1].series.cpu[0], 2);
  assert.equal(merged[1].source, "history");
});

test("聚合历史不会与桶内 raw 混画，实时尾部按相同粒度聚合", () => {
  const response = {
    step_ms: 10000,
    points: [{
      run_id: "a",
      seq: 10,
      timestamp: 0,
      series: { cpu: [8], count: [4], latency: [20], lastgc: [1234567890000] },
      min: { cpu: [2] },
      max: { cpu: [12] },
    }],
    runs: [{
      id: "a",
      aggregation: {
        cpu: { kinds: ["gauge"] },
        count: { kinds: ["delta"] },
        latency: { kinds: ["histogram"] },
        lastgc: { kinds: ["event"] },
      },
    }],
    watermarks: [{ run_id: "a", seq: 10 }],
  };
  const live = [
    { run_id: "a", seq: 10, timestamp: 9000, duration_ns: 1000, series: { cpu: [99], count: [9], latency: [100], lastgc: [0] } },
    { run_id: "a", seq: 11, timestamp: 11000, duration_ns: 1000, series: { cpu: [2], count: [1], latency: [105], lastgc: [0] } },
    { run_id: "a", seq: 12, timestamp: 12000, duration_ns: 1000, series: { cpu: [4], count: [2], latency: [109], lastgc: [1234567890123] } },
  ];

  const merged = mergeHistoryAndLive(response, live, 0, 20000);
  assert.deepEqual(merged.map((sample) => sample.timestamp), [0, 10000]);
  assert.equal(merged[0].series.cpu[0], 8);
  assert.equal(merged[1].source, "live-rollup");
  assert.equal(merged[1].aggregate, true);
  assert.deepEqual(merged[1].series, {
    cpu: [3],
    count: [3],
    latency: [9],
    lastgc: [1234567890123],
  });
  assert.deepEqual(merged[1].min.cpu, [2]);
  assert.deepEqual(merged[1].max.cpu, [4]);
});

test("聚合当前桶只合并水位后的实时样本", () => {
  const response = {
    step_ms: 10000,
    points: [{
      run_id: "a",
      seq: 10,
      timestamp: 0,
      duration_ns: 1000,
      series: { cpu: [8], count: [4], latency: [20], lastgc: [1700000000000] },
      min: { cpu: [2], count: [1], latency: [2], lastgc: [1700000000000] },
      max: { cpu: [12], count: [3], latency: [7], lastgc: [1700000000000] },
    }],
    runs: [{
      id: "a",
      aggregation: {
        cpu: { kinds: ["gauge"] },
        count: { kinds: ["delta"] },
        latency: { kinds: ["histogram"] },
        lastgc: { kinds: ["event"] },
      },
    }],
    watermarks: [{ run_id: "a", seq: 10 }],
  };
  const live = [
    { run_id: "a", seq: 10, timestamp: 8000, duration_ns: 1000, series: { cpu: [99], count: [99], latency: [100], lastgc: [1700000000000] } },
    { run_id: "a", seq: 11, timestamp: 9000, duration_ns: 1000, series: { cpu: [2], count: [3], latency: [105], lastgc: [1700000005000] } },
  ];

  const [point] = mergeHistoryAndLive(response, live, 0, 10000);
  assert.equal(point.source, "history+live-rollup");
  assert.equal(point.seq, 11);
  assert.deepEqual(point.series, {
    cpu: [5],
    count: [7],
    latency: [25],
    lastgc: [1700000005000],
  });
  assert.deepEqual(point.min.cpu, [2]);
  assert.deepEqual(point.max.cpu, [12]);
});

test("新运行尚无持久 Run 元数据时使用实时配置的 delta、histogram 和 event 语义", () => {
  const liveConfig = {
    history: { run_id: "new-run" },
    events: ["lastgc"],
    series: [
      {
        name: "count",
        type: "scatter",
        subplots: [{ name: "count", aggregation: "delta" }],
      },
      {
        name: "latency",
        type: "heatmap",
        aggregation: "histogram",
        buckets: [1],
      },
    ],
  };
  const live = [
    {
      run_id: "new-run",
      seq: 1,
      timestamp: 1000,
      duration_ns: 1000,
      series: { count: [2], latency: [100], lastgc: [1700000000000] },
    },
    {
      run_id: "new-run",
      seq: 2,
      timestamp: 2000,
      duration_ns: 1000,
      series: { count: [3], latency: [105], lastgc: [1700000005000] },
    },
  ];

  const [point] = mergeHistoryAndLive(
    { step_ms: 10000, points: [], runs: [], watermarks: [] },
    live,
    0,
    10000,
    liveRunFromConfig(liveConfig),
  );

  assert.deepEqual(point.series, {
    count: [5],
    latency: [5],
    lastgc: [1700000005000],
  });
  assert.equal(point.incomplete.latency[0], true);
  assert.equal(point.coverage_ns.latency[0], 1000);
});

test("持久 Run 聚合配置优先于同 run_id 的实时后备配置", () => {
  const response = {
    step_ms: 10000,
    points: [],
    runs: [{ id: "run", aggregation: { count: { kinds: ["gauge"] } } }],
  };
  const fallback = {
    id: "run",
    aggregation: { count: { kinds: ["delta"] } },
  };
  const [point] = mergeHistoryAndLive(response, [
    { run_id: "run", seq: 1, timestamp: 1000, duration_ns: 1, series: { count: [2] } },
    { run_id: "run", seq: 2, timestamp: 2000, duration_ns: 1, series: { count: [3] } },
  ], 0, 10000, fallback);

  assert.equal(point.series.count[0], 2.5);
});

test("累计直方图尾部没有基线时保留已提交值并明确标记不完整", () => {
  const response = {
    step_ms: 10000,
    points: [{
      run_id: "a",
      seq: 10,
      timestamp: 0,
      duration_ns: 1000,
      series: { latency: [20] },
      min: { latency: [2] },
      max: { latency: [7] },
    }],
    runs: [{ id: "a", aggregation: { latency: { kinds: ["histogram"] } } }],
    watermarks: [{ run_id: "a", seq: 10 }],
  };

  const [point] = mergeHistoryAndLive(response, [{
    run_id: "a",
    seq: 11,
    timestamp: 9000,
    duration_ns: 1000,
    series: { latency: [105] },
  }], 0, 10000);

  assert.equal(point.series.latency[0], 20);
  assert.equal(point.incomplete.latency[0], true);
  assert.equal(point.coverage_ns.latency[0], 1000);
  assert.match(
    envelopeSummary(
      { aggregate: true, summary: "聚合桶摘要" },
      2,
      7,
      point.incomplete.latency[0],
    ),
    /缺少累计基线，仅显示已提交部分/,
  );
});

test("累计直方图断序只合并可证明的差分并标记尾部不完整", () => {
  const response = {
    step_ms: 10000,
    points: [{
      run_id: "a",
      seq: 10,
      timestamp: 0,
      duration_ns: 1000,
      series: { latency: [20] },
    }],
    runs: [{ id: "a", aggregation: { latency: { kinds: ["histogram"] } } }],
    watermarks: [{ run_id: "a", seq: 10 }],
  };
  const live = [
    { run_id: "a", seq: 10, timestamp: 7000, duration_ns: 1000, series: { latency: [100] } },
    { run_id: "a", seq: 11, timestamp: 8000, duration_ns: 1000, series: { latency: [105] } },
    { run_id: "a", seq: 13, timestamp: 9000, duration_ns: 1000, series: { latency: [110] } },
  ];

  const [point] = mergeHistoryAndLive(response, live, 0, 10000);
  assert.equal(point.series.latency[0], 25);
  assert.equal(point.incomplete.latency[0], true);
  assert.equal(point.coverage_ns.latency[0], 2000);
});

test("运行切换、序号缺口和显式缺口都会断开曲线", () => {
  const runs = [{ id: "a", config_hash: "v1", config }];
  const entries = makeSchemaEntries(runs, null);
  const points = [
    { run_id: "a", seq: 1, timestamp: 1000, series: { cpu: [1] } },
    { run_id: "a", seq: 3, timestamp: 3000, series: { cpu: [3] } },
    { run_id: "a", seq: 4, timestamp: 5000, series: { cpu: [4] } },
  ];
  const gaps = [{ run_id: "a", from: 3500, to: 4500 }];
  const chart = buildChartData(points, entries, gaps);

  assert.deepEqual(chart.series.get("v1:cpu")[0], [1, null, 3, null, 4]);
  assert.equal(chart.times.get("v1").length, 5);
});

test("聚合摘要保留主值、最小值和最大值", () => {
  const entries = makeSchemaEntries([{ id: "a", config_hash: "v1", config }], null);
  const chart = buildChartData(
    [{
      run_id: "a",
      seq: 1,
      timestamp: 1000,
      series: { cpu: [2] },
      min: { cpu: [1] },
      max: { cpu: [4] },
    }],
    entries,
  );
  assert.deepEqual(chart.series.get("v1:cpu")[0], [2]);
  assert.deepEqual(chart.minimums.get("v1:cpu")[0], [1]);
  assert.deepEqual(chart.maximums.get("v1:cpu")[0], [4]);
});

test("聚合桶序号自然跨越时不会被误判为缺口", () => {
  const entries = makeSchemaEntries([{ id: "a", config_hash: "v1", config }], null);
  const chart = buildChartData([
    { run_id: "a", seq: 10, timestamp: 1000, aggregate: true, series: { cpu: [1] } },
    { run_id: "a", seq: 20, timestamp: 2000, aggregate: true, series: { cpu: [2] } },
  ], entries);
  assert.deepEqual(chart.series.get("v1:cpu")[0], [1, 2]);
});

test("fallback 实时配置绑定当前运行，不会再追加一套重复图", () => {
  const liveConfig = { ...config, history: { run_id: "live-run" } };
  const entries = makeSchemaEntries([], liveConfig);
  assert.equal(entries.length, 1);
  assert.deepEqual([...entries[0].runIds], ["live-run"]);
  assert.equal(entries.some((entry) => entry.runIds.has("live-run")), true);
});

test("手输单点跨运行查询不带 run_id，图表点击才限定所属运行", () => {
  const points = [{ run_id: 2, seq: 1, timestamp: 20000, series: { cpu: [2] } }];
  const runs = [{ id: 2, config_hash: "run-2" }];

  assert.equal(runIdForPointRequest(points, runs, 1000, null), undefined);
  assert.equal(runIdForPointRequest(points, runs, 20000, "run-2"), 2);
});

test("聚合事件保留真实 Unixms，不把标志值画成时间或密度", () => {
  const eventConfig = { ...config, events: ["lastgc"] };
  const entries = makeSchemaEntries([{ id: "a", config_hash: "v1", config: eventConfig }]);
  const chart = buildChartData([{
    run_id: "a",
    seq: 20,
    timestamp: 1735689600000,
    aggregate: true,
    series: { cpu: [1], lastgc: [1735689500000] },
  }], entries);

  assert.equal(chart.events.get("lastgc")[0].getTime(), 1735689500000);
  assert.equal(chart.events.get("lastgc")[0].density, undefined);

  const flags = buildChartData([{
    run_id: "a",
    seq: 21,
    timestamp: 1735689600000,
    aggregate: true,
    series: { cpu: [1], lastgc: [1] },
  }], entries);
  assert.deepEqual(flags.events.get("lastgc"), []);
});

test("长范围坐标显示日期，聚合摘要包含最小值和最大值", () => {
  const from = Date.UTC(2026, 0, 1);
  assert.equal(timeTickFormat([from, from + 7 * 86400000]), "%Y-%m-%d\n%H:%M");
  assert.equal(timeTickFormat([from, from + 60000]), "%H:%M:%S");
  assert.equal(
    envelopeSummary({ aggregate: true, summary: "聚合桶摘要" }, 1, 4),
    "聚合桶摘要<br><b>最小值</b>: 1<br><b>最大值</b>: 4",
  );
});

test("不同配置的运行保留各自热力图桶结构", () => {
  const oldConfig = {
    events: [],
    series: [{ name: "heat", type: "heatmap", buckets: [0, 1] }],
  };
  const newConfig = {
    events: [],
    series: [{ name: "heat", type: "heatmap", buckets: [0, 1, 2] }],
  };
  const entries = makeSchemaEntries([
    { id: "old", config_hash: "old-hash", config: oldConfig },
    { id: "new", config_hash: "new-hash", config: newConfig },
  ]);
  const chart = buildChartData([
    { run_id: "old", seq: 1, timestamp: 1000, series: { heat: [1, 2] } },
    { run_id: "new", seq: 1, timestamp: 2000, series: { heat: [3, 4, 5] } },
  ], entries);

  assert.equal(chart.series.get("old-hash:heat").length, 2);
  assert.equal(chart.series.get("new-hash:heat").length, 3);
});
