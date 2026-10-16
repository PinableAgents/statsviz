import test from "node:test";
import assert from "node:assert/strict";
import {
  DebouncedRangeQuery,
  canAutoRefreshRange,
  clearPresetSelection,
  isFollowingLive,
  pausedWindowBounds,
  sliderBounds,
} from "./historyState.js";

test("固定历史和暂停画面不会被实时配置或重连刷新替换", () => {
  assert.equal(isFollowingLive({ mode: "follow", paused: false }), true);
  assert.equal(isFollowingLive({ mode: "follow", paused: true }), false);
  assert.equal(isFollowingLive({ mode: "history", paused: false }), false);
});

test("刷新 meta 会推进滑块和后移操作可用的上限", () => {
  const first = sliderBounds({ from: 1000, to: 10000, interval_ms: 500 }, 3000, 9000);
  const refreshed = sliderBounds({ from: 1000, to: 14000, interval_ms: 500 }, 3000, 13000);
  assert.deepEqual(first, { min: 4000, max: 10000, value: 9000, step: 1000 });
  assert.deepEqual(refreshed, { min: 4000, max: 14000, value: 13000, step: 1000 });
});

test("新交互会立即取消旧请求，再在防抖后发起查询", async () => {
  const calls = [];
  const query = new DebouncedRangeQuery(
    () => calls.push("cancel"),
    (interactive) => calls.push(`run:${interactive}`),
    5,
  );

  query.schedule(true);
  assert.deepEqual(calls, ["cancel"]);
  await new Promise((resolve) => setTimeout(resolve, 15));
  assert.deepEqual(calls, ["cancel", "run:true"]);
});

test("自动刷新不会打断在途请求，并按尝试时间节流", () => {
  assert.equal(canAutoRefreshRange({ inFlight: {}, lastAttemptAt: 0, now: 20000, refreshMs: 10000 }), false);
  assert.equal(canAutoRefreshRange({ inFlight: null, lastAttemptAt: 15000, now: 20000, refreshMs: 10000 }), false);
  assert.equal(canAutoRefreshRange({ inFlight: null, lastAttemptAt: 9000, now: 20000, refreshMs: 10000 }), true);
});

test("进入自定义或缩放范围会取消预设高亮", () => {
  const inputs = [{ checked: false }, { checked: true }, { checked: false }];
  clearPresetSelection(inputs);
  assert.deepEqual(inputs.map((input) => input.checked), [false, false, false]);
});

test("暂停跟随从 1m 切换到 5m 保持结束时间且后续 WS 不推移", () => {
  const end = 1700000000000;
  const bounds = pausedWindowBounds(end, 5 * 60 * 1000);
  const pausedView = { mode: "follow", paused: true };

  assert.equal(bounds.to - bounds.from, 300000);
  assert.equal(bounds.to, end);
  assert.equal(isFollowingLive(pausedView), false);
});
