import test from "node:test";
import assert from "node:assert/strict";
import HistoryClient, {
  HistoryUnavailableError,
  LatestRequest,
  StaleRequestError,
  buildHTTPBase,
} from "./historyClient.js";

const jsonResponse = (payload) => ({
  ok: true,
  status: 200,
  headers: { get: () => "application/json" },
  json: async () => payload,
});

test("HTTP 根地址从自定义 WebSocket 地址派生", () => {
  assert.equal(
    buildHTTPBase(
      "wss://example.test/custom/stats/ws?token=x",
      "https://ignored.test/debug/statsviz/",
    ).href,
    "https://example.test/custom/stats/",
  );
  assert.equal(
    buildHTTPBase(
      "ws://example.test/custom/stats/ws/",
      "https://ignored.test/debug/statsviz/",
    ).href,
    "http://example.test/custom/stats/",
  );
});

test("新请求取消旧请求且旧结果不能覆盖", async () => {
  const pending = [];
  const request = new LatestRequest((url, options) =>
    new Promise((resolve) => pending.push({ url, options, resolve })),
  );

  const first = request.json("https://example.test/first");
  const second = request.json("https://example.test/second");
  assert.equal(pending[0].options.signal.aborted, true);

  pending[1].resolve(jsonResponse({ value: 2 }));
  assert.deepEqual(await second, { value: 2 });
  pending[0].resolve(jsonResponse({ value: 1 }));
  await assert.rejects(first, StaleRequestError);
});

test("默认 fetch 使用 globalThis 作为 receiver", async () => {
  const originalFetch = globalThis.fetch;
  globalThis.fetch = function () {
    assert.equal(this, globalThis);
    return Promise.resolve(jsonResponse({ receiver: "globalThis" }));
  };
  try {
    const request = new LatestRequest();
    assert.deepEqual(await request.json("https://example.test/meta"), {
      receiver: "globalThis",
    });
  } finally {
    globalThis.fetch = originalFetch;
  }
});

test("显式取消会使不遵守 AbortSignal 的解码结果失效", async () => {
  let finishDecode;
  const request = new LatestRequest(async () => ({
    ok: true,
    status: 200,
    headers: { get: () => "application/json" },
    json: () => new Promise((resolve) => {
      finishDecode = resolve;
    }),
  }));

  const pending = request.json("https://example.test/slow-decode");
  await Promise.resolve();
  request.cancel();
  finishDecode({ stale: true });
  await assert.rejects(pending, StaleRequestError);
});

test("切换视窗取消慢单点解码后不会应用旧结果，默认容差交给服务端", async () => {
  let finishDecode;
  let requestedURL;
  const client = new HistoryClient(new URL("https://example.test/debug/statsviz/"), (url) => {
    requestedURL = url;
    return Promise.resolve({
      ok: true,
      status: 200,
      headers: { get: () => "application/json" },
      json: () => new Promise((resolve) => {
        finishDecode = resolve;
      }),
    });
  });
  let applied = false;
  const pending = client.point({ at: 1234 }).then(() => {
    applied = true;
  });
  await Promise.resolve();

  assert.equal(requestedURL.searchParams.has("tolerance_ms"), false);
  client.cancelPoint();
  finishDecode({ found: true, sample: { timestamp: 1234 } });
  await assert.rejects(pending, StaleRequestError);
  assert.equal(applied, false);
});

test("调用方显式提供单点容差时仍会传给服务端", async () => {
  let requestedURL;
  const client = new HistoryClient(new URL("https://example.test/debug/statsviz/"), (url) => {
    requestedURL = url;
    return Promise.resolve(jsonResponse({ found: false }));
  });

  await client.point({ at: 1234, runId: 7, toleranceMs: 9000 });
  assert.equal(requestedURL.searchParams.get("run_id"), "7");
  assert.equal(requestedURL.searchParams.get("tolerance_ms"), "9000");
});

test("404 或 HTML 响应被识别为旧服务", async () => {
  const request = new LatestRequest(async () => ({
    ok: false,
    status: 404,
    headers: { get: () => "text/html" },
  }));
  await assert.rejects(
    request.json("https://example.test/history/meta"),
    HistoryUnavailableError,
  );
});
