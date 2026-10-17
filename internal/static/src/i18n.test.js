import assert from "node:assert/strict";
import { existsSync, readFileSync, readdirSync } from "node:fs";
import test from "node:test";
import {
  errorMessage, getLanguage, languageStorageKey, onLanguageChange,
  resolveLanguage, setLanguage, t,
} from "./i18n.js";
import { HistoryUnavailableError } from "./historyClient.js";
import { envelopeSummary } from "./historyData.js";
import { localizePlotConfig, plotInfo, plotLabels } from "./plotTranslations.js";
import { zh } from "./translations.js";

test("已保存语言优先，浏览器中英文地区码可识别，其他语言回退英文", () => {
  assert.equal(resolveLanguage("en", ["zh-CN"]), "en");
  assert.equal(resolveLanguage(null, ["fr-FR", "zh-TW", "en-US"]), "zh");
  assert.equal(resolveLanguage("invalid", ["en-GB"]), "en");
  assert.equal(resolveLanguage(null, ["ja-JP"]), "en");
});

test("切换语言保存偏好并通知界面；禁用存储时仍能切换", () => {
  const previous = getLanguage();
  const storage = Object.getOwnPropertyDescriptor(globalThis, "localStorage");
  const saved = new Map();
  const changes = [];
  const unsubscribe = onLanguageChange((language) => changes.push(language));
  try {
    Object.defineProperty(globalThis, "localStorage", {
      configurable: true,
      value: { setItem: (key, value) => saved.set(key, value) },
    });
    setLanguage("en");
    changes.length = 0;
    setLanguage("zh");
    assert.equal(saved.get(languageStorageKey), "zh");
    assert.deepEqual(changes, ["zh"]);
    assert.equal(t("History loaded; {count} samples dropped in total", { count: 3 }), "历史已加载，累计丢弃 3 个样本");
    Object.defineProperty(globalThis, "localStorage", { configurable: true, get() { throw new Error("blocked"); } });
    assert.doesNotThrow(() => setLanguage("en"));
    assert.equal(t("View history"), "View history");
    assert.equal(t("constructor", {}, "zh"), "constructor");
  } finally {
    unsubscribe();
    if (storage) Object.defineProperty(globalThis, "localStorage", storage);
    else delete globalThis.localStorage;
    setLanguage(previous);
  }
});

test("当前错误和历史悬浮内容可在同一份数据上切换语言", () => {
  const previous = getLanguage();
  try {
    setLanguage("zh");
    const error = new HistoryUnavailableError("History endpoint unavailable");
    assert.equal(errorMessage(error), "历史接口不可用");
    assert.equal(errorMessage(new Error("from must be less than to")), "起始时间必须早于结束时间");
    assert.equal(errorMessage(new Error("at is required")), "at 为必填项");
    const metadata = { aggregate: true };
    assert.match(envelopeSummary(metadata, null, 7, true), /最小值.*缺失.*缺少累计基线/);
    setLanguage("en");
    assert.equal(errorMessage(error), "History endpoint unavailable");
    assert.match(envelopeSummary(metadata, null, 7, true), /Minimum.*Missing.*Missing cumulative baseline/);
    assert.equal(envelopeSummary(null), "Gap");
    assert.equal(envelopeSummary({ aggregate: false }), "Raw sample");
  } finally {
    setLanguage(previous);
  }
});

test("图表翻译保留 CPU、GC、指标路径和数据标识，切回英文恢复原文", () => {
  const source = {
    name: "cpu-scavenger", title: "CPU (Scavenger)", type: "bar",
    infoText: "Original description", metrics: ["/cpu/classes/scavenge/assist:cpu-seconds"],
    layout: { yaxis: { title: "cpu-seconds / second" } },
    subplots: [{ name: "assist", unitfmt: "%{y:.4s}s" }],
  };
  const original = structuredClone(source);
  const translated = localizePlotConfig(source, "zh");
  assert.equal(translated.title, "CPU（内存归还）");
  assert.equal(translated.subplots[0].name, "辅助归还");
  assert.match(translated.infoText, /GC 内存归还器/);
  assert.match(translated.infoText, /\/cpu\/classes\/scavenge\/assist/);
  assert.equal(translated.name, source.name);
  assert.deepEqual(translated.metrics, source.metrics);
  assert.equal(translated.subplots[0].unitfmt, source.subplots[0].unitfmt);
  assert.deepEqual(source, original);
  assert.deepEqual(localizePlotConfig(source, "en"), original);
  const custom = { ...source, name: "my-custom-plot" };
  assert.deepEqual(localizePlotConfig(custom, "zh"), custom);
});

test("所有静态界面翻译键均有中文译文", () => {
  const html = readFileSync(new URL("../index.html", import.meta.url), "utf8");
  for (const [, key] of html.matchAll(/data-i18n(?:-title|-placeholder|-aria-label)?="([^"]+)"/g)) {
    assert.ok(Object.hasOwn(zh, key), `缺少界面翻译：${key}`);
  }
});

const plotsDir = new URL("../../plot/", import.meta.url);
test("所有内置图表说明、标题、图例均有中文译文", {
  skip: !existsSync(plotsDir) && "独立前端容器未挂载 Go 源码；完整仓库中校验图表翻译覆盖率",
}, () => {
  for (const name of readdirSync(plotsDir).filter((name) => /^plot_.*\.go$/.test(name))) {
    const source = readFileSync(new URL(name, plotsDir), "utf8");
    const plotName = source.match(/Name:\s*"([^"]+)"/)?.[1];
    assert.ok(Object.hasOwn(plotInfo, plotName), `缺少图表说明：${plotName}`);
    for (const [, label] of source.matchAll(/(?:Title|YName|ZName):\s*"([^"]+)"/g)) {
      assert.ok(Object.hasOwn(plotLabels, label), `缺少图表文案：${label}`);
    }
    const subplotNames = [...source.matchAll(/Name:\s*"([^"]+)"/g)].slice(1);
    for (const [, label] of subplotNames) {
      assert.ok(Object.hasOwn(plotLabels, label), `缺少图例翻译：${label}`);
    }
  }
});
