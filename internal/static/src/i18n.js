import { zh } from "./translations.js";

export const languageStorageKey = "statsviz-language";
const listeners = new Set();

const supportedLanguage = (value) => {
  if (typeof value !== "string") return null;
  const language = value.toLowerCase().split(/[-_]/)[0];
  return language === "en" || language === "zh" ? language : null;
};

export const resolveLanguage = (saved, preferred = []) =>
  supportedLanguage(saved) || preferred.map(supportedLanguage).find(Boolean) || "en";

const savedLanguage = () => {
  try {
    return globalThis.localStorage?.getItem(languageStorageKey);
  } catch {
    return null;
  }
};

let language = resolveLanguage(
  savedLanguage(),
  globalThis.navigator?.languages || [globalThis.navigator?.language],
);

export const getLanguage = () => language;
export const getLocale = () => language === "zh" ? "zh-CN" : "en-US";

export const t = (message, values = {}, targetLanguage = language) => {
  const template = targetLanguage === "zh" && Object.hasOwn(zh, message) ? zh[message] : message;
  return template.replace(/\{(\w+)\}/g, (match, key) =>
    Object.hasOwn(values, key) ? String(values[key]) : match,
  );
};

export const applyTranslations = (root = globalThis.document) => {
  if (!root) return;
  for (const [attribute, target] of [
    ["data-i18n", null],
    ["data-i18n-title", "title"],
    ["data-i18n-placeholder", "placeholder"],
    ["data-i18n-aria-label", "aria-label"],
  ]) {
    for (const element of root.querySelectorAll(`[${attribute}]`)) {
      const value = t(element.getAttribute(attribute));
      if (target) element.setAttribute(target, value);
      else element.textContent = value;
    }
  }
  if (root.documentElement) root.documentElement.lang = getLocale();
  for (const select of root.querySelectorAll("[data-language-select]")) select.value = language;
};

export const setLanguage = (value) => {
  const next = supportedLanguage(value) || "en";
  try {
    globalThis.localStorage?.setItem(languageStorageKey, next);
  } catch {
    // 浏览器禁用存储时，语言选择仍在当前页面生效。
  }
  if (next === language) return;
  language = next;
  applyTranslations();
  for (const listener of listeners) listener(language);
};

export const onLanguageChange = (listener) => {
  listeners.add(listener);
  return () => listeners.delete(listener);
};

export const initI18n = () => {
  applyTranslations();
  globalThis.document?.querySelectorAll("[data-language-select]").forEach((select) => {
    select.addEventListener("change", (event) => setLanguage(event.target.value));
  });
};

export const errorMessage = (error, fallback) => {
  if (error?.translationKey) return t(error.translationKey, error.translationValues);
  const message = error?.message;
  if (!message) return t(fallback);
  const parameter = message.match(/^(\w+) (is required|must be an integer|must be a non-negative integer)$/);
  if (language === "zh" && parameter) {
    const reason = {
      "is required": "为必填项",
      "must be an integer": "必须为整数",
      "must be a non-negative integer": "必须为非负整数",
    }[parameter[2]];
    return `${parameter[1]} ${reason}`;
  }
  return t(message);
};
