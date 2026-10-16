import * as theme from "./theme.js";

export let running = true;
export let gcEnabled = true;
export let timerange = 60;

let initialized = false;
let currentCallbacks = null;

export function updateVisibility(plotManager = currentCallbacks?.getPlotManager?.()) {
  if (!plotManager) return;
  const tagInputs = [...document.querySelectorAll("#navCategories input[data-tag]")];
  const searchInput = document.getElementById("plot-search");
  const activeTags = tagInputs
    .filter((input) => input.checked)
    .map((input) => input.dataset.tag);
  const query = searchInput?.value.trim() || "";

  for (const plot of plotManager.plots) {
    plot.setVisible(
      activeTags.some((tag) => plot.hasTag(tag)) && plot.matches(query),
    );
  }
}

export function initNav(callbacks) {
  currentCallbacks = callbacks;
  if (initialized) {
    updateVisibility();
    return;
  }
  initialized = true;

  const gcButton = document.getElementById("btn-gc-events");
  gcButton.checked = gcEnabled;
  gcButton.addEventListener("change", () => {
    gcEnabled = gcButton.checked;
    callbacks.onRedraw(true);
  });

  const playButton = document.getElementById("btn-play");
  playButton.checked = running;
  playButton.addEventListener("change", () => {
    running = playButton.checked;
    callbacks.onPause(!running);
  });

  const themeButton = document.getElementById("btn-darkmode");
  themeButton.addEventListener("change", () => {
    const newTheme = theme.getThemeMode() === "dark" ? "light" : "dark";
    localStorage.setItem("theme-mode", newTheme);
    theme.updateThemeMode();
    for (const plot of callbacks.getPlotManager()?.plots || []) plot.updateTheme();
    callbacks.onRedraw(true);
  });

  const tagInputs = [...document.querySelectorAll("#navCategories input[data-tag]")];
  tagInputs.forEach((input) => (input.checked = true));
  for (const input of tagInputs) {
    input.addEventListener("click", (event) => {
      if (!event.altKey) return;
      event.preventDefault();
      setTimeout(() => {
        const isSolo = tagInputs.every((item) => item.checked === (item === input));
        tagInputs.forEach((item) => (item.checked = isSolo || item === input));
        updateVisibility();
        callbacks.onVisibility();
      });
    });
    input.addEventListener("change", () => {
      updateVisibility();
      callbacks.onVisibility();
    });
  }

  document.getElementById("plot-search").addEventListener("input", () => {
    for (const plot of callbacks.getPlotManager()?.plots || []) {
      if (plot.isMaximized()) plot.minimize();
    }
    updateVisibility();
    callbacks.onVisibility();
  });

  const rangeInputs = [...document.querySelectorAll('input[name="range"]')];
  for (const input of rangeInputs) {
    input.addEventListener("change", () => {
      if (!input.checked || input.value === "custom") return;
      timerange = Number(input.value);
      callbacks.onPreset(timerange);
    });
  }
  updateVisibility();
}
