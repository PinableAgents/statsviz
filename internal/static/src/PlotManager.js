import { Plot, createVerticalLines } from "./plot.js";

const debounce = (fn, delay) => {
  let timer = null;
  const wrapped = (...args) => {
    clearTimeout(timer);
    timer = setTimeout(() => fn(...args), delay);
  };
  wrapped.cancel = () => clearTimeout(timer);
  return wrapped;
};

const MAX_EVENT_LINES = 120;

const limitedEvents = (events) => {
  if (events.length <= MAX_EVENT_LINES) return events;
  const step = Math.ceil(events.length / MAX_EVENT_LINES);
  return events.filter((_, index) => index % step === 0).slice(-MAX_EVENT_LINES);
};

export default class PlotManager {
  #shapesCache = new Map();
  #lastGcEnabled = null;
  #staggerHandle = null;
  #resizeHandler;
  #rangeHandler;
  #cursorAt = null;
  #disposed = false;

  constructor(schemaEntries, handlers = {}) {
    this.container = document.getElementById("plots");
    const multipleSchemas = schemaEntries.length > 1;
    this.#rangeHandler = debounce((range) => handlers.onRange?.(range), 220);
    this.plots = schemaEntries.flatMap((entry, entryIndex) =>
      (entry.config.series || []).map((plot, plotIndex) => {
        const suffix = multipleSchemas
          ? ` · ${entry.startedAt ? new Date(entry.startedAt).toLocaleString() : entry.key.slice(0, 8)}`
          : "";
        return new Plot(
          structuredClone(plot),
          entry.key,
          `plot-${entryIndex}-${plotIndex}`,
          suffix,
          {
            onPoint: handlers.onPoint,
            onRange: (range) => this.#rangeHandler(range),
          },
        );
      }),
    );
    this.#attach();

    this.#resizeHandler = debounce(() => this.#resizeAll(), 100);
    window.addEventListener("resize", this.#resizeHandler);
    requestAnimationFrame(() => this.#resizeAll());
  }

  #attach() {
    this.container.replaceChildren();
    for (const plot of this.plots) {
      const div = document.createElement("div");
      div.id = plot.name();
      div.className = "plot-card";
      this.container.appendChild(div);
      plot.createElement(div);
    }
  }

  update(data, gcEnabled, range, force = false) {
    if (this.#disposed) return;
    const shapes = new Map();
    if (gcEnabled) {
      const gcStateChanged = this.#lastGcEnabled !== gcEnabled;
      for (const [name, fullSeries] of data.events) {
        const eventSeries = limitedEvents(fullSeries);
        const cached = this.#shapesCache.get(name);
        const eventsChanged =
          !cached ||
          cached.eventCount !== fullSeries.length ||
          cached.lastEvent !== fullSeries.at(-1)?.getTime();
        if (gcStateChanged || eventsChanged) {
          const lines = createVerticalLines(eventSeries);
          this.#shapesCache.set(name, {
            lines,
            eventCount: fullSeries.length,
            lastEvent: fullSeries.at(-1)?.getTime(),
          });
        }
        shapes.set(name, this.#shapesCache.get(name)?.lines || []);
      }
    } else if (this.#lastGcEnabled !== false) {
      this.#shapesCache.clear();
    }
    this.#lastGcEnabled = gcEnabled;

    if (this.#staggerHandle !== null) cancelAnimationFrame(this.#staggerHandle);
    const visiblePlots = this.plots.filter((plot) => plot.isVisible());
    let index = 0;
    const processBatch = () => {
      const start = performance.now();
      while (index < visiblePlots.length && performance.now() - start < 12) {
        visiblePlots[index].update(range, data, shapes, force, this.#cursorAt);
        index++;
      }
      if (index < visiblePlots.length) {
        this.#staggerHandle = requestAnimationFrame(processBatch);
      } else {
        this.#staggerHandle = null;
      }
    };
    processBatch();
  }

  setCursor(timestamp) {
    this.#cursorAt = timestamp;
  }

  #resizeAll() {
    for (const plot of this.plots) {
      if (!plot.isVisible()) continue;
      plot.resize();
    }
  }

  dispose() {
    this.#disposed = true;
    if (this.#staggerHandle !== null) cancelAnimationFrame(this.#staggerHandle);
    this.#rangeHandler.cancel();
    this.#resizeHandler?.cancel();
    window.removeEventListener("resize", this.#resizeHandler);
    for (const plot of this.plots) plot.dispose();
    this.plots = [];
    this.container.replaceChildren();
  }
}
