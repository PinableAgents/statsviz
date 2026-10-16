import * as theme from "./theme.js";
import {
  defaultPlotHeight,
  newConfigObject,
  newLayoutObject,
  themeColors,
} from "./plotConfig.js";
import { formatFunction } from "./utils.js";
import { envelopeSummary, timeTickFormat } from "./historyData.js";
import Plotly from "plotly.js-cartesian-dist";

// Bucket edges are stored in cfg.custom_data.
const heatmapBucketLabel = (bucket, edges, formatYUnit) => {
  if (bucket === 0) {
    return `(-Inf, ${formatYUnit(edges[bucket])})`;
  }
  if (bucket === edges.length - 1) {
    return `[${formatYUnit(edges[bucket])}, +Inf)`;
  }
  return `[${formatYUnit(edges[bucket - 1])}, ${formatYUnit(edges[bucket])})`;
};

const plotsDiv = document.getElementById("plots");

class Plot {
  #htmlElt;
  #plotlyLayout;
  #plotlyConfig;
  #lastData;
  #updateCount;
  #maximized;
  #cfg;
  #dataTemplate;
  #cachedWidth;
  #inViewport = false;
  #observer;
  #bucketLabels;
  #heatmapHoverText;
  #heatmapHoverCols;
  #schemaKey;
  #instanceName;
  #titleSuffix;
  #handlers;
  #lastRange;

  constructor(cfg, schemaKey, instanceName, titleSuffix = "", handlers = {}) {
    cfg.layout.paper_bgcolor = themeColors[theme.getThemeMode()].paper_bgcolor;
    cfg.layout.plot_bgcolor = themeColors[theme.getThemeMode()].plot_bgcolor;
    cfg.layout.font_color = themeColors[theme.getThemeMode()].font_color;

    this.#maximized = false;
    this.#cfg = cfg;
    this.#schemaKey = schemaKey;
    this.#instanceName = instanceName;
    this.#titleSuffix = titleSuffix;
    this.#handlers = handlers;
    this.#updateCount = 0;
    this.#dataTemplate = [];
    this.#lastData = [{ x: new Date() }];
    this.#cachedWidth = null;

    if (this.#cfg.type == "heatmap") {
      const hover = this.#cfg.hover;
      const formatYUnit = formatFunction(hover.yunit);
      const edges = this.#cfg.custom_data;
      this.#bucketLabels = edges.map((_, y) =>
        heatmapBucketLabel(y, edges, formatYUnit),
      );
      this.#heatmapHoverCols = -1;
      this.#dataTemplate.push({
        type: "heatmap",
        x: null,
        y: this.#cfg.buckets,
        z: null,
        showlegend: false,
        colorscale: this.#cfg.colorscale,
        hoverongaps: false,
        hovertemplate:
          `<b>${hover.yname}</b>: %{text}<br>` +
          `<b>${hover.zname}</b>: %{z}<br>%{customdata}<extra></extra>`,
      });
    } else {
      this.#dataTemplate = this.#cfg.subplots.map((subplot) => {
        return {
          type: this.#cfg.type,
          x: null,
          y: null,
          name: subplot.name,
          hovertemplate: `<b>${subplot.unitfmt}</b><br>%{customdata}<extra></extra>`,
        };
      });
    }

    this.#plotlyLayout = newLayoutObject(cfg, this.#maximized);
    this.#plotlyLayout.title.text += this.#titleSuffix;
    this.#plotlyConfig = newConfigObject(cfg, this.#maximized);
  }

  name() {
    return this.#instanceName;
  }

  sourceName() {
    return this.#cfg.name;
  }

  ownsElement(element) {
    return this.#htmlElt === element;
  }

  schemaKey() {
    return this.#schemaKey;
  }

  hasTag(tag) {
    return (this.#cfg.tags || []).includes(tag);
  }

  matches(query) {
    if (!query) return true;
    if (!this.#cfg.metrics) return false;
    const q = query.toLowerCase();
    return this.#cfg.metrics.some((m) => m.toLowerCase().includes(q));
  }

  setVisible(visible) {
    this.#htmlElt.hidden = !visible;
  }

  isVisible() {
    return !this.#htmlElt.hidden;
  }

  createElement(div) {
    this.#htmlElt = div;

    this.#observer = new IntersectionObserver((entries) => {
      entries.forEach((entry) => {
        this.#inViewport = entry.isIntersecting;
        if (this.#inViewport) {
          this.#react();
        }
      });
    });
    this.#observer.observe(this.#htmlElt);

    // Measure the final CSS width.
    this.#cachedWidth = div.clientWidth;
    this.#plotlyLayout.width = this.#cachedWidth;
    this.#plotlyLayout.height = defaultPlotHeight;

    // Pass a single data with no data to create an empty plot (this removes
    // the 'bad time formatting' warning at startup).

    Plotly.newPlot(
      this.#htmlElt,
      this.#lastData,
      this.#plotlyLayout,
      this.#plotlyConfig,
    );

    this.#htmlElt.on("plotly_click", (event) => {
      const timestamp = new Date(event.points?.[0]?.x).getTime();
      if (Number.isFinite(timestamp)) {
        this.#handlers.onPoint?.(timestamp, this.#schemaKey);
      }
    });
    this.#htmlElt.on("plotly_relayout", (event) => {
      const from = new Date(event["xaxis.range[0]"]).getTime();
      const to = new Date(event["xaxis.range[1]"]).getTime();
      if (!Number.isFinite(from) || !Number.isFinite(to) || to <= from) return;
      if (
        this.#lastRange &&
        Math.abs(from - this.#lastRange[0]) < 2 &&
        Math.abs(to - this.#lastRange[1]) < 2
      ) {
        return;
      }
      this.#handlers.onRange?.([from, to]);
    });

    this.#htmlElt.infoText = (this.#cfg.infoText || "")
      .split("\n")
      .map((line) => `<p>${line}</p>`)
      .join("");
  }

  #hovertextGrid(nCols) {
    if (this.#heatmapHoverText && this.#heatmapHoverCols === nCols) {
      return this.#heatmapHoverText;
    }
    this.#heatmapHoverCols = nCols;
    this.#heatmapHoverText = this.#bucketLabels.map((label) =>
      Array(nCols).fill(label),
    );
    return this.#heatmapHoverText;
  }

  #extractData(data) {
    const key = `${this.#schemaKey}:${this.#cfg.name}`;
    const times = data.times.get(this.#schemaKey) || [];
    const metadata = data.metadata.get(this.#schemaKey) || [];
    const serie = data.series.get(key) || [];
    const minimums = data.minimums.get(key) || [];
    const maximums = data.maximums.get(key) || [];

    if (this.#cfg.type == "heatmap") {
      this.#dataTemplate[0].x = times;
      this.#dataTemplate[0].z = serie;
      this.#dataTemplate[0].text = this.#hovertextGrid(times.length);
      this.#dataTemplate[0].customdata = this.#bucketLabels.map((_, row) =>
        metadata.map((item, column) =>
          envelopeSummary(
            item,
            minimums[row]?.[column],
            maximums[row]?.[column],
            item?.incomplete?.[this.#cfg.name]?.[row],
          ),
        ),
      );
      this.#dataTemplate[0].colorbar = { len: "350", lenmode: "pixels" };
    } else {
      for (let i = 0; i < this.#dataTemplate.length; i++) {
        const values = serie[i] || [];
        const mins = minimums[i] || [];
        const maxs = maximums[i] || [];
        this.#dataTemplate[i].x = times;
        this.#dataTemplate[i].y = values;
        this.#dataTemplate[i].customdata = metadata.map((item, index) =>
          envelopeSummary(
            item,
            mins[index],
            maxs[index],
            item?.incomplete?.[this.#cfg.name]?.[i],
          ),
        );
        const hasEnvelope = mins.some((value) => value !== null) || maxs.some((value) => value !== null);
        this.#dataTemplate[i].error_y = hasEnvelope
          ? {
              type: "data",
              symmetric: false,
              array: values.map((value, index) =>
                value === null || maxs[index] === null ? 0 : Math.max(0, maxs[index] - value),
              ),
              arrayminus: values.map((value, index) =>
                value === null || mins[index] === null ? 0 : Math.max(0, value - mins[index]),
              ),
              thickness: 0.7,
              width: 0,
              visible: true,
            }
          : undefined;

        this.#dataTemplate[i].stackgroup = this.#cfg.subplots[i].stackgroup;
        this.#dataTemplate[i].hoveron = this.#cfg.subplots[i].hoveron;
        this.#dataTemplate[i].type =
          this.#cfg.subplots[i].type || this.#cfg.type;
        this.#dataTemplate[i].marker = {
          color: this.#cfg.subplots[i].color,
        };
      }
    }
    return this.#dataTemplate;
  }

  update(xrange, data, shapes, force, cursorAt) {
    this.#lastData = this.#extractData(data);
    this.#updateCount++;
    if (
      force ||
      this.#cfg.updateFreq == 0 ||
      this.#updateCount % this.#cfg.updateFreq == 0
    ) {
      // Update layout with vertical shapes if necessary.
      if (this.#cfg.events != "") {
        this.#plotlyLayout.shapes = [...(shapes.get(this.#cfg.events) || [])];
      } else {
        this.#plotlyLayout.shapes = [];
      }
      if (cursorAt !== null) {
        this.#plotlyLayout.shapes.push({
          type: "line",
          x0: cursorAt,
          x1: cursorAt,
          yref: "paper",
          y0: 0,
          y1: 1,
          line: { color: "#dc3545", width: 1.5, dash: "dot" },
        });
      }

      // Move the xaxis time range.
      this.#plotlyLayout.xaxis.range = xrange;
      this.#lastRange = xrange;
      this.#plotlyLayout.xaxis.tickformat = timeTickFormat(xrange);

      if (this.#maximized) {
        this.#plotlyConfig.responsive = true;
      } else {
        this.#plotlyLayout.height = defaultPlotHeight;
        this.#plotlyConfig.responsive = false;
      }

      // Use cached width - only recalculated on resize
      this.#plotlyLayout.width = this.#cachedWidth;

      if (this.#inViewport) {
        this.#react();
      }
    }
  }

  isMaximized() {
    return this.#maximized;
  }

  maximize() {
    this.#maximized = true;
    const plotsDiv = document.getElementById("plots", this.#maximized);

    this.#plotlyLayout = newLayoutObject(this.#cfg, this.#maximized);
    this.#plotlyLayout.title.text += this.#titleSuffix;
    this.#plotlyConfig = newConfigObject(this.#cfg, this.#maximized);

    this.#cachedWidth = plotsDiv.clientWidth;
    this.#plotlyLayout.width = this.#cachedWidth;
    this.#plotlyLayout.height = plotsDiv.parentElement.clientHeight - 50;

    this.#react();
  }

  minimize() {
    this.#maximized = false;

    this.#plotlyLayout = newLayoutObject(this.#cfg, this.#maximized);
    this.#plotlyLayout.title.text += this.#titleSuffix;
    this.#plotlyConfig = newConfigObject(this.#cfg, this.#maximized);

    this.#cachedWidth = this.#htmlElt.clientWidth;
    this.#plotlyLayout.width = this.#cachedWidth;

    this.#react();
  }

  resize() {
    this.updateCachedWidth();
    const layoutUpdate = { width: this.#cachedWidth };
    if (this.#maximized) {
      const plotsDiv = document.getElementById("plots");
      layoutUpdate.height = plotsDiv.parentElement.clientHeight - 50;
    } else {
      layoutUpdate.height = defaultPlotHeight;
    }
    Plotly.relayout(this.#htmlElt, layoutUpdate);
  }

  updateCachedWidth() {
    if (this.#maximized) {
      this.#cachedWidth = plotsDiv.clientWidth;
    } else {
      this.#cachedWidth = this.#htmlElt.clientWidth;
    }
  }

  #react() {
    Plotly.react(
      this.#htmlElt,
      this.#lastData,
      this.#plotlyLayout,
      this.#plotlyConfig,
    );
  }

  updateTheme() {
    const mode = theme.getThemeMode();
    const { paper_bgcolor, plot_bgcolor, font_color } = themeColors[mode];

    this.#cfg.layout.paper_bgcolor = paper_bgcolor;
    this.#cfg.layout.plot_bgcolor = plot_bgcolor;
    this.#cfg.layout.font_color = font_color;

    Plotly.relayout(this.#htmlElt, {
      paper_bgcolor: paper_bgcolor,
      plot_bgcolor: plot_bgcolor,
      "font.color": font_color,
    });
  }

  dispose() {
    this.#observer?.disconnect();
    if (this.#htmlElt) {
      this.#htmlElt.removeAllListeners?.("plotly_click");
      this.#htmlElt.removeAllListeners?.("plotly_relayout");
      Plotly.purge(this.#htmlElt);
    }
  }
}

// Create 'vertical lines' shapes for each of the given timestamps.
const createVerticalLines = (tss) => {
  const shapes = [];
  for (let i = 0, n = tss.length; i < n; i++) {
    const d = tss[i];
    shapes.push({
      type: "line",
      x0: d,
      x1: d,
      yref: "paper",
      y0: 0,
      y1: 1,
      line: {
        color: "rgb(55, 128, 191)",
        width: 1,
        dash: "longdashdot",
      },
    });
  }
  return shapes;
};

export { createVerticalLines, Plot };
