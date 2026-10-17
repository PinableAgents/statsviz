import * as ui from "./ui.js";
import Plotly from "plotly.js-cartesian-dist";
import { formatFunction } from "./utils.js";
import { getLocale, t } from "./i18n.js";
import { zh } from "./translations.js";

Plotly.register({
  moduleType: "locale",
  name: "zh-CN",
  dictionary: zh,
  format: {
    days: ["星期日", "星期一", "星期二", "星期三", "星期四", "星期五", "星期六"],
    shortDays: ["日", "一", "二", "三", "四", "五", "六"],
    months: Array.from({ length: 12 }, (_, index) => `${index + 1}月`),
    shortMonths: Array.from({ length: 12 }, (_, index) => `${index + 1}月`),
    date: "%Y-%m-%d",
  },
});

export const defaultPlotHeight = 480;

export const newConfigObject = (cfg, isMaximized) => {
  return {
    responsive: isMaximized,
    locale: getLocale(),
    displaylogo: false,
    displayModeBar: true,
    modeBarButtonsToRemove: [
      "2D",
      "select2d",
      "lasso2d",
      "toggleSpikelines",
      "sendChartToCloud",
    ],
    modeBarButtonsToAdd: [
      {
        name: "info",
        title: t("Plot info"),
        icon: Plotly.Icons.question,
        click: ui.onClickPlotInfo,
      },
      {
        name: isMaximized ? "minimize" : "maximize",
        title: t(isMaximized ? "Minimize" : "Maximize"),
        icon: isMaximized ? Plotly.Icons.zoom_minus : Plotly.Icons.zoom_plus,
        click: ui.onClickPlotMaximize(cfg),
      },
    ],
    toImageButtonOptions: {
      format: "png",
      filename: cfg.name,
      scale: 2,
    },
  };
};

const copyArrayOrNull = (o) => {
  return (Array.isArray(o) && [...o]) || null;
};

export const newLayoutObject = (cfg, isMaximized) => {
  const layout = {
    title: {
      y: isMaximized ? 0.98 : 0.94,
      font: {
        family: "Roboto",
        size: 18,
      },
      text: cfg.title,
    },
    margin: {
      t: 50,
      r: 20,
      l: 60,
      b: cfg.type === "heatmap" ? 66 : 0,
    },
    paper_bgcolor: cfg.layout.paper_bgcolor,
    plot_bgcolor: cfg.layout.plot_bgcolor,
    font: {
      color: cfg.layout.font_color,
    },
    hovermode: cfg.type === "heatmap" ? "closest" : "x",
    barmode: cfg.layout.barmode,
    xaxis: {
      tickformat: "%H:%M'%S″",
      type: "date",
      fixedrange: false,
      autorange: false,
      rangeslider: { visible: false },
    },
    yaxis: {
      tickmode: cfg.layout.yaxis.tickmode,
      ticktext: copyArrayOrNull(cfg.layout.yaxis.ticktext),
      tickvals: copyArrayOrNull(cfg.layout.yaxis.tickvals),
      title: { text: cfg.layout.yaxis.title },
      fixedrange: true,
      rangemode: "normal",
      tickformat: "s",
      ticksuffix: cfg.layout.yaxis.ticksuffix,
      minexponent: 3,
      showexponent: "all",
      exponentformat: "SI",
      showticksuffix: "yes",
      separatethousands: false,
    },
    showlegend: true,
    legend: {
      orientation: "h",
      xanchor: "center",
      x: 0.5,
      y: -0.05,
    },
  };

  if (layout.yaxis.tickmode == "array") {
    // Format yaxis ticks
    const formatYUnit = formatFunction(cfg.hover.yunit);
    for (let i = 0; i < layout.yaxis.ticktext.length; i++) {
      layout.yaxis.ticktext[i] = formatYUnit(layout.yaxis.ticktext[i]);
    }
  }

  return layout;
};

export const themeColors = {
  light: {
    paper_bgcolor: "#f8f8f8",
    plot_bgcolor: "#ffffdd",
    font_color: "#434343",
  },
  dark: {
    paper_bgcolor: "#181a1c",
    plot_bgcolor: "#282a2c",
    font_color: "#fff",
  },
};
