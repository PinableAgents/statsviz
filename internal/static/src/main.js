import { Popover, Tooltip } from "bootstrap";
import { connect } from "./app.js";
import * as theme from "./theme.js";
import { initI18n, onLanguageChange, t } from "./i18n.js";
import "bootstrap/dist/css/bootstrap.min.css";
import "./style.css";

initI18n();
theme.updateThemeMode();

const updateTooltips = () => {
  document.querySelectorAll('[data-bs-toggle="tooltip"]').forEach((el) => {
    const title = t(el.dataset.i18nTitle || el.getAttribute("title"));
    const existing = Tooltip.getInstance(el);
    if (existing) {
      el.setAttribute("data-bs-original-title", title);
      el.removeAttribute("title");
      existing.setContent({ ".tooltip-inner": title });
      return;
    }
    el.setAttribute("title", title);
    new Tooltip(el, {
      trigger: "hover focus",
      delay: { show: 700, hide: 100 },
      animation: true,
      placement: "bottom",
    });
  });
};

updateTooltips();
onLanguageChange(() => {
  updateTooltips();
  document.querySelectorAll(".modebar-btn").forEach((el) => Popover.getInstance(el)?.hide());
});
connect();
