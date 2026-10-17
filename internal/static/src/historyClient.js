import { t } from "./i18n.js";

class LocalizedError extends Error {
  constructor(message, values = {}) {
    super(t(message, values));
    this.translationKey = message;
    this.translationValues = values;
  }
}

export class HistoryUnavailableError extends LocalizedError {}
export class StaleRequestError extends LocalizedError {}

const defaultFetch = (...args) => globalThis.fetch(...args);

export const buildHTTPBase = (websocketURL, pageURL) => {
  if (websocketURL) {
    const url = new URL(websocketURL, pageURL);
    url.protocol = url.protocol === "wss:" ? "https:" : "http:";
    const pathParts = url.pathname.replace(/\/+$/, "").split("/");
    pathParts.pop();
    url.pathname = `${pathParts.join("/")}/`;
    url.search = "";
    url.hash = "";
    return url;
  }
  return new URL("./", pageURL);
};

const readJSON = async (response) => {
  const contentType = response.headers?.get?.("content-type") || "";
  if (response.status === 404 || !contentType.includes("application/json")) {
    throw new HistoryUnavailableError("History endpoint unavailable");
  }
  if (!response.ok) {
    const payload = await response.json().catch(() => ({}));
    throw new LocalizedError(payload.error || "History request failed ({status})", { status: response.status });
  }
  return response.json();
};

export class LatestRequest {
  #fetch;
  #controller = null;
  #generation = 0;

  constructor(fetchImpl = defaultFetch) {
    this.#fetch = fetchImpl;
  }

  async json(url) {
    this.cancel();
    const generation = ++this.#generation;
    const controller = new AbortController();
    this.#controller = controller;

    try {
      const response = await this.#fetch(url, {
        signal: controller.signal,
        headers: { Accept: "application/json" },
      });
      if (controller.signal.aborted || generation !== this.#generation) {
        throw new StaleRequestError("Request canceled");
      }
      const payload = await readJSON(response);
      if (controller.signal.aborted || generation !== this.#generation) {
        throw new StaleRequestError("Request expired");
      }
      return payload;
    } catch (error) {
      if (controller.signal.aborted || generation !== this.#generation) {
        throw new StaleRequestError("Request canceled");
      }
      throw error;
    } finally {
      if (generation === this.#generation) this.#controller = null;
    }
  }

  cancel() {
    this.#generation++;
    if (this.#controller) this.#controller.abort();
    this.#controller = null;
  }
}

export default class HistoryClient {
  #base;
  #metaRequest;
  #rangeRequest;
  #pointRequest;

  constructor(base, fetchImpl = defaultFetch) {
    this.#base = base;
    this.#metaRequest = new LatestRequest(fetchImpl);
    this.#rangeRequest = new LatestRequest(fetchImpl);
    this.#pointRequest = new LatestRequest(fetchImpl);
  }

  meta() {
    return this.#metaRequest.json(new URL("history/meta", this.#base));
  }

  range({ from, to, plots = [], maxPoints = 1500 }) {
    const url = new URL("history/range", this.#base);
    url.searchParams.set("from", String(Math.trunc(from)));
    url.searchParams.set("to", String(Math.trunc(to)));
    url.searchParams.set("max_points", String(maxPoints));
    if (plots.length) url.searchParams.set("plots", plots.join(","));
    return this.#rangeRequest.json(url);
  }

  point({ at, runId, toleranceMs }) {
    const url = new URL("history/point", this.#base);
    url.searchParams.set("at", String(Math.trunc(at)));
    if (runId !== undefined && runId !== null) {
      url.searchParams.set("run_id", String(runId));
    }
    if (toleranceMs !== undefined && toleranceMs !== null) {
      url.searchParams.set("tolerance_ms", String(Math.trunc(toleranceMs)));
    }
    return this.#pointRequest.json(url);
  }

  cancelRange() {
    this.#rangeRequest.cancel();
  }

  cancelPoint() {
    this.#pointRequest.cancel();
  }
}
