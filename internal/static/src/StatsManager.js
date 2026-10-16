import { TimeWindowCache } from "./historyData.js";

export default class StatsManager {
  #cache;
  #config;

  constructor(retentionMilliseconds, config, maxSamples = 20000) {
    this.#config = config;
    this.#cache = new TimeWindowCache(retentionMilliseconds, maxSamples);
  }

  pushData(payload) {
    return this.#cache.add(payload, "live");
  }

  between(from, to) {
    return this.#cache.between(from, to);
  }

  latest() {
    return this.#cache.latest();
  }

  config() {
    return this.#config;
  }

  reset(config) {
    this.#config = config;
  }
}
