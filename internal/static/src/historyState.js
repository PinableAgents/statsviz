export const isFollowingLive = (view) => view.mode === "follow" && !view.paused;

export const sliderBounds = (meta, durationMs, currentTo) => {
  const minEnd = Number(meta.from) + durationMs;
  const maxEnd = Number(meta.to);
  return {
    min: Math.min(minEnd, maxEnd),
    max: maxEnd,
    value: Math.min(maxEnd, Math.max(minEnd, currentTo)),
    step: Math.max(1000, Number(meta.interval_ms) || 1000),
  };
};

export const canAutoRefreshRange = ({ inFlight, lastAttemptAt, now, refreshMs }) =>
  !inFlight && now - lastAttemptAt >= refreshMs;

export const clearPresetSelection = (inputs) => {
  for (const input of inputs) input.checked = false;
};

export const pausedWindowBounds = (to, durationMs) => ({
  from: to - durationMs,
  to,
});

export class DebouncedRangeQuery {
  #cancelRequest;
  #run;
  #delay;
  #timer = null;

  constructor(cancelRequest, run, delay = 220) {
    this.#cancelRequest = cancelRequest;
    this.#run = run;
    this.#delay = delay;
  }

  schedule(interactive = true) {
    this.cancel();
    this.#timer = setTimeout(() => {
      this.#timer = null;
      this.#run(interactive);
    }, this.#delay);
  }

  cancel() {
    if (this.#timer !== null) clearTimeout(this.#timer);
    this.#timer = null;
    this.#cancelRequest();
  }
}
