import { clamp } from "./utils.js";

export default class WebSocketClient {
  #uri;
  #timeout;
  #onConfig;
  #onData;
  #onOpen;

  /**
   * @param {string} uri           WebSocket URI
   * @param {Function} onConfig    callback(configData)
   * @param {Function} onData      callback(messageData)
   * @param {number} [initialTimeout=250]
   * @param {Function} [onOpen]   连接建立后的回调
   */
  constructor(uri, onConfig, onData, initialTimeout = 250, onOpen = null) {
    this.#uri = uri;
    this.#timeout = initialTimeout;
    this.#onConfig = onConfig;
    this.#onData = onData;
    this.#onOpen = onOpen;
    this.#connect();
  }

  #connect() {
    const ws = new WebSocket(this.#uri);
    console.info(`WS connecting to ${this.#uri}`);

    ws.onopen = () => {
      this.#timeout = 250; // reset backoff
      this.#onOpen?.();
    };

    ws.onclose = (ev) => {
      console.warn(`WS closed: ${ev.code}`);
      const delay = clamp((this.#timeout *= 2), 250, 5000);
      setTimeout(() => this.#connect(), delay);
    };

    ws.onerror = (err) => {
      console.error("WS error", err);
      ws.close();
    };

    ws.onmessage = (ev) => {
      const msg = JSON.parse(ev.data);
      if (msg.event === "config") {
        this.#onConfig(msg.data);
      } else {
        this.#onData(msg.data);
      }
    };
  }
}
