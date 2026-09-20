// 主线程门面：负责创建 Worker、转发命令、分发事件、处理 Blob 落盘。
import { REQ, EVT, DOWNLOAD_STATUS } from '../shared/protocol.js';

export class DownloadManager extends EventTarget {
  constructor(workerUrl = new URL('../workers/download.worker.js', import.meta.url)) {
    super();
    this.worker = new Worker(workerUrl, { type: 'module' });
    this._requestSeq = 0;
    this._exportWaiters = new Map();
    this.worker.onmessage = (event) => this._onMessage(event.data);
    this.worker.onerror = (event) => {
      this.dispatchEvent(new CustomEvent('worker-error', { detail: event.message }));
    };
  }

  init({ concurrency, rateBytesPerSecond } = {}) {
    return this._request({
      type: REQ.INIT,
      concurrency,
      rateBytesPerSecond
    }, REQ.LIST_RESTORED);
  }

  add(url, filename, { autoStart = true, headers } = {}) {
    const id = this._idFor(url, filename);
    this._send({ type: REQ.ADD, id, url, filename, autoStart, headers });
    return id;
  }

  pause(id) {
    this._send({ type: REQ.PAUSE, id });
  }

  resume(id) {
    this._send({ type: REQ.RESUME, id });
  }

  cancel(id) {
    this._send({ type: REQ.CANCEL, id });
  }

  pauseAll() {
    this._send({ type: REQ.PAUSE_ALL });
  }

  resumeAll() {
    this._send({ type: REQ.RESUME_ALL });
  }

  setRate(rateBytesPerSecond) {
    this._send({ type: REQ.SET_RATE, rateBytesPerSecond });
  }

  setConcurrency(concurrency) {
    this._send({ type: REQ.SET_CONCURRENCY, concurrency });
  }

  // 合并分块并触发浏览器保存（Blob URL）。支持任意大小：Blob 内部按块引用，不复制字节。
  async save(id, suggestedFilename) {
    const result = await this._request({ type: REQ.EXPORT, id }, 'blob');
    const filename = suggestedFilename || result.filename || id;
    const url = URL.createObjectURL(result.blob);
    const anchor = document.createElement('a');
    anchor.href = url;
    anchor.download = filename;
    document.body.appendChild(anchor);
    anchor.click();
    anchor.remove();
    setTimeout(() => URL.revokeObjectURL(url), 60_000);
    return { filename, size: result.size };
  }

  _idFor(url, filename) {
    const key = `${url}::${filename || ''}`;
    let hash = 5381;
    for (let i = 0; i < key.length; i += 1) {
      hash = ((hash << 5) + hash + key.charCodeAt(i)) >>> 0;
    }
    return `dl_${hash.toString(36)}_${Date.now().toString(36)}`;
  }

  _send(message) {
    this.worker.postMessage(message);
  }

  _request(message, replyType) {
    const requestId = `r${(this._requestSeq += 1)}`;
    return new Promise((resolve, reject) => {
      this._exportWaiters.set(requestId, { resolve, reject, replyType });
      this.worker.postMessage({ ...message, requestId });
    });
  }

  _onMessage(message) {
    if (!message || !message.type) return;
    if (message.type === 'ready') return;

    if (message.type === REQ.LIST_RESTORED && message.items) {
      this.dispatchEvent(new CustomEvent(EVT.RESTORED, { detail: message.items }));
      const waiter = message.requestId ? this._exportWaiters.get(message.requestId) : null;
      if (waiter) {
        this._exportWaiters.delete(message.requestId);
        waiter.resolve(message.items);
      }
      return;
    }

    if (message.type === 'blob') {
      const waiter = this._exportWaiters.get(message.requestId);
      if (waiter) {
        this._exportWaiters.delete(message.requestId);
        waiter.resolve(message);
      }
      return;
    }

    if (message.type === 'error' && message.requestId) {
      const waiter = this._exportWaiters.get(message.requestId);
      if (waiter) {
        this._exportWaiters.delete(message.requestId);
        waiter.reject(new Error(message.message));
        return;
      }
    }

    this.dispatchEvent(
      new CustomEvent(message.type, {
        detail: message
      })
    );
  }
}

export { DOWNLOAD_STATUS };
