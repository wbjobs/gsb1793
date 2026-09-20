import { IdbChunkStorage } from './idb-storage.js';

/**
 * 主线程 API：
 *   const dm = new DownloadManager();
 *   dm.on('progress', ({file}) => ...);
 *   await dm.add(url);  dm.pause(id);  dm.resume(id);
 *   dm.setConcurrency(6); dm.setRate(2 * 1024 * 1024);
 *   await dm.saveToDisk(id);  // 完成后导出为本地文件
 */
export class DownloadManager {
  constructor(workerUrl = new URL('./download-worker.js', import.meta.url)) {
    this.worker = new Worker(workerUrl, { type: 'module' });
    this.storage = new IdbChunkStorage();
    this._listeners = new Map();
    this.files = new Map();
    this.settings = { concurrency: 3, rateLimit: 0 };
    this.ready = new Promise((resolve) => {
      this._resolveReady = resolve;
    });

    this.worker.onmessage = (e) => this._onMessage(e.data);
    this.worker.onerror = (err) => this._dispatch('worker-error', { message: err.message });
  }

  _onMessage(msg) {
    const { type, payload } = msg;
    if (type === 'ready') {
      this.settings = payload.settings;
      for (const f of payload.files) this.files.set(f.id, f);
      this._resolveReady();
      this._dispatch('ready', payload);
      return;
    }
    if (type === 'added') {
      // add() 通过一次性 added 监听器 resolve，这里无需额外处理
    }
    if (type === 'error') {
      this._dispatch('error', payload);
      return;
    }
    if (type === 'fatal') {
      this._dispatch('fatal', payload);
      return;
    }
    if (type === 'settings') {
      this.settings = { ...this.settings, ...payload };
    }
    if (payload && payload.file && payload.file.id) {
      const next = payload.file;
      this.files.set(next.id, next);
    }
    if (type && type.startsWith('evt:')) {
      this._dispatch(type.slice(4), payload);
    } else {
      this._dispatch(type, payload);
    }
  }

  _call(type, payload = {}) {
    this.worker.postMessage({ type, payload });
  }

  on(event, fn) {
    if (!this._listeners.has(event)) this._listeners.set(event, new Set());
    this._listeners.get(event).add(fn);
    return () => this._listeners.get(event)?.delete(fn);
  }

  _dispatch(event, payload) {
    const set = this._listeners.get(event);
    if (set) for (const fn of set) {
      try { fn(payload); } catch (err) { console.error(err); }
    }
  }

  add(url, { filename, autoStart = true } = {}) {
    return new Promise((resolve) => {
      const handler = (payload) => {
        this._listeners.get('added')?.delete(handler);
        resolve(payload.file);
      };
      this.on('added', handler);
      this._call('add', { url, filename, autoStart });
    });
  }

  pause(fileId) { this._call('pause', { fileId }); }
  resume(fileId) { this._call('resume', { fileId }); }
  remove(fileId) { this._call('remove', { fileId }); this.files.delete(fileId); }
  setConcurrency(concurrency) { this._call('set-concurrency', { concurrency }); }
  setRate(rateLimit) { this._call('set-rate', { rateLimit }); }

  /**
   * 把分块流式合并并保存到磁盘。
   * 优先用 File System Access API（流式写、恒定内存）；
   * 不支持时退化为 Blob 拼接 + a[download]（此时整文件会短暂驻留内存）。
   */
  async saveToDisk(fileId, { signal } = {}) {
    const file = this.files.get(fileId);
    if (!file || !file.chunkCount) throw new Error('file not ready for save');

    const supportsFSA = typeof globalThis.showSaveFilePicker === 'function';
    if (!supportsFSA) return this._saveViaBlob(file);

    const pickerOpts = {
      suggestedName: file.filename,
      types: [{ description: 'Binary', accept: { 'application/octet-stream': [] } }],
    };
    let handle;
    try {
      handle = await globalThis.showSaveFilePicker(pickerOpts);
    } catch (err) {
      if (err && err.name === 'AbortError') return false; // 用户取消
      throw err;
    }
    const writable = await handle.createWritable();
    let written = 0;
    try {
      await this.storage.streamChunks(fileId, file.chunkCount, async (rec) => {
        if (signal && signal.aborted) throw new DOMException('aborted', 'AbortError');
        await writable.write(rec.blob);
        written += rec.size;
        this._dispatch('save-progress', { fileId, written, size: file.size });
      });
      await writable.close();
    } catch (err) {
      try { await writable.abort(); } catch { /* ignore */ }
      throw err;
    }
    return true;
  }

  async _saveViaBlob(file) {
    const records = await this.storage.listChunks(file.id);
    if (records.length === 0) throw new Error('no chunks to save');
    const blob = new Blob(records.map((r) => r.blob), { type: 'application/octet-stream' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = file.filename;
    document.body.appendChild(a);
    a.click();
    a.remove();
    setTimeout(() => URL.revokeObjectURL(url), 60_000);
    return true;
  }
}
