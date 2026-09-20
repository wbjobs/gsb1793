import { ConcurrencyGate } from './ConcurrencyGate.js';
import { RateLimiter } from './RateLimiter.js';
import { HttpError, backoffDelay } from './retry.js';
import { probe, fetchRange, fetchWhole, pumpBody } from './http.js';
import { DOWNLOAD_STATUS } from '../shared/protocol.js';

export const DEFAULT_OPTIONS = Object.freeze({
  concurrency: 3,
  rateBytesPerSecond: 0,
  chunkSize: 4 * 1024 * 1024,
  maxRetries: 8,
  retryBaseMs: 500,
  retryMaxMs: 30_000,
  headers: {}
});

const sleep = (ms, signal) =>
  new Promise((resolve, reject) => {
    if (signal && signal.aborted) {
      reject(Object.assign(new Error('aborted'), { name: 'AbortError' }));
      return;
    }
    const timer = setTimeout(() => {
      if (signal) signal.removeEventListener('abort', onAbort);
      resolve();
    }, ms);
    const onAbort = () => {
      clearTimeout(timer);
      reject(Object.assign(new Error('aborted'), { name: 'AbortError' }));
    };
    if (signal) signal.addEventListener('abort', onAbort, { once: true });
  });

function sameRemote(a, b) {
  if (!a || !b) return false;
  if (a.etag || b.etag) return a.etag === b.etag;
  if (a.lastModified || b.lastModified) return a.lastModified === b.lastModified;
  return a.total != null && b.total != null && a.total === b.total;
}

export class DownloadEngine {
  // storage: 实现 ChunkStorage 接口；fetchImpl 可注入用于测试。
  constructor(storage, options = {}, deps = {}) {
    this.storage = storage;
    this.options = { ...DEFAULT_OPTIONS, ...options };
    this.fetchImpl = deps.fetchImpl || null;
    this.now = deps.now || (() => Date.now());
    this.wait = deps.wait || ((ms) => new Promise((resolve) => setTimeout(resolve, ms)));
    this.createBlob =
      deps.createBlob ||
      ((parts) => (typeof Blob !== 'undefined' ? new Blob(parts) : null));

    this.gate = new ConcurrencyGate(this.options.concurrency);
    this.limiter = new RateLimiter(this.options.rateBytesPerSecond, this.now);
    this.files = new Map();
    this.listeners = new Set();
    this._pumpScheduled = false;
    this._roundRobin = [];
    this._globalBytesAt = { t: this.now(), bytes: 0 };
  }

  on(listener) {
    this.listeners.add(listener);
    return () => this.listeners.delete(listener);
  }

  emit(event) {
    for (const listener of this.listeners) {
      try {
        listener(event);
      } catch (error) {
        console.error('download event listener error:', error);
      }
    }
  }

  setConcurrency(value) {
    this.options.concurrency = Math.max(1, value);
    this.gate.setMax(this.options.concurrency);
    this._schedulePump();
  }

  setRate(bytesPerSecond) {
    this.limiter.setRate(bytesPerSecond);
    this.options.rateBytesPerSecond = Math.max(0, bytesPerSecond);
  }

  // 当前全局限速下的实测总速度（字节/秒），由 UI 定时读取。
  globalSpeed() {
    const downloaded = [...this.files.values()].reduce((sum, f) => sum + f.downloaded, 0);
    const now = this.now();
    const { t, bytes } = this._globalBytesAt;
    const dt = (now - t) / 1000;
    if (dt <= 0) return 0;
    const speed = (downloaded - bytes) / dt;
    this._globalBytesAt = { t: now, bytes: downloaded };
    return Math.max(0, speed);
  }

  // 添加下载。断点续传：同 id 再次 add 时先读取本地元数据与已完成分块。
  async addFile({ id, url, filename, autoStart = true, headers } = {}) {
    if (!id || !url) throw new Error('id 和 url 必填');
    const existing = this.files.get(id);
    if (existing && existing.status !== DOWNLOAD_STATUS.REMOVED) return this._snapshot(existing);

    const stored = await this.storage.getMeta(id);
    const file = {
      id,
      url,
      filename: filename || (stored && stored.filename) || url.split('/').pop() || id,
      status: DOWNLOAD_STATUS.QUEUED,
      total: (stored && stored.total) || null,
      downloaded: 0,
      rangeSupported: stored ? !!stored.rangeSupported : null,
      mode: stored && stored.rangeSupported === false ? 'whole' : 'range',
      chunks: stored ? stored.chunkCount || 0 : 0,
      chunkSize: (stored && stored.chunkSize) || this.options.chunkSize,
      chunkCount: stored && stored.chunkCount != null ? stored.chunkCount : null,
      etag: (stored && stored.etag) || null,
      lastModified: (stored && stored.lastModified) || null,
      error: null,
      headers: headers || this.options.headers,
      pending: [],
      inflight: 0,
      generation: 0,
      controller: null,
      whenDrained: null
    };
    this.files.set(id, file);
    await this._persist(file);
    this._emitState(file);
    if (autoStart) await this.activate(file);
    return this._snapshot(file);
  }

  async listRestored() {
    const metas = await this.storage.allMeta();
    return metas
      .filter((meta) => meta.status !== DOWNLOAD_STATUS.REMOVED)
      .map((meta) => ({
        id: meta.id,
        url: meta.url,
        filename: meta.filename,
        status: meta.status === DOWNLOAD_STATUS.COMPLETE ? DOWNLOAD_STATUS.COMPLETE : DOWNLOAD_STATUS.PAUSED,
        total: meta.total,
        downloaded: meta.downloaded || 0,
        rangeSupported: meta.rangeSupported,
        chunks: meta.chunkCount || 0,
        chunkSize: meta.chunkSize,
        chunkCount: meta.chunkCount,
        error: null
      }));
  }

  async activate(file) {
    file.generation += 1;
    file.status = DOWNLOAD_STATUS.PROBING;
    file.error = null;
    file.controller = new AbortController();
    this._emitState(file);

    // 探测同样需要重试（网络抖动 / 5xx），退避可被暂停中断。
    let info = null;
    for (let attempt = 0; attempt <= this.options.maxRetries; attempt += 1) {
      if (file.controller.signal.aborted) return;
      try {
        info = await probe(file.url, {
          fetchImpl: this.fetchImpl,
          signal: file.controller.signal,
          headers: file.headers
        });
        break;
      } catch (error) {
        if (error && error.name === 'AbortError') return;
        const retryable = (error && error.retryable) !== false;
        if (!retryable || attempt === this.options.maxRetries) {
          this._fail(file, error);
          return;
        }
        const delay = backoffDelay(attempt, this.options.retryBaseMs, this.options.retryMaxMs, null, this.now);
        this.emit({ type: 'retry', id: file.id, attempt: attempt + 1, delayMs: delay, phase: 'probe', message: error.message });
        try {
          await this._interruptibleWait(file, delay, file.generation);
        } catch (waitError) {
          if (waitError && waitError.name === 'AbortError') return;
          throw waitError;
        }
      }
    }
    if (file.status === DOWNLOAD_STATUS.REMOVED || file.controller.signal.aborted) return;

    // 断点校验：实体标识不一致（文件已变化）则清空旧分块重来。
    const storedIdentity = { etag: file.etag, lastModified: file.lastModified, total: file.total };
    const remoteIdentity = { etag: info.etag, lastModified: info.lastModified, total: info.total };
    const hasIdentity = file.etag || file.lastModified || file.chunks > 0;
    if (hasIdentity && info.total != null && !sameRemote(storedIdentity, remoteIdentity)) {
      await this.storage.clearChunks(file.id);
      file.chunks = 0;
      file.downloaded = 0;
      file.chunkCount = null;
      this.emit({ type: 'retry', id: file.id, reason: 'remote-changed', attempt: 0, message: '远端文件已变化，重新下载' });
    }

    file.etag = info.etag;
    file.lastModified = info.lastModified;
    file.total = info.total;
    file.rangeSupported = !!info.rangeSupported;
    file.mode = info.rangeSupported ? 'range' : 'whole';

    // 先把待下载队列准备好，再翻转状态并触发泵，避免泵空转竞态。
    if (file.mode === 'range') {
      await this._setupRange(file);
    } else {
      file.chunkCount = 1;
      await this.storage.clearChunks(file.id);
      file.chunks = 0;
      file.downloaded = 0;
      file.pending = [{ kind: 'whole', attempt: 0 }];
    }
    if (file.controller.signal.aborted) return;
    file.status = DOWNLOAD_STATUS.DOWNLOADING;
    await this._persist(file);
    this._emitState(file);
    this._emitProgress(file);
    this._schedulePump();
  }

  async _setupRange(file) {
    const total = file.total;
    if (total == null) {
      // 极少情况：宣称 Range 但拿不到长度。退化为整文件单流。
      file.mode = 'whole';
      file.rangeSupported = false;
      file.chunkCount = 1;
      await this.storage.clearChunks(file.id);
      file.chunks = 0;
      file.downloaded = 0;
      file.pending = [{ kind: 'whole', attempt: 0 }];
      return;
    }
    file.chunkSize = file.chunkSize || this.options.chunkSize;
    file.chunkCount = Math.max(1, Math.ceil(total / file.chunkSize));
    const storedChunks = await this.storage.listChunks(file.id);
    const doneSet = new Set();
    let downloaded = 0;
    for (const item of storedChunks) {
      if (item.index >= 0 && item.index < file.chunkCount) {
        doneSet.add(item.index);
        const start = item.index * file.chunkSize;
        downloaded += item.size || Math.min(file.chunkSize, total - start);
      }
    }
    file.chunks = doneSet.size;
    file.downloaded = Math.min(total, downloaded);
    file.pending = [];
    for (let index = 0; index < file.chunkCount; index += 1) {
      if (!doneSet.has(index)) file.pending.push({ kind: 'range', index, attempt: 0 });
    }
    await this._persist(file);
  }

  // 全局泵：在所有下载中的文件间轮转派发分块，受总并发门与令牌桶约束。
  _schedulePump() {
    if (this._pumpScheduled) return;
    this._pumpScheduled = true;
    const run = () => {
      this._pumpScheduled = false;
      this._pump();
    };
    if (typeof queueMicrotask === 'function') queueMicrotask(run);
    else Promise.resolve().then(run);
  }

  _eligibleFiles() {
    const files = [...this.files.values()].filter(
      (file) => file.status === DOWNLOAD_STATUS.DOWNLOADING && file.pending.length > 0
    );
    files.sort((a, b) => a.inflight - b.inflight);
    return files;
  }

  _pump() {
    while (this.gate.tryAcquire()) {
      const files = this._eligibleFiles();
      if (files.length === 0) {
        this.gate.release();
        return;
      }
      // 轮转：优先当前在途最少的文件，保证多文件公平共享并发额度。
      const file = files[0];
      const job = file.pending.shift();
      file.inflight += 1;
      this._runJob(file, job).finally(() => {
        file.inflight -= 1;
        this.gate.release();
        this._afterJob(file, job);
      });
    }
  }

  async _runJob(file, job) {
    const generation = file.generation;
    if (job.kind === 'whole') return this._runWhole(file, job, generation);
    return this._runRange(file, job, generation);
  }

  async _runRange(file, job, generation) {
    const index = job.index;
    const signal = file.controller.signal;
    this.emit({ type: 'chunk:start', id: file.id, index });

    try {
      const start = index * file.chunkSize;
      const end = Math.min(file.total - 1, start + file.chunkSize - 1);
      const ranged = await fetchRange(file.url, start, end, {
        fetchImpl: this.fetchImpl,
        signal,
        headers: file.headers
      });

      if (ranged.downgraded) {
        // 服务端实际不支持 Range（探测时可能只看到 Accept-Ranges 头）。
        ranged.response.body && ranged.response.body.cancel &&
          ranged.response.body.cancel().catch(() => {});
        if (generation === file.generation) await this._downgradeToWhole(file, generation);
        return;
      }
      if (ranged.start !== start || (ranged.end != null && ranged.end !== end)) {
        throw Object.assign(new Error('Range 响应边界不匹配'), { retryable: true });
      }

      const parts = [];
      const onData = async (chunk) => {
        await this.limiter.acquire(chunk.byteLength, this.wait);
        parts.push(chunk);
      };
      const received = await pumpBody(ranged.response, onData);
      const expected = end - start + 1;
      if (received !== expected) {
        throw Object.assign(new Error(`分块 ${index} 长度不足: ${received}/${expected}`), {
          retryable: true
        });
      }
      if (generation !== file.generation) return;
      const blob = this.createBlob(parts);
      await this.storage.putChunk(file.id, index, blob);
      this._chunkDone(file, index, expected, generation);
    } catch (error) {
      if (error && error.name === 'AbortError') {
        // 暂停：分块回队，恢复时重新请求；降级 whole / 删除：丢弃。
        if (file.status === DOWNLOAD_STATUS.PAUSED && file.mode === 'range') {
          file.pending.push({ kind: 'range', index, attempt: job.attempt });
        }
        return;
      }
      if (generation !== file.generation) return;
      const retryable = (error && error.retryable) || (error instanceof HttpError && error.retryable);
      if (retryable && job.attempt < this.options.maxRetries) {
        const delay = backoffDelay(
          job.attempt,
          this.options.retryBaseMs,
          this.options.retryMaxMs,
          error.retryAfter || null,
          this.now
        );
        this.emit({
          type: 'retry',
          id: file.id,
          index,
          attempt: job.attempt + 1,
          delayMs: delay,
          message: error.message
        });
        try {
          await this._interruptibleWait(file, delay, generation);
        } catch (waitError) {
          if (waitError && waitError.name === 'AbortError') {
            if (file.status === DOWNLOAD_STATUS.PAUSED && file.mode === 'range') {
              file.pending.push({ kind: 'range', index, attempt: job.attempt });
            }
            return;
          }
          throw waitError;
        }
        if (generation !== file.generation) return;
        file.pending.push({ kind: 'range', index, attempt: job.attempt + 1 });
        this._schedulePump();
      } else {
        this._fail(file, error);
      }
    }
  }

  async _runWhole(file, job, generation) {
    const signal = file.controller.signal;
    file.downloaded = 0;
    try {
      const { response, total } = await fetchWhole(file.url, {
        fetchImpl: this.fetchImpl,
        signal,
        headers: file.headers
      });
      if (file.total == null && total != null) {
        file.total = total;
        await this._persist(file);
        this._emitState(file);
      }
      const parts = [];
      let received = 0;
      let lastEmit = 0;
      await pumpBody(response, async (chunk) => {
        await this.limiter.acquire(chunk.byteLength, this.wait);
        parts.push(chunk);
        received += chunk.byteLength;
        file.downloaded = file.total != null ? Math.min(file.total, received) : received;
        const now = this.now();
        if (now - lastEmit > 250) {
          lastEmit = now;
          this._emitProgress(file);
        }
      });
      if (generation !== file.generation) return;
      const blob = this.createBlob(parts);
      await this.storage.putChunk(file.id, 0, blob);
      file.downloaded = received;
      this._wholeDone(file, generation, received);
    } catch (error) {
      if (error && error.name === 'AbortError') {
        if (file.status === DOWNLOAD_STATUS.PAUSED && file.mode === 'whole') {
          file.pending.push({ kind: 'whole', attempt: job.attempt });
        }
        return;
      }
      if (generation !== file.generation) return;
      const retryable = (error && error.retryable) !== false;
      if (retryable && job.attempt < this.options.maxRetries) {
        const delay = backoffDelay(
          job.attempt,
          this.options.retryBaseMs,
          this.options.retryMaxMs,
          error.retryAfter || null,
          this.now
        );
        this.emit({
          type: 'retry',
          id: file.id,
          attempt: job.attempt + 1,
          delayMs: delay,
          message: error.message
        });
        try {
          await this._interruptibleWait(file, delay, generation);
        } catch (waitError) {
          if (waitError && waitError.name === 'AbortError') {
            if (file.status === DOWNLOAD_STATUS.PAUSED && file.mode === 'whole') {
              file.pending.push({ kind: 'whole', attempt: job.attempt });
            }
            return;
          }
          throw waitError;
        }
        if (generation !== file.generation) return;
        file.pending.push({ kind: 'whole', attempt: job.attempt + 1 });
        this._schedulePump();
      } else {
        this._fail(file, error);
      }
    }
  }

  async _downgradeToWhole(file, generation) {
    file.mode = 'whole';
    file.rangeSupported = false;
    file.chunkCount = 1;
    file.controller && file.controller.abort();
    file.controller = new AbortController();
    file.generation += 1;
    await this.storage.clearChunks(file.id);
    file.chunks = 0;
    file.downloaded = 0;
    file.pending = [{ kind: 'whole', attempt: 0 }];
    await this._persist(file);
    this._emitState(file);
    this._schedulePump();
  }

  _interruptibleWait(file, ms, generation) {
    return sleep(ms, file.controller.signal);
  }

  _chunkDone(file, index, size, generation) {
    file.chunks += 1;
    file.downloaded = Math.min(file.total || size, file.downloaded + size);
    this.emit({ type: 'chunk', id: file.id, index, size, chunks: file.chunks, chunkCount: file.chunkCount });
    this._emitProgress(file);
    this._persist(file).catch((error) => this._fail(file, error));
  }

  _wholeDone(file, generation, size) {
    file.chunks = 1;
    file.downloaded = size;
    file.total = size;
  }

  _afterJob(file, job) {
    if (file.status !== DOWNLOAD_STATUS.DOWNLOADING) {
      if (file.inflight === 0 && file.whenDrained) {
        const resolve = file.whenDrained;
        file.whenDrained = null;
        resolve();
      }
      // 暂停/错误时被中止的分块可能刚刚回队，保证恢复时泵已处于待调度状态
      if (file.status === DOWNLOAD_STATUS.PAUSED || file.status === DOWNLOAD_STATUS.ERROR) {
        this._schedulePump();
      }
      return;
    }
    if (file.pending.length === 0 && file.inflight === 0) {
      const totalChunks = file.mode === 'whole' ? 1 : file.chunkCount;
      if (file.chunks === totalChunks && totalChunks != null) {
        file.status = DOWNLOAD_STATUS.COMPLETE;
        awaitPersist(this, file, () => {
          this._emitState(file);
          this.emit({
            type: 'fileDone',
            id: file.id,
            filename: file.filename,
            total: file.total,
            downloaded: file.downloaded
          });
        });
        return;
      }
    }
    this._schedulePump();
  }

  async pauseFile(id) {
    const file = this.files.get(id);
    if (!file || ![DOWNLOAD_STATUS.QUEUED, DOWNLOAD_STATUS.PROBING, DOWNLOAD_STATUS.DOWNLOADING, DOWNLOAD_STATUS.ERROR].includes(file.status)) {
      return;
    }
    file.status = DOWNLOAD_STATUS.PAUSED;
    file.controller && file.controller.abort();
    await this._persist(file);
    this._emitState(file);
    if (file.inflight > 0) {
      await new Promise((resolve) => {
        file.whenDrained = resolve;
      });
    }
  }

  async pauseAll() {
    const ids = [...this.files.values()]
      .filter((file) =>
        [DOWNLOAD_STATUS.QUEUED, DOWNLOAD_STATUS.PROBING, DOWNLOAD_STATUS.DOWNLOADING].includes(file.status)
      )
      .map((file) => file.id);
    await Promise.all(ids.map((id) => this.pauseFile(id)));
  }

  // 恢复浏览器重启前的任务：从存储元数据载入内存并激活（断点续传）。
  async restoreFile(id, { autoStart = false } = {}) {
    if (this.files.has(id)) return this._snapshot(this.files.get(id));
    const stored = await this.storage.getMeta(id);
    if (!stored) throw new Error(`本地不存在该下载: ${id}`);
    const file = {
      id,
      url: stored.url,
      filename: stored.filename,
      status: stored.status === DOWNLOAD_STATUS.COMPLETE ? DOWNLOAD_STATUS.COMPLETE : DOWNLOAD_STATUS.PAUSED,
      total: stored.total,
      downloaded: stored.downloaded || 0,
      rangeSupported: stored.rangeSupported,
      mode: stored.mode || (stored.rangeSupported === false ? 'whole' : 'range'),
      chunks: stored.chunks || 0,
      chunkSize: stored.chunkSize || this.options.chunkSize,
      chunkCount: stored.chunkCount,
      etag: stored.etag || null,
      lastModified: stored.lastModified || null,
      error: null,
      headers: this.options.headers,
      pending: [],
      inflight: 0,
      generation: 0,
      controller: null,
      whenDrained: null
    };
    this.files.set(id, file);
    this._emitState(file);
    if (autoStart && file.status !== DOWNLOAD_STATUS.COMPLETE) {
      await this.activate(file);
    }
    return this._snapshot(file);
  }

  async resumeFile(id) {
    let file = this.files.get(id);
    if (!file) {
      await this.restoreFile(id, { autoStart: false });
      file = this.files.get(id);
    }
    if (!file) throw new Error(`未知下载: ${id}`);
    if (file.status === DOWNLOAD_STATUS.COMPLETE) return;
    if (file.status === DOWNLOAD_STATUS.DOWNLOADING || file.status === DOWNLOAD_STATUS.PROBING) return;
    file.controller = new AbortController();
    await this.activate(file);
  }

  async resumeAll() {
    const ids = [...this.files.values()]
      .filter((file) => file.status === DOWNLOAD_STATUS.PAUSED || file.status === DOWNLOAD_STATUS.ERROR)
      .map((file) => file.id);
    for (const id of ids) {
      await this.resumeFile(id);
    }
  }

  async cancelFile(id) {
    let file = this.files.get(id);
    if (!file) {
      // 浏览器重启后、未载入内存的任务：直接清理持久化数据
      await this.storage.clearChunks(id).catch(() => {});
      await this.storage.deleteMeta(id).catch(() => {});
      this.emit({ type: 'state', id, state: { id, status: DOWNLOAD_STATUS.REMOVED } });
      return;
    }
    file.status = DOWNLOAD_STATUS.REMOVED;
    file.generation += 1;
    file.controller && file.controller.abort();
    const waitDrain =
      file.inflight > 0
        ? new Promise((resolve) => {
            file.whenDrained = resolve;
          })
        : Promise.resolve();
    this.files.delete(id);
    this.emit({ type: 'state', id, state: this._snapshot({ ...file, status: DOWNLOAD_STATUS.REMOVED }) });
    await waitDrain;
    await this.storage.clearChunks(id).catch(() => {});
    await this.storage.deleteMeta(id).catch(() => {});
  }

  _fail(file, error) {
    file.status = DOWNLOAD_STATUS.ERROR;
    file.error = (error && error.message) || String(error);
    this._persist(file).catch(() => {});
    this._emitState(file);
    this.emit({ type: 'error', id: file.id, message: file.error });
  }

  async _persist(file) {
    await this.storage.putMeta({
      id: file.id,
      url: file.url,
      filename: file.filename,
      status: file.status,
      total: file.total,
      downloaded: file.downloaded,
      rangeSupported: file.rangeSupported,
      mode: file.mode,
      chunks: file.chunks,
      chunkCount: file.chunkCount,
      chunkSize: file.chunkSize,
      etag: file.etag,
      lastModified: file.lastModified,
      updatedAt: this.now()
    });
  }

  _snapshot(file) {
    return {
      id: file.id,
      url: file.url,
      filename: file.filename,
      status: file.status,
      total: file.total,
      downloaded: file.downloaded,
      rangeSupported: file.rangeSupported,
      mode: file.mode,
      chunks: file.chunks,
      chunkCount: file.chunkCount,
      chunkSize: file.chunkSize,
      error: file.error
    };
  }

  _emitState(file) {
    this.emit({ type: 'state', id: file.id, state: this._snapshot(file) });
  }

  _emitProgress(file) {
    this.emit({
      type: 'progress',
      id: file.id,
      downloaded: file.downloaded,
      total: file.total,
      chunks: file.chunks,
      chunkCount: file.chunkCount
    });
  }

  // 分块合并：从 IndexedDB 顺序读出并构造 Blob（Blob 之间为引用拼接，不复制字节）。
  async exportFile(id) {
    const file = this.files.get(id) || (await this.storage.getMeta(id));
    if (!file) throw new Error(`未知下载: ${id}`);
    const records = await this.storage.allChunks(id);
    const totalChunks = file.mode === 'whole' || file.rangeSupported === false ? 1 : file.chunkCount;
    if (records.length !== totalChunks) {
      throw new Error(`分块不完整: ${records.length}/${totalChunks}，无法合并`);
    }
    records.sort((a, b) => a.index - b.index);
    const blob = this.createBlob(records.map((record) => record.data));
    return { blob, filename: file.filename, size: file.total || blob.size };
  }
}

function awaitPersist(engine, file, then) {
  engine._persist(file).then(then).catch((error) => engine._fail(file, error));
}
