import {
  DEFAULT_CHUNK_SIZE,
  DEFAULT_CONCURRENCY,
  DEFAULT_RATE_LIMIT,
  DEFAULT_MAX_RETRIES,
  DEFAULT_RETRY_BASE_MS,
  DEFAULT_RETRY_MAX_MS,
  PROGRESS_THROTTLE_MS,
  FileStatus,
} from './constants.js';
import { RateLimiter, isAbortError } from './rate-limiter.js';
import { probe, fetchRange, fetchFull } from './fetcher.js';

let nextId = 1;

function defaultId() {
  return `f${Date.now().toString(36)}_${(nextId++).toString(36)}_${Math.random().toString(36).slice(2, 8)}`;
}

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

/**
 * 下载引擎：环境无关（浏览器 Worker / Node 测试均可运行）。
 *
 * 调度模型：
 * - 全局并发槽 concurrency，所有文件共享；
 * - 每个活跃文件维护一个分块就绪队列，调度器在活跃文件间轮转取任务，
 *   保证多文件下载公平，不会被一个大文件占满全部连接；
 * - 全局一个令牌桶 limiter，对所有连接的读取字节统一限速。
 */
export class DownloadEngine {
  /**
   * @param {object} storage 实现 putChunk/getChunk/listChunks/deleteChunksFrom/deleteFile
   * @param {object} [opts]
   */
  constructor(storage, opts = {}) {
    this.storage = storage;
    this.concurrency = opts.concurrency || DEFAULT_CONCURRENCY;
    this.chunkSize = opts.chunkSize || DEFAULT_CHUNK_SIZE;
    this.maxRetries = opts.maxRetries ?? DEFAULT_MAX_RETRIES;
    this.retryBaseMs = opts.retryBaseMs ?? DEFAULT_RETRY_BASE_MS;
    this.retryMaxMs = opts.retryMaxMs ?? DEFAULT_RETRY_MAX_MS;
    this.fetchImpl = opts.fetchImpl;
    this.now = opts.now || (() => Date.now());
    this.sleep = opts.sleep || sleep;
    this.limiter = opts.limiter || new RateLimiter(opts.rateLimit ?? DEFAULT_RATE_LIMIT, this.now, this.sleep);

    /** @type {Map<string, any>} fileId -> file state */
    this.files = new Map();
    /** @type {Map<number, {fileId,index,start,end}>} 运行中的分块任务 */
    this.jobs = new Map();
    /** fileId -> AbortController（暂停时 abort 所有在飞请求） */
    this._controllers = new Map();
    this._jobSeq = 0;
    this._activeFileOrder = [];
    this._lastProgressEmit = new Map();
    this._scheduled = false;
    this._listeners = new Set();
  }

  onEvent(fn) {
    this._listeners.add(fn);
    return () => this._listeners.delete(fn);
  }

  emit(type, data) {
    const evt = { type, ...data, ts: this.now() };
    for (const fn of this._listeners) {
      try { fn(evt); } catch { /* listener 异常不影响下载 */ }
    }
  }

  setConcurrency(n) {
    this.concurrency = Math.max(1, Math.floor(n));
    this._schedule();
  }

  setRate(bytesPerSec) {
    this.limiter.setRate(bytesPerSec);
  }

  /** 新增下载；已有相同 id（持久化恢复）时用 stored 注入元数据 */
  async addFile({ id, url, filename, size = null, supportsRange = null, etag = null, autoStart = true } = {}) {
    const fileId = id || defaultId();
    if (this.files.has(fileId)) throw new Error(`file already exists: ${fileId}`);
    const file = {
      id: fileId,
      url,
      filename: filename || filenameFromUrl(url),
      size,
      supportsRange,
      etag,
      status: autoStart ? FileStatus.QUEUED : FileStatus.PAUSED,
      chunkSize: this.chunkSize,
      chunkCount: size != null ? Math.max(1, Math.ceil(size / this.chunkSize)) : null,
      downloaded: 0,
      error: null,
      retries: 0,
      createdAt: this.now(),
      queue: [],
      enqueued: new Set(),
      probeDone: size != null && supportsRange != null,
    };
    this.files.set(fileId, file);
    this.emit('file-added', { file: this._publicFile(file) });

    if (file.probeDone) {
      await this._buildPlan(file);
    } else {
      this._queueProbe(file);
    }
    if (autoStart) this._activate(file);
    return this._publicFile(file);
  }

  pause(fileId) {
    const file = this.files.get(fileId);
    if (!file) return;
    if (file.status === FileStatus.COMPLETE) return;
    file.status = FileStatus.PAUSED;
    const ctrl = this._controllers.get(fileId);
    if (ctrl) ctrl.abort();
    // 取消尚未运行的分块任务（放回队列，恢复时重新调度）
    for (const [jobId, job] of this.jobs) {
      if (job.fileId === fileId) {
        this.jobs.delete(jobId);
        file.queue.unshift({ index: job.index, start: job.start, end: job.end });
      }
    }
    this._deactivate(fileId);
    this.emitProgress(file, true);
  }

  resume(fileId) {
    const file = this.files.get(fileId);
    if (!file) return;
    if (file.status === FileStatus.COMPLETE) return;
    file.status = FileStatus.QUEUED;
    file.error = null;
    this._activate(file);
  }

  /** 删除任务；deleteData=true 时同时清除全部已下载分块 */
  async removeFile(fileId, { deleteData = true } = {}) {
    const file = this.files.get(fileId);
    if (file) {
      file.status = FileStatus.REMOVED;
      const ctrl = this._controllers.get(fileId);
      if (ctrl) ctrl.abort();
      for (const [jobId, job] of this.jobs) {
        if (job.fileId === fileId) this.jobs.delete(jobId);
      }
      this._deactivate(fileId);
    }
    if (deleteData) await this.storage.deleteFile(fileId);
    this.files.delete(fileId);
    this.emit('file-removed', { fileId });
    this._schedule();
  }

  getFiles() {
    return [...this.files.values()].map((f) => this._publicFile(f));
  }

  /** 需要持久化的元数据（供 Worker 写入 IDB files store） */
  serializeFile(fileId) {
    const f = this.files.get(fileId);
    if (!f) return null;
    return {
      id: f.id,
      url: f.url,
      filename: f.filename,
      size: f.size,
      supportsRange: f.supportsRange,
      etag: f.etag,
      status: f.status === FileStatus.REMOVED ? f.status : (f.status === FileStatus.DOWNLOADING ? FileStatus.QUEUED : f.status),
      chunkSize: f.chunkSize,
      chunkCount: f.chunkCount,
      downloaded: f.downloaded,
      error: f.error,
      retries: f.retries,
      createdAt: f.createdAt,
    };
  }

  // ----- 内部实现 -----------------------------------------------------------

  _publicFile(f) {
    return {
      id: f.id,
      url: f.url,
      filename: f.filename,
      size: f.size,
      supportsRange: f.supportsRange,
      status: f.status,
      chunkSize: f.chunkSize,
      chunkCount: f.chunkCount,
      downloaded: f.downloaded,
      error: f.error,
      retries: f.retries,
      progress: f.size ? f.downloaded / f.size : 0,
    };
  }

  _activate(file) {
    if (file.status === FileStatus.COMPLETE || file.status === FileStatus.REMOVED) return;
    file.status = FileStatus.QUEUED;
    if (!this._activeFileOrder.includes(file.id)) this._activeFileOrder.push(file.id);
    // 每次从暂停态激活都换一个新的 controller（旧的已经 abort 过）
    const prev = this._controllers.get(file.id);
    if (!prev || prev.signal.aborted) this._controllers.set(file.id, new AbortController());
    this._schedule();
  }

  _deactivate(fileId) {
    const idx = this._activeFileOrder.indexOf(fileId);
    if (idx !== -1) this._activeFileOrder.splice(idx, 1);
  }

  _queueProbe(file) {
    if (file.enqueued.has('probe')) return;
    file.enqueued.add('probe');
    file.queue.push({ probe: true });
  }

  _enqueueChunk(file, index) {
    if (file.enqueued.has(index)) return;
    file.enqueued.add(index);
    const start = index * file.chunkSize;
    const end = file.size != null
      ? Math.min(start + file.chunkSize, file.size) - 1
      : start + file.chunkSize - 1;
    file.queue.push({ index, start, end });
  }

  /** 从活跃文件轮转中取下一个任务（逐文件 dequeue 实现公平） */
  _nextTask() {
    for (let round = 0; round < this._activeFileOrder.length; round++) {
      const fileId = this._activeFileOrder[0];
      this._activeFileOrder.push(this._activeFileOrder.shift());
      const file = this.files.get(fileId);
      if (!file || file.status === FileStatus.PAUSED || file.status === FileStatus.COMPLETE) continue;
      const task = file.queue.shift();
      if (task) {
        if (task.probe) file.enqueued.delete('probe');
        else file.enqueued.delete(task.index);
        return { file, task };
      }
    }
    return null;
  }

  _schedule() {
    if (this._scheduled) return;
    this._scheduled = true;
    queueMicrotask(() => {
      this._scheduled = false;
      this._fillSlots();
    });
  }

  _fillSlots() {
    while (this.jobs.size < this.concurrency) {
      const picked = this._nextTask();
      if (!picked) break;
      const { file, task } = picked;
      const jobId = ++this._jobSeq;
      this.jobs.set(jobId, { fileId: file.id, ...task });
      if (file.status === FileStatus.QUEUED) {
        file.status = FileStatus.DOWNLOADING;
        this.emitProgress(file, true);
      }
      this._runJob(jobId, file, task);
    }
  }

  async _runJob(jobId, file, task) {
    const signal = this._controllers.get(file.id).signal;
    try {
      if (task.probe) {
        await this._runProbe(file, signal);
      } else if (task.full) {
        await this._runFull(file, signal);
      } else {
        await this._runChunkWithRetry(file, task, signal);
      }
    } catch (err) {
      if (isAbortError(err) || signal.aborted) {
        // 暂停/删除导致的中止：pause() 已把任务放回队列，这里直接结束
      } else {
        this._failFile(file, err);
        this._requeue(file, task);
      }
    } finally {
      this.jobs.delete(jobId);
      this._maybeComplete(file);
      this._schedule();
    }
  }

  /** 带指数退避 + 抖动的自动重试；退避期间可被暂停打断 */
  async _withRetry(file, label, fn) {
    let attempt = 0;
    for (;;) {
      try {
        return await fn(attempt);
      } catch (err) {
        if (isAbortError(err)) throw err;
        attempt += 1;
        if (attempt > this.maxRetries) throw err;
        file.retries += 1;
        const backoff = Math.min(
          this.retryMaxMs,
          this.retryBaseMs * 2 ** (attempt - 1),
        );
        const jitter = Math.floor(backoff * 0.5 * Math.random());
        const waitMs = backoff + jitter;
        this.emit('retry', {
          fileId: file.id,
          label,
          attempt,
          maxRetries: this.maxRetries,
          waitMs,
          message: String(err && err.message || err),
        });
        await this._interruptibleSleep(waitMs, this._controllers.get(file.id).signal);
      }
    }
  }

  _interruptibleSleep(ms, signal) {
    return new Promise((resolve, reject) => {
      if (signal.aborted) {
        reject(Object.assign(new Error('aborted'), { name: 'AbortError' }));
        return;
      }
      const t = setTimeout(() => {
        signal.removeEventListener('abort', onAbort);
        resolve();
      }, ms);
      const onAbort = () => {
        clearTimeout(t);
        reject(Object.assign(new Error('aborted'), { name: 'AbortError' }));
      };
      signal.addEventListener('abort', onAbort, { once: true });
    });
  }

  async _runProbe(file, signal) {
    const info = await this._withRetry(file, 'probe', () =>
      probe(file.url, { fetchImpl: this.fetchImpl, signal }));
    file.size = info.size;
    file.supportsRange = info.supportsRange;
    file.etag = info.etag || file.etag;
    await this._buildPlan(file);
    this.emitProgress(file, true);
  }

  /** 恢复任务时的轻量再校验：ETag 变化说明服务器文件已更换，必须清空重来 */
  async revalidate(fileId) {
    const file = this.files.get(fileId);
    if (!file || !file.probeDone) return;
    const signal = this._controllers.get(fileId).signal;
    try {
      const info = await probe(file.url, { fetchImpl: this.fetchImpl, signal });
      if (file.etag && info.etag && info.etag !== file.etag) {
        this.emit('file-changed', { fileId, oldEtag: file.etag, newEtag: info.etag });
        await this.storage.deleteChunksFrom(fileId, 0);
        file.size = info.size;
        file.supportsRange = info.supportsRange;
        file.etag = info.etag;
        file.downloaded = 0;
        file.queue = [];
        file.enqueued = new Set();
        await this._buildPlan(file);
      }
    } catch (err) {
      if (isAbortError(err)) throw err;
      // 离线或服务器不支持探测时沿用本地计划，允许稍后继续尝试
      this.emit('revalidate-skip', { fileId, message: String(err && err.message || err) });
    }
  }

  async _buildPlan(file) {
    file.probeDone = true;
    if (!file.supportsRange || file.size == null) {
      // 不支持 Range（或拿不到长度）：整文件单流，无法从中间续传
      file.chunkCount = null;
      file.queue = [];
      file.enqueued = new Set();
      if (file.downloaded > 0) {
        // 历史数据按 Range 模式存的，与新计划不兼容
        await this.storage.deleteChunksFrom(file.id, 0);
        file.downloaded = 0;
      }
      file.enqueued.add('full');
      file.queue.push({ full: true });
      return;
    }
    file.chunkCount = Math.max(1, Math.ceil(file.size / file.chunkSize));
    file.queue = [];
    file.enqueued = new Set();
    const done = await this.storage.completedChunkIndexes(file.id, file.chunkCount);
    file.downloaded = 0;
    for (let i = 0; i < file.chunkCount; i++) {
      if (done.has(i)) {
        file.downloaded += this._chunkLength(file, i);
      } else {
        this._enqueueChunk(file, i);
      }
    }
  }

  _chunkLength(file, index) {
    if (file.size == null) return file.chunkSize;
    return Math.min(file.chunkSize, file.size - index * file.chunkSize);
  }

  async _runChunkWithRetry(file, task, signal) {
    const parts = [];
    let attemptStart = 0;
    let counted = 0;
    await this._withRetry(file, `chunk#${task.index}`, async () => {
      parts.length = 0;
      attemptStart = 0;
      file.downloaded -= counted; // 回滚上次失败尝试已计入的字节
      counted = 0;
      // 断点续传：同一块内已写入部分也支持区间重试（仅本任务生命周期内有效）
      const rangeStart = task.start + attemptStart;
      await fetchRange(file.url, rangeStart, task.end, {
        fetchImpl: this.fetchImpl,
        signal,
        limiter: this.limiter,
        onData: async (bytes) => {
          parts.push(new Uint8Array(bytes));
          attemptStart += bytes.byteLength;
          file.downloaded += bytes.byteLength;
          counted += bytes.byteLength;
          this.emitProgress(file);
        },
      });
    });
    const blob = makeBlob(parts);
    await this.storage.putChunk({
      fileId: file.id,
      index: task.index,
      start: task.start,
      end: task.end,
      size: blob.size,
      blob,
      done: true,
    });
  }

  async _runFull(file, signal) {
    // 无 Range 模式：从头开始，已有的部分数据全部废弃
    await this.storage.deleteChunksFrom(file.id, 0);
    file.downloaded = 0;
    const chunkSize = file.chunkSize;
    let parts = [];
    let partsBytes = 0;
    let index = 0;
    let attemptCounted = 0;

    const flushPart = async (force) => {
      while (partsBytes >= chunkSize || (force && partsBytes > 0)) {
        const take = Math.min(chunkSize, partsBytes);
        const merged = parts.length === 1 ? parts[0] : concat(parts, partsBytes);
        const slice = merged.subarray(0, take);
        const rest = merged.subarray(take);
        const blob = makeBlob([slice]);
        await this.storage.putChunk({
          fileId: file.id,
          index,
          start: index * chunkSize,
          end: index * chunkSize + take - 1,
          size: blob.size,
          blob,
          done: true,
        });
        index += 1;
        parts = rest.byteLength > 0 ? [rest] : [];
        partsBytes = rest.byteLength;
      }
    };

    const result = await this._withRetry(file, 'full', (attempt) => {
      if (attempt > 0) {
        // 重试必须从头开始：清掉本任务之前写入的分块并回滚进度
        file.downloaded -= attemptCounted;
        attemptCounted = 0;
        index = 0;
      }
      return fetchFull(file.url, {
        fetchImpl: this.fetchImpl,
        signal,
        limiter: this.limiter,
        onData: async (bytes) => {
          parts.push(new Uint8Array(bytes));
          partsBytes += bytes.byteLength;
          file.downloaded += bytes.byteLength;
          attemptCounted += bytes.byteLength;
          this.emitProgress(file);
          await flushPart(false);
        },
      });
    });
    // 注意：full 模式重试会从头再来（上面 deleteChunksFrom 在每次 _runFull 生效）
    await flushPart(true);
    if (result.total == null) file.size = file.downloaded;
    else file.size = result.total;
    file.chunkCount = index;
  }

  _requeue(file, task) {
    // 失败任务放到队首，用户点恢复后优先重试
    if (task.probe) file.enqueued.add('probe');
    else file.enqueued.add(task.index);
    file.queue.unshift(task);
  }

  _failFile(file, err) {
    file.status = FileStatus.ERROR;
    file.error = String(err && err.message || err);
    this._deactivate(file.id);
    this.emit('file-error', { fileId: file.id, error: file.error });
    this.emitProgress(file, true);
  }

  _maybeComplete(file) {
    if (file.status !== FileStatus.DOWNLOADING && file.status !== FileStatus.QUEUED) return;
    const hasRunning = [...this.jobs.values()].some((j) => j.fileId === file.id);
    if (hasRunning || file.queue.length > 0) return;
    if (!file.probeDone) return;
    if (file.supportsRange && file.size != null && file.downloaded < file.size) return;
    file.status = FileStatus.COMPLETE;
    this._deactivate(file.id);
    this.emit('file-complete', { fileId: file.id, file: this._publicFile(file) });
    this.emitProgress(file, true);
  }

  emitProgress(file, force = false) {
    const last = this._lastProgressEmit.get(file.id) || 0;
    if (!force && this.now() - last < PROGRESS_THROTTLE_MS) return;
    this._lastProgressEmit.set(file.id, this.now());
    this.emit('progress', { file: this._publicFile(file) });
  }
}

function makeBlob(parts) {
  if (typeof Blob === 'undefined') {
    // Node 18+ 全局有 Blob；极端环境退回带 arrayBuffer 的简单封装
    const total = parts.reduce((n, p) => n + p.byteLength, 0);
    const buf = concat(parts, total);
    return {
      size: total,
      arrayBuffer: async () => buf.buffer.slice(buf.byteOffset, buf.byteOffset + total),
    };
  }
  // 复制到精确大小的缓冲区，避免 SharedArrayBuffer / 大底层缓冲导致单块体积虚高
  const total = parts.reduce((n, p) => n + p.byteLength, 0);
  const buf = new ArrayBuffer(total);
  const view = new Uint8Array(buf);
  let off = 0;
  for (const p of parts) {
    view.set(p, off);
    off += p.byteLength;
  }
  return new Blob([buf], { type: 'application/octet-stream' });
}

function concat(parts, total) {
  const out = new Uint8Array(total);
  let off = 0;
  for (const p of parts) {
    out.set(p, off);
    off += p.byteLength;
  }
  return out;
}

function filenameFromUrl(url) {
  try {
    const u = new URL(url, 'http://x');
    const name = u.pathname.split('/').filter(Boolean).pop();
    return name ? decodeURIComponent(name) : 'download.bin';
  } catch {
    return 'download.bin';
  }
}
