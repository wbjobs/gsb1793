/**
 * 令牌桶限速器。
 *
 * 全局唯一实例，所有分块的每个读取批次（READ_FLUSH_BYTES）在真正处理数据前
 * 先向桶中申请令牌，从而把所有并发连接的总吞吐限制在 rate bytes/s。
 *
 * 模型：
 * - 令牌按经过时间连续补充（懒计算），桶容量有限，避免空闲后的大突发；
 * - 等待者按 FIFO 排队，队首令牌不足时各自挂一个“预计补足时刻”的定时器，
 *   醒来后重新检查；不满足则继续等，保证不超发、不饿死；
 * - acquire 支持 AbortSignal：暂停任务时立即从队列移除。
 */

const MAX_QUEUE = 10_000;

export class RateLimiter {
  /**
   * @param {number} [rateBytesPerSec] 0 表示不限速
   * @param {() => number} [now] 可注入时钟
   * @param {(ms:number)=>any} [sleep] 可注入 sleep
   */
  constructor(rateBytesPerSec = 0, now = () => Date.now(), sleep = (ms) => new Promise((r) => setTimeout(r, ms))) {
    this._rate = Math.max(0, rateBytesPerSec);
    this._now = now;
    this._sleep = sleep;
    this._tokens = 0;
    this._last = now();
    this._waiters = [];
    this._maxBurst = 4 * 1024 * 1024;
  }

  get rate() {
    return this._rate;
  }

  /** 动态调整限速，0 = 关闭限速 */
  setRate(rateBytesPerSec) {
    this._refill();
    this._rate = Math.max(0, rateBytesPerSec);
    this._wakeAll();
  }

  _refill() {
    const t = this._now();
    const elapsedSec = (t - this._last) / 1000;
    this._last = t;
    if (this._rate > 0) {
      this._tokens = Math.min(this._maxBurst, this._tokens + elapsedSec * this._rate);
    } else {
      this._tokens = 0;
    }
  }

  /**
   * 申请 bytes 个令牌；限速开启时整体吞吐不超过设定速率。
   * @param {number} bytes
   * @param {{signal?: AbortSignal}} [opts]
   */
  async acquire(bytes, opts = {}) {
    if (this._rate === 0 || bytes <= 0) return;
    const signal = opts.signal;
    if (signal && signal.aborted) throw makeAbortError();

    const step = this._maxBurst;
    while (bytes > 0) {
      const want = Math.min(bytes, step);
      await this._acquireOne(want, signal);
      bytes -= want;
    }
  }

  _acquireOne(bytes, signal) {
    return new Promise((resolve, reject) => {
      const waiter = { bytes, resolve, reject, signal: signal || null, timer: null };
      waiter.onAbort = () => this._removeWaiter(waiter, makeAbortError());
      if (signal) signal.addEventListener('abort', waiter.onAbort, { once: true });
      this._waiters.push(waiter);
      if (this._waiters.length >= MAX_QUEUE) {
        this._removeWaiter(waiter, new Error('rate limiter queue overflow'));
        return;
      }
      this._serve();
    });
  }

  /** 从头开始，尽量满足队首；不足时给队首安排下一次唤醒 */
  _serve() {
    if (this._rate === 0) {
      const all = this._waiters.splice(0);
      for (const w of all) this._finishWaiter(w);
      return;
    }
    const head = this._waiters[0];
    if (!head || head.timer !== null) return;
    this._refill();
    if (head.signal && head.signal.aborted) {
      this._waiters.shift();
      this._rejectWaiter(head, makeAbortError());
      this._serve();
      return;
    }
    if (this._tokens + 1e-6 >= head.bytes) {
      this._waiters.shift();
      this._tokens -= head.bytes;
      this._finishWaiter(head);
      this._serve();
      return;
    }
    const needMs = Math.max(1, Math.ceil(((head.bytes - this._tokens) / this._rate) * 1000));
    head.timer = setTimeout(() => {
      head.timer = null;
      this._serve();
    }, needMs);
  }

  _wakeAll() {
    if (this._rate === 0) {
      const all = this._waiters.splice(0);
      for (const w of all) this._finishWaiter(w);
      return;
    }
    for (const w of this._waiters) {
      if (w.timer !== null) {
        clearTimeout(w.timer);
        w.timer = null;
      }
    }
    this._serve();
  }

  _removeWaiter(waiter, error) {
    const idx = this._waiters.indexOf(waiter);
    if (idx !== -1) this._waiters.splice(idx, 1);
    if (waiter.timer !== null) {
      clearTimeout(waiter.timer);
      waiter.timer = null;
    }
    this._rejectWaiter(waiter, error);
    this._serve();
  }

  _finishWaiter(w) {
    if (w.signal) w.signal.removeEventListener('abort', w.onAbort);
    if (w.timer !== null) clearTimeout(w.timer);
    w.resolve();
  }

  _rejectWaiter(w, error) {
    if (w.signal) w.signal.removeEventListener('abort', w.onAbort);
    if (w.timer !== null) clearTimeout(w.timer);
    w.reject(error);
  }
}

export function makeAbortError() {
  if (typeof DOMException !== 'undefined') {
    return new DOMException('The operation was aborted.', 'AbortError');
  }
  const err = new Error('The operation was aborted.');
  err.name = 'AbortError';
  return err;
}

export function isAbortError(err) {
  return !!err && err.name === 'AbortError';
}
