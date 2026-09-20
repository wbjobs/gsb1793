// 测试夹具：
// - fakeStorage：内存版 ChunkStorage（记录 dataSize 而不保留全部字节，支持 1GB 用例）
// - mockFetch：确定性 Range/整文件服务，支持故障注入、延迟、Abort、200 降级
import { isRetryableStatus } from '../../src/engine/retry.js';

export class FakeStorage {
  constructor() {
    this.metas = new Map();
    this.chunks = new Map();
    this.chunkData = new Map();
  }
  _key(id, index) {
    return `${id}:${index}`;
  }
  async putMeta(meta) {
    this.metas.set(meta.id, { ...meta });
  }
  async getMeta(id) {
    return this.metas.has(id) ? { ...this.metas.get(id) } : null;
  }
  async allMeta() {
    return [...this.metas.values()].map((meta) => ({ ...meta }));
  }
  async deleteMeta(id) {
    this.metas.delete(id);
  }
  async putChunk(id, index, data) {
    const key = this._key(id, index);
    this.chunks.set(key, { id, index, size: dataSize(data) });
    this.chunkData.set(key, data);
  }
  async getChunk(id, index) {
    return this.chunkData.get(this._key(id, index)) ?? null;
  }
  async listChunks(id) {
    return [...this.chunks.values()]
      .filter((item) => item.id === id)
      .map((item) => ({ index: item.index, size: item.size }));
  }
  async chunkCount(id) {
    return (await this.listChunks(id)).length;
  }
  async allChunks(id) {
    const items = await this.listChunks(id);
    items.sort((a, b) => a.index - b.index);
    return items.map((item) => ({ index: item.index, data: this.chunkData.get(this._key(id, item.index)) }));
  }
  async clearChunks(id) {
    for (const key of [...this.chunks.keys()]) {
      if (key.startsWith(`${id}:`)) {
        this.chunks.delete(key);
        this.chunkData.delete(key);
      }
    }
  }
}

function dataSize(data) {
  if (!data) return 0;
  if (data.byteLength != null) return data.byteLength;
  if (data.size != null) return data.size;
  return 0;
}

// ---- 确定性字节（与 server/server.mjs 同一套公式，便于做内容校验）----
function byteAt(offset) {
  const seedText = 'deterministic-fixture';
  const blockIndex = Math.floor(offset / 64);
  const i = offset % 64;
  let value = (seedText.charCodeAt(i % seedText.length) * 31 + i * 7) & 0xff;
  const stamp = BigInt(blockIndex) * 0x9e3779b97f4a7c15n & 0xffffffffffffffffn;
  value ^= Number((stamp >> BigInt(i * 8)) & 0xffn);
  return value;
}

export function expectedByteAt(offset) {
  return byteAt(offset);
}

function makeBuffer(start, length) {
  const buffer = Buffer.alloc(length);
  for (let offset = 0; offset < length; offset += 1) {
    buffer[offset] = byteAt(start + offset);
  }
  return new Uint8Array(buffer.buffer, buffer.byteOffset, buffer.byteLength);
}

// 64 字节对齐快速路径：与 byteAt 相同公式，但整块批量生成。
function makeBufferFast(start, length) {
  if (start % 64 !== 0) return makeBuffer(start, length);
  const buffer = Buffer.alloc(length);
  const seedText = 'deterministic-fixture';
  const blocks = Math.ceil(length / 64);
  for (let b = 0; b < blocks; b += 1) {
    const blockIndex = start / 64 + b;
    const stamp = (BigInt(blockIndex) * 0x9e3779b97f4a7c15n) & 0xffffffffffffffffn;
    const pos = b * 64;
    const len = Math.min(64, length - pos);
    for (let i = 0; i < len; i += 1) {
      const seedByte = (seedText.charCodeAt(i % seedText.length) * 31 + i * 7) & 0xff;
      buffer[pos + i] = seedByte ^ Number((stamp >> BigInt(i * 8)) & 0xffn);
    }
  }
  return new Uint8Array(buffer.buffer, buffer.byteOffset, buffer.byteLength);
}


class MockHeaders {
  constructor(map = {}) {
    this._map = new Map(Object.entries(map).map(([key, value]) => [key.toLowerCase(), String(value)]));
  }
  get(name) {
    const key = String(name).toLowerCase();
    return this._map.has(key) ? this._map.get(key) : null;
  }
}

function streamFor(rangeStart, rangeEnd, { chunkSize = 64 * 1024, signal } = {}) {
  let cursor = rangeStart;
  let aborted = false;
  if (signal) signal.addEventListener('abort', () => { aborted = true; }, { once: true });
  return new ReadableStream({
    pull(controller) {
      if (aborted || (signal && signal.aborted)) {
        controller.error(Object.assign(new Error('aborted'), { name: 'AbortError' }));
        return;
      }
      if (cursor > rangeEnd) {
        controller.close();
        return;
      }
      let next = Math.min(rangeEnd, cursor + chunkSize - 1);
      if (cursor % 64 !== 0) {
        next = Math.min(next, cursor + (64 - (cursor % 64)) - 1);
      }
      controller.enqueue(makeBufferFast(cursor, next - cursor + 1));
      cursor = next + 1;
    },
    cancel() {
      aborted = true;
    }
  });
}

// config: { total, failFirst(url)=>number, disableRange, latencyMs }
export function createMockFetch(config = {}) {
  const total = config.total ?? 1024;
  const failLeft = new Map();
  const failFirst = config.failFirst || (() => 0);

  return function mockFetch(url, init = {}) {
    const urlString = String(url);
    const key = urlString.split('?')[0];
    // 两种故障语义都支持：
    //  - failFirst() 返回剩余失败次数（计数器模式，?fail=N 静态注入）
    //  - failFirst() 依据外部状态每次返回 0/非 0（动态恢复模式）
    if (!failLeft.has(key) && config.failFirstReturnsRemaining !== false) {
      const initial = failFirst(urlString);
      failLeft.set(key, initial);
    }
    const dynamicFailures = config.failFirstReturnsRemaining === false ? failFirst(urlString) : 0;

    const headers0 = init.headers || {};
    const rangeHeader = (headers0.Range || headers0.range || null);

    const signal = init.signal || null;
    if (signal && signal.aborted) {
      return Promise.reject(Object.assign(new Error('aborted'), { name: 'AbortError' }));
    }

    const remaining = config.failFirstReturnsRemaining === false
      ? dynamicFailures
      : failLeft.get(key) || 0;
    if (remaining > 0) {
      if (config.failFirstReturnsRemaining !== false) failLeft.set(key, remaining - 1);
      const error = new Error('HTTP 500 mock failure');
      error.status = 500;
      error.retryable = isRetryableStatus(500);
      error.retryAfter = '0';
      return Promise.resolve({
        ok: false,
        status: 500,
        statusText: 'Internal Server Error',
        headers: new MockHeaders({ 'Retry-After': '0' }),
        async arrayBuffer() {
          throw error;
        }
      });
    }

    // 整文件模式（无 Range 或服务端不支持 Range，直接 200）
    if (!rangeHeader || config.disableRange) {
      return delay(config.latencyMs, signal).then(() => ({
        ok: true,
        status: 200,
        statusText: 'OK',
        headers: new MockHeaders({
          'Content-Length': String(total),
          'Accept-Ranges': config.disableRange ? 'none' : 'bytes',
          ETag: `"mock-${total}"`,
          'Last-Modified': 'Mon, 01 Jan 2024 00:00:00 GMT'
        }),
        body: streamFor(0, total - 1, { signal })
      }));
    }

    const match = /bytes=(\d+)-(\d*)/.exec(rangeHeader);
    const start = Number(match[1]);
    const end = match[2] === '' ? total - 1 : Number(match[2]);
    if (start >= total) {
      return Promise.resolve({
        ok: false,
        status: 416,
        statusText: 'Range Not Satisfiable',
        headers: new MockHeaders({ 'Content-Range': `bytes */${total}` }),
        body: null
      });
    }
    const realEnd = Math.min(end, total - 1);
    return delay(config.latencyMs, signal).then(() => ({
      ok: true,
      status: 206,
      statusText: 'Partial Content',
      headers: new MockHeaders({
        'Content-Length': String(realEnd - start + 1),
        'Content-Range': `bytes ${start}-${realEnd}/${total}`,
        'Accept-Ranges': 'bytes',
        ETag: `"mock-${total}"`,
        'Last-Modified': 'Mon, 01 Jan 2024 00:00:00 GMT'
      }),
      body: streamFor(start, realEnd, { signal })
    }));
  };
}

function delay(ms, signal) {
  if (!ms) return Promise.resolve();
  if (signal && signal.aborted) {
    return Promise.reject(Object.assign(new Error('aborted'), { name: 'AbortError' }));
  }
  return new Promise((resolve, reject) => {
    const timer = setTimeout(resolve, ms);
    if (signal) {
      signal.addEventListener(
        'abort',
        () => {
          clearTimeout(timer);
          reject(Object.assign(new Error('aborted'), { name: 'AbortError' }));
        },
        { once: true }
      );
    }
  });
}

// 可控制的虚拟时钟，配合 RateLimiter 的 now/wait
export function createFakeClock(start = 1_000_000) {
  let time = start;
  const pending = new Set();
  return {
    now: () => time,
    wait: (ms) =>
      new Promise((resolve) => {
        const entry = { at: time + ms, resolve };
        pending.add(entry);
      }),
    async advance(ms) {
      const target = time + ms;
      const due = [...pending].filter((entry) => entry.at <= target);
      due.sort((a, b) => a.at - b.at);
      for (const entry of due) {
        time = Math.max(time, entry.at);
        pending.delete(entry);
        entry.resolve();
      }
      time = target;
      await Promise.resolve();
      await Promise.resolve();
    }
  };
}

export function waitFor(check, { timeout = 5000, interval = 10 } = {}) {
  const start = Date.now();
  return new Promise((resolve, reject) => {
    const tick = () => {
      let result;
      try {
        result = check();
      } catch (error) {
        reject(error);
        return;
      }
      if (result) resolve(result);
      else if (Date.now() - start > timeout) reject(new Error('waitFor 超时'));
      else setTimeout(tick, interval);
    };
    tick();
  });
}
