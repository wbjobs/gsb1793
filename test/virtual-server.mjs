/**
 * 进程内“HTTP 服务器”：不绑定端口（受限沙箱中 socket() 被 seccomp 拒绝），
 * 直接提供一个与全局 fetch 同形的函数注入给引擎（{ fetchImpl }）。
 *
 * 支持：
 * - Range 请求（206 + Content-Range + Accept-Ranges: bytes）
 * - /no-range：无视 Range 返回 200 整文件
 * - 故障注入：/set-fail?n=N 后每 N 个请求在发送中途以 ECONNRESET 失败
 * - 响应正文是 ReadableStream（与浏览器 fetch.body 行为对齐）
 *
 * 数据由 PRNG 即时生成，可表达 1GB+ 文件而几乎不占内存。
 */

export function prngByte(seed, index) {
  let x = (seed ^ Math.imul(index, 2654435761)) >>> 0;
  x ^= x << 13; x >>>= 0;
  x ^= x >> 17;
  x ^= x << 5; x >>>= 0;
  return x & 0xff;
}

export function generate(start, end, seed = 1) {
  const buf = Buffer.alloc(end - start + 1);
  for (let i = start; i <= end; i++) buf[i - start] = prngByte(seed, i);
  return buf;
}

class VirtualHeaders {
  constructor(init = {}) {
    this._m = new Map();
    for (const [k, v] of Object.entries(init)) this._m.set(k.toLowerCase(), String(v));
  }
  get(name) {
    const v = this._m.get(String(name).toLowerCase());
    return v === undefined ? null : v;
  }
  set(name, value) { this._m.set(String(name).toLowerCase(), String(value)); }
}

class VirtualResponse {
  constructor(body, { status = 200, headers = {} } = {}) {
    this.status = status;
    this.ok = status >= 200 && status < 300;
    this.headers = new VirtualHeaders(headers);
    this.body = body;
  }

  async arrayBuffer() {
    if (this.body == null) return new ArrayBuffer(0);
    const reader = this.body.getReader();
    const parts = [];
    for (;;) {
      const { done, value } = await reader.read();
      if (done) break;
      parts.push(value);
    }
    const total = parts.reduce((n, p) => n + p.byteLength, 0);
    const out = new Uint8Array(total);
    let off = 0;
    for (const p of parts) { out.set(p, off); off += p.byteLength; }
    return out.buffer;
  }
}

export class VirtualServer {
  constructor({ seed = 42, chunkBytes = 64 * 1024 } = {}) {
    this.seed = seed;
    this.chunkBytes = chunkBytes;
    this.state = {
      activeConnections: 0,
      maxActiveConnections: 0,
      ranges: [],
      requestCount: 0,
    };
    this._failEvery = 0;
    this._httpFailEvery = 0;
    this.fetch = this.fetch.bind(this);
  }

  url(path = '/file') {
    return `virtual://local${path}`;
  }

  async setFailEvery(n) { this._failEvery = n; }
  async setHttpFailEvery(n) { this._httpFailEvery = n; }

  async fetch(input, init = {}) {
    const url = new URL(String(typeof input === 'string' ? input : input.url || input), 'http://x');
    const method = (init.method || 'GET').toUpperCase();
    const size = Number(url.searchParams.get('size') || 1024 * 1024);

    if (method === 'HEAD') {
      this.state.requestCount += 1;
      const supportsRange = url.pathname !== '/no-range';
      return new VirtualResponse(null, {
        status: 200,
        headers: {
          'content-length': String(size),
          ...(supportsRange ? { 'accept-ranges': 'bytes', etag: `"seed-${this.seed}-${size}"` } : {}),
        },
      });
    }

    if (url.pathname === '/set-fail') {
      this._failEvery = Number(url.searchParams.get('n') || 1);
      return new VirtualResponse(null, { status: 200, headers: { 'content-length': '0' } });
    }
    if (url.pathname === '/reset-fail') {
      this._failEvery = 0;
      return new VirtualResponse(null, { status: 200, headers: { 'content-length': '0' } });
    }

    const rangeHeader = init.headers?.Range || init.headers?.range || null;
    const supportsRange = url.pathname !== '/no-range';
    let start = 0;
    let end = size - 1;
    let isRange = false;
    if (supportsRange && rangeHeader) {
      const m = String(rangeHeader).match(/bytes=(\d*)-(\d*)/);
      if (m) {
        isRange = true;
        if (m[1] !== '') start = Number(m[1]);
        if (m[2] !== '') end = Number(m[2]);
        if (m[1] !== '' && m[2] === '') end = size - 1;
        end = Math.min(end, size - 1);
      }
    }
    this.state.ranges.push(isRange ? [start, end] : [0, size - 1]);
    // 只有正文 GET 请求参与故障注入计数
    this.state.getCount = (this.state.getCount || 0) + 1;
    const willFail = this._failEvery > 0 && this.state.getCount % this._failEvery === 0;
    if (!willFail && this._httpFailEvery > 0 && this.state.getCount % this._httpFailEvery === 0) {
      return new VirtualResponse(null, {
        status: 500,
        headers: { 'content-type': 'text/plain', 'content-length': '0' },
      });
    }

    const signal = init.signal || null;
    const { seed, chunkBytes } = this;
    const server = this;

    const stream = new ReadableStream({
      start(controller) {
        server.state.activeConnections += 1;
        server.state.maxActiveConnections = Math.max(
          server.state.maxActiveConnections, server.state.activeConnections);
        let pos = start;
        let sent = 0;
        const failAfter = willFail ? Math.min(chunkBytes * 2, end - start + 1) : Infinity;

        const onAbort = () => {
          cleanup();
          controller.error(Object.assign(new Error('The operation was aborted.'), { name: 'AbortError' }));
        };
        if (signal) signal.addEventListener('abort', onAbort, { once: true });

        function cleanup() {
          if (signal) signal.removeEventListener('abort', onAbort);
          server.state.activeConnections = Math.max(0, server.state.activeConnections - 1);
        }

        function step() {
          try {
            if (signal && signal.aborted) {
              cleanup();
              controller.error(Object.assign(new Error('aborted'), { name: 'AbortError' }));
              return;
            }
            if (pos > end) {
              cleanup();
              controller.close();
              return;
            }
            const take = Math.min(chunkBytes, end - pos + 1);
            const buf = generate(pos, pos + take - 1, seed);
            pos += take;
            sent += take;
            if (sent >= failAfter) {
              cleanup();
              controller.error(Object.assign(
                new Error('read ECONNRESET'), { code: 'ECONNRESET' }));
              return;
            }
            controller.enqueue(new Uint8Array(buf));
            queueMicrotask(step);
          } catch (err) {
            cleanup();
            controller.error(err);
          }
        }
        queueMicrotask(step);
      },
    });

    if (isRange) {
      return new VirtualResponse(stream, {
        status: 206,
        headers: {
          'content-type': 'application/octet-stream',
          'accept-ranges': 'bytes',
          'content-length': String(end - start + 1),
          'content-range': `bytes ${start}-${end}/${size}`,
          etag: `"seed-${seed}-${size}"`,
        },
      });
    }
    return new VirtualResponse(stream, {
      status: 200,
      headers: {
        'content-type': 'application/octet-stream',
        'content-length': String(size),
        ...(supportsRange ? { 'accept-ranges': 'bytes', etag: `"seed-${seed}-${size}"` } : {}),
      },
    });
  }
}
