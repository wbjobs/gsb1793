import { READ_FLUSH_BYTES, ABORT_ERROR_NAME } from './constants.js';
import { isAbortError, makeAbortError } from './rate-limiter.js';

export class HttpError extends Error {
  constructor(message, { status, url, acceptRanges = null } = {}) {
    super(message);
    this.name = 'HttpError';
    this.status = status;
    this.url = url;
    this.acceptRanges = acceptRanges;
  }
}

export class RangeUnsupportedError extends Error {
  constructor(url) {
    super('server does not support byte ranges');
    this.name = 'RangeUnsupportedError';
    this.url = url;
  }
}

export class TruncatedError extends Error {
  constructor(expected, actual) {
    super(`response truncated: expected ${expected} bytes, got ${actual}`);
    this.name = 'TruncatedError';
    this.expected = expected;
    this.actual = actual;
  }
}

/**
 * HEAD 探测：返回文件大小、是否支持 Range、etag。
 * 不支持 HEAD 的服务器会自动降级为 GET 探测（带 0-0 Range，不下载正文）。
 */
export async function probe(url, { fetchImpl = fetch, signal } = {}) {
  let resp;
  try {
    resp = await fetchImpl(url, { method: 'HEAD', signal });
  } catch (err) {
    if (isAbortError(err)) throw err;
    resp = await fetchImpl(url, {
      method: 'GET',
      headers: { Range: 'bytes=0-0' },
      signal,
    });
  }
  if (!resp.ok && resp.status !== 206) {
    throw new HttpError(`probe failed: HTTP ${resp.status}`, {
      status: resp.status,
      url,
      acceptRanges: resp.headers.get('accept-ranges'),
    });
  }
  const acceptRanges = (resp.headers.get('accept-ranges') || '').toLowerCase();
  const supportsRange = resp.status === 206 || acceptRanges === 'bytes';
  const length = Number(resp.headers.get('content-length'));
  return {
    size: Number.isFinite(length) ? length : null,
    supportsRange,
    etag: resp.headers.get('etag'),
    lastModified: resp.headers.get('last-modified'),
    status: resp.status,
  };
}

/**
 * 抓取一个字节区间，边读边限速、边回调数据。
 *
 * @returns {Promise<{received:number, status:number, total:?number}>}
 */
export async function fetchRange(url, start, end, {
  fetchImpl = fetch,
  signal,
  limiter = null,
  onData = null,
  highWaterMark = READ_FLUSH_BYTES,
} = {}) {
  const expected = end - start + 1;
  const resp = await fetchImpl(url, {
    headers: { Range: `bytes=${start}-${end}` },
    signal,
  });

  if (resp.status === 200) {
    // 服务器无视 Range，返回整个文件：交给上层走无 Range 模式，本次分块中止
    throw new RangeUnsupportedError(url);
  }
  if (resp.status !== 206) {
    throw new HttpError(`range request failed: HTTP ${resp.status}`, {
      status: resp.status,
      url,
      acceptRanges: resp.headers.get('accept-ranges'),
    });
  }
  if (!resp.body) {
    // 极老环境没有 ReadableStream，退化为 arrayBuffer
    const buf = new Uint8Array(await resp.arrayBuffer());
    if (limiter) await limiter.acquire(buf.byteLength, { signal });
    if (onData) await onData(buf.subarray(0, expected), 0, expected);
    if (buf.byteLength < expected) throw new TruncatedError(expected, buf.byteLength);
    return { received: expected, status: 206, total: parseTotal(resp) };
  }

  const reader = resp.body.getReader();
  let received = 0;
  let pending = [];
  let pendingBytes = 0;
  const abortIfNeeded = () => {
    if (signal && signal.aborted) throw makeAbortError();
  };

  const flush = async () => {
    if (pendingBytes === 0) return;
    const chunk = pending.length === 1
      ? pending[0]
      : concatBytes(pending, pendingBytes);
    const slice = chunk.subarray(0, Math.min(chunk.byteLength, expected - received));
    if (limiter) await limiter.acquire(slice.byteLength, { signal });
    if (onData) await onData(slice, start + received, expected);
    received += slice.byteLength;
    pending = [];
    pendingBytes = 0;
  };

  try {
    for (;;) {
      abortIfNeeded();
      const { done, value } = await reader.read();
      if (done) break;
      if (!value || value.byteLength === 0) continue;
      abortIfNeeded();
      pending.push(value);
      pendingBytes += value.byteLength;
      if (pendingBytes >= highWaterMark) await flush();
    }
    await flush();
  } catch (err) {
    if (isAbortError(err) || (err && err.name === ABORT_ERROR_NAME)) {
      try { await reader.cancel(); } catch { /* ignore */ }
    }
    throw err;
  }

  if (received !== expected) throw new TruncatedError(expected, received);
  return { received, status: 206, total: parseTotal(resp) };
}

/**
 * 无 Range 支持时整文件流式下载，从 offset 开始。
 * 注意：服务器无法从中间续传，offset 必须是 0（上层会在恢复时先清空旧数据）。
 */
export async function fetchFull(url, {
  fetchImpl = fetch,
  signal,
  limiter = null,
  onData = null,
  highWaterMark = READ_FLUSH_BYTES,
} = {}) {
  const resp = await fetchImpl(url, { signal });
  if (!resp.ok) {
    throw new HttpError(`download failed: HTTP ${resp.status}`, {
      status: resp.status,
      url,
    });
  }
  const total = Number(resp.headers.get('content-length'));
  const totalSize = Number.isFinite(total) ? total : null;
  if (!resp.body) {
    const buf = new Uint8Array(await resp.arrayBuffer());
    if (limiter) await limiter.acquire(buf.byteLength, { signal });
    if (onData) await onData(buf, 0, buf.byteLength);
    return { received: buf.byteLength, status: resp.status, total: totalSize };
  }

  const reader = resp.body.getReader();
  let received = 0;
  let pending = [];
  let pendingBytes = 0;
  const abortIfNeeded = () => {
    if (signal && signal.aborted) throw makeAbortError();
  };
  const flush = async () => {
    if (pendingBytes === 0) return;
    const chunk = pending.length === 1 ? pending[0] : concatBytes(pending, pendingBytes);
    if (limiter) await limiter.acquire(chunk.byteLength, { signal });
    if (onData) await onData(chunk, received, totalSize);
    received += chunk.byteLength;
    pending = [];
    pendingBytes = 0;
  };

  try {
    for (;;) {
      abortIfNeeded();
      const { done, value } = await reader.read();
      if (done) break;
      if (!value || value.byteLength === 0) continue;
      abortIfNeeded();
      pending.push(value);
      pendingBytes += value.byteLength;
      if (pendingBytes >= highWaterMark) await flush();
    }
    await flush();
  } catch (err) {
    if (isAbortError(err) || (err && err.name === ABORT_ERROR_NAME)) {
      try { await reader.cancel(); } catch { /* ignore */ }
    }
    throw err;
  }
  return { received, status: resp.status, total: totalSize };
}

function parseTotal(resp) {
  const cr = resp.headers.get('content-range'); // bytes start-end/total
  if (!cr) return null;
  const total = Number(cr.split('/')[1]);
  return Number.isFinite(total) ? total : null;
}

function concatBytes(parts, total) {
  const out = new Uint8Array(total);
  let off = 0;
  for (const p of parts) {
    out.set(p, off);
    off += p.byteLength;
  }
  return out;
}
