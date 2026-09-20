import { HttpError, isRetryableStatus } from './retry.js';

// 解析 Content-Range: bytes start-end/total
export function parseContentRange(headerValue) {
  if (!headerValue) return null;
  const match = /bytes\s+(\d+)-(\d+)\/(?:(\d+)|\*)/.exec(headerValue);
  if (!match) return null;
  return {
    start: Number(match[1]),
    end: Number(match[2]),
    total: match[3] ? Number(match[3]) : null
  };
}

// 探测服务端能力：是否支持 Range、实体标识（etag/lastModified）、总大小。
// 用 bytes=0-0 只取 1 字节，尽快释放连接。
export async function probe(url, { fetchImpl, signal, headers = {} } = {}) {
  const doFetch = fetchImpl || fetch;
  let response;
  try {
    response = await doFetch(url, {
      method: 'GET',
      headers: { ...headers, Range: 'bytes=0-0' },
      signal
    });
  } catch (error) {
    if (error && error.name === 'AbortError') throw error;
    throw Object.assign(new Error(`probe 失败: ${error.message}`), {
      cause: error,
      retryable: true
    });
  }

  if (!response.ok && response.status !== 206) {
    const error = new HttpError(
      response.status,
      response.statusText,
      url,
      isRetryableStatus(response.status)
    );
    error.retryAfter = response.headers.get && response.headers.get('Retry-After');
    if (response.body && response.body.cancel) response.body.cancel().catch(() => {});
    throw error;
  }

  try {
    const rangeSupported =
      response.status === 206 ||
      (response.headers.get &&
        (response.headers.get('Accept-Ranges') || '').toLowerCase().includes('bytes'));

    const contentRange = parseContentRange(response.headers.get && response.headers.get('Content-Range'));
    const headerLength = Number(response.headers.get && response.headers.get('Content-Length'));
    const total =
      contentRange && contentRange.total != null
        ? contentRange.total
        : Number.isFinite(headerLength)
          ? headerLength
          : null;

    return {
      rangeSupported,
      status: response.status,
      etag: (response.headers.get && response.headers.get('ETag')) || null,
      lastModified: (response.headers.get && response.headers.get('Last-Modified')) || null,
      total,
      supportsContentRangeOn200: contentRange != null
    };
  } finally {
    if (response.body && response.body.cancel) {
      response.body.cancel().catch(() => {});
    }
  }
}
// 发起一个 Range 请求。返回 { response, start, end, total } 或在 200 降级时标记 whole。
export async function fetchRange(url, start, end, { fetchImpl, signal, headers = {} } = {}) {
  const doFetch = fetchImpl || fetch;
  const response = await doFetch(url, {
    headers: { ...headers, Range: `bytes=${start}-${end}` },
    signal
  });
  if (!response.ok && response.status !== 206) {
    const retryable = isRetryableStatus(response.status);
    throw new HttpError(response.status, response.statusText, url, retryable);
  }
  if (response.status === 200) return { response, downgraded: true };
  const contentRange = parseContentRange(response.headers.get && response.headers.get('Content-Range'));
  return {
    response,
    downgraded: false,
    start: contentRange ? contentRange.start : start,
    end: contentRange ? contentRange.end : end,
    total: contentRange ? contentRange.total : null
  };
}

export async function fetchWhole(url, { fetchImpl, signal, headers = {} } = {}) {
  const doFetch = fetchImpl || fetch;
  const response = await doFetch(url, { headers: { ...headers }, signal });
  if (!response.ok) {
    const error = new HttpError(response.status, response.statusText, url, isRetryableStatus(response.status));
    error.retryAfter = response.headers.get && response.headers.get('Retry-After');
    if (response.body && response.body.cancel) response.body.cancel().catch(() => {});
    throw error;
  }
  return {
    response,
    total: Number(response.headers.get && response.headers.get('Content-Length')) || null
  };
}

// 通过 ReadableStream 逐块消费响应体。
// onData 可返回 Promise（用于限速等待 / 持久化），从而对读取端施加背压。
export async function pumpBody(response, onData) {
  if (!response.body || !response.body.getReader) {
    const buffer = new Uint8Array(await response.arrayBuffer());
    await onData(buffer);
    return buffer.byteLength;
  }
  const reader = response.body.getReader();
  let total = 0;
  for (;;) {
    const { done, value } = await reader.read();
    if (done) break;
    if (value && value.byteLength > 0) {
      await onData(value);
      total += value.byteLength;
    }
  }
  return total;
}
