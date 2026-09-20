// 可重试的 HTTP 状态：429 / 5xx，以及网络层错误（fetch reject、流中断）。
export function isRetryableStatus(status) {
  return status === 408 || status === 425 || status === 429 || (status >= 500 && status <= 599);
}

// 指数退避 + 抖动；遵守 Retry-After（秒 或 HTTP 日期）。
export function backoffDelay(attempt, baseMs, maxMs, retryAfter, now = Date.now) {
  let delay = Math.min(maxMs, baseMs * 2 ** attempt);
  delay *= 0.5 + Math.random() * 0.5;
  if (retryAfter) {
    const seconds = Number(retryAfter);
    if (Number.isFinite(seconds)) {
      delay = Math.max(delay, seconds * 1000);
    } else {
      const at = Date.parse(retryAfter);
      if (Number.isFinite(at)) delay = Math.max(delay, at - now());
    }
  }
  return Math.max(0, Math.min(maxMs, Math.round(delay)));
}

export class HttpError extends Error {
  constructor(status, statusText, url, retryable) {
    super(`HTTP ${status} ${statusText} (${url})`);
    this.name = 'HttpError';
    this.status = status;
    this.retryable = retryable;
  }
}
