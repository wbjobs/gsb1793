// 全局令牌桶限速器。bytesPerSecond 为 0 表示不限速。
// 允许初始 1 秒突发额度，稳态平均速率严格等于设定值。
export class RateLimiter {
  constructor(bytesPerSecond = 0, now = () => Date.now()) {
    this._now = now;
    this.setRate(bytesPerSecond);
  }

  setRate(bytesPerSecond) {
    const rate = Math.max(0, Math.floor(bytesPerSecond));
    if (rate === this.rate) return;
    this.rate = rate;
    // 容量 = 1 秒额度，避免长暂停后“无限”突发
    this.capacity = rate;
    this.tokens = rate;
    this.lastRefill = this._now();
  }

  _refill() {
    if (this.rate === 0) return;
    const now = this._now();
    const elapsedMs = now - this.lastRefill;
    if (elapsedMs > 0) {
      this.tokens = Math.min(
        this.capacity,
        this.tokens + (this.rate * elapsedMs) / 1000
      );
      this.lastRefill = now;
    }
  }

  async acquire(wanted, wait) {
    let remaining = wanted;
    while (remaining > 0) {
      if (this.rate === 0) return wanted;
      this._refill();
      if (this.tokens >= 1) {
        const grant = Math.min(remaining, this.tokens);
        this.tokens -= grant;
        remaining -= grant;
      } else {
        const waitMs = Math.min(
          1000,
          Math.max(1, Math.ceil((Math.max(0, 1 - this.tokens) / this.rate) * 1000))
        );
        await wait(waitMs);
      }
    }
    return wanted;
  }
}
