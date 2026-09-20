// 简单信号量：控制全局限速内同时在传的分块数。
export class ConcurrencyGate {
  constructor(max) {
    this.max = Math.max(1, max);
    this.active = 0;
    this._waiters = [];
  }

  setMax(max) {
    this.max = Math.max(1, max);
    while (this._waiters.length > 0 && this.active < this.max) {
      this.active += 1;
      this._waiters.shift()();
    }
  }

  tryAcquire() {
    if (this.active < this.max) {
      this.active += 1;
      return true;
    }
    return false;
  }

  async acquire() {
    if (this.active < this.max) {
      this.active += 1;
      return;
    }
    await new Promise((resolve) => this._waiters.push(resolve));
  }

  release() {
    if (this._waiters.length > 0 && this.active <= this.max) {
      this._waiters.shift()();
      return;
    }
    this.active = Math.max(0, this.active - 1);
  }
}
