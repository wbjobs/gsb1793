import { test } from 'node:test';
import assert from 'node:assert/strict';
import { RateLimiter } from '../src/engine/RateLimiter.js';
import { ConcurrencyGate } from '../src/engine/ConcurrencyGate.js';
import { backoffDelay, isRetryableStatus } from '../src/engine/retry.js';
import { parseContentRange } from '../src/engine/http.js';

const realWait = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

test('RateLimiter: 不限速模式立即放行', async () => {
  const limiter = new RateLimiter(0);
  const start = Date.now();
  await limiter.acquire(10 ** 9, realWait);
  assert.ok(Date.now() - start < 50);
});

test('RateLimiter: 稳态平均速率准确（100KB/s，扣除 1 秒突发额度）', async () => {
  // 桶容量 = 1 秒额度（100KB，首秒突发）。消费 250KB 应耗时 ≈1.5s。
  const RATE = 100_000;
  const limiter = new RateLimiter(RATE);
  const start = Date.now();
  const consumers = [];
  for (let i = 0; i < 250; i += 1) {
    consumers.push(limiter.acquire(1_000, realWait));
  }
  await Promise.all(consumers);
  const elapsed = Date.now() - start;
  assert.ok(elapsed >= 1_300, `过快，未限速：${elapsed}ms`);
  assert.ok(elapsed <= 2_400, `过慢：${elapsed}ms`);
  // 扣除突发后的稳态速率
  const steady = (250_000 - RATE) / (elapsed / 1000);
  assert.ok(steady >= RATE * 0.8, `稳态速率偏低：${Math.round(steady)}`);
  assert.ok(steady <= RATE * 1.35, `稳态速率偏高：${Math.round(steady)}`);
});

test('RateLimiter: setRate 动态生效', async () => {
  const limiter = new RateLimiter(1_000_000);
  await limiter.acquire(1_000_000, realWait);
  limiter.setRate(50_000); // 降速到 50KB/s（桶重置为 50KB）
  const start = Date.now();
  await limiter.acquire(100_000, realWait); // 突发 50KB + 1s 补 50KB
  const elapsed = Date.now() - start;
  assert.ok(elapsed >= 850, `降速未生效：${elapsed}ms`);
  assert.ok(elapsed <= 2_200, `降速后过慢：${elapsed}ms`);
});

test('ConcurrencyGate: 限制同时进入数并在释放后放行', () => {
  const gate = new ConcurrencyGate(2);
  assert.equal(gate.tryAcquire(), true);
  assert.equal(gate.tryAcquire(), true);
  assert.equal(gate.tryAcquire(), false);
  gate.release();
  assert.equal(gate.tryAcquire(), true);
});

test('ConcurrencyGate: setMax 唤醒等待者', async () => {
  const gate = new ConcurrencyGate(1);
  await gate.acquire();
  let acquired = false;
  const waiter = gate.acquire().then(() => {
    acquired = true;
  });
  gate.setMax(2);
  await waiter;
  assert.equal(acquired, true);
});

test('backoffDelay: 指数增长且不超过上限', () => {
  const fixed = () => 0;
  const d0 = backoffDelay(0, 100, 10000, null, fixed);
  const d1 = backoffDelay(1, 100, 10000, null, fixed);
  const d4 = backoffDelay(4, 100, 10000, null, fixed);
  assert.ok(d0 >= 50 && d0 <= 100);
  assert.ok(d1 >= 100 && d1 <= 200);
  assert.ok(d4 >= 500 && d4 <= 1600);
  assert.ok(backoffDelay(20, 100, 10000, null, fixed) <= 10000);
});

test('backoffDelay: Retry-After 秒优先生效', () => {
  const delay = backoffDelay(0, 100, 10000, '5', () => 0);
  assert.ok(delay >= 5000);
});

test('isRetryableStatus: 429 与 5xx 可重试，404/416 不可', () => {
  assert.equal(isRetryableStatus(429), true);
  assert.equal(isRetryableStatus(503), true);
  assert.equal(isRetryableStatus(404), false);
  assert.equal(isRetryableStatus(416), false);
});

test('parseContentRange: 标准与未知长度两种形式', () => {
  assert.deepEqual(parseContentRange('bytes 0-1023/4096'), { start: 0, end: 1023, total: 4096 });
  assert.deepEqual(parseContentRange('bytes 100-200/*'), { start: 100, end: 200, total: null });
  assert.equal(parseContentRange(null), null);
  assert.equal(parseContentRange('items 0-10/100'), null);
});
