import { test } from 'node:test';
import assert from 'node:assert/strict';
import { RateLimiter } from '../src/rate-limiter.js';

test('不限速时立即放行', async () => {
  const limiter = new RateLimiter(0);
  const t0 = performance.now();
  await limiter.acquire(10_000_000);
  assert.ok(performance.now() - t0 < 20);
});

test('限速总量准确：稳态吞吐不超过设定速率（误差 ±25%）', async () => {
  const rate = 400_000;
  const limiter = new RateLimiter(rate);
  // 先耗尽桶容量（4MB 突发），再测量稳态 1.5s 内的实际放行量
  await limiter.acquire(4 * 1024 * 1024);
  const target = 600_000;
  const t0 = Date.now();
  const tasks = [];
  for (let i = 0; i < 6; i++) tasks.push(limiter.acquire(target / 6));
  await Promise.all(tasks);
  const elapsed = (Date.now() - t0) / 1000;
  const actualRate = target / elapsed;
  assert.ok(actualRate <= rate * 1.25, `rate ${actualRate} exceeds ${rate}`);
  assert.ok(actualRate >= rate * 0.75, `rate ${actualRate} far below ${rate}`);
});

test('setRate(0) 立即解除等待', async () => {
  const limiter = new RateLimiter(1024);
  let resolved = false;
  const p = limiter.acquire(10_000_000).then(() => { resolved = true; });
  await new Promise((r) => setTimeout(r, 30));
  assert.equal(resolved, false);
  limiter.setRate(0);
  await p;
  assert.equal(resolved, true);
});

test('abort 等待中的申请立即失败', async () => {
  const limiter = new RateLimiter(1024);
  const ac = new AbortController();
  const p = limiter.acquire(10_000_000, { signal: ac.signal });
  setTimeout(() => ac.abort(), 20);
  await assert.rejects(p, /aborted/);
});

test('并发申请按 FIFO 不超发', async () => {
  // 10KB/s：两个各 10KB 的请求总耗时应 >= 1s
  const limiter = new RateLimiter(10_240);
  const t0 = Date.now();
  await Promise.all([limiter.acquire(10_240), limiter.acquire(10_240)]);
  const elapsed = Date.now() - t0;
  assert.ok(elapsed >= 900, `expected throttling, got ${elapsed}ms`);
});
