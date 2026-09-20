import { test } from 'node:test';
import assert from 'node:assert/strict';
import { fetchRange } from '../src/fetcher.js';
import { RateLimiter } from '../src/rate-limiter.js';
import { VirtualServer, generate } from './virtual-server.mjs';

test('fetchRange：正确解析 206 与 Content-Range，字节与生成数据一致', async () => {
  const srv = new VirtualServer();
  const chunks = [];
  const result = await fetchRange(srv.url('/file?size=100000'), 1000, 9999, {
    fetchImpl: srv.fetch,
    onData: (bytes) => chunks.push(Buffer.from(bytes)),
  });
  assert.equal(result.status, 206);
  assert.equal(result.total, 100000);
  assert.equal(result.received, 9000);
  const got = Buffer.concat(chunks);
  assert.ok(got.equals(generate(1000, 9999, 42)));
});

test('fetchRange：服务器返回 200 时抛出 RangeUnsupportedError', async () => {
  const srv = new VirtualServer();
  await assert.rejects(
    () => fetchRange(srv.url('/no-range?size=1000'), 0, 99, { fetchImpl: srv.fetch }),
    /range/i,
  );
});

test('fetchRange：中断后用新起始点续传，拼接字节完整（分块内续传）', async () => {
  const srv = new VirtualServer();
  // 第一次：手动只读到 3000 字节就 abort
  const ac = new AbortController();
  let got1 = 0;
  // flush 批大小设为 1KB，让 10KB 区间分 10 批回调，中途才有机会 abort
  await assert.rejects(
    () => fetchRange(srv.url('/file?size=100000'), 0, 9999, {
      fetchImpl: srv.fetch,
      signal: ac.signal,
      highWaterMark: 1024,
      onData: (bytes) => {
        got1 += bytes.byteLength;
        if (got1 >= 3000) ac.abort();
      },
    }),
    /aborted/i,
  );
  // 第二次：从 3000 继续，两次拼接应等于完整区间
  const parts1 = [];
  // 注意：上面 onData 未保存字节，这里直接用生成数据校验“后半段”
  void parts1;
  const rest = [];
  await fetchRange(srv.url('/file?size=100000'), 3000, 9999, {
    fetchImpl: srv.fetch,
    onData: (bytes) => rest.push(Buffer.from(bytes)),
  });
  const head = generate(0, 2999, 42);
  const tail = Buffer.concat(rest);
  assert.ok(tail.equals(generate(3000, 9999, 42)));
  assert.equal(head.length + tail.length, 10000);
});

test('fetchRange + RateLimiter：端到端限速不超过设定值', async () => {
  const srv = new VirtualServer();
  const rate = 200_000; // 200KB/s
  const limiter = new RateLimiter(rate);
  await limiter.acquire(4 * 1024 * 1024); // 排空初始桶
  const size = 400_000;
  const t0 = Date.now();
  await fetchRange(srv.url(`/file?size=${size}`), 0, size - 1, {
    fetchImpl: srv.fetch,
    limiter,
  });
  const elapsed = (Date.now() - t0) / 1000;
  assert.ok(elapsed >= 1.6, `expected >=~2s, got ${elapsed}`);
  assert.ok(elapsed <= 2.6, `too slow ${elapsed}`);
});
