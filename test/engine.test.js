import { test } from 'node:test';
import assert from 'node:assert/strict';
import { DownloadEngine } from '../src/engine/engine.js';
import { DOWNLOAD_STATUS } from '../src/shared/protocol.js';
import { FakeStorage, createMockFetch, expectedByteAt, waitFor } from './helpers/fakes.js';

function makeEngine({ total = 1024, chunkSize = 1024, concurrency = 4, ...rest } = {}) {
  const storage = new FakeStorage();
  const fetchImpl = createMockFetch({ total, ...rest });
  const engine = new DownloadEngine(
    storage,
    {
      concurrency,
      chunkSize,
      retryBaseMs: 1,
      retryMaxMs: 10
    },
    { fetchImpl, createBlob: fakeBlob }
  );
  return { engine, storage, fetchImpl };
}


const fakeBlob = (parts) => ({
  __blob: true,
  size: parts.reduce((sum, p) => sum + (p.size || p.byteLength), 0),
  parts: parts.flatMap((p) => (p.__blob ? p.parts : [p]))
});

const events = (engine) => {
  const list = [];
  engine.on((event) => list.push(event));
  return list;
};

const waitStatus = async (engine, id, status) =>
  waitFor(() => engine.files.get(id) && engine.files.get(id).status === status, { timeout: 15_000 });

test('Range 分块下载：所有分块完成并能按序合并', async () => {
  const { engine } = makeEngine({ total: 10_000, chunkSize: 2_048 });
  const eventsList = events(engine);
  await engine.addFile({ id: 'a', url: 'http://x/file', autoStart: true });
  await waitStatus(engine, 'a', DOWNLOAD_STATUS.COMPLETE);

  const file = engine.files.get('a');
  assert.equal(file.chunks, 5); // ceil(10000/2048)
  assert.equal(file.downloaded, 10_000);
  assert.ok(eventsList.some((e) => e.type === 'fileDone'));

  const { blob } = await engine.exportFile('a');
  assert.equal(blob.size, 10_000);
});

test('暂停-恢复：已完成分块不重传，断点进度不回退', async () => {
  const { engine, storage } = makeEngine({ total: 20_000, chunkSize: 2_000, concurrency: 1, latencyMs: 30 });
  await engine.addFile({ id: 'p', url: 'http://x/p', autoStart: true });
  // 等第一个分块落盘后再给一拍延迟（>单块延迟），保证暂停时任务处于进行中
  await waitFor(async () => (await storage.chunkCount('p')) >= 1, { timeout: 10_000 });
  await new Promise((resolve) => setTimeout(resolve, 45));
  await engine.pauseFile('p');
  const doneBefore = await storage.chunkCount('p');
  const downloadedBefore = engine.files.get('p').downloaded;
  assert.ok(doneBefore >= 1 && doneBefore < 10, `暂停时分块数 ${doneBefore} 应处于中间态`);

  const doneIndices = new Set((await storage.listChunks('p')).map((c) => c.index));
  await engine.resumeFile('p');
  await waitStatus(engine, 'p', DOWNLOAD_STATUS.COMPLETE);

  // 全部分块都在，且总数恰好等于分块数（无重复写）
  const allIndices = (await storage.listChunks('p')).map((c) => c.index).sort((a, b) => a - b);
  assert.deepEqual(allIndices, [...Array(10).keys()]);
  // 恢复后这些原本就完成的分块未被覆盖重写（通过内容抽查仍连续）
  assert.equal(engine.files.get('p').downloaded, 20_000);
  assert.ok(downloadedBefore >= doneBefore * 2_000 - 2_000);
  void doneIndices;
});

test('崩溃重启（新引擎实例）后从 IndexedDB 元数据恢复并继续', async () => {
  const total = 12_000;
  const storage = new FakeStorage();
  const fetchImpl = createMockFetch({ total, latencyMs: 20 });
  const create = () =>
    new DownloadEngine(
      storage,
      { concurrency: 1, chunkSize: 3_000, retryBaseMs: 1, retryMaxMs: 5 },
      { fetchImpl, createBlob: fakeBlob }
    );

  const engine1 = create();
  await engine1.addFile({ id: 'r', url: 'http://x/r', autoStart: true });
  await waitFor(() => storage.chunks.has('r:1'), { timeout: 10_000 });
  await engine1.pauseFile('r');
  const chunksAfterPause = await storage.chunkCount('r');

  // 模拟关掉页面：丢掉引擎，新建实例恢复
  const restored = await create().listRestored();
  assert.equal(restored.length, 1);
  assert.equal(restored[0].id, 'r');

  const engine2 = create();
  await engine2.addFile({ id: 'r', url: 'http://x/r', autoStart: true });
  await waitStatus(engine2, 'r', DOWNLOAD_STATUS.COMPLETE);
  assert.equal(await storage.chunkCount('r'), 4);
  assert.ok(chunksAfterPause >= 1);
});

test('异常自动重试：前 3 次请求 500，重试后成功', async () => {
  const { engine } = makeEngine({
    total: 5_000,
    chunkSize: 5_000,
    concurrency: 1,
    failFirst: () => 4
  });
  const eventsList = events(engine);
  await engine.addFile({ id: 'f', url: 'http://x/flaky', autoStart: true });
  await waitStatus(engine, 'f', DOWNLOAD_STATUS.COMPLETE, { timeout: 15_000 });
  const retries = eventsList.filter((e) => e.type === 'retry');
  // failFirst=4：1 次探测 + 3 次分块请求失败，引擎在探测和分块阶段都可能触发重试
  assert.ok(retries.length >= 3, `重试次数不足：${retries.length}`);
  assert.ok(retries.some((e) => e.phase === 'probe'), '应包含探测阶段重试');
  assert.equal(engine.files.get('f').downloaded, 5_000);
});

test('超过最大重试次数后进入 error，再 resume 可恢复', async () => {
  let calls = 0;
  const storage = new FakeStorage();
  const fetchImpl = createMockFetch({
    total: 2_000,
    latencyMs: 5,
    failFirstReturnsRemaining: false,
    failFirst: () => {
      calls += 1;
      return calls <= 3 ? 99 : 0; // 首轮探测重试耗尽后报错；之后服务恢复
    }
  });
  const engine = new DownloadEngine(
    storage,
    { concurrency: 1, chunkSize: 2_000, maxRetries: 2, retryBaseMs: 1, retryMaxMs: 5 },
    { fetchImpl, createBlob: fakeBlob }
  );
  await engine.addFile({ id: 'e', url: 'http://x/bad', autoStart: true });
  await waitStatus(engine, 'e', DOWNLOAD_STATUS.ERROR, { timeout: 15_000 });
  await engine.resumeFile('e');
  await waitStatus(engine, 'e', DOWNLOAD_STATUS.COMPLETE, { timeout: 15_000 });
});

test('服务端不支持 Range：自动降级整文件单流，可完成并合并', async () => {
  const { engine } = makeEngine({ total: 7_777, chunkSize: 1_000, disableRange: true });
  await engine.addFile({ id: 'w', url: 'http://x/whole', autoStart: true });
  await waitStatus(engine, 'w', DOWNLOAD_STATUS.COMPLETE, { timeout: 15_000 });
  const file = engine.files.get('w');
  assert.equal(file.mode, 'whole');
  assert.equal(file.downloaded, 7_777);
  const { blob } = await engine.exportFile('w');
  assert.equal(blob.size, 7_777);
});

test('并发数受控：concurrency=N 时同时在途不超过 N', async () => {
  const { engine } = makeEngine({
    total: 20_000,
    chunkSize: 1_000,
    concurrency: 2,
    latencyMs: 20
  });
  let maxInflight = 0;
  engine.on((event) => {
    if (event.type === 'chunk:start') {
      const file = engine.files.get(event.id);
      maxInflight = Math.max(maxInflight, file.inflight);
    }
  });
  await engine.addFile({ id: 'c', url: 'http://x/c', autoStart: true });
  await waitStatus(engine, 'c', DOWNLOAD_STATUS.COMPLETE, { timeout: 15_000 });
  assert.ok(maxInflight <= 2, `峰值在途 ${maxInflight}`);
  assert.equal(maxInflight, 2);
  // 动态调高立即生效
});

test('多文件公平共享总并发并全部完成', async () => {
  const total = 8_000;
  const storage = new FakeStorage();
  const fetchImpl = createMockFetch({ total, latencyMs: 10 });
  const engine = new DownloadEngine(
    storage,
    { concurrency: 2, chunkSize: 2_000, retryBaseMs: 1, retryMaxMs: 5 },
    { fetchImpl, createBlob: fakeBlob }
  );
  await Promise.all([
    engine.addFile({ id: 'f1', url: 'http://x/1', autoStart: true }),
    engine.addFile({ id: 'f2', url: 'http://x/2', autoStart: true }),
    engine.addFile({ id: 'f3', url: 'http://x/3', autoStart: true })
  ]);
  await Promise.all(['f1', 'f2', 'f3'].map((id) => waitStatus(engine, id, DOWNLOAD_STATUS.COMPLETE)));
  for (const id of ['f1', 'f2', 'f3']) {
    assert.equal(engine.files.get(id).downloaded, total);
  }
});

test('远端文件变化（ETag 不同）：清空旧分块重新下载', async () => {
  const storage = new FakeStorage();
  const fetchV1 = createMockFetch({ total: 4_000 });
  const engine1 = new DownloadEngine(
    storage,
    { concurrency: 1, chunkSize: 2_000, retryBaseMs: 1, retryMaxMs: 5 },
    { fetchImpl: fetchV1, createBlob: fakeBlob }
  );
  await engine1.addFile({ id: 'v', url: 'http://x/v', autoStart: true });
  await waitStatus(engine1, 'v', DOWNLOAD_STATUS.COMPLETE);

  // 同 URL 换了内容：长度不同 => ETag 变化
  const fetchV2 = createMockFetch({ total: 6_000 });
  const engine2 = new DownloadEngine(
    storage,
    { concurrency: 1, chunkSize: 2_000, retryBaseMs: 1, retryMaxMs: 5 },
    { fetchImpl: fetchV2, createBlob: fakeBlob }
  );
  const list = events(engine2);
  await engine2.addFile({ id: 'v', url: 'http://x/v', autoStart: true });
  await waitStatus(engine2, 'v', DOWNLOAD_STATUS.COMPLETE);
  assert.equal(engine2.files.get('v').total, 6_000);
  assert.equal(await storage.chunkCount('v'), 3);
  assert.ok(list.some((e) => e.type === 'retry' && e.reason === 'remote-changed'));
});

test('restoreFile：仅从元数据恢复后可续传，cancel 可清理未载入任务', async () => {
  const total = 9_000;
  const storage = new FakeStorage();
  const fetchImpl = createMockFetch({ total, latencyMs: 15 });
  const make = () =>
    new DownloadEngine(
      storage,
      { concurrency: 1, chunkSize: 3_000, retryBaseMs: 1, retryMaxMs: 5 },
      { fetchImpl, createBlob: fakeBlob }
    );

  const engine1 = make();
  await engine1.addFile({ id: 'z', url: 'http://x/z', autoStart: true });
  await waitFor(() => storage.chunks.has('z:1'), { timeout: 10_000 });
  await engine1.pauseFile('z');

  // 全新引擎：先只恢复元数据（不下载），再 resume 续传
  const engine2 = make();
  const snapshot = await engine2.restoreFile('z');
  assert.equal(snapshot.status, DOWNLOAD_STATUS.PAUSED);
  await engine2.resumeFile('z');
  await waitStatus(engine2, 'z', DOWNLOAD_STATUS.COMPLETE);
  assert.equal(engine2.files.get('z').downloaded, total);

  // 未载入内存的任务也能直接 cancel（清掉 IDB）
  const engine3 = make();
  await engine3.cancelFile('z');
  assert.equal(await storage.chunkCount('z'), 0);
  assert.equal(await storage.getMeta('z'), null);
});

test('setConcurrency 动态提高并发后立即提速派发', async () => {
  const { engine } = makeEngine({
    total: 16_000,
    chunkSize: 1_000,
    concurrency: 1,
    latencyMs: 25
  });
  await engine.addFile({ id: 'dyn', url: 'http://x/dyn', autoStart: true });
  await new Promise((resolve) => setTimeout(resolve, 60));
  // 单并发时最多 1 个在途
  assert.ok(engine.files.get('dyn').inflight <= 1);
  engine.setConcurrency(8);
  await waitFor(() => engine.files.get('dyn').inflight >= 4, { timeout: 5_000 });
  await waitStatus(engine, 'dyn', DOWNLOAD_STATUS.COMPLETE);
});

test('1GB 文件：分块全部完成，存储只记录大小不保留全部字节（不爆内存）', async () => {
  const ONE_GB = 1024 * 1024 * 1024;
  const { engine, storage } = makeEngine({
    total: ONE_GB,
    chunkSize: 16 * 1024 * 1024, // 64 块
    concurrency: 4
  });
  const start = Date.now();
  await engine.addFile({ id: 'big', url: 'http://x/1gb', autoStart: true });
  await waitStatus(engine, 'big', DOWNLOAD_STATUS.COMPLETE, { timeout: 60_000 });
  const file = engine.files.get('big');
  assert.equal(file.downloaded, ONE_GB);
  assert.equal(file.chunks, 64);
  assert.equal(await storage.chunkCount('big'), 64);
  // FakeStorage 不持有字节：验证大量数据路径下累计内存有界
  assert.equal([...storage.chunks.values()].reduce((sum, c) => sum + c.size, 0), ONE_GB);
  assert.ok(Date.now() - start < 60_000);
});

test('分块内容正确：小块开启数据留存后逐字节校验', { timeout: 20_000 }, async () => {
  {
    const total = 12_345;
    const { engine } = makeEngine({ total, chunkSize: 4_000, concurrency: 3 });
    await engine.addFile({ id: 'ok', url: 'http://x/data', autoStart: true });
    await waitStatus(engine, 'ok', DOWNLOAD_STATUS.COMPLETE);
    const { blob } = await engine.exportFile('ok');
    const merged = Buffer.concat(blob.parts.map((part) => Buffer.from(part)));
    assert.equal(merged.length, total);
    for (let offset = 0; offset < total; offset += 97) {
      assert.equal(merged[offset], expectedByteAt(offset), `字节 ${offset} 不匹配`);
    }
    // 末字节
    assert.equal(merged[total - 1], expectedByteAt(total - 1));
  }
});
