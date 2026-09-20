# 并发分块下载器（Fetch + Streams + IndexedDB + Web Worker）

纯前端多文件并发下载器。支持 HTTP Range 分块、暂停/继续、断点续传、全局精确
限速、异常自动重试，面向 1GB 以上大文件设计，下载全程内存占用恒定。

## 运行

```bash
# 演示页面（必须用 http 服务打开，Worker / ES Module 不支持 file://）
npm start                 # http://localhost:8080

# 内核自动化测试（零依赖、零端口，进程内虚拟 HTTP 服务器）
npm test

# 1GB+ 大文件专项（1,073,742,601 字节，验证不崩 + 区间全覆盖）
npm run test:big
```

## 目录结构

| 文件 | 职责 |
| --- | --- |
| `index.html` / `style.css` / `src/ui.js` | 演示界面：多任务添加、全局控制、重试日志 |
| `src/download-manager.js` | 主线程 API（Worker RPC、流式合并导出磁盘） |
| `src/download-worker.js` | 唯一下载 Worker：承载引擎、元数据持久化、崩溃恢复 |
| `src/engine.js` | 环境无关的下载内核：调度、公平队列、重试、暂停、完成判定 |
| `src/fetcher.js` | Range/整文件抓取：HEAD 探测、206 校验、Streams 边读边限速、截断检测 |
| `src/rate-limiter.js` | 令牌桶：跨所有连接的全局 bytes/s 精确限速，支持 abort |
| `src/idb-storage.js` | IndexedDB：任务元数据 + 8MB 分块 Blob，keys-only 扫描已完成块 |
| `src/storage-memory.js` | 与 IDB 同接口的内存存储（内核可在 Node 直接测试） |
| `test/virtual-server.mjs` | 进程内 HTTP 服务器：Range / 无 Range / 连接中断故障注入 / PRNG 大文件 |

## 关键设计

**分块与并发**
- HEAD 探测文件大小与 `Accept-Ranges`；按固定 8MB 切块（`DEFAULT_CHUNK_SIZE`）。
- 全局并发槽（默认 3，1–12 可调）。调度器在活跃文件间轮转取任务，多文件公平，
  不会被一个大文件占满所有连接。
- 每个分块独立请求 `Range: bytes=start-end`，校验状态码必须为 206，
  并按 `Content-Range` 校对；服务器返回 200（不支持 Range）自动降级为整文件流。

**断点续传 / 暂停恢复**
- 分块完成即以 Blob 写入 IndexedDB（记录 `[fileId,index]`），任务元数据节流落盘。
- 暂停 = `AbortController.abort()` 中断全部在飞请求；已完成分块全部保留。
- 恢复（或重开页面）时用 keys-only 游标扫描已存在的分块索引，只对缺失块发请求；
  Worker 重启后未完成任务默认保持暂停态，点“继续”才联网，并先比对 ETag，
  文件已更换则清空旧分块重新下载。
- 同一块内若已读到一半断连，重试会从 `已起始偏移` 继续请求（块内续传）。

**限速**
- 全局唯一令牌桶（`RateLimiter`），所有分块每读 256KB 先申请令牌再处理数据，
  限制的是所有连接的总吞吐；FIFO 等待队列保证不超发、不饿死，可随时动态调速或关闭。
- 桶容量 4MB 限制空闲后的突发；`AbortSignal` 可立即取消等待。

**异常重试**
- 网络错误、连接重置、截断（收到字节数与区间不符）均触发指数退避 + 抖动重试
  （默认 5 次，500ms 起，上限 15s），退避期间可被暂停打断。
- 超过重试上限任务进入 `error` 态，界面可一键继续；不会影响其他文件。

**1GB+ 不崩**
- 下载路径：响应流按 256KB 消费，限速后聚合成 8MB Blob 立刻落 IDB，
  内存中最多保留 `并发数 × 8MB` 量级；1GB 仅约 130 条记录。
- 导出路径：优先 File System Access API，逐块 `writable.write(blob)` 流式写盘；
  不支持的浏览器退化为 Blob 拼接 + `a[download]`。

## API

```js
import { DownloadManager } from './src/download-manager.js';

const dm = new DownloadManager();
dm.on('progress', ({ file }) => render(file));
dm.on('retry', ({ label, attempt, waitMs, message }) => console.log(label, attempt));

await dm.add('https://example.com/huge.iso');
dm.setConcurrency(6);                 // 并发数
dm.setRate(2 * 1024 * 1024);          // 2MB/s，0 为不限速
dm.pause(id); dm.resume(id); dm.remove(id);
await dm.saveToDisk(id);              // 完成后流式导出
```

## 验收标准对照

| 标准 | 实现 / 测试 |
| --- | --- |
| 下载中断后可续 | `engine.test.mjs` 暂停/恢复用例：换新引擎+同一存储，已完成块不重复请求，字节完整 |
| 并发数可控 | 服务端记录同时在飞连接数，断言 `maxActiveConnections <= 设定值`；支持运行中动态调整 |
| 限速准确 | 令牌桶单测 + `fetchRange` 端到端用例，稳态吞吐误差 ±25% |
| 1GB 文件不崩 | `npm run test:big`：1,073,742,601 字节，分块边界齐全、区间全覆盖、峰值 RSS 约 230MB |
| 异常自动重试 | 连接中断故障注入下自动重试成功；超出上限进入 error 态，手动恢复后完成 |
