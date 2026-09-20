# 分块并发下载器

多文件并发下载管理器：HTTP Range 分块下载、暂停/恢复、全局令牌桶限速、断点续传、异常自动重试。
基于 **Fetch + Streams + IndexedDB + Web Worker**，可稳定处理 1GB 以上文件。

## 快速开始

```bash
npm start          # 启动零依赖测试服务器（默认 5173 端口）
# 打开 http://localhost:5173/

npm test           # 运行全部测试（Node 内置 test runner，无第三方依赖）
```

演示服务器提供确定性虚拟文件，不占磁盘：
- `http://localhost:5173/files/demo-64mb.bin`
- `http://localhost:5173/files/demo-1gb.bin`（1GB，流式生成）
- `http://localhost:5173/files/gen-5mb.bin`（自定义大小：`gen-Nmb.bin` / `gen-Ngb.bin`）
- `http://localhost:5173/files/demo-16mb.bin?fail=3`（前 3 次请求返回 500，验证自动重试）

页面文本框每行一个任务：写裸 URL，或 `文件名 URL`；只填文件名时走同源 `/files/<name>`。

## 架构

```
index.html / src/main/app.js          UI（进度、按钮、速度、设置持久化）
src/main/DownloadManager.js           主线程门面：Worker 通信、Blob 落盘
src/workers/download.worker.js        Worker 宿主：引擎事件 <-> postMessage
src/engine/engine.js                  下载引擎（与环境无关，可直接在 Node 中测试）
├── ConcurrencyGate.js                全局并发信号量（在途分块上限，可调）
├── RateLimiter.js                    全局令牌桶（字节/秒，可调，0=不限）
├── retry.js                          指数退避 + 抖动 + Retry-After
├── http.js                           Range 探测 / 请求 / ReadableStream 消费
└── storage-idb.js                    IndexedDB 分块存储（复合键 [fileId,index]）
test/                                 引擎端到端测试 / 原语单测 / HTTP 服务器测试
server/server.mjs                     零依赖演示服务器（Range/ETag/416/故障注入）
```

### 核心机制

**Range 分块**
- 每个文件按 `chunkSize`（默认 4MB）切分，分块用独立的 `Range: bytes=start-end` 请求。
- 探测阶段用 `Range: bytes=0-0` 确认 `206`/`Accept-Ranges`，并记录 `ETag`、`Last-Modified`、总大小。
- 服务端不支持 Range 时自动降级为整文件单流（UI 标注“整文件模式”）。

**并发控制**
- `ConcurrencyGate` 是所有文件共享的信号量：总在途分块数 = 设置的并发数。
- 任务按“在途最少的文件优先”轮转派发，多文件公平共享并发额度。
- 运行中调整并发数立即生效（提高时唤醒等待者）。

**限速**
- 全局令牌桶：桶容量 = 1 秒额度（允许 1 秒初始突发），之后按设定字节/秒补充。
- 令牌申请嵌入 `ReadableStream` 的读取回调（`onData`），对读取端形成背压，
  所有文件、所有在途分块共享同一个桶，因此**总速率**严格收敛到设定值。
- `setRate()` 运行时生效，`0` 表示不限速。

**断点续传 / 暂停恢复**
- 每个分块下载完立即作为 Blob 写入 IndexedDB（`chunks` store，复合键 `[fileId,index]`），
  元数据（已完成数、ETag、chunkSize 等）写入 `files` store。
- 暂停：`AbortController` 中止所有在途请求，未完成分块回队；已完成分块保留。
- 恢复：重新探测后用 `listChunks()` 扫描已完成分块位图，**只下载缺失分块**。
- 恢复时校验 ETag/Last-Modified/大小，远端文件变化则清空旧分块重新下载。
- 浏览器崩溃/关闭后重开页面：从 IndexedDB 恢复任务列表，用户点“恢复”继续。

**异常重试**
- 可重试错误：网络失败、流中断、408/425/429/5xx；404/416 等不可重试错误立即失败。
- 指数退避（`500ms` 起步，上限 `30s`）+ 随机抖动，尊重 `Retry-After` 头。
- 默认每个分块最多重试 8 次；退避等待可被暂停中断。
- 重试耗尽进入 `error` 状态，保留已完成分块，用户可手动恢复。

**1GB+ 大文件内存安全**
- 字节永远不在内存中拼接：分块 Blob 直接落 IndexedDB。
- 合并阶段顺序读出分块构造 `new Blob([chunkBlobs...])`——Blob 之间按引用拼接，
  浏览器内部按需落盘/换页，不复制全部字节。
- 内存占用上界 ≈ `并发数 × 分块大小`（默认 3 × 4MB = 12MB），与文件总大小无关。

## API（主线程）

```js
const manager = new DownloadManager();
await manager.init({ concurrency: 3, rateBytesPerSecond: 512 * 1024 });

manager.add('https://example.com/big.iso', 'big.iso'); // 添加并自动开始
manager.pause(id);
manager.resume(id);
manager.cancel(id);          // 删除任务及已下载分块
manager.setConcurrency(6);
manager.setRate(1024 * 1024);
await manager.save(id);      // 合并分块并触发浏览器下载
manager.pauseAll();
manager.resumeAll();

manager.addEventListener('state',    (e) => console.log(e.detail.state));
manager.addEventListener('progress', (e) => console.log(e.detail.downloaded, e.detail.total));
manager.addEventListener('retry',    (e) => console.log('重试', e.detail.attempt));
manager.addEventListener('speed',    (e) => console.log(e.detail.bytesPerSecond));
manager.addEventListener('fileDone', (e) => console.log('完成'));
```

## 验收标准对照

| 要求 | 实现 / 验证 |
| --- | --- |
| 下载中断后可续 | 分块即时持久化 + 恢复时只补缺块；`engine.test.js` 暂停-恢复、崩溃重启两个用例 |
| 并发数可控 | 全局信号量 + 多文件轮转；测试断言峰值在途 == 设定并发 |
| 限速准确 | 全局令牌桶 + 流读取背压；`primitives.test.js` 断言稳态速率落在 ±35% 窗口（含真实时钟） |
| 1GB 文件不崩 | 分块 Blob 进 IndexedDB、内存仅在途分块；`engine.test.js` 用 1GB 虚拟文件全流程验证 |
| 异常自动重试 | 退避/抖动/Retry-After；`engine.test.js` 前 3 次 500 后成功、耗尽后手动恢复两个用例 |
| Range 请求 | `206`/`Content-Range`/`416`/`Accept-Ranges` 处理；`server.test.js` 真实协议测试 |
| 分块合并 | 顺序读出 + Blob 引用拼接；测试逐字节校验内容正确性 |
| 远端文件变化 | ETag/Last-Modified 校验，不一致自动清空重来；引擎测试覆盖 |

## 目录

```
src/shared/protocol.js      消息协议与状态枚举
src/shared/format.js        字节/速度展示
```
