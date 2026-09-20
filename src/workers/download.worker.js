// 下载引擎运行在 Worker 中：
// - 分块字节的读取、限速等待、重试都不阻塞主线程 UI
// - 分块 Blob 直接由 IndexedDB（Worker 内）持久化，主线程零拷贝
import { DownloadEngine } from '../engine/engine.js';
import { IdbChunkStorage } from '../engine/storage-idb.js';
import { REQ } from '../shared/protocol.js';

const storage = new IdbChunkStorage();
const engine = new DownloadEngine(storage);

engine.on((event) => {
  self.postMessage(event);
});

// 通知主线程可重新发起下载
self.postMessage({ type: 'ready' });

self.onmessage = async (event) => {
  const msg = event.data || {};
  try {
    switch (msg.type) {
      case REQ.INIT:
        if (Number.isFinite(msg.concurrency)) engine.setConcurrency(msg.concurrency);
        if (Number.isFinite(msg.rateBytesPerSecond)) engine.setRate(msg.rateBytesPerSecond);
        self.postMessage({ type: REQ.LIST_RESTORED, requestId: msg.requestId, items: await engine.listRestored() });
        break;
      case REQ.ADD:
        await engine.addFile({
          id: msg.id,
          url: msg.url,
          filename: msg.filename,
          autoStart: msg.autoStart !== false,
          headers: msg.headers
        });
        break;
      case REQ.PAUSE:
        await engine.pauseFile(msg.id);
        break;
      case REQ.RESUME:
        await engine.resumeFile(msg.id);
        break;
      case REQ.CANCEL:
        await engine.cancelFile(msg.id);
        break;
      case REQ.PAUSE_ALL:
        await engine.pauseAll();
        break;
      case REQ.RESUME_ALL:
        await engine.resumeAll();
        break;
      case REQ.SET_RATE:
        engine.setRate(msg.rateBytesPerSecond);
        break;
      case REQ.SET_CONCURRENCY:
        engine.setConcurrency(msg.concurrency);
        break;
      case REQ.EXPORT: {
        const result = await engine.exportFile(msg.id);
        // Blob 可结构化克隆，零拷贝传回主线程触发保存
        self.postMessage(
          {
            type: 'blob',
            id: msg.id,
            requestId: msg.requestId,
            filename: result.filename,
            size: result.size,
            blob: result.blob
          },
          // 无 transfer 也可克隆 Blob；保持显式简洁
          undefined
        );
        break;
      }
      default:
        break;
    }
  } catch (error) {
    self.postMessage({
      type: 'error',
      id: msg.id,
      requestId: msg.requestId,
      message: (error && error.message) || String(error)
    });
  }
};

// 定期上报全局实测速度
setInterval(() => {
  self.postMessage({ type: 'speed', bytesPerSecond: engine.globalSpeed() });
}, 500);
