import { DownloadEngine } from './engine.js';
import { IdbChunkStorage } from './idb-storage.js';
import { FileStatus, DEFAULT_CONCURRENCY, DEFAULT_CHUNK_SIZE, DEFAULT_RATE_LIMIT } from './constants.js';

const storage = new IdbChunkStorage();
const engine = new DownloadEngine(storage);

function post(type, payload = {}) {
  self.postMessage({ type, payload });
}

// 所有引擎事件直接透传给主线程 UI
engine.onEvent((evt) => {
  const { type, ...rest } = evt;
  post(`evt:${type}`, rest);
});

async function loadSettings() {
  return (await storage.getKv('settings')) || {};
}

async function saveSettings(settings) {
  await storage.setKv('settings', settings);
}

let settings = null;

async function persistFileMeta(fileId) {
  try {
    const meta = engine.serializeFile(fileId);
    if (meta) await storage.putFileMeta(meta);
  } catch (err) {
    post('evt:log', { level: 'warn', message: `persist meta failed: ${err && err.message}` });
  }
}

function persistAll() {
  for (const id of engine.files.keys()) persistFileMeta(id);
}

async function init() {
  settings = await loadSettings();
  engine.setConcurrency(settings.concurrency || DEFAULT_CONCURRENCY);
  engine.limiter.setRate(settings.rateLimit ?? DEFAULT_RATE_LIMIT);

  // 页面重新打开后恢复任务：未完成的以暂停态重建（尊重用户退出前的选择），
  // 已完成的保持 complete，等待用户点“保存到磁盘”。
  let metas = [];
  try {
    metas = await storage.getAllFileMeta();
  } catch (err) {
    post('evt:log', { level: 'error', message: `load meta failed: ${err && err.message}` });
  }
  for (const meta of metas) {
    // chunkSize 必须在 addFile 建计划前恢复，保证切块边界与历史分块一致
    engine.chunkSize = meta.chunkSize || settings.chunkSize || DEFAULT_CHUNK_SIZE;
    await engine.addFile({
      id: meta.id,
      url: meta.url,
      filename: meta.filename,
      size: meta.size,
      supportsRange: meta.supportsRange,
      etag: meta.etag,
      autoStart: false,
    });
    const state = engine.files.get(meta.id);
    if (meta.status === FileStatus.COMPLETE) {
      state.status = FileStatus.COMPLETE;
    } else {
      state.status = FileStatus.PAUSED;
    }
    state.retries = meta.retries || 0;
  }
  engine.chunkSize = settings.chunkSize || DEFAULT_CHUNK_SIZE;
  post('ready', {
    files: engine.getFiles(),
    settings: {
      concurrency: engine.concurrency,
      rateLimit: engine.limiter.rate,
      chunkSize: settings.chunkSize || DEFAULT_CHUNK_SIZE,
    },
  });
}

// 元数据在关键节点落盘（节流）
let persistTimer = 0;
function schedulePersist() {
  if (persistTimer) return;
  persistTimer = setTimeout(() => {
    persistTimer = 0;
    persistAll();
  }, 800);
}

self.onmessage = async (e) => {
  const { type, payload = {} } = e.data || {};
  try {
    switch (type) {
      case 'add': {
        const added = await engine.addFile({
          url: payload.url,
          filename: payload.filename,
          autoStart: payload.autoStart !== false,
        });
        await persistFileMeta(added.id);
        post('added', { file: added });
        break;
      }
      case 'pause':
        engine.pause(payload.fileId);
        await persistFileMeta(payload.fileId);
        break;
      case 'resume':
        if (engine.files.get(payload.fileId)?.probeDone) {
          await engine.revalidate(payload.fileId).catch(() => {});
        }
        engine.resume(payload.fileId);
        await persistFileMeta(payload.fileId);
        break;
      case 'remove':
        await engine.removeFile(payload.fileId, { deleteData: true });
        break;
      case 'set-concurrency':
        engine.setConcurrency(payload.concurrency);
        settings.concurrency = engine.concurrency;
        await saveSettings(settings);
        post('settings', { concurrency: engine.concurrency, rateLimit: engine.limiter.rate });
        break;
      case 'set-rate':
        engine.setRate(payload.rateLimit);
        settings.rateLimit = engine.limiter.rate;
        await saveSettings(settings);
        post('settings', { concurrency: engine.concurrency, rateLimit: engine.limiter.rate });
        break;
      case 'get-files':
        post('files', { files: engine.getFiles() });
        break;
      default:
        break;
    }
  } catch (err) {
    post('error', { message: String(err && err.message || err) });
  }
};

init().catch((err) => post('fatal', { message: String(err && err.message || err) }));
