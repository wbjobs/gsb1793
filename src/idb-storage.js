/**
 * IndexedDB 分块存储。
 *
 * 为什么不用 OPFS / File System Access API 存分块：
 * - 下载发生在 Worker 中，IndexedDB 在 Worker 里可直接用，兼容性最好；
 * - 分块以 Blob 写入，Blob 结构化克隆是零拷贝/惰性落盘的，内存占用恒定，
 *   1GB+ 文件也只是 ~130 条 8MB 记录；
 * - 合并保存由主线程流式读取，不在内存里拼接整个文件。
 *
 * stores:
 *   files  keyPath=id      —— 任务元数据（断点恢复入口）
 *   chunks keyPath=[fileId,index] —— 分块 Blob
 */

export const DB_NAME = 'chunked-downloader';
export const DB_VERSION = 1;
export const STORE_FILES = 'files';
export const STORE_CHUNKS = 'chunks';
export const STORE_KV = 'kv';

let dbPromise = null;

export function openDb({ indexedDb = globalThis.indexedDB, name = DB_NAME, version = DB_VERSION } = {}) {
  if (dbPromise) return dbPromise;
  dbPromise = new Promise((resolve, reject) => {
    if (!indexedDb) {
      reject(new Error('IndexedDB is not available in this environment'));
      return;
    }
    const req = indexedDb.open(name, version);
    req.onupgradeneeded = () => {
      const db = req.result;
      if (!db.objectStoreNames.contains(STORE_FILES)) {
        db.createObjectStore(STORE_FILES, { keyPath: 'id' });
      }
      if (!db.objectStoreNames.contains(STORE_CHUNKS)) {
        const store = db.createObjectStore(STORE_CHUNKS, { keyPath: ['fileId', 'index'] });
        store.createIndex('fileId', 'fileId', { unique: false });
      }
      if (!db.objectStoreNames.contains(STORE_KV)) {
        db.createObjectStore(STORE_KV);
      }
    };
    req.onsuccess = () => resolve(req.result);
    req.onerror = () => reject(req.error);
    req.onblocked = () => reject(new Error('IndexedDB open blocked by another tab'));
  });
  return dbPromise;
}

export function resetDbHandle() {
  dbPromise = null;
}

function reqAsPromise(request) {
  return new Promise((resolve, reject) => {
    request.onsuccess = () => resolve(request.result);
    request.onerror = () => reject(request.error);
  });
}

async function tx(storeNames, mode, fn) {
  const db = await openDb();
  const transaction = db.transaction(storeNames, mode);
  const stores = (Array.isArray(storeNames) ? storeNames : [storeNames])
    .reduce((acc, n) => { acc[n] = transaction.objectStore(n); return acc; }, {});
  const result = await fn(stores);
  await new Promise((resolve, reject) => {
    transaction.oncomplete = () => resolve();
    transaction.onerror = () => reject(transaction.error);
    transaction.onabort = () => reject(transaction.error || new Error('transaction aborted'));
  });
  return result;
}

export class IdbChunkStorage {
  constructor(opts) {
    this._opts = opts;
  }

  async _open() {
    return openDb(this._opts);
  }

  async putFileMeta(meta) {
    await tx(STORE_FILES, 'readwrite', (stores) => reqAsPromise(stores[STORE_FILES].put(meta)));
  }

  async getFileMeta(id) {
    return tx(STORE_FILES, 'readonly', (stores) => reqAsPromise(stores[STORE_FILES].get(id)));
  }

  async getAllFileMeta() {
    return tx(STORE_FILES, 'readonly', (stores) => reqAsPromise(stores[STORE_FILES].getAll()));
  }

  async deleteFileMeta(id) {
    await tx(STORE_FILES, 'readwrite', (stores) => reqAsPromise(stores[STORE_FILES].delete(id)));
  }

  async getKv(key) {
    return tx(STORE_KV, 'readonly', (stores) => reqAsPromise(stores[STORE_KV].get(key)));
  }

  async setKv(key, value) {
    await tx(STORE_KV, 'readwrite', (stores) => reqAsPromise(stores[STORE_KV].put(value, key)));
  }

  async putChunk(rec) {
    await tx(STORE_CHUNKS, 'readwrite', (stores) => reqAsPromise(stores[STORE_CHUNKS].put(rec)));
  }

  async getChunk(fileId, index) {
    return tx(STORE_CHUNKS, 'readonly', (stores) =>
      reqAsPromise(stores[STORE_CHUNKS].get([fileId, index])));
  }

  /** 流式逐个读取分块，避免一次性把整个文件加载进内存 */
  async streamChunks(fileId, chunkCount, onChunk) {
    const db = await this._open();
    for (let i = 0; i < chunkCount; i++) {
      const rec = await new Promise((resolve, reject) => {
        const transaction = db.transaction(STORE_CHUNKS, 'readonly');
        const req = transaction.objectStore(STORE_CHUNKS).get([fileId, i]);
        req.onsuccess = () => resolve(req.result);
        req.onerror = () => reject(req.error);
      });
      if (rec) await onChunk(rec, i);
    }
  }

  async listChunks(fileId) {
    const db = await this._open();
    return new Promise((resolve, reject) => {
      const transaction = db.transaction(STORE_CHUNKS, 'readonly');
      const index = transaction.objectStore(STORE_CHUNKS).index('fileId');
      const req = index.getAll(IDBKeyRange.only(fileId));
      req.onsuccess = () => resolve((req.result || []).sort((a, b) => a.index - b.index));
      req.onerror = () => reject(req.error);
    });
  }

  async deleteChunksFrom(fileId, fromIndex) {
    const db = await this._open();
    const records = await this.listChunks(fileId);
    if (records.length === 0) return;
    await new Promise((resolve, reject) => {
      const transaction = db.transaction(STORE_CHUNKS, 'readwrite');
      const store = transaction.objectStore(STORE_CHUNKS);
      let pending = 0;
      for (const rec of records) {
        if (rec.index >= fromIndex) {
          pending += 1;
          const del = store.delete([fileId, rec.index]);
          del.onsuccess = () => { pending -= 1; if (pending === 0) resolve(); };
          del.onerror = () => reject(del.error);
        }
      }
      if (pending === 0) resolve();
      transaction.onabort = () => reject(transaction.error || new Error('aborted'));
    });
  }

  async deleteFile(fileId) {
    await this.deleteChunksFrom(fileId, 0);
    await this.deleteFileMeta(fileId);
  }

  async completedChunkIndexes(fileId, chunkCount) {
    // keys-only 查询：不拉 Blob，只判断哪些分块存在
    const db = await this._open();
    return new Promise((resolve, reject) => {
      const set = new Set();
      const transaction = db.transaction(STORE_CHUNKS, 'readonly');
      const store = transaction.objectStore(STORE_CHUNKS);
      const range = IDBKeyRange.bound([fileId, 0], [fileId, chunkCount - 1]);
      const req = store.openKeyCursor(range);
      req.onsuccess = () => {
        const cursor = req.result;
        if (cursor) {
          set.add(cursor.key[1]);
          cursor.continue();
        } else {
          resolve(set);
        }
      };
      req.onerror = () => reject(req.error);
    });
  }
}
