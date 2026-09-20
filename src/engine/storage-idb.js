// 分块存储接口（引擎依赖）：
//   putMeta(fileMeta)
//   getMeta(id) -> fileMeta | null
//   allMeta() -> fileMeta[]
//   deleteMeta(id)
//   putChunk(id, index, data)   data 为 Blob/Uint8Array 等结构化克隆值
//   getChunk(id, index) -> data | null
//   allChunks(id) -> [{index, data}]（合并/校验时使用）
//   chunkCount(id) -> number
//   clearChunks(id)
//
// IndexedDB 实现：分块 Blob 直接进 IDB，全程不在主线程/内存中拼接整文件，
// 因此 1GB+ 文件的内存占用只与“在途分块数 × 分块大小”有关。

const DB_NAME = 'download-manager';
const DB_VERSION = 1;
const META_STORE = 'files';
const CHUNK_STORE = 'chunks';

function openDb(indexedDB = globalThis.indexedDB) {
  return new Promise((resolve, reject) => {
    const request = indexedDB.open(DB_NAME, DB_VERSION);
    request.onupgradeneeded = () => {
      const db = request.result;
      if (!db.objectStoreNames.contains(META_STORE)) {
        db.createObjectStore(META_STORE, { keyPath: 'id' });
      }
      if (!db.objectStoreNames.contains(CHUNK_STORE)) {
        // 复合键 [fileId, index]，天然按文件 + 分块序号排序
        const store = db.createObjectStore(CHUNK_STORE, { keyPath: ['id', 'index'] });
        store.createIndex('byId', 'id', { unique: false });
      }
    };
    request.onsuccess = () => resolve(request.result);
    request.onerror = () => reject(request.error);
  });
}

function txPromise(tx) {
  return new Promise((resolve, reject) => {
    tx.oncomplete = () => resolve();
    tx.onerror = () => reject(tx.error);
    tx.onabort = () => reject(tx.error);
  });
}

function reqPromise(request) {
  return new Promise((resolve, reject) => {
    request.onsuccess = () => resolve(request.result);
    request.onerror = () => reject(request.error);
  });
}

export class IdbChunkStorage {
  constructor(indexedDBImpl) {
    this._idb = indexedDBImpl || globalThis.indexedDB;
    this._dbPromise = null;
  }

  _db() {
    if (!this._dbPromise) this._dbPromise = openDb(this._idb);
    return this._dbPromise;
  }

  async putMeta(meta) {
    const db = await this._db();
    const tx = db.transaction(META_STORE, 'readwrite');
    tx.objectStore(META_STORE).put(meta);
    await txPromise(tx);
  }

  async getMeta(id) {
    const db = await this._db();
    const tx = db.transaction(META_STORE, 'readonly');
    return reqPromise(tx.objectStore(META_STORE).get(id));
  }

  async allMeta() {
    const db = await this._db();
    const tx = db.transaction(META_STORE, 'readonly');
    return reqPromise(tx.objectStore(META_STORE).getAll());
  }

  async deleteMeta(id) {
    const db = await this._db();
    const tx = db.transaction(META_STORE, 'readwrite');
    tx.objectStore(META_STORE).delete(id);
    await txPromise(tx);
  }

  async putChunk(id, index, data) {
    const db = await this._db();
    const tx = db.transaction(CHUNK_STORE, 'readwrite');
    tx.objectStore(CHUNK_STORE).put({ id, index, data });
    await txPromise(tx);
  }

  async getChunk(id, index) {
    const db = await this._db();
    const tx = db.transaction(CHUNK_STORE, 'readonly');
    const record = await reqPromise(tx.objectStore(CHUNK_STORE).get([id, index]));
    return record ? record.data : null;
  }

  async chunkCount(id) {
    const db = await this._db();
    const tx = db.transaction(CHUNK_STORE, 'readonly');
    const count = await reqPromise(tx.objectStore(CHUNK_STORE).index('byId').count(IDBKeyRange.only(id)));
    return count || 0;
  }

  // 只取分块索引与大小（游标不反序列化 Blob 数据），用于断点恢复时重建位图。
  async listChunks(id) {
    const db = await this._db();
    const tx = db.transaction(CHUNK_STORE, 'readonly');
    const request = tx
      .objectStore(CHUNK_STORE)
      .index('byId')
      .openCursor(IDBKeyRange.only(id));
    const items = [];
    await new Promise((resolve, reject) => {
      request.onsuccess = () => {
        const cursor = request.result;
        if (cursor) {
          items.push({ index: cursor.primaryKey[1], size: cursor.value.size });
          cursor.continue();
        } else {
          resolve();
        }
      };
      request.onerror = () => reject(request.error);
    });
    await txPromise(tx);
    return items;
  }

  async allChunks(id) {
    const db = await this._db();
    const tx = db.transaction(CHUNK_STORE, 'readonly');
    const records = await reqPromise(
      tx.objectStore(CHUNK_STORE).index('byId').getAll(IDBKeyRange.only(id))
    );
    records.sort((a, b) => a.index - b.index);
    return records.map((record) => ({ index: record.index, data: record.data }));
  }

  async clearChunks(id) {
    const db = await this._db();
    const tx = db.transaction(CHUNK_STORE, 'readwrite');
    const request = tx
      .objectStore(CHUNK_STORE)
      .index('byId')
      .openCursor(IDBKeyRange.only(id));
    await new Promise((resolve, reject) => {
      request.onsuccess = () => {
        const cursor = request.result;
        if (cursor) {
          cursor.delete();
          cursor.continue();
        } else {
          resolve();
        }
      };
      request.onerror = () => reject(request.error);
    });
    await txPromise(tx);
  }
}
