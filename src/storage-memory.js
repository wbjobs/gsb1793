/**
 * 内存版分块存储，与 src/idb-storage.js 的 IndexedDB 实现接口一致，
 * 供 Node 单元测试使用。
 *
 * 记录结构（与 IDB chunk store 的 value 对齐）：
 * { fileId, index, start, end, size, blob, done }
 */
export class MemoryChunkStorage {
  constructor() {
    /** @type {Map<string, any[]>} fileId -> 按 index 存放的记录 */
    this.files = new Map();
  }

  async putChunk(rec) {
    let list = this.files.get(rec.fileId);
    if (!list) {
      list = [];
      this.files.set(rec.fileId, list);
    }
    list[rec.index] = { ...rec };
  }

  async getChunk(fileId, index) {
    const list = this.files.get(fileId);
    return list ? list[index] || null : null;
  }

  async listChunks(fileId) {
    const list = this.files.get(fileId);
    if (!list) return [];
    return list.filter(Boolean).sort((a, b) => a.index - b.index);
  }

  async deleteChunksFrom(fileId, fromIndex) {
    const list = this.files.get(fileId);
    if (!list) return;
    for (let i = fromIndex; i < list.length; i++) delete list[i];
  }

  async deleteFile(fileId) {
    this.files.delete(fileId);
  }

  async completedChunkIndexes(fileId, chunkCount) {
    const list = this.files.get(fileId);
    if (!list) return new Set();
    const set = new Set();
    for (let i = 0; i < chunkCount; i++) {
      const rec = list[i];
      if (rec && rec.done) set.add(i);
    }
    return set;
  }
}
