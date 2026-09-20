// 全局共享常量。内核代码不依赖任何浏览器 API，可直接在 Node 中测试。

export const DEFAULT_CHUNK_SIZE = 8 * 1024 * 1024; // 单个分块 8MB
export const DEFAULT_CONCURRENCY = 3; // 全局同时进行的分块请求数
export const DEFAULT_RATE_LIMIT = 0; // bytes/s，0 表示不限速
export const DEFAULT_MAX_RETRIES = 5;
export const DEFAULT_RETRY_BASE_MS = 500;
export const DEFAULT_RETRY_MAX_MS = 15000;
export const READ_FLUSH_BYTES = 256 * 1024; // 流式落盘/限速批大小 256KB
export const PROGRESS_THROTTLE_MS = 300;

// 文件状态
export const FileStatus = Object.freeze({
  QUEUED: 'queued',
  DOWNLOADING: 'downloading',
  PAUSED: 'paused',
  COMPLETE: 'complete',
  ERROR: 'error',
  REMOVED: 'removed',
});

// 下载中断信号触发时使用的错误名
export const ABORT_ERROR_NAME = 'AbortError';
