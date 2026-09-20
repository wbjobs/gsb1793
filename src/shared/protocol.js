// 主线程 <-> Worker 消息协议。所有消息均为纯结构化克隆可序列化对象。

export const REQ = Object.freeze({
  INIT: 'init',
  ADD: 'add',
  PAUSE: 'pause',
  RESUME: 'resume',
  CANCEL: 'cancel',
  REMOVE: 'remove',
  PAUSE_ALL: 'pauseAll',
  RESUME_ALL: 'resumeAll',
  SET_RATE: 'setRate',
  SET_CONCURRENCY: 'setConcurrency',
  EXPORT: 'export',
  LIST_RESTORED: 'listRestored'
});

export const EVT = Object.freeze({
  STATE: 'state',
  PROGRESS: 'progress',
  CHUNK: 'chunk',
  RETRY: 'retry',
  FILE_DONE: 'fileDone',
  ERROR: 'error',
  RESTORED: 'restored',
  BLOB: 'blob',
  SAVED: 'saved'
});

export const DOWNLOAD_STATUS = Object.freeze({
  QUEUED: 'queued',
  PROBING: 'probing',
  DOWNLOADING: 'downloading',
  PAUSED: 'paused',
  COMPLETE: 'complete',
  ERROR: 'error',
  REMOVED: 'removed'
});
