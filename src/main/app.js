import { DownloadManager } from './DownloadManager.js';
import { DOWNLOAD_STATUS, EVT } from '../shared/protocol.js';
import { formatBytes, formatSpeed, pct } from '../shared/format.js';

const SETTINGS_KEY = 'download-manager-settings';
const settings = loadSettings();

const els = {
  urls: document.querySelector('#urls'),
  concurrency: document.querySelector('#concurrency'),
  rate: document.querySelector('#rate'),
  add: document.querySelector('#add'),
  pauseAll: document.querySelector('#pause-all'),
  resumeAll: document.querySelector('#resume-all'),
  list: document.querySelector('#file-list'),
  emptyTip: document.querySelector('#empty-tip'),
  globalSpeed: document.querySelector('#global-speed')
};

els.concurrency.value = settings.concurrency;
els.rate.value = settings.rateKBps;

const manager = new DownloadManager();
const rows = new Map();

manager.init({
  concurrency: settings.concurrency,
  rateBytesPerSecond: settings.rateKBps * 1024
});

manager.addEventListener(EVT.RESTORED, (event) => {
  for (const item of event.detail) {
    upsertRow(item.id, item);
    if (item.status !== DOWNLOAD_STATUS.COMPLETE) {
      // 重启浏览器后默认保持暂停，由用户显式恢复（断点续传）
      updateRow(item.id, { status: DOWNLOAD_STATUS.PAUSED });
    }
  }
});

manager.addEventListener('state', (event) => {
  upsertRow(event.detail.id, event.detail.state);
});
manager.addEventListener('progress', (event) => {
  const { id, downloaded, total, chunks, chunkCount } = event.detail;
  const row = rows.get(id);
  if (!row) return;
  row.state = { ...(row.state || {}), downloaded, total, chunks, chunkCount };
  renderProgress(row);
});
manager.addEventListener('chunk', (event) => {
  const row = rows.get(event.detail.id);
  if (row) {
    row.chunkFlash = `分块 ${event.detail.index + 1}/${event.detail.chunkCount} 完成`;
  }
});
manager.addEventListener('retry', (event) => {
  const row = rows.get(event.detail.id);
  if (row) {
    row.retryText = `重试 ${event.detail.attempt}${event.detail.delayMs ? `（${(event.detail.delayMs / 1000).toFixed(1)}s 后）` : ''}：${event.detail.message || event.detail.reason || ''}`;
    renderCard(row);
  }
});
manager.addEventListener('fileDone', () => {
  // state 事件会渲染 complete
});
manager.addEventListener('error', (event) => {
  if (!event.detail.id) return;
  const row = rows.get(event.detail.id);
  if (row) {
    row.retryText = `错误：${event.detail.message}`;
    renderCard(row);
  }
});
manager.addEventListener('speed', (event) => {
  els.globalSpeed.textContent = `总速度 ${formatSpeed(event.detail.bytesPerSecond)}`;
});

els.add.addEventListener('click', () => {
  const lines = els.urls.value.split('\n').map((line) => line.trim()).filter(Boolean);
  for (const line of lines) {
    const parsed = parseLine(line);
    manager.add(parsed.url, parsed.filename);
  }
  els.urls.value = '';
});

els.concurrency.addEventListener('change', () => {
  const value = clampInt(els.concurrency.value, 1, 16, 3);
  settings.concurrency = value;
  saveSettings();
  manager.setConcurrency(value);
});

els.rate.addEventListener('change', () => {
  const value = Math.max(0, clampInt(els.rate.value, 0, 1024 * 1024, 0));
  settings.rateKBps = value;
  saveSettings();
  manager.setRate(value * 1024);
});

els.pauseAll.addEventListener('click', () => manager.pauseAll());
els.resumeAll.addEventListener('click', () => manager.resumeAll());

function parseLine(line) {
  // "文件名 URL" 或纯 URL；纯 URL 时允许直接填 demo 文件名走同源 /files/
  const match = /^(\S+)\s+(https?:\/\/\S+)$/.exec(line);
  if (match) return { filename: match[1], url: match[2] };
  if (/^https?:\/\//.test(line)) return { filename: null, url: line };
  return { filename: line, url: `/files/${encodeURIComponent(line)}` };
}

function upsertRow(id, state) {
  let row = rows.get(id);
  if (!row) {
    const li = document.createElement('li');
    li.className = 'file-card';
    els.list.appendChild(li);
    row = { id, el: li, state: null, retryText: '' };
    rows.set(id, row);
  }
  row.state = { ...(row.state || {}), ...state };
  els.emptyTip.style.display = 'none';
  renderCard(row);
  return row;
}

function updateRow(id, patch) {
  const row = rows.get(id);
  if (row) {
    row.state = { ...(row.state || {}), ...patch };
    renderCard(row);
  }
}

function renderCard(row) {
  const state = row.state || {};
  const status = state.status || DOWNLOAD_STATUS.QUEUED;
  const canPause = [DOWNLOAD_STATUS.QUEUED, DOWNLOAD_STATUS.PROBING, DOWNLOAD_STATUS.DOWNLOADING].includes(status);
  const canResume = status === DOWNLOAD_STATUS.PAUSED || status === DOWNLOAD_STATUS.ERROR;
  const complete = status === DOWNLOAD_STATUS.COMPLETE;

  row.el.innerHTML = `
    <div class="file-head">
      <span class="file-name">${escapeHtml(state.filename || row.id)}</span>
      <span class="badge ${status}">${statusLabel(status)}</span>
    </div>
    <div class="file-meta">${escapeHtml(state.url || '')}</div>
    <div class="bar"><div style="width:${pct(state.downloaded || 0, state.total || 0).toFixed(2)}%"></div></div>
    <div class="file-foot">
      <span class="stats">${formatBytes(state.downloaded || 0)} / ${formatBytes(state.total || 0)}
        · 分块 ${state.chunks || 0}/${state.chunkCount || '?'}
        ${state.mode === 'whole' ? ' · 整文件模式（服务端不支持 Range）' : ''}
      </span>
      <span class="btns">
        <button data-act="pause" ${canPause ? '' : 'disabled'}>暂停</button>
        <button data-act="resume" ${canResume ? '' : 'disabled'}>恢复</button>
        <button data-act="save" ${complete ? '' : 'disabled'}>保存</button>
        <button data-act="cancel">删除</button>
      </span>
    </div>
    ${row.retryText ? `<div class="retry-line">${escapeHtml(row.retryText)}</div>` : ''}
  `;
  row.el.querySelector('[data-act="pause"]').addEventListener('click', () => manager.pause(row.id));
  row.el.querySelector('[data-act="resume"]').addEventListener('click', () => {
    row.retryText = '';
    manager.resume(row.id);
  });
  row.el.querySelector('[data-act="save"]').addEventListener('click', async () => {
    try {
      const saved = await manager.save(row.id, state.filename);
      row.retryText = `已触发保存：${saved.filename}（${formatBytes(saved.size)}）`;
      renderCard(row);
    } catch (error) {
      row.retryText = `合并失败：${error.message}`;
      renderCard(row);
    }
  });
  row.el.querySelector('[data-act="cancel"]').addEventListener('click', async () => {
    await manager.cancel(row.id);
    row.el.remove();
    rows.delete(row.id);
    if (rows.size === 0) els.emptyTip.style.display = '';
  });
  renderProgress(row);
}

function renderProgress(row) {
  const state = row.state || {};
  const fill = row.el.querySelector('.bar > div');
  const stats = row.el.querySelector('.stats');
  if (!fill || !stats) return;
  fill.style.width = `${pct(state.downloaded || 0, state.total || 0).toFixed(2)}%`;
  stats.textContent =
    `${formatBytes(state.downloaded || 0)} / ${formatBytes(state.total || 0)}` +
    ` · 分块 ${state.chunks || 0}/${state.chunkCount || '?'}` +
    (state.mode === 'whole' ? ' · 整文件模式（服务端不支持 Range）' : '');
}

function statusLabel(status) {
  return {
    [DOWNLOAD_STATUS.QUEUED]: '排队',
    [DOWNLOAD_STATUS.PROBING]: '探测',
    [DOWNLOAD_STATUS.DOWNLOADING]: '下载中',
    [DOWNLOAD_STATUS.PAUSED]: '已暂停',
    [DOWNLOAD_STATUS.COMPLETE]: '已完成',
    [DOWNLOAD_STATUS.ERROR]: '错误',
    [DOWNLOAD_STATUS.REMOVED]: '已删除'
  }[status] || status;
}

function escapeHtml(value) {
  return String(value).replace(/[&<>"']/g, (char) =>
    ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[char])
  );
}

function clampInt(value, min, max, fallback) {
  const num = Number.parseInt(value, 10);
  if (!Number.isFinite(num)) return fallback;
  return Math.min(max, Math.max(min, num));
}

function loadSettings() {
  try {
    return { concurrency: 3, rateKBps: 0, ...JSON.parse(localStorage.getItem(SETTINGS_KEY) || '{}') };
  } catch {
    return { concurrency: 3, rateKBps: 0 };
  }
}

function saveSettings() {
  localStorage.setItem(SETTINGS_KEY, JSON.stringify(settings));
}
