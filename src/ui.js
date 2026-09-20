import { DownloadManager } from './download-manager.js';

const $ = (sel) => document.querySelector(sel);

const dm = new DownloadManager();

const els = {
  urls: $('#urls'),
  add: $('#btn-add'),
  addPaused: $('#btn-add-paused'),
  concurrency: $('#concurrency'),
  concurrencyValue: $('#concurrency-value'),
  rate: $('#rate'),
  list: $('#file-list'),
  emptyTip: $('#empty-tip'),
  log: $('#log'),
};

const STATUS_TEXT = {
  queued: '等待中',
  downloading: '下载中',
  paused: '已暂停',
  complete: '已完成',
  error: '失败（可继续重试）',
  removed: '已删除',
};

function fmtBytes(n) {
  if (n == null) return '未知大小';
  if (n < 1024) return `${n} B`;
  const units = ['KB', 'MB', 'GB', 'TB'];
  let v = n / 1024;
  let i = 0;
  while (v >= 1024 && i < units.length - 1) { v /= 1024; i += 1; }
  return `${v.toFixed(v >= 100 ? 0 : v >= 10 ? 1 : 2)} ${units[i]}`;
}

function log(message, level = '') {
  const line = document.createElement('div');
  line.className = `log-line ${level}`;
  const time = new Date().toLocaleTimeString();
  line.textContent = `[${time}] ${message}`;
  els.log.appendChild(line);
  while (els.log.children.length > 200) els.log.removeChild(els.log.firstChild);
  els.log.scrollTop = els.log.scrollHeight;
}

function render() {
  const files = [...dm.files.values()];
  els.emptyTip.style.display = files.length === 0 ? '' : 'none';
  // 全量重渲染：任务数量少，简单可靠；进度条更新只改宽度文本，不重建 DOM
  const existing = new Map();
  for (const node of els.list.children) existing.set(node.dataset.id, node);

  for (const file of files) {
    let node = existing.get(file.id);
    if (!node) {
      node = buildNode(file);
      els.list.appendChild(node);
    }
    updateNode(node, file);
    existing.delete(file.id);
  }
  for (const node of existing.values()) node.remove();
}

function buildNode(file) {
  const node = document.createElement('div');
  node.className = 'file-item';
  node.dataset.id = file.id;
  node.innerHTML = `
    <div class="file-head">
      <div>
        <div class="file-name"></div>
        <div class="file-url"></div>
      </div>
      <span class="badge"></span>
    </div>
    <div class="progress-wrap">
      <div class="progress-bar"><div class="progress-fill"></div></div>
      <div class="progress-meta">
        <span class="bytes"></span>
        <span class="percent"></span>
      </div>
    </div>
    <div class="file-error-text" hidden></div>
    <div class="file-actions">
      <button class="act-pause">暂停</button>
      <button class="act-resume">继续</button>
      <button class="act-retry">重试</button>
      <button class="act-save success">保存到磁盘</button>
      <button class="act-remove danger">删除</button>
    </div>
  `;
  node.querySelector('.file-name').textContent = file.filename;
  node.querySelector('.file-url').textContent = file.url;
  node.querySelector('.act-pause').onclick = () => dm.pause(file.id);
  node.querySelector('.act-resume').onclick = () => dm.resume(file.id);
  node.querySelector('.act-retry').onclick = () => dm.resume(file.id);
  node.querySelector('.act-save').onclick = async () => {
    try {
      const ok = await dm.saveToDisk(file.id);
      if (ok) log(`已导出 ${file.filename}`, 'ok');
    } catch (err) {
      log(`保存失败：${err.message}`, 'error');
    }
  };
  node.querySelector('.act-remove').onclick = () => dm.remove(file.id);
  return node;
}

function updateNode(node, file) {
  const badge = node.querySelector('.badge');
  badge.className = `badge ${file.status}`;
  badge.textContent = STATUS_TEXT[file.status] || file.status;

  const pct = file.size ? Math.min(100, file.progress * 100) : 0;
  node.querySelector('.progress-fill').style.width = `${pct}%`;
  node.querySelector('.percent').textContent = file.size ? `${pct.toFixed(1)}%` : '';
  node.querySelector('.bytes').textContent = file.size
    ? `${fmtBytes(file.downloaded)} / ${fmtBytes(file.size)}${file.supportsRange === false ? '（服务器不支持 Range，无法分块续传）' : ''}`
    : fmtBytes(file.downloaded);

  const errBox = node.querySelector('.file-error-text');
  if (file.error) {
    errBox.hidden = false;
    errBox.textContent = file.error;
  } else {
    errBox.hidden = true;
  }

  const active = file.status === 'downloading' || file.status === 'queued';
  node.querySelector('.act-pause').disabled = !active;
  node.querySelector('.act-resume').disabled = active || file.status === 'complete';
  node.querySelector('.act-retry').style.display = file.status === 'error' ? '' : 'none';
  node.querySelector('.act-save').disabled = file.status !== 'complete';
}

async function addUrls(autoStart) {
  const urls = els.urls.value.split('\n').map((u) => u.trim()).filter(Boolean);
  if (urls.length === 0) return;
  for (const url of urls) {
    try {
      const file = await dm.add(url, { autoStart });
      log(`已添加：${file.filename}`);
    } catch (err) {
      log(`添加失败：${url} ${err.message}`, 'error');
    }
  }
  els.urls.value = '';
}

els.add.onclick = () => addUrls(true);
els.addPaused.onclick = () => addUrls(false);
els.concurrency.oninput = () => {
  const n = Number(els.concurrency.value);
  els.concurrencyValue.textContent = String(n);
  dm.setConcurrency(n);
  log(`并发数调整为 ${n}`);
};
els.rate.onchange = () => {
  const rate = Number(els.rate.value);
  dm.setRate(rate);
  log(rate === 0 ? '已关闭限速' : `限速调整为 ${fmtBytes(rate)}/s`);
};

dm.on('ready', ({ settings }) => {
  els.concurrency.value = settings.concurrency;
  els.concurrencyValue.textContent = String(settings.concurrency);
  els.rate.value = String(settings.rateLimit);
  render();
  log('下载器已就绪，已恢复上次的任务（未完成任务保持暂停，点击继续即可断点续传）', 'ok');
});
dm.on('progress', render);
dm.on('file-complete', ({ file }) => {
  render();
  log(`下载完成：${file.filename}，可点“保存到磁盘”导出`, 'ok');
});
dm.on('file-error', ({ fileId, error }) => {
  render();
  const file = dm.files.get(fileId);
  log(`任务失败：${file ? file.filename : fileId} — ${error}`, 'error');
});
dm.on('retry', ({ label, attempt, maxRetries, waitMs, message }) => {
  log(`${label} 第 ${attempt}/${maxRetries} 次重试（${waitMs}ms 后）：${message}`, 'warn');
});
dm.on('file-changed', ({ fileId }) => {
  log(`服务器文件已变化（ETag 不同），已清空旧分块重新下载：${fileId}`, 'warn');
});
dm.on('worker-error', ({ message }) => log(`Worker 错误：${message}`, 'error'));
dm.on('fatal', ({ message }) => log(`致命错误：${message}`, 'error'));
