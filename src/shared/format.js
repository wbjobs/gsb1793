export function formatBytes(bytes) {
  if (!Number.isFinite(bytes) || bytes < 0) return '?';
  if (bytes < 1024) return `${bytes} B`;
  const units = ['KB', 'MB', 'GB', 'TB'];
  let value = bytes;
  let unit = -1;
  do {
    value /= 1024;
    unit += 1;
  } while (value >= 1024 && unit < units.length - 1);
  return `${value.toFixed(value >= 100 ? 0 : value >= 10 ? 1 : 2)} ${units[unit]}`;
}

export function formatSpeed(bytesPerSecond) {
  return bytesPerSecond > 0 ? `${formatBytes(bytesPerSecond)}/s` : '—';
}

export function pct(downloaded, total) {
  return total > 0 ? Math.min(100, (downloaded / total) * 100) : 0;
}
