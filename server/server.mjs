// 零依赖测试服务器：
//   GET /                     演示页（静态文件）
//   GET /src/*, /styles.css   静态资源
//   GET /api/files            可用的演示文件清单
//   GET /files/:name          确定性虚拟文件，完整支持 Range / ETag / Last-Modified
//                             查询参数 ?fail=N：该 URL 的前 N 次请求返回 500（验证自动重试）
// 用法：node server/server.mjs [--port 5173]
import http from 'node:http';
import { createReadStream, existsSync, statSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { dirname, extname, join, normalize } from 'node:path';

const ROOT = join(dirname(fileURLToPath(import.meta.url)), '..');
const PORT = Number(process.argv.includes('--port') ? process.argv[process.argv.indexOf('--port') + 1] : 5173);

const DEMO_FILES = [
  { name: 'demo-1mb.bin', size: 1 * 1024 * 1024 },
  { name: 'demo-16mb.bin', size: 16 * 1024 * 1024 },
  { name: 'demo-64mb.bin', size: 64 * 1024 * 1024 },
  { name: 'demo-256mb.bin', size: 256 * 1024 * 1024 },
  { name: 'demo-1gb.bin', size: 1024 * 1024 * 1024 }
];

const MIME = {
  '.html': 'text/html; charset=utf-8',
  '.js': 'text/javascript; charset=utf-8',
  '.mjs': 'text/javascript; charset=utf-8',
  '.css': 'text/css; charset=utf-8',
  '.json': 'application/json; charset=utf-8',
  '.bin': 'application/octet-stream'
};

// 每个完整 URL 的故障计数（?fail=N）
const failCounters = new Map();

function resolveDemoFile(name) {
  const preset = DEMO_FILES.find((file) => file.name === name);
  if (preset) return preset;
  const match = /^gen-(\d+)(mb|gb)\.bin$/i.exec(name);
  if (match) {
    const n = Number(match[1]);
    const multiplier = match[2].toLowerCase() === 'gb' ? 1024 * 1024 * 1024 : 1024 * 1024;
    return { name, size: n * multiplier };
  }
  return null;
}

// 确定性字节：按 64KB 块生成，块内前 8 字节带块索引印章，其余为种子循环。
// 任意偏移的内容都可复现（测试端用同一公式逐字节校验）。
const PATTERN_SEED = 'deterministic-fixture';
function patternBlock(blockIndex) {
  const block = Buffer.alloc(64 * 1024);
  for (let i = 0; i < block.length; i += 1) {
    block[i] = (PATTERN_SEED.charCodeAt(i % PATTERN_SEED.length) * 31 + i * 7) & 0xff;
  }
  const stamp = BigInt(blockIndex) * 0x9e3779b97f4a7c15n & 0xffffffffffffffffn;
  for (let i = 0; i < 8; i += 1) {
    block[i] ^= Number((stamp >> BigInt(i * 8)) & 0xffn);
  }
  return block;
}

function sendGeneratedRange(response, file, start, end, statusCode, headers) {
  response.writeHead(statusCode, headers);
  const BLOCK = 64 * 1024;
  let offset = start;
  let firstBlock = Math.floor(start / BLOCK);
  let blockIndex = firstBlock;
  const pump = () => {
    while (offset <= end) {
      const blockStart = blockIndex * BLOCK;
      const blockEnd = blockStart + BLOCK - 1;
      const sliceStart = Math.max(offset, blockStart);
      const sliceEnd = Math.min(end, blockEnd);
      const chunk = patternBlock(blockIndex).subarray(sliceStart - blockStart, sliceEnd - blockStart + 1);
      offset = sliceEnd + 1;
      blockIndex += 1;
      if (!response.write(chunk)) {
        response.once('drain', pump);
        return;
      }
    }
    response.end();
  };
  pump();
}

function etagFor(file) {
  return `"${file.name}-${file.size}"`;
}

function commonHeaders(file) {
  return {
    'Content-Type': 'application/octet-stream',
    'Accept-Ranges': 'bytes',
    'ETag': etagFor(file),
    'Last-Modified': 'Mon, 01 Jan 2024 00:00:00 GMT',
    'Cache-Control': 'no-store'
  };
}

function rangeHeaders(file, start, end) {
  return {
    ...commonHeaders(file),
    'Content-Length': String(end - start + 1),
    'Content-Range': `bytes ${start}-${end}/${file.size}`
  };
}

function serveFile(request, response, pathname, query) {
  const name = decodeURIComponent(pathname.replace(/^\/files\//, ''));
  const file = resolveDemoFile(name);
  if (!file) {
    response.writeHead(404).end('unknown demo file');
    return;
  }

  const failN = Number(new URLSearchParams(query).get('fail') || 0);
  if (failN > 0) {
    const key = `${pathname}?${query}`;
    const left = (failCounters.get(key) ?? failN) - 1;
    failCounters.set(key, left);
    if (left >= 0) {
      response.writeHead(500, { 'Content-Type': 'text/plain', 'Retry-After': '1' }).end(`注入故障：剩余 ${left + 1} 次`);
      return;
    }
  }

  const range = request.headers.range;
  if (range) {
    const match = /bytes=(\d*)-(\d*)/.exec(range);
    if (match) {
      let start = match[1] === '' ? null : Number(match[1]);
      let end = match[2] === '' ? null : Number(match[2]);
      if (start === null) {
        start = Math.max(0, file.size - (end || 0));
        end = file.size - 1;
      } else if (end === null) {
        end = file.size - 1;
      }
      if (start > end || start >= file.size || end >= file.size) {
        response.writeHead(416, { 'Content-Range': `bytes */${file.size}` }).end();
        return;
      }
      sendGeneratedRange(response, file, start, Math.min(end, file.size - 1), 206, rangeHeaders(file, start, Math.min(end, file.size - 1)));
      return;
    }
  }

  const headers200 = { ...commonHeaders(file), 'Content-Length': String(file.size) };
  response.writeHead(200, headers200);
  sendGeneratedRange(response, file, 0, file.size - 1, 200, headers200);
}

function serveStatic(request, response, pathname) {
  const rel = pathname === '/' ? '/index.html' : pathname;
  const safePath = normalize(rel).replace(/^(\.\.[/\\])+/, '');
  const filePath = join(ROOT, safePath);
  if (!filePath.startsWith(ROOT) || !existsSync(filePath) || !statSync(filePath).isFile()) {
    response.writeHead(404).end('not found');
    return;
  }
  response.writeHead(200, {
    'Content-Type': MIME[extname(filePath)] || 'application/octet-stream'
  });
  createReadStream(filePath).pipe(response);
}

export function handler(request, response) {
  const url = new URL(request.url, `http://${request.headers.host || 'localhost'}`);
  if (url.pathname === '/api/files') {
    response.writeHead(200, { 'Content-Type': 'application/json' });
    response.end(JSON.stringify(DEMO_FILES));
    return;
  }
  if (url.pathname.startsWith('/files/')) {
    serveFile(request, response, url.pathname, url.searchParams.toString());
    return;
  }
  serveStatic(request, response, url.pathname);
}

// 仅在被直接执行时监听端口（被 import 时不监听，便于测试复用）。
const isMain = process.argv[1] && import.meta.url === new URL(`file://${process.argv[1]}`).href;
if (isMain) {
  const server = http.createServer(handler);
  server.listen(PORT, () => {
    console.log(`下载器演示页:  http://localhost:${PORT}/`);
    console.log(`1GB 测试文件: http://localhost:${PORT}/files/demo-1gb.bin`);
    console.log(`故障注入示例: http://localhost:${PORT}/files/demo-16mb.bin?fail=3`);
  });
}
