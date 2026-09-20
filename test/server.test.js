import { test } from 'node:test';
import assert from 'node:assert/strict';
import { EventEmitter } from 'node:events';
import { handler } from '../server/server.mjs';

// 进程内调用 request handler，不监听 TCP（沙箱/CI 均可运行）。
// 同步收集响应：write 始终返回 true（无背压），end 时兑现。
function rawRequest(requestTarget, { method = 'GET', headers = {} } = {}) {
  return new Promise((resolve, reject) => {
    const chunks = [];
    const sink = new EventEmitter();
    sink.statusCode = 200;
    sink.headersSent = false;
    sink.writeHead = function writeHead(status, responseHeaders = {}) {
      this.statusCode = status;
      this.headers = responseHeaders;
      this.headersSent = true;
      return this;
    };
    sink.setHeader = function setHeader(name, value) {
      this.headers = { ...(this.headers || {}), [name]: value };
    };
    sink.write = function write(chunk) {
      chunks.push(Buffer.from(chunk));
      return true;
    };
    sink.end = function end(chunk) {
      if (chunk) chunks.push(Buffer.from(chunk));
      this.emit('finish');
      resolve({ status: this.statusCode, headers: this.headers || {}, body: Buffer.concat(chunks) });
      return this;
    };
    const request = {
      method,
      url: requestTarget,
      headers: { host: 'localhost', ...headers }
    };
    try {
      handler(request, sink);
    } catch (error) {
      reject(error);
    }
  });
}

test('服务器：Range 请求返回 206 / Content-Range / ETag 与 100 字节', async () => {
  const response = await rawRequest('/files/demo-1mb.bin', { headers: { range: 'bytes=100-199' } });
  assert.equal(response.status, 206);
  assert.equal(response.headers['Content-Range'], 'bytes 100-199/1048576');
  assert.match(response.headers.ETag, /demo-1mb\.bin-1048576/);
  assert.equal(response.headers['Accept-Ranges'], 'bytes');
  assert.equal(response.body.length, 100);
});

test('服务器：无 Range 返回 200、Accept-Ranges 与完整 Content-Length', async () => {
  const response = await rawRequest('/files/demo-1mb.bin');
  assert.equal(response.status, 200);
  assert.equal(response.headers['Accept-Ranges'], 'bytes');
  assert.equal(String(response.headers['Content-Length']), '1048576');
  assert.equal(response.body.length, 1048576);
});

test('服务器：Range 内容确定且跨请求一致', async () => {
  const r1 = await rawRequest('/files/demo-1mb.bin', { headers: { range: 'bytes=0-999' } });
  const r2 = await rawRequest('/files/demo-1mb.bin', { headers: { range: 'bytes=0-999' } });
  assert.deepEqual(r1.body, r2.body);
  const a = await rawRequest('/files/demo-1mb.bin', { headers: { range: 'bytes=0-511' } });
  const b = await rawRequest('/files/demo-1mb.bin', { headers: { range: 'bytes=512-1023' } });
  const full = await rawRequest('/files/demo-1mb.bin', { headers: { range: 'bytes=0-1023' } });
  assert.deepEqual(Buffer.concat([a.body, b.body]), full.body);
});

test('服务器：非法 Range 返回 416', async () => {
  const response = await rawRequest('/files/demo-1mb.bin', { headers: { range: 'bytes=999999999-' } });
  assert.equal(response.status, 416);
  assert.equal(response.headers['Content-Range'], 'bytes */1048576');
});

test('服务器：?fail=2 前两次 500（带 Retry-After）第三次恢复', async () => {
  const r1 = await rawRequest('/files/demo-1mb.bin?fail=2');
  const r2 = await rawRequest('/files/demo-1mb.bin?fail=2');
  const r3 = await rawRequest('/files/demo-1mb.bin?fail=2', { headers: { range: 'bytes=0-99' } });
  assert.equal(r1.status, 500);
  assert.equal(r2.status, 500);
  assert.equal(r1.headers['Retry-After'], '1');
  assert.equal(r3.status, 206);
  assert.equal(r3.body.length, 100);
});

test('服务器：自定义大小 gen-5mb.bin', async () => {
  const response = await rawRequest('/files/gen-5mb.bin', { headers: { range: 'bytes=0-9' } });
  assert.equal(response.status, 206);
  assert.equal(response.headers['Content-Range'], 'bytes 0-9/5242880');
});

test('服务器：未知文件 404，静态页 200', async () => {
  const missing = await rawRequest('/files/nope.bin');
  assert.equal(missing.status, 404);
  const home = await rawRequest('/');
  assert.equal(home.status, 200);
  assert.match(home.body.toString('utf8'), /分块并发下载器/);
});
