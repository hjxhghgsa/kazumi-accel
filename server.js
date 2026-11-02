'use strict';
/*
 * KazumiAccel —— Kazumi 本地加速代理（火火）
 * 监听 127.0.0.1:8899（仅本机）。两模式：
 *
 *  ① 重写模式  GET /p?u=<编码后的视频地址>
 *     - 把目标视频拉进【内存】缓冲（零磁盘缓存）。
 *     - 源支持 Range -> 直接转发 Range（高效、可拖动）。
 *     - 源不支持 Range -> 整段缓冲到内存后再按客户端 Range 切片返回，
 *       于是「随便拖动进度条」对任意源都成立。
 *
 *  ② 透明代理模式（当 Kazumi「全局代理」指向 http://127.0.0.1:8899 时用）
 *     - 普通请求：转发并回传。
 *     - CONNECT（https 隧道）：建立 TCP 隧道透传。
 *
 * 所有缓冲只在内存，绝不写盘 —— 满足「零缓存」。
 */

const http = require('http');
const https = require('https');
const net = require('net');
const { URL } = require('url');

const PORT = +(process.env.PORT || 8899);
const MAX_BUFFER = 1500 * 1024 * 1024; // 超过 1.5GB 不缓冲，改透传（防 OOM）
const UA =
  'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 ' +
  '(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36';

function libOf(u) {
  return u.protocol === 'https:' ? https : http;
}

// 去掉逐跳头
function stripHop(h) {
  const out = {};
  const skip = new Set([
    'proxy-connection', 'connection', 'keep-alive', 'proxy-authenticate',
    'proxy-authorization', 'te', 'trailers', 'transfer-encoding', 'upgrade',
  ]);
  for (const k in h) if (!skip.has(k.toLowerCase())) out[k] = h[k];
  return out;
}

// 给上游的请求头：浏览器 UA + 同源 referer（绕过部分防盗链）
function upstreamHeaders(target) {
  const u = new URL(target);
  return {
    'user-agent': UA,
    'referer': u.origin + '/',
    'accept': '*/*',
    'accept-language': 'zh-CN,zh;q=0.9',
  };
}

// 内存缓冲池：同一地址只下一次源
const buffers = new Map();

function fetchFull(target) {
  if (buffers.has(target)) return buffers.get(target);
  const p = new Promise((resolve, reject) => {
    const req = libOf(new URL(target)).request(target, {
      method: 'GET',
      headers: upstreamHeaders(target),
      timeout: 30000,
    });
    req.on('timeout', () => req.destroy(new Error('upstream timeout')));
    req.on('response', (res) => {
      if (res.statusCode !== 200 && res.statusCode !== 206) {
        return reject(new Error('upstream ' + res.statusCode));
      }
      const chunks = [];
      let len = 0;
      res.on('data', (c) => {
        chunks.push(c);
        len += c.length;
        if (len > MAX_BUFFER) {
          // 太大：放弃缓冲，标记后用透传
          req.destroy();
          reject(new Error('TOO_LARGE'));
        }
      });
      res.on('end', () =>
        resolve({
          buf: Buffer.concat(chunks),
          contentType: res.headers['content-type'] || 'application/octet-stream',
          total: len,
        })
      );
    });
    req.on('error', reject);
    req.end();
  });
  buffers.set(target, p);
  return p;
}

// 预检：源是否支持 Range + 内容长度
function head(target) {
  return new Promise((resolve, reject) => {
    const req = libOf(new URL(target)).request(target, {
      method: 'HEAD',
      headers: upstreamHeaders(target),
      timeout: 15000,
    });
    req.on('timeout', () => req.destroy(new Error('head timeout')));
    req.on('response', (res) => {
      resolve({
        acceptRanges: res.headers['accept-ranges'] || '',
        contentLength: +(res.headers['content-length'] || 0),
        status: res.statusCode,
      });
      req.destroy();
    });
    req.on('error', reject);
    req.end();
  });
}

// 透传（带上客户端 Range）
function pipeProxy(req, res, target) {
  const headers = Object.assign(upstreamHeaders(target), stripHop(req.headers));
  if (req.headers['range']) headers['range'] = req.headers['range'];
  const r = libOf(new URL(target)).request(target, {
    method: req.method === 'HEAD' ? 'HEAD' : 'GET',
    headers,
    timeout: 30000,
  });
  r.on('timeout', () => r.destroy(new Error('upstream timeout')));
  r.on('response', (up) => {
    const out = Object.assign({}, up.headers, {
      'access-control-allow-origin': '*',
    });
    res.writeHead(up.statusCode, out);
    up.pipe(res);
  });
  r.on('error', (e) => {
    if (!res.headersSent) res.writeHead(502);
    res.end('upstream error: ' + e.message);
  });
  req.on('error', () => r.destroy());
  if (req.method !== 'GET' && req.method !== 'HEAD') req.pipe(r);
  else r.end();
}

// 重写模式主逻辑
async function handleRewrite(req, res, target) {
  try {
    let probe;
    try {
      probe = await head(target);
    } catch (_) {
      probe = { acceptRanges: '', contentLength: 0, status: 0 };
    }
    const supportsRange = /bytes/i.test(probe.acceptRanges);
    const tooBig = probe.contentLength > MAX_BUFFER;

    // 源支持 Range，或太大 -> 直接透传（支持 range 时本就能拖动）
    if (supportsRange || tooBig) {
      return pipeProxy(req, res, target);
    }

    // 源不支持 Range 且不大 -> 整段缓冲到内存，再用内存满足任意 Range
    let data;
    try {
      data = await fetchFull(target);
    } catch (e) {
      if (e.message === 'TOO_LARGE' || e.message === 'upstream timeout') {
        return pipeProxy(req, res, target); // 退化为透传
      }
      throw e;
    }

    const { buf, contentType, total } = data;
    const range = req.headers['range'];
    if (range) {
      const m = /bytes=(\d*)-(\d*)/.exec(range);
      let start = m && m[1] ? +m[1] : 0;
      let end = m && m[2] ? +m[2] : total - 1;
      if (end >= total) end = total - 1;
      if (start > end || start >= total) {
        res.writeHead(416, { 'content-range': `bytes */${total}` });
        return res.end();
      }
      const slice = buf.subarray(start, end + 1);
      res.writeHead(206, {
        'content-type': contentType,
        'content-length': slice.length,
        'content-range': `bytes ${start}-${end}/${total}`,
        'accept-ranges': 'bytes',
        'access-control-allow-origin': '*',
      });
      return res.end(slice);
    }
    res.writeHead(200, {
      'content-type': contentType,
      'content-length': total,
      'accept-ranges': 'bytes',
      'access-control-allow-origin': '*',
    });
    res.end(buf);
  } catch (e) {
    if (!res.headersSent) res.writeHead(502);
    res.end('upstream error: ' + e.message);
  }
}

function handler(req, res) {
  const url = new URL(req.url, `http://${req.headers.host || '127.0.0.1'}`);
  if (url.pathname === '/p') {
    const target = url.searchParams.get('u');
    if (!target) {
      res.writeHead(400);
      return res.end('missing u');
    }
    return handleRewrite(req, res, target);
  }
  // 透明代理：绝对 URI
  if (/^https?:\/\//i.test(req.url)) {
    return pipeProxy(req, res, req.url);
  }
  if (url.pathname === '/' || url.pathname === '/health') {
    res.writeHead(200, { 'content-type': 'text/html; charset=utf-8' });
    return res.end(
      '<h1>KazumiAccel 本地加速代理</h1>' +
      '<p>重写用法：<code>http://127.0.0.1:8899/p?u=&lt;URL编码后的视频地址&gt;</code></p>' +
      '<p>也可把本机地址 <code>http://127.0.0.1:8899</code> 填进 Kazumi 的「全局代理」。</p>'
    );
  }
  res.writeHead(404);
  res.end('not found');
}

const server = http.createServer(handler);

// CONNECT 隧道（https 走全局代理时用）
server.on('connect', (req, cltSocket, head) => {
  const idx = req.url.lastIndexOf(':');
  const host = idx >= 0 ? req.url.slice(0, idx) : req.url;
  const port = idx >= 0 ? +(req.url.slice(idx + 1)) || 443 : 443;
  const srv = net.connect(port, host, () => {
    cltSocket.write('HTTP/1.1 200 Connection Established\r\n\r\n');
    if (head && head.length) srv.write(head);
    srv.pipe(cltSocket);
    cltSocket.pipe(srv);
  });
  srv.on('error', () => cltSocket.destroy());
  cltSocket.on('error', () => srv.destroy());
});

server.listen(PORT, '127.0.0.1', () => {
  console.log(`[KazumiAccel] listening on http://127.0.0.1:${PORT}`);
});
