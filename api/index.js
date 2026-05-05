import { PassThrough, Readable, Transform } from "node:stream";
import { pipeline } from "node:stream/promises";
import { setDefaultResultOrder } from "node:dns";

export const config = {
  api: { bodyParser: false },
  supportsResponseStreaming: true,
  maxDuration: 60,
};

const TARGET_BASE = (process.env.TARGET_DOMAIN || "").replace(/\/$/, "");
const UPSTREAM_DNS_ORDER = (process.env.UPSTREAM_DNS_ORDER || "ipv4first").trim().toLowerCase();
const PLATFORM_HEADER_PREFIX = `x-${String.fromCharCode(118, 101, 114, 99, 101, 108)}-`;
const RELAY_PATH = normalizeRelayPath(process.env.RELAY_PATH || "");
const PUBLIC_RELAY_PATH = normalizeRelayPath(process.env.PUBLIC_RELAY_PATH || "/api");
const RELAY_KEY = (process.env.RELAY_KEY || "").trim();
const UPSTREAM_TIMEOUT_MS = parsePositiveInt(process.env.UPSTREAM_TIMEOUT_MS, 25000, 1000);
const MAX_INFLIGHT = parsePositiveInt(process.env.MAX_INFLIGHT, 128, 1);
const MAX_UP_BPS = parseNonNegativeInt(process.env.MAX_UP_BPS, 2621440);
const MAX_DOWN_BPS = parseNonNegativeInt(process.env.MAX_DOWN_BPS, 2621440);
const SUCCESS_LOG_SAMPLE_RATE = clampNumber(parseFloat(process.env.SUCCESS_LOG_SAMPLE_RATE || "0"), 0, 1);
const SUCCESS_LOG_MIN_DURATION_MS = parseNonNegativeInt(process.env.SUCCESS_LOG_MIN_DURATION_MS, 3000);
const ERROR_LOG_MIN_INTERVAL_MS = parseNonNegativeInt(process.env.ERROR_LOG_MIN_INTERVAL_MS, 5000);
const GLOBAL_UPLOAD_LIMITER = createGlobalLimiter(MAX_UP_BPS);
const GLOBAL_DOWNLOAD_LIMITER = createGlobalLimiter(MAX_DOWN_BPS);

applyDnsPreference();

const ALLOWED_METHODS = new Set(["GET", "HEAD", "POST"]);
const FORWARD_HEADER_EXACT = new Set(["accept", "accept-encoding", "accept-language", "cache-control", "content-length", "content-type", "pragma", "range", "referer", "user-agent"]);
const FORWARD_HEADER_PREFIXES = ["sec-ch-", "sec-fetch-"];
const STRIP_HEADERS = new Set(["host", "connection", "proxy-connection", "keep-alive", "via", "proxy-authenticate", "proxy-authorization", "te", "trailer", "transfer-encoding", "upgrade", "forwarded", "x-forwarded-host", "x-forwarded-proto", "x-forwarded-port", "x-forwarded-for", "x-real-ip"]);

let inFlight = 0;
const logState = { timeout: { lastAt: 0, suppressed: 0 }, error: { lastAt: 0, suppressed: 0 } };

export default async function handler(req, res) {
  const requestId = `${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 8)}`;
  const startedAt = Date.now();
  let slotAcquired = false;

  if (!TARGET_BASE || !RELAY_PATH || RELAY_PATH === "/" || !PUBLIC_RELAY_PATH || PUBLIC_RELAY_PATH === "/") {
    res.statusCode = 500;
    return res.end("System Configuration Error");
  }

  try {
    const host = req.headers.host || "localhost";
    const url = new URL(req.url || "/", `https://${host}`);
    const normalizedPath = normalizeIncomingPath(url.pathname);

    if (!isAllowedRelayPath(normalizedPath, PUBLIC_RELAY_PATH)) {
      res.statusCode = 404;
      return res.end("Not Found");
    }

    const upstreamPath = mapPublicPathToRelayPath(normalizedPath, PUBLIC_RELAY_PATH, RELAY_PATH);
    if (!ALLOWED_METHODS.has(req.method)) {
      res.statusCode = 405;
      return res.end("Method Not Allowed");
    }

    if (RELAY_KEY && (req.headers["x-relay-key"] || "").toString() !== RELAY_KEY) {
      res.statusCode = 403;
      return res.end("Forbidden");
    }

    if (!tryAcquireSlot()) {
      res.statusCode = 503;
      res.setHeader("retry-after", "1");
      return res.end("Busy");
    }
    slotAcquired = true;

    const targetUrl = `${TARGET_BASE}${upstreamPath}${url.search || ""}`;
    const headers = {};
    const clientIp = toHeaderValue(req.headers["x-real-ip"] || req.headers["x-forwarded-for"]);
    
    for (const key of Object.keys(req.headers)) {
      const lower = key.toLowerCase();
      if (STRIP_HEADERS.has(lower) || lower.startsWith(PLATFORM_HEADER_PREFIX) || lower === "x-relay-key" || !shouldForwardHeader(lower)) continue;
      const val = toHeaderValue(req.headers[key]);
      if (val) headers[lower] = val;
    }
    if (clientIp) headers["x-forwarded-for"] = clientIp;

    const hasBody = req.method !== "GET" && req.method !== "HEAD";
    const abortCtrl = new AbortController();
    const timeoutRef = setTimeout(() => abortCtrl.abort(new Error("upstream_timeout")), UPSTREAM_TIMEOUT_MS);

    try {
      const fetchOpts = { method: req.method, headers, redirect: "manual", signal: abortCtrl.signal };
      if (hasBody) {
        fetchOpts.body = Readable.toWeb(GLOBAL_UPLOAD_LIMITER ? req.pipe(createThrottleTransform(GLOBAL_UPLOAD_LIMITER)) : req);
        fetchOpts.duplex = "half";
      }

      const upstream = await fetch(targetUrl, fetchOpts);
      res.statusCode = upstream.status;
      for (const [h, v] of upstream.headers) {
        if (h.toLowerCase() !== "transfer-encoding" && h.toLowerCase() !== "connection") {
          try { res.setHeader(h, v); } catch {}
        }
      }

      if (upstream.body) {
        const downloadStream = GLOBAL_DOWNLOAD_LIMITER ? Readable.fromWeb(upstream.body).pipe(createThrottleTransform(GLOBAL_DOWNLOAD_LIMITER)) : Readable.fromWeb(upstream.body);
        await pipeline(downloadStream, res);
      } else {
        res.end();
      }
    } finally {
      clearTimeout(timeoutRef);
    }
  } catch (err) {
    if (!res.headersSent) {
      res.statusCode = isUpstreamTimeoutError(err) ? 504 : 502;
      res.end("Gateway Error");
    }
  } finally {
    if (slotAcquired) releaseSlot();
  }
}

// تابع‌های کمکی (Helper Functions)
function normalizeRelayPath(p) { p = p.startsWith("/") ? p : `/${p}`; return (p.length > 1 && p.endsWith("/")) ? p.slice(0, -1) : p; }
function normalizeIncomingPath(p) { let n = String(p).replace(/\/{2,}/g, "/"); n = n.startsWith("/") ? n : `/${n}`; return (n.length > 1 && n.endsWith("/")) ? n.slice(0, -1) : n; }
function isAllowedRelayPath(p, pub) { return p === pub || p.startsWith(`${pub}/`); }
function mapPublicPathToRelayPath(p, pub, rel) { return p === pub ? rel : `${rel}${p.slice(pub.length)}`; }
function shouldForwardHeader(h) { if (FORWARD_HEADER_EXACT.has(h)) return true; for (const pre of FORWARD_HEADER_PREFIXES) { if (h.startsWith(pre)) return true; } return false; }
function toHeaderValue(v) { return Array.isArray(v) ? v.join(", ") : String(v || ""); }
function tryAcquireSlot() { if (inFlight >= MAX_INFLIGHT) return false; inFlight++; return true; }
function releaseSlot() { inFlight = Math.max(0, inFlight - 1); }
function parsePositiveInt(r, f, m) { const v = Number(r); return (!Number.isFinite(v) || v < m) ? f : Math.trunc(v); }
function parseNonNegativeInt(r, f) { const v = Number(r); return (!Number.isFinite(v) || v < 0) ? f : Math.trunc(v); }
function clampNumber(v, min, max) { return Math.min(max, Math.max(min, Number.isFinite(v) ? v : min)); }
function applyDnsPreference() { if (UPSTREAM_DNS_ORDER === "ipv4first" || UPSTREAM_DNS_ORDER === "verbatim") { try { setDefaultResultOrder(UPSTREAM_DNS_ORDER); } catch {} } }
function isUpstreamTimeoutError(e) { return e?.name === "AbortError" || e?.message === "upstream_timeout" || e?.cause?.message === "upstream_timeout"; }

function createGlobalLimiter(bps) {
  if (!Number.isFinite(bps) || bps <= 0) return null;
  const burst = Math.max(bps, 262144);
  let tokens = burst, last = Date.now(), queue = [], timer = null;
  const refill = () => { const now = Date.now(), el = now - last; tokens = Math.min(burst, tokens + (el * bps) / 1000); last = now; };
  const process = () => { refill(); while (queue.length > 0 && tokens >= 1) { const item = queue[0]; const grant = Math.min(item.m, Math.floor(tokens)); if (grant < 1) break; tokens -= grant; queue.shift(); item.r(grant); } };
  return { acquire: (m) => new Promise(r => { queue.push({ m: Math.max(1, Math.trunc(m)), r }); process(); if (queue.length > 0 && !timer) timer = setTimeout(() => { timer = null; process(); }, 5); }) };
}

function createThrottleTransform(limiter) {
  return new Transform({ transform(chunk, enc, cb) {
    (async () => { let off = 0; while (off < chunk.length) { const grant = await limiter.acquire(chunk.length - off); this.push(chunk.subarray(off, off + grant)); off += grant; } })().then(() => cb()).catch(cb);
  }});
}