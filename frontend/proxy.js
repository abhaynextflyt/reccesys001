/**
 * WebSocket-aware Proxy with Redis capacity tracking
 *
 * Routes incoming camera and frontend WebSocket connections to relay
 * servers that still have capacity (max 2 cameras + 2 frontends each).
 *
 * Install dependencies:
 *   npm install ws ioredis http-proxy
 *
 * Run:
 *   node proxy.js
 *
 * Expects relay servers running on ports defined in RELAY_PORTS (default: 8081,8082,8083)
 * Expects Redis running on REDIS_URL (default: redis://localhost:6379)
 */

const http    = require('http');
const crypto  = require('crypto');
const { URL } = require('url');
const WebSocket       = require('ws');
const { createProxyServer } = require('http-proxy');
const Redis   = require('ioredis');

// ─── CONFIG ────────────────────────────────────────────────────────────────
const PROXY_PORT    = parseInt(process.env.PROXY_PORT   || '8080');
const REDIS_URL     = process.env.REDIS_URL             || 'redis://localhost:6379';
const RELAY_PORTS   = (process.env.RELAY_PORTS          || '8081,8082,8083')
                        .split(',').map(Number);
const RELAY_HOST    = process.env.RELAY_HOST            || 'localhost';
const MAX_CAMERAS   = parseInt(process.env.MAX_CAMERAS  || '2');
const MAX_FRONTENDS = parseInt(process.env.MAX_FRONTENDS|| '2');

// Path prefixes — must match what server.js uses
const PI_PATH_PREFIX = '/ws/pi/';
const FEED_PATH      = '/ws/feed';

// Redis key patterns
const CAPACITY_KEY   = (port) => `relay:${port}:capacity`;   // hash: { cameras, frontends }
const SESSION_KEY    = (token) => `session:${token}`;         // string: port number
const CAPACITY_TTL   = 20;    // seconds — relay servers must refresh within this window
const SESSION_TTL    = 3600;  // seconds — sticky session lifetime

// ─── REDIS ─────────────────────────────────────────────────────────────────
const redis = new Redis(REDIS_URL);

redis.on('connect', () => console.log('[Redis] Connected'));
redis.on('error',   (e) => console.error('[Redis] Error:', e.message));

// ─── HTTP PROXY INSTANCE ────────────────────────────────────────────────────
const proxy = createProxyServer({ ws: true });

proxy.on('error', (err, req, res) => {
  console.error('[Proxy] Error:', err.message);
  if (res && res.writeHead && !res.headersSent) {
    res.writeHead(502);
    res.end('Bad gateway');
  }
});

// ─── HELPERS ───────────────────────────────────────────────────────────────

/** Deterministic token from a client identifier (IP or pi_id). */
function makeSessionToken(identifier) {
  return crypto.createHash('md5').update(identifier).digest('hex').slice(0, 16);
}

/** Read the latest capacity for a relay port from Redis. */
async function getCapacity(port) {
  const data = await redis.hgetall(CAPACITY_KEY(port));
  if (!data || Object.keys(data).length === 0) return null;
  return {
    port,
    cameras:   parseInt(data.cameras   || '0'),
    frontends: parseInt(data.frontends || '0'),
  };
}

/**
 * Find a relay server that has room for the given connection type.
 * @param {'camera'|'frontend'} type
 * @returns {Promise<number|null>} port number or null if all full
 */
async function findAvailableRelay(type) {
  for (const port of RELAY_PORTS) {
    const cap = await getCapacity(port);
    if (!cap) {
      // No heartbeat in Redis yet — relay might be starting up; skip it.
      continue;
    }
    const cameraOk   = cap.cameras   < MAX_CAMERAS;
    const frontendOk = cap.frontends < MAX_FRONTENDS;

    if (type === 'camera'   && cameraOk)   return port;
    if (type === 'frontend' && frontendOk) return port;
  }
  return null;
}

/**
 * Get or assign a sticky relay port for this session token.
 * Falls back to a fresh relay if the stored one is now full or gone.
 */
async function getStickyRelay(token, type) {
  const stored = await redis.get(SESSION_KEY(token));
  if (stored) {
    const port = parseInt(stored);
    const cap  = await getCapacity(port);
    if (cap) {
      const stillOk = type === 'camera'
        ? cap.cameras   < MAX_CAMERAS
        : cap.frontends < MAX_FRONTENDS;
      if (stillOk) {
        // Refresh TTL so active sessions don't expire mid-stream
        await redis.expire(SESSION_KEY(token), SESSION_TTL);
        return port;
      }
    }
    // Stored relay is full or dead — fall through to pick a new one
    await redis.del(SESSION_KEY(token));
  }

  const port = await findAvailableRelay(type);
  if (port) {
    await redis.set(SESSION_KEY(token), port, 'EX', SESSION_TTL);
  }
  return port;
}

/** Reject a WebSocket upgrade with an HTTP error response. */
function rejectUpgrade(socket, code, message) {
  socket.write(
    `HTTP/1.1 ${code} ${message}\r\n` +
    'Connection: close\r\n\r\n'
  );
  socket.destroy();
}

// ─── HTTP SERVER ────────────────────────────────────────────────────────────
const server = http.createServer((req, res) => {
  // Proxy health endpoint — returns aggregated state across all relays
  if (req.url === '/health') {
    (async () => {
      const relays = await Promise.all(RELAY_PORTS.map(getCapacity));
      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({
        status: 'ok',
        relays: relays.map(r => r || null),
        proxy_port: PROXY_PORT,
      }));
    })().catch(() => { res.writeHead(500); res.end(); });
    return;
  }

  // Forward any HTTP request (e.g. /health on individual relays) to first alive relay
  const target = `http://${RELAY_HOST}:${RELAY_PORTS[0]}`;
  proxy.web(req, res, { target }, (err) => {
    console.error('[Proxy] HTTP forward error:', err.message);
    res.writeHead(502);
    res.end('Bad gateway');
  });
});

// ─── WEBSOCKET UPGRADE HANDLER ──────────────────────────────────────────────
server.on('upgrade', async (req, socket, head) => {
  const url  = req.url;
  const ip   = req.socket.remoteAddress || 'unknown';

  // ── Camera connection (/ws/pi/<pi_id>) ────────────────────────────────
  if (url.startsWith(PI_PATH_PREFIX)) {
    const piId = url.slice(PI_PATH_PREFIX.length).split('/')[0];
    if (!piId) {
      rejectUpgrade(socket, 400, 'Bad Request');
      return;
    }

    const token = makeSessionToken(`pi:${piId}`);
    const port  = await getStickyRelay(token, 'camera').catch(() => null);

    if (!port) {
      console.warn(`[Proxy] No relay capacity for camera ${piId}`);
      rejectUpgrade(socket, 503, 'No relay capacity');
      return;
    }

    console.log(`[Proxy] Camera ${piId} → relay :${port}`);
    proxy.ws(req, socket, head, { target: `ws://${RELAY_HOST}:${port}` }, (err) => {
      console.error(`[Proxy] WS proxy error (camera ${piId}):`, err.message);
      socket.destroy();
    });
    return;
  }

  // ── Frontend connection (/ws/feed) ─────────────────────────────────────
  if (url === FEED_PATH) {
    const token = makeSessionToken(`fe:${ip}`);
    const port  = await getStickyRelay(token, 'frontend').catch(() => null);

    if (!port) {
      console.warn(`[Proxy] No relay capacity for frontend ${ip}`);
      rejectUpgrade(socket, 503, 'No relay capacity');
      return;
    }

    console.log(`[Proxy] Frontend ${ip} → relay :${port}`);
    proxy.ws(req, socket, head, { target: `ws://${RELAY_HOST}:${port}` }, (err) => {
      console.error(`[Proxy] WS proxy error (frontend ${ip}):`, err.message);
      socket.destroy();
    });
    return;
  }

  // Unknown path
  rejectUpgrade(socket, 404, 'Not Found');
});

// ─── START ──────────────────────────────────────────────────────────────────
server.listen(PROXY_PORT, () => {
  console.log(`[Proxy] Listening on port ${PROXY_PORT}`);
  console.log(`[Proxy] Relay servers: ${RELAY_PORTS.map(p => `localhost:${p}`).join(', ')}`);
  console.log(`[Proxy] Redis: ${REDIS_URL}`);
  console.log(`[Proxy] Limits: ${MAX_CAMERAS} cameras / ${MAX_FRONTENDS} frontends per relay`);
  console.log('');
  console.log(`[Proxy] Camera endpoint:   ws://localhost:${PROXY_PORT}/ws/pi/<pi_id>`);
  console.log(`[Proxy] Frontend endpoint: ws://localhost:${PROXY_PORT}/ws/feed`);
  console.log(`[Proxy] Health:            http://localhost:${PROXY_PORT}/health`);
});
