/**
 * Relay Server — with Redis capacity tracking + hard connection limits
 *
 * Install dependency:  npm install ws ioredis
 * Run:
 *   SERVER_ID=relay-1 PORT=8081 node server.js
 *   SERVER_ID=relay-2 PORT=8082 node server.js
 */
require('dotenv').config();
const WebSocket = require('ws');
const http      = require('http');
const Redis     = require('ioredis');

// ─── CONFIG ────────────────────────────────────────────────────────────────
const PORT          = parseInt(process.env.PORT         || '8081');
const SERVER_ID     = process.env.SERVER_ID             || `relay-${PORT}`;
const PI_PATH_PREFIX= process.env.PI_PATH_PREFIX        || '/ws/pi/';
const FEED_PATH     = process.env.FEED_PATH             || '/ws/feed';
const MAX_CAMERAS   = parseInt(process.env.MAX_CAMERAS  || '2');
const MAX_FRONTENDS = parseInt(process.env.MAX_FRONTENDS|| '2');
const REDIS_URL     = process.env.REDIS_URL             || 'redis://localhost:6379';

const HEARTBEAT_INTERVAL = 10_000;   // ms — how often to refresh Redis TTL
const CAPACITY_TTL       = 20;       // seconds — must be > HEARTBEAT_INTERVAL/1000

// ─── REDIS ─────────────────────────────────────────────────────────────────
const redis = new Redis(REDIS_URL);
redis.on('connect', () => console.log(`[${SERVER_ID}] Redis connected`));
redis.on('error',   (e) => console.error(`[${SERVER_ID}] Redis error:`, e.message));

const CAPACITY_KEY = `relay:${PORT}:capacity`;

async function publishCapacity() {
  try {
    await redis.hset(CAPACITY_KEY, {
      cameras:   pis.size,
      frontends: frontends.size,
      server_id: SERVER_ID,
      port:      PORT,
    });
    await redis.expire(CAPACITY_KEY, CAPACITY_TTL);
  } catch (err) {
    console.warn(`[${SERVER_ID}] Redis publish failed:`, err.message);
  }
}

// ─── IN-MEMORY STATE ───────────────────────────────────────────────────────
const pis       = new Map();   // pi_id → { ws, info, telemetry, connectedAt }
const frontends = new Set();

// ─── HELPERS ───────────────────────────────────────────────────────────────
function broadcastToFrontends(msg) {
  const payload = JSON.stringify(msg);
  for (const ws of frontends) {
    if (ws.readyState === WebSocket.OPEN) ws.send(payload);
  }
}

function sendPiList(ws) {
  for (const [piId, pi] of pis) {
    if (ws.readyState === WebSocket.OPEN) {
      ws.send(JSON.stringify({
        type:      'pi_connected',
        pi_id:     piId,
        info:      pi.info,
        telemetry: pi.telemetry,
      }));
    }
  }
}

// ─── HTTP SERVER ────────────────────────────────────────────────────────────
const server = http.createServer((req, res) => {
  if (req.url === '/health') {
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({
      status:           'ok',
      server_id:        SERVER_ID,
      port:             PORT,
      connected_pis:    Array.from(pis.keys()),
      frontend_clients: frontends.size,
      capacity: {
        cameras:   { used: pis.size,       max: MAX_CAMERAS },
        frontends: { used: frontends.size, max: MAX_FRONTENDS },
      },
    }));
    return;
  }
  res.writeHead(404);
  res.end();
});

// ─── WEBSOCKET SERVER ───────────────────────────────────────────────────────
const wss = new WebSocket.Server({ server });

wss.on('connection', (ws, req) => {
  const url = req.url;

  // ── Camera connection (/ws/pi/<pi_id>) ──────────────────────────────────
  if (url.startsWith(PI_PATH_PREFIX)) {
    const piId = url.slice(PI_PATH_PREFIX.length).split('/')[0];
    if (!piId) { ws.close(1008, 'Missing PI_ID in URL'); return; }

    // ── CAPACITY CHECK ──
    if (pis.size >= MAX_CAMERAS) {
      console.warn(`[${SERVER_ID}] Rejecting camera ${piId} — at capacity (${pis.size}/${MAX_CAMERAS})`);
      ws.close(1008, 'server_full');
      return;
    }

    let registered = false;

    ws.on('message', (data) => {
      if (!registered) {
        try {
          const text = Buffer.isBuffer(data) ? data.toString('utf-8') : data;
          const msg  = JSON.parse(text);
          if (msg.type !== 'register') { ws.close(1008, 'Expected register message'); return; }
          registered = true;
          pis.set(piId, { ws, info: msg, telemetry: null, connectedAt: Date.now() });
          console.log(`[${SERVER_ID}] Camera registered: ${piId} (${pis.size}/${MAX_CAMERAS})`);
          broadcastToFrontends({ type: 'pi_connected', pi_id: piId, info: msg });
          publishCapacity();
        } catch (err) {
          console.warn(`[${SERVER_ID}] ${piId}: invalid registration`, err.message);
          ws.close(1008, 'Invalid registration');
        }
        return;
      }

      const buf = Buffer.isBuffer(data) ? data : Buffer.from(data);
      const newlineIdx = buf.indexOf('\n');

      if (newlineIdx === -1) {
        try {
          const msg = JSON.parse(buf.toString('utf-8'));
          if (msg.type === 'health') {
            const pi = pis.get(piId);
            if (pi) {
              pi.telemetry = {
                cpu_temp:       msg.cpu_temp,
                cpu_percent:    msg.cpu_percent,
                memory_percent: msg.memory_percent,
                wifi_signal:    msg.wifi_signal,
                frames_sent:    msg.frames_sent,
                bytes_sent:     msg.bytes_sent,
                timestamp:      msg.timestamp || Date.now() / 1000,
              };
              broadcastToFrontends({ type: 'telemetry', pi_id: piId, telemetry: pi.telemetry });
            }
          }
        } catch { console.warn(`[${SERVER_ID}] ${piId}: invalid JSON message`); }
        return;
      }

      const headerStr = buf.slice(0, newlineIdx).toString('utf-8');
      const jpegBytes = buf.slice(newlineIdx + 1);
      let header;
      try { header = JSON.parse(headerStr); }
      catch { console.warn(`[${SERVER_ID}] ${piId}: invalid frame header`); return; }

      broadcastToFrontends({
        type:       'frame',
        pi_id:      piId,
        frame_num:  header.frame_num,
        timestamp:  header.timestamp,
        size:       header.size,
        resolution: header.resolution,
        fps_actual: header.fps_actual,
        image:      jpegBytes.toString('base64'),
      });
    });

    ws.on('close', () => {
      if (pis.has(piId)) {
        pis.delete(piId);
        console.log(`[${SERVER_ID}] Camera disconnected: ${piId} (${pis.size}/${MAX_CAMERAS})`);
        broadcastToFrontends({ type: 'pi_disconnected', pi_id: piId });
        publishCapacity();
      }
    });

    ws.on('error', (err) => console.error(`[${SERVER_ID}] Camera ${piId} error:`, err.message));
    return;
  }

  // ── Frontend connection (/ws/feed) ───────────────────────────────────────
  if (url === FEED_PATH) {
    if (frontends.size >= MAX_FRONTENDS) {
      console.warn(`[${SERVER_ID}] Rejecting frontend — at capacity (${frontends.size}/${MAX_FRONTENDS})`);
      ws.close(1008, 'server_full');
      return;
    }

    frontends.add(ws);
    console.log(`[${SERVER_ID}] Frontend connected (${frontends.size}/${MAX_FRONTENDS})`);
    sendPiList(ws);
    publishCapacity();

    ws.on('close', () => {
      frontends.delete(ws);
      console.log(`[${SERVER_ID}] Frontend disconnected (${frontends.size}/${MAX_FRONTENDS})`);
      publishCapacity();
    });

    ws.on('error', (err) => console.error(`[${SERVER_ID}] Frontend error:`, err.message));
    return;
  }

  ws.close(1008, 'Invalid path');
});

// ─── HEARTBEAT ──────────────────────────────────────────────────────────────
setInterval(publishCapacity, HEARTBEAT_INTERVAL);

// ─── START ──────────────────────────────────────────────────────────────────
server.listen(PORT, async () => {
  console.log(`[${SERVER_ID}] Relay server on port ${PORT}`);
  console.log(`[${SERVER_ID}] Limits: ${MAX_CAMERAS} cameras / ${MAX_FRONTENDS} frontends`);
  await publishCapacity();
});
