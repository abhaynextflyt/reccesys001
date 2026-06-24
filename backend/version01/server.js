/**
 * Relay Server
 * Accepts WebSocket connections from Pi cameras and browser frontends.
 * Forwards JPEG frames and health telemetry from Pi to all frontend clients.
 *
 * Install dependency:  npm install ws
 * Run:                 node relay_server.js
 */

const WebSocket = require('ws');
const http = require('http');

const PORT = process.env.PORT || 8080;
const PI_PATH_PREFIX = process.env.PI_PATH_PREFIX || '/ws/pi/';
const FEED_PATH = process.env.FEED_PATH || '/ws/feed';

const pis = new Map();      // pi_id -> { ws, info, telemetry, connectedAt }
const frontends = new Set();

function broadcastToFrontends(msg) {
  const payload = JSON.stringify(msg);
  for (const ws of frontends) {
    if (ws.readyState === WebSocket.OPEN) {
      ws.send(payload);
    }
  }
}

function sendPiList(ws) {
  for (const [piId, pi] of pis) {
    if (ws.readyState === WebSocket.OPEN) {
      ws.send(JSON.stringify({
        type: 'pi_connected',
        pi_id: piId,
        info: pi.info,
        telemetry: pi.telemetry,
      }));
    }
  }
}

const server = http.createServer((req, res) => {
  if (req.url === '/health') {
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({
      status: 'ok',
      connected_pis: Array.from(pis.keys()),
      frontend_clients: frontends.size,
    }));
    return;
  }
  res.writeHead(404);
  res.end();
});

const wss = new WebSocket.Server({ server });

wss.on('connection', (ws, req) => {
  const url = req.url;

  // ---- Pi Camera Connection ----
  if (url.startsWith(PI_PATH_PREFIX)) {
    const piId = url.slice(PI_PATH_PREFIX.length).split('/')[0];
    if (!piId) {
      ws.close(1008, 'Missing PI_ID in URL');
      return;
    }

    let registered = false;

    ws.on('message', (data) => {
      if (!registered) {
        try {
          const text = Buffer.isBuffer(data) ? data.toString('utf-8') : data;
          const msg = JSON.parse(text);
          if (msg.type !== 'register') {
            ws.close(1008, 'Expected register message');
            return;
          }
          registered = true;
          pis.set(piId, { ws, info: msg, telemetry: null, connectedAt: Date.now() });
          console.log(`[Pi] Registered ${piId} (${msg.camera_type} ${msg.resolution?.join('x')})`);
          broadcastToFrontends({ type: 'pi_connected', pi_id: piId, info: msg });
        } catch (err) {
          console.warn(`[Pi] ${piId}: invalid registration`, err.message);
          ws.close(1008, 'Invalid registration');
        }
        return;
      }

      // After registration: data can be binary frame or JSON health report
      const buf = Buffer.isBuffer(data) ? data : Buffer.from(data);
      const newlineIdx = buf.indexOf('\n');

      // No newline = pure JSON message (health, etc.)
      if (newlineIdx === -1) {
        try {
          const msg = JSON.parse(buf.toString('utf-8'));
          
          if (msg.type === 'health') {
            // Store latest telemetry and broadcast to frontends
            const pi = pis.get(piId);
            if (pi) {
              pi.telemetry = {
                cpu_temp: msg.cpu_temp,
                cpu_percent: msg.cpu_percent,
                memory_percent: msg.memory_percent,
                wifi_signal: msg.wifi_signal,
                frames_sent: msg.frames_sent,
                bytes_sent: msg.bytes_sent,
                timestamp: msg.timestamp || Date.now() / 1000,
              };
              broadcastToFrontends({
                type: 'telemetry',
                pi_id: piId,
                telemetry: pi.telemetry,
              });
            }
          }
          // Silently ignore other JSON message types
        } catch (err) {
          console.warn(`[Pi] ${piId}: invalid JSON message`);
        }
        return;
      }

      // Binary frame: <header_json>\n<jpeg_bytes>
      const headerStr = buf.slice(0, newlineIdx).toString('utf-8');
      const jpegBytes = buf.slice(newlineIdx + 1);

      let header;
      try {
        header = JSON.parse(headerStr);
      } catch (err) {
        console.warn(`[Pi] ${piId}: invalid frame header`);
        return;
      }

      broadcastToFrontends({
        type: 'frame',
        pi_id: piId,
        frame_num: header.frame_num,
        timestamp: header.timestamp,
        size: header.size,
        resolution: header.resolution,
        fps_actual: header.fps_actual,
        image: jpegBytes.toString('base64'),
      });
    });

    ws.on('close', () => {
      if (pis.has(piId)) {
        console.log(`[Pi] Disconnected ${piId}`);
        pis.delete(piId);
        broadcastToFrontends({ type: 'pi_disconnected', pi_id: piId });
      }
    });

    ws.on('error', (err) => {
      console.error(`[Pi] ${piId} error:`, err.message);
    });

    return;
  }

  // ---- Frontend Connection ----
  if (url === FEED_PATH) {
    frontends.add(ws);
    console.log(`[Frontend] Connected (total: ${frontends.size})`);
    sendPiList(ws);

    ws.on('close', () => {
      frontends.delete(ws);
      console.log(`[Frontend] Disconnected (total: ${frontends.size})`);
    });

    ws.on('error', (err) => {
      console.error('[Frontend] error:', err.message);
    });

    return;
  }

  // Unknown path
  ws.close(1008, 'Invalid path');
});

server.listen(PORT, () => {
  console.log(`Relay server listening on port ${PORT}`);
  console.log(`  Pi endpoint:       ws://host:${PORT}${PI_PATH_PREFIX}<pi_id>`);
  console.log(`  Frontend endpoint: ws://host:${PORT}${FEED_PATH}`);
  console.log(`  Health check:      http://localhost:${PORT}/health`);
});