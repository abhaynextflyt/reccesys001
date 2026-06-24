'use client';

import { useEffect, useRef, useState, useCallback } from 'react';
import styles from './collectivefeed.module.css';

export default function MultiCameraPage() {
  const [feeds, setFeeds] = useState(new Map());
  const [connected, setConnected] = useState(false);
  const wsRef = useRef(null);
  const reconnectTimerRef = useRef(null);

  const connect = useCallback(() => {
    const wsUrl = process.env.NEXT_PUBLIC_WS_URL || 'ws://localhost:8080/ws/feed';
    const ws = new WebSocket(wsUrl);

    ws.onopen = () => {
      setConnected(true);
      console.log('[Feed] Connected');
    };

    ws.onmessage = (event) => {
      try {
        const msg = JSON.parse(event.data);

        if (msg.type === 'pi_connected') {
          setFeeds((prev) => {
            const next = new Map(prev);
            next.set(msg.pi_id, {
              info: msg.info,
              telemetry: msg.telemetry || null,
              image: null,
              frameNum: 0,
              resolution: msg.info?.resolution,
              fpsActual: msg.info?.fps,
              lastUpdate: null,
            });
            return next;
          });
        } else if (msg.type === 'pi_disconnected') {
          setFeeds((prev) => {
            const next = new Map(prev);
            next.delete(msg.pi_id);
            return next;
          });
        } else if (msg.type === 'frame') {
          setFeeds((prev) => {
            const next = new Map(prev);
            const existing = next.get(msg.pi_id) || {};
            next.set(msg.pi_id, {
              ...existing,
              image: msg.image,
              frameNum: msg.frame_num,
              resolution: msg.resolution,
              fpsActual: msg.fps_actual,
              lastUpdate: msg.timestamp,
            });
            return next;
          });
        } else if (msg.type === 'telemetry') {
          setFeeds((prev) => {
            const next = new Map(prev);
            const existing = next.get(msg.pi_id);
            if (existing) {
              next.set(msg.pi_id, {
                ...existing,
                telemetry: msg.telemetry,
              });
            }
            return next;
          });
        }
      } catch (err) {
        console.error('[Feed] Message parse error:', err);
      }
    };

    ws.onclose = () => {
      setConnected(false);
      wsRef.current = null;
      reconnectTimerRef.current = setTimeout(connect, 3000);
    };

    ws.onerror = (err) => {
      console.error('[Feed] WebSocket error:', err);
      ws.close();
    };

    wsRef.current = ws;
  }, []);

  useEffect(() => {
    connect();
    return () => {
      if (wsRef.current) {
        wsRef.current.close();
      }
      if (reconnectTimerRef.current) {
        clearTimeout(reconnectTimerRef.current);
      }
    };
  }, [connect]);

  const formatTime = (ts) => {
    if (!ts) return '-';
    const d = new Date(ts * 1000);
    return d.toLocaleTimeString();
  };

  const formatBytes = (bytes) => {
    if (bytes === undefined || bytes === null) return '-';
    if (bytes < 1024) return `${bytes} B`;
    if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KB`;
    return `${(bytes / (1024 * 1024)).toFixed(1)} MB`;
  };

  const formatTemp = (temp) => {
    if (temp === undefined || temp === null || temp === 0) return 'N/A';
    return `${temp.toFixed(1)}°C`;
  };

  const formatSignal = (sig) => {
    if (sig === undefined || sig === null || sig === -100) return 'N/A';
    return `${sig} dBm`;
  };

  const getSignalColor = (sig) => {
    if (sig === undefined || sig === null || sig === -100) return '#64748b';
    if (sig >= -50) return '#22c55e';
    if (sig >= -65) return '#eab308';
    return '#ef4444';
  };

  return (
    <div className={styles.container}>
      <header className={styles.header}>
        <h1 className={styles.title}>Multi-Camera Feed</h1>
        <div className={styles.status}>
          <span
            className={styles.statusDot}
            style={{ backgroundColor: connected ? '#22c55e' : '#ef4444' }}
          />
          <span className={styles.statusText}>
            {connected ? 'Connected' : 'Disconnected'}
          </span>
          <span className={styles.count}>
            {feeds.size} camera{feeds.size !== 1 ? 's' : ''}
          </span>
        </div>
      </header>

      {feeds.size === 0 ? (
        <div className={styles.empty}>
          <p>Waiting for camera streams...</p>
          <p className={styles.emptySub}>Start a Pi uploader to see the feed.</p>
        </div>
      ) : (
        <div className={styles.grid}>
          {Array.from(feeds.entries()).map(([piId, feed]) => (
            <div key={piId} className={styles.card}>
              <div className={styles.imageWrapper}>
                {feed.image ? (
                  <img
                    src={`data:image/jpeg;base64,${feed.image}`}
                    alt={`Camera ${piId}`}
                    className={styles.image}
                  />
                ) : (
                  <div className={styles.placeholder}>
                    <span>Connecting to {piId}...</span>
                  </div>
                )}
                <span className={styles.liveBadge}>LIVE</span>
              </div>

              <div className={styles.meta}>
                <div className={styles.metaHeader}>
                  <span className={styles.piId}>{piId}</span>
                  <span className={styles.badge}>
                    {feed.info?.camera_type || 'usb'}
                  </span>
                </div>

                {/* Telemetry Panel */}
                {feed.telemetry && (
                  <div className={styles.telemetry}>
                    <div className={styles.telemetryRow}>
                      <div className={styles.telemetryItem}>
                        <span className={styles.telemetryLabel}>CPU Temp</span>
                        <span className={styles.telemetryValue} style={{ color: feed.telemetry.cpu_temp > 70 ? '#ef4444' : '#22c55e' }}>
                          {formatTemp(feed.telemetry.cpu_temp)}
                        </span>
                      </div>
                      <div className={styles.telemetryItem}>
                        <span className={styles.telemetryLabel}>CPU Load</span>
                        <span className={styles.telemetryValue}>
                          {feed.telemetry.cpu_percent !== undefined && feed.telemetry.cpu_percent !== null
                            ? `${feed.telemetry.cpu_percent.toFixed(1)}%`
                            : 'N/A'}
                        </span>
                      </div>
                      <div className={styles.telemetryItem}>
                        <span className={styles.telemetryLabel}>Memory</span>
                        <span className={styles.telemetryValue}>
                          {feed.telemetry.memory_percent !== undefined && feed.telemetry.memory_percent !== null
                            ? `${feed.telemetry.memory_percent.toFixed(1)}%`
                            : 'N/A'}
                        </span>
                      </div>
                      <div className={styles.telemetryItem}>
                        <span className={styles.telemetryLabel}>Wi-Fi</span>
                        <span className={styles.telemetryValue} style={{ color: getSignalColor(feed.telemetry.wifi_signal) }}>
                          {formatSignal(feed.telemetry.wifi_signal)}
                        </span>
                      </div>
                    </div>
                  </div>
                )}

                <div className={styles.stats}>
                  <div className={styles.stat}>
                    <span className={styles.statLabel}>Frame</span>
                    <span className={styles.statValue}>
                      {feed.frameNum.toLocaleString()}
                    </span>
                  </div>
                  <div className={styles.stat}>
                    <span className={styles.statLabel}>FPS</span>
                    <span className={styles.statValue}>
                      {feed.fpsActual ?? '-'}
                    </span>
                  </div>
                  <div className={styles.stat}>
                    <span className={styles.statLabel}>Resolution</span>
                    <span className={styles.statValue}>
                      {(feed.resolution || []).join('×') || '-'}
                    </span>
                  </div>
                  <div className={styles.stat}>
                    <span className={styles.statLabel}>Quality</span>
                    <span className={styles.statValue}>
                      {feed.info?.jpeg_quality ?? '-'}
                    </span>
                  </div>
                  <div className={styles.stat}>
                    <span className={styles.statLabel}>Updated</span>
                    <span className={styles.statValue}>
                      {formatTime(feed.lastUpdate)}
                    </span>
                  </div>
                  <div className={styles.stat}>
                    <span className={styles.statLabel}>Firmware</span>
                    <span className={styles.statValue}>
                      {feed.info?.firmware_version ?? '-'}
                    </span>
                  </div>
                  {feed.telemetry && (
                    <>
                      <div className={styles.stat}>
                        <span className={styles.statLabel}>Frames Sent</span>
                        <span className={styles.statValue}>
                          {feed.telemetry.frames_sent?.toLocaleString() ?? '-'}
                        </span>
                      </div>
                      <div className={styles.stat}>
                        <span className={styles.statLabel}>Data Sent</span>
                        <span className={styles.statValue}>
                          {formatBytes(feed.telemetry.bytes_sent)}
                        </span>
                      </div>
                    </>
                  )}
                </div>
              </div>
            </div>
          ))}
        </div>
      )}
    </div>
  );
}