"""Local mock ingest server for ``usb_camera_uploader.py``.

Accepts WebSocket connections at ``ws://HOST:PORT/ws/pi/<pi_id>``, parses
the three message types defined in LLD §5.3.2 (``register``, ``frame``,
``health``), and logs basic throughput stats. Frames can optionally be
written to disk for visual verification.

Usage
-----
    # Terminal 1 — start the server
    python3 tests/mock_ingest_server.py

    # Terminal 2 — point the client at it
    export INGEST_URL="ws://localhost:8765"
    python3 usb_camera_uploader.py

Optional flags
--------------
    --host        Bind address (default: localhost)
    --port        Bind port    (default: 8765)
    --save-dir    If set, write each received JPEG to this directory
    --save-every  Save every Nth frame only (default: 30)
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import time
from pathlib import Path
from typing import Optional

import websockets
from websockets.server import WebSocketServerProtocol

logger = logging.getLogger("mock_ingest")


class _Stats:
    """Per-connection counters used for periodic logging."""

    def __init__(self, pi_id: str) -> None:
        self.pi_id = pi_id
        self.frames = 0
        self.bytes = 0
        self.start = time.time()
        self.last_log = self.start

    def record(self, payload_size: int) -> None:
        self.frames += 1
        self.bytes += payload_size

    def maybe_log(self, interval: float = 5.0) -> None:
        now = time.time()
        if now - self.last_log < interval:
            return
        elapsed = now - self.start
        fps = self.frames / elapsed if elapsed else 0.0
        mbps = (self.bytes * 8) / (elapsed * 1_000_000) if elapsed else 0.0
        logger.info(
            "[%s] %d frames | %.1f FPS | %.2f Mbps | %.1f MB total",
            self.pi_id,
            self.frames,
            fps,
            mbps,
            self.bytes / 1_000_000,
        )
        self.last_log = now


def _split_frame_message(data: bytes) -> tuple[dict, bytes]:
    """Split ``<JSON header>\\n<JPEG bytes>`` into (header, payload)."""
    newline = data.index(b"\n")
    header = json.loads(data[:newline])
    payload = data[newline + 1:]
    return header, payload


async def _handle(
    ws: WebSocketServerProtocol,
    save_dir: Optional[Path],
    save_every: int,
) -> None:
    """Per-connection coroutine."""
    # websockets >=13 exposes the URI on ws.request.path; older versions on ws.path.
    if hasattr(ws, "request") and ws.request is not None:
        path = ws.request.path
    else:
        path = getattr(ws, "path", "/")
    pi_id = path.rsplit("/", 1)[-1] or "unknown"
    logger.info("[%s] Connected from %s (path=%s)", pi_id, ws.remote_address, path)

    stats = _Stats(pi_id)

    try:
        async for message in ws:
            if isinstance(message, str):
                # Text frames are JSON-only: register or health.
                try:
                    obj = json.loads(message)
                except json.JSONDecodeError:
                    logger.warning("[%s] Bad JSON text frame: %r", pi_id, message[:80])
                    continue

                msg_type = obj.get("type")
                if msg_type == "register":
                    logger.info(
                        "[%s] REGISTER firmware=%s res=%s fps=%s camera=%s",
                        pi_id,
                        obj.get("firmware_version"),
                        obj.get("resolution"),
                        obj.get("fps"),
                        obj.get("camera_type"),
                    )
                elif msg_type == "health":
                    logger.info(
                        "[%s] HEALTH frames=%s bytes=%s cpu_temp=%s cpu%%=%s mem%%=%s wifi=%s",
                        pi_id,
                        obj.get("frames_sent"),
                        obj.get("bytes_sent"),
                        obj.get("cpu_temp"),
                        obj.get("cpu_percent"),
                        obj.get("memory_percent"),
                        obj.get("wifi_signal"),
                    )
                else:
                    logger.info("[%s] TEXT (%s): %s", pi_id, msg_type, message[:120])
                continue

            # Binary message: frame envelope.
            try:
                header, jpeg = _split_frame_message(message)
            except (ValueError, json.JSONDecodeError) as exc:
                logger.warning("[%s] Bad frame envelope: %s", pi_id, exc)
                continue

            stats.record(len(jpeg))
            stats.maybe_log()

            if save_dir and stats.frames % save_every == 0:
                fname = save_dir / f"{pi_id}_{header.get('frame_num', stats.frames):06d}.jpg"
                fname.write_bytes(jpeg)
                logger.debug("[%s] Saved %s (%d bytes)", pi_id, fname, len(jpeg))

    except websockets.ConnectionClosed as exc:
        logger.info(
            "[%s] Disconnected (code=%s reason=%r) after %d frames",
            pi_id,
            getattr(exc, "code", "?"),
            getattr(exc, "reason", ""),
            stats.frames,
        )
    except Exception as exc:  # noqa: BLE001
        logger.exception("[%s] Handler error: %s", pi_id, exc)


async def _serve(host: str, port: int, save_dir: Optional[Path], save_every: int) -> None:
    async def handler(ws):
        await _handle(ws, save_dir, save_every)

    async with websockets.serve(
        handler,
        host,
        port,
        max_size=None,           # Accept frames of any size.
        ping_interval=20,        # Match the client (LLD §5.3.3).
        ping_timeout=10,
        compression=None,
    ):
        logger.info("Mock ingest listening on ws://%s:%d/ws/pi/<pi_id>", host, port)
        if save_dir:
            logger.info("Saving every %dth frame to %s", save_every, save_dir)
        await asyncio.Future()  # run forever


def main() -> None:
    parser = argparse.ArgumentParser(description="Mock WSS ingest server.")
    parser.add_argument("--host", default="localhost")
    parser.add_argument("--port", type=int, default=8765)
    parser.add_argument(
        "--save-dir",
        type=Path,
        default=None,
        help="If set, save received JPEGs into this directory.",
    )
    parser.add_argument(
        "--save-every",
        type=int,
        default=30,
        help="Save every Nth frame only (default: 30).",
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )

    if args.save_dir:
        args.save_dir.mkdir(parents=True, exist_ok=True)

    try:
        asyncio.run(_serve(args.host, args.port, args.save_dir, args.save_every))
    except KeyboardInterrupt:
        logger.info("Mock ingest shutting down")


if __name__ == "__main__":
    main()
