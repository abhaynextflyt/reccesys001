"""USB Camera WebSocket Streamer.

Implements Modules 1-4 from the Low-Level Design document v2.1.0-usb.

Module 1: Configuration Layer
    - Loads environment variables with sensible defaults.
    - Performs type coercion (e.g. str -> int for USB_CAMERA_INDEX).
    - Declares global state variables mutated by later modules.

Module 2: Camera Interface
    - init_camera(): opens the USB camera through OpenCV, configures
      resolution / FPS / FOURCC / buffer size, warms it up, and verifies
      the resulting capabilities.
    - capture_jpeg_frame(): asynchronously captures a frame, converts it
      from BGR to RGB, and encodes it as a JPEG using Pillow.

Module 3: Network Transport
    - upload_loop(): persistent WSS client with registration handshake,
      anchor-based FPS timing, 60s health reports, and exponential-backoff
      reconnect.

Module 4: System Telemetry
    - get_cpu_temp(), get_cpu_percent(), get_memory_percent(),
      get_wifi_signal(): Linux-specific stats with safe fallbacks.

Module 5: Bootstrap & Orchestration
    - main(): configures logging, initializes the camera, and drives
      ``upload_loop()`` via ``asyncio.run``. Releases the camera handle
      on graceful shutdown (SIGTERM / SIGINT).
"""

from __future__ import annotations

import asyncio
import io
import json
import logging
import os
import re
import subprocess
import time
from typing import Optional

import cv2
from PIL import Image

try:
    import psutil
except ImportError:  # psutil is optional; telemetry degrades gracefully.
    psutil = None  # type: ignore[assignment]

try:
    import websockets
    from websockets.exceptions import ConnectionClosed, WebSocketException
except ImportError:  # Allow import of this module even without websockets.
    websockets = None  # type: ignore[assignment]
    ConnectionClosed = WebSocketException = Exception  # type: ignore[assignment,misc]

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# MODULE 1: Configuration Layer
# ---------------------------------------------------------------------------
#
# All tunables live here as module-level constants. Values that may change
# per-deployment are sourced from environment variables; the rest are fixed
# defaults documented in the LLD configuration schema (section 5.1.2).

# --- Environment-driven configuration --------------------------------------
PI_ID: str = os.environ.get("PI_ID", "pi-default")
INGEST_URL: str = os.environ.get(
    "INGEST_URL",
    "wss://ingest.example.com/stream",
)

# USB_CAMERA_INDEX arrives from the environment as a string; coerce to int.
try:
    USB_CAMERA_INDEX: int = int(os.environ.get("USB_CAMERA_INDEX", "0"))
except ValueError:
    logger.warning(
        "Invalid USB_CAMERA_INDEX=%r, falling back to 0",
        os.environ.get("USB_CAMERA_INDEX"),
    )
    USB_CAMERA_INDEX = 0

# --- Static configuration --------------------------------------------------
RESOLUTION: tuple[int, int] = (1280, 720)   # (width, height); width % 16 == 0
FPS: int = 15                                # 1 <= FPS <= 60
JPEG_QUALITY: int = 85                       # 1 <= q <= 100
CHUNK_SIZE: int = 8192                       # WSS send chunk; power of 2
RECONNECT_BASE: int = 1                      # seconds, > 0
RECONNECT_MAX: int = 60                      # seconds, > RECONNECT_BASE

# Firmware / protocol identifiers (used in registration + frame headers).
FIRMWARE_VERSION: str = "2.1.0-usb"
CAMERA_TYPE: str = "usb_opencv"

# Health reports are emitted at most once per HEALTH_INTERVAL seconds.
HEALTH_INTERVAL: float = 60.0

# Process start time, used to derive boot_time (uptime) in registration.
_PROCESS_START: float = time.time()

# Camera warm-up: number of frames to discard after opening the device.
_WARMUP_FRAMES: int = 5

# --- Global state (mutated by later modules) -------------------------------
#
# Thread safety: these globals are mutated only on the single asyncio event
# loop thread, except `cap` which is also read from the default thread pool
# executor inside capture_jpeg_frame(). Because cv2.VideoCapture.read() is
# only invoked from the executor while the event loop awaits its result, the
# two contexts never touch `cap` concurrently and no locking is required.
cap: Optional[cv2.VideoCapture] = None
connection_attempts: int = 0
frames_sent: int = 0
bytes_sent: int = 0
last_health_report: float = time.time()


# ---------------------------------------------------------------------------
# MODULE 2: Camera Interface
# ---------------------------------------------------------------------------

# Ordered list of (index, backend) pairs to attempt when opening the camera.
# V4L2 is preferred on Linux; CAP_ANY lets OpenCV pick whatever it can find
# (useful for non-Linux dev environments).
_CAMERA_BACKENDS: tuple[tuple[int, int], ...] = (
    (USB_CAMERA_INDEX, cv2.CAP_V4L2),
    (USB_CAMERA_INDEX, cv2.CAP_ANY),
)


def init_camera() -> cv2.VideoCapture:
    """Open and configure the USB camera.

    Returns:
        An opened, warmed-up ``cv2.VideoCapture`` instance ready for
        ``cap.read()``. The module-level ``cap`` global is also assigned
        so ``capture_jpeg_frame()`` can use it.

    Raises:
        RuntimeError: if no backend can open the camera. The process is
            expected to crash so systemd restarts the service.
    """
    global cap

    # 1. Backend discovery -------------------------------------------------
    opened: Optional[cv2.VideoCapture] = None
    for index, backend in _CAMERA_BACKENDS:
        candidate = cv2.VideoCapture(index, backend)
        if candidate.isOpened():
            logger.info(
                "Opened USB camera index=%d backend=%s",
                index,
                _backend_name(backend),
            )
            opened = candidate
            break
        candidate.release()
        logger.debug(
            "Failed to open camera index=%d backend=%s",
            index,
            _backend_name(backend),
        )

    if opened is None:
        # LLD §7.1 E001 — camera hardware failure, critical.
        raise RuntimeError(
            f"[E001] Failed to open USB camera at index {USB_CAMERA_INDEX} "
            "with any known backend (V4L2, ANY)"
        )

    # 2. Property configuration -------------------------------------------
    width, height = RESOLUTION
    opened.set(cv2.CAP_PROP_FRAME_WIDTH, width)
    opened.set(cv2.CAP_PROP_FRAME_HEIGHT, height)
    opened.set(cv2.CAP_PROP_FPS, FPS)
    opened.set(cv2.CAP_PROP_BUFFERSIZE, 1)
    opened.set(cv2.CAP_PROP_FOURCC, cv2.VideoWriter_fourcc(*"MJPG"))

    # 3. Warm-up: discard the first few frames. Cameras often emit black
    # or partially-exposed frames immediately after opening.
    for i in range(1, _WARMUP_FRAMES + 1):
        ret, _ = opened.read()
        if not ret:
            logger.warning("Warm-up frame %d/%d failed", i, _WARMUP_FRAMES)

    # 4. Capability verification: log what the driver actually gave us.
    actual_width = int(opened.get(cv2.CAP_PROP_FRAME_WIDTH))
    actual_height = int(opened.get(cv2.CAP_PROP_FRAME_HEIGHT))
    actual_fps = opened.get(cv2.CAP_PROP_FPS)
    logger.info(
        "Camera ready: %dx%d @ %.1f FPS (requested %dx%d @ %d FPS)",
        actual_width,
        actual_height,
        actual_fps,
        width,
        height,
        FPS,
    )

    cap = opened
    return opened


async def capture_jpeg_frame() -> bytes:
    """Capture a single frame and return it as JPEG-encoded bytes.

    The blocking ``cap.read()`` call is offloaded to the default
    ``ThreadPoolExecutor`` so the asyncio event loop remains responsive.

    Returns:
        Non-empty JPEG byte string.

    Raises:
        RuntimeError: if the camera is not initialized or the read fails.
    """
    if cap is None or not cap.isOpened():
        raise RuntimeError("Camera is not initialized; call init_camera() first")

    # 1-2. Offload the blocking V4L2 ioctl/read to a worker thread.
    loop = asyncio.get_event_loop()
    ret, frame = await loop.run_in_executor(None, cap.read)

    # 3. Validate.
    if not ret or frame is None:
        # LLD §7.1 E002 — capture timeout / underrun; caller decides recovery.
        raise RuntimeError("[E002] Frame capture failed")

    # 4. OpenCV decodes as BGR; PIL expects RGB.
    rgb_frame = cv2.cvtColor(frame, cv2.COLOR_BGR2RGB)

    # 5-6. Encode as JPEG via Pillow with the configured quality.
    pil_image = Image.fromarray(rgb_frame)
    buf = io.BytesIO()
    pil_image.save(
        buf,
        format="JPEG",
        quality=JPEG_QUALITY,
        optimize=True,
        progressive=False,
    )

    return buf.getvalue()


def _backend_name(backend: int) -> str:
    """Return a human-readable name for an OpenCV capture backend constant."""
    mapping = {
        cv2.CAP_V4L2: "V4L2",
        cv2.CAP_ANY: "ANY",
    }
    return mapping.get(backend, f"backend#{backend}")


# ---------------------------------------------------------------------------
# MODULE 4: System Telemetry
# ---------------------------------------------------------------------------
#
# All telemetry helpers must be best-effort: a missing sensor or a denied
# permission must never crash the upload loop. On failure they return None
# (or a sentinel) and log at debug level.

_THERMAL_PATH = "/sys/class/thermal/thermal_zone0/temp"
_PROC_WIRELESS = "/proc/net/wireless"


# Sentinel returned by Wi-Fi probe when the signal is unknown
# (LLD §5.4.1 specifies -100 as the failure value).
_WIFI_UNKNOWN: int = -100


def get_cpu_temp() -> float:
    """Return the CPU temperature in °C; ``0.0`` if unavailable (LLD §5.4.1)."""
    try:
        with open(_THERMAL_PATH, "r", encoding="ascii") as fh:
            raw = fh.read().strip()
        # /sys exposes millidegrees Celsius as an integer string.
        return int(raw) / 1000.0
    except (OSError, ValueError) as exc:
        logger.debug("get_cpu_temp() failed: %s", exc)
        return 0.0


def get_cpu_percent() -> float:
    """Return system-wide CPU utilization percentage.

    Blocks for ~1 second (``psutil.cpu_percent(interval=1)``); only safe to
    call from the health-report path (every 60s), never per frame.
    Returns ``0.0`` if psutil is unavailable or the probe fails.
    """
    if psutil is None:
        return 0.0
    try:
        return float(psutil.cpu_percent(interval=1))
    except Exception as exc:  # noqa: BLE001 - psutil raises diverse errors
        logger.debug("get_cpu_percent() failed: %s", exc)
        return 0.0


def get_memory_percent() -> float:
    """Return system memory utilization percentage; ``0.0`` on failure."""
    if psutil is None:
        return 0.0
    try:
        return float(psutil.virtual_memory().percent)
    except Exception as exc:  # noqa: BLE001
        logger.debug("get_memory_percent() failed: %s", exc)
        return 0.0


def get_wifi_signal() -> int:
    """Return Wi-Fi signal strength in dBm; ``-100`` if unknown (LLD §5.4.2).

    Tries ``iwconfig`` first (gives dBm directly); falls back to
    ``/proc/net/wireless``. If iwconfig reports a relative quality value
    (``Signal level=42/100``) we cannot convert it to dBm, so we return the
    ``-100`` unknown sentinel.
    """
    # --- Attempt 1: iwconfig --------------------------------------------
    try:
        proc = subprocess.run(
            ["iwconfig"],
            capture_output=True,
            text=True,
            timeout=2,
            check=False,
        )
        # Prefer absolute dBm reading.
        match = re.search(r"Signal level=(-?\d+)\s*dBm", proc.stdout)
        if match:
            return int(match.group(1))
        # Relative quality form: cannot map deterministically to dBm.
        if re.search(r"Signal level=\d+/\d+", proc.stdout):
            return _WIFI_UNKNOWN
    except (FileNotFoundError, subprocess.TimeoutExpired, OSError) as exc:
        logger.debug("iwconfig probe failed: %s", exc)

    # --- Attempt 2: /proc/net/wireless ----------------------------------
    try:
        with open(_PROC_WIRELESS, "r", encoding="ascii") as fh:
            lines = fh.readlines()
        # Skip the two header lines; the first data line is our primary iface.
        for line in lines[2:]:
            parts = line.split()
            if len(parts) >= 4:
                # parts[3] is the signal level (e.g. "-42."); strip the dot.
                return int(float(parts[3].rstrip(".")))
    except (OSError, ValueError, IndexError) as exc:
        logger.debug("/proc/net/wireless probe failed: %s", exc)

    return _WIFI_UNKNOWN


# ---------------------------------------------------------------------------
# MODULE 3: Network Transport
# ---------------------------------------------------------------------------

# WebSocket client tuning (LLD section 5.3.3).
_WS_PING_INTERVAL: float = 20.0
_WS_PING_TIMEOUT: float = 10.0
_WS_CLOSE_TIMEOUT: float = 5.0
_WS_MAX_QUEUE: int = 128

# LLD §7.2: after this many consecutive cap.read() failures, treat the
# camera as dead and crash so systemd restarts the unit.
_MAX_CONSECUTIVE_CAPTURE_FAILURES: int = 3


def _build_ws_uri() -> str:
    """Construct the per-device WebSocket URI: ``{INGEST_URL}/ws/pi/{PI_ID}``."""
    return f"{INGEST_URL.rstrip('/')}/ws/pi/{PI_ID}"


def _build_registration() -> dict:
    """Build the registration handshake payload (LLD §5.3.2)."""
    return {
        "type": "register",
        "pi_id": PI_ID,
        "resolution": list(RESOLUTION),
        "fps": FPS,
        "jpeg_quality": JPEG_QUALITY,
        "timestamp": time.time(),
        "firmware_version": FIRMWARE_VERSION,
        "boot_time": time.time() - _PROCESS_START,
        "camera_type": CAMERA_TYPE,
        "camera_index": USB_CAMERA_INDEX,
    }


def _build_frame_header(frame_num: int, size: int) -> dict:
    """Build the per-frame JSON header (LLD §5.3.2)."""
    return {
        "type": "frame",
        "pi_id": PI_ID,
        "frame_num": frame_num,
        "timestamp": time.time(),
        "size": size,
        "resolution": list(RESOLUTION),
        "fps_actual": FPS,
    }


def _build_health_report() -> dict:
    """Build a health/status snapshot (LLD §5.3.2)."""
    return {
        "type": "health",
        "pi_id": PI_ID,
        "frames_sent": frames_sent,
        "bytes_sent": bytes_sent,
        "cpu_temp": get_cpu_temp(),
        "cpu_percent": get_cpu_percent(),
        "memory_percent": get_memory_percent(),
        "wifi_signal": get_wifi_signal(),
    }


def _encode_frame_message(header: dict, jpeg: bytes) -> bytes:
    """Serialize a frame as ``<JSON header>\\n<binary JPEG>``."""
    return json.dumps(header, separators=(",", ":")).encode("utf-8") + b"\n" + jpeg


def _backoff_delay(attempt: int) -> float:
    """Exponential backoff with a ceiling (LLD §5.3.1)."""
    return float(min(RECONNECT_BASE * (2 ** attempt), RECONNECT_MAX))


async def upload_loop() -> None:
    """Persistent WebSocket upload loop.

    Never returns under normal operation: any exception inside the streaming
    section triggers a reconnect with exponential backoff. The process is
    expected to be supervised by systemd, which handles SIGTERM and restart.
    """
    global connection_attempts, frames_sent, bytes_sent, last_health_report

    if websockets is None:
        raise RuntimeError(
            "websockets package is required for upload_loop(); "
            "install with: pip install websockets"
        )

    uri = _build_ws_uri()
    frame_interval = 1.0 / FPS

    while True:
        try:
            logger.info("Connecting to %s (attempt %d)", uri, connection_attempts + 1)
            async with websockets.connect(
                uri,
                ping_interval=_WS_PING_INTERVAL,
                ping_timeout=_WS_PING_TIMEOUT,
                close_timeout=_WS_CLOSE_TIMEOUT,
                compression=None,
                max_size=None,
                max_queue=_WS_MAX_QUEUE,
            ) as ws:
                # --- Registration handshake -----------------------------
                await ws.send(json.dumps(_build_registration()))
                logger.info("Registered as %s", PI_ID)
                connection_attempts = 0  # reset on successful connect

                # --- Frame streaming loop -------------------------------
                next_frame_time = time.time()
                last_health_report = time.time()
                consecutive_capture_failures = 0

                while True:
                    try:
                        jpeg = await capture_jpeg_frame()
                        consecutive_capture_failures = 0
                    except RuntimeError as exc:
                        # LLD §7.2: skip frame and continue; bail after 3.
                        consecutive_capture_failures += 1
                        logger.warning(
                            "[E002] Capture failed (%d/%d): %s",
                            consecutive_capture_failures,
                            _MAX_CONSECUTIVE_CAPTURE_FAILURES,
                            exc,
                        )
                        if (
                            consecutive_capture_failures
                            >= _MAX_CONSECUTIVE_CAPTURE_FAILURES
                        ):
                            raise RuntimeError(
                                "[E001] Camera presumed dead after "
                                f"{consecutive_capture_failures} consecutive "
                                "capture failures"
                            ) from exc
                        continue

                    frames_sent += 1

                    header = _build_frame_header(frames_sent, len(jpeg))
                    message = _encode_frame_message(header, jpeg)
                    await ws.send(message)
                    bytes_sent += len(message)

                    # --- Health report (every HEALTH_INTERVAL seconds) --
                    now = time.time()
                    if now - last_health_report >= HEALTH_INTERVAL:
                        try:
                            await ws.send(json.dumps(_build_health_report()))
                            last_health_report = now
                            logger.debug(
                                "Health report sent (frames=%d, bytes=%d)",
                                frames_sent,
                                bytes_sent,
                            )
                        except Exception as exc:  # noqa: BLE001
                            # LLD §7.2: health report failure must not stop streaming.
                            logger.warning("Health report failed: %s", exc)
                            last_health_report = now

                    # --- Anchor-based FPS pacing ------------------------
                    next_frame_time += frame_interval
                    sleep_time = next_frame_time - time.time()
                    if sleep_time > 0:
                        await asyncio.sleep(sleep_time)
                    else:
                        logger.warning("Behind by %.3fs", -sleep_time)
                        next_frame_time = time.time()  # catch-up mode

        except (ConnectionClosed, WebSocketException) as exc:
            # LLD §7.1 E003/E004/E005 — network/protocol failures.
            connection_attempts += 1
            delay = _backoff_delay(connection_attempts)
            logger.error(
                "[E004] Connection lost (%s): %s. Reconnecting in %.1fs",
                type(exc).__name__,
                exc,
                delay,
            )
            await asyncio.sleep(delay)
        except OSError as exc:
            # LLD §7.1 E003 — DNS / TCP connect failures surface as OSError.
            connection_attempts += 1
            delay = _backoff_delay(connection_attempts)
            logger.error(
                "[E003] Network error (%s): %s. Reconnecting in %.1fs",
                type(exc).__name__,
                exc,
                delay,
            )
            await asyncio.sleep(delay)


# ---------------------------------------------------------------------------
# MODULE 5: Bootstrap & Orchestration
# ---------------------------------------------------------------------------

_LOG_FORMAT: str = "%(asctime)s [%(levelname)s] %(message)s"


def _release_camera() -> None:
    """Release the V4L2 device handle if it is still open."""
    global cap
    if cap is not None and cap.isOpened():
        try:
            cap.release()
            logger.info("[%s] Camera released", PI_ID)
        except Exception as exc:  # noqa: BLE001 - cleanup must never raise
            logger.warning("Camera release failed: %s", exc)
    cap = None


def main() -> None:
    """Process entry point (LLD §5.5).

    1. Configure logging.
    2. Initialize the camera (RuntimeError here crashes the process so
       systemd restarts the unit).
    3. Run the upload loop until SIGTERM / SIGINT, then release the camera.
    """
    logging.basicConfig(level=logging.INFO, format=_LOG_FORMAT)
    logger.info("[%s] Starting usb_camera_uploader %s", PI_ID, FIRMWARE_VERSION)

    init_camera()

    try:
        asyncio.run(upload_loop())
    except (KeyboardInterrupt, asyncio.CancelledError):
        # SIGINT (Ctrl+C) or asyncio cancellation triggered by SIGTERM.
        logger.info("[%s] Shutdown signal received", PI_ID)
    finally:
        _release_camera()
        logger.info("[%s] Graceful shutdown complete", PI_ID)


if __name__ == "__main__":
    main()
