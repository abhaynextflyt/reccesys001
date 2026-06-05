# Low-Level Design (LLD) Document
## USB Camera WebSocket Streamer — `usb_camera_uploader.py`

**Version**: 2.1.0-usb  
**Date**: June 2026  
**Author**: Engineering Team  
**Status**: Production Ready

---

## 1. Document Control

| Version | Date | Author | Changes |
|---------|------|--------|---------|
| V1.0 | 2026-06-04 | Updated | Migrated to USB/OpenCV, added LLD, health monitoring, systemd integration |


---

## 2. Scope & Purpose

### 2.1 Scope
This LLD defines the internal architecture, module decomposition, interface contracts, data structures, error handling strategies, and deployment configuration for the `usb_camera_uploader.py` service.

### 2.1 Purpose
- Enable multiple developers to understand, modify, and extend the codebase
- Define precise contracts between modules for unit testing
- Document the migration path from PiCamera2 (CSI) to USB (UVC) cameras
- Provide operational runbooks for DevOps deployment

### 2.3 Target Audience
- Software Engineers (implementation reference)
- QA Engineers (test case derivation)
- DevOps/SRE (deployment and monitoring)
- Technical Writers (API documentation)

---

## 3. High-Level Context

### 3.1 System Position

```
┌─────────────────────────────────────────────────────────────────────────┐
│                           SYSTEM CONTEXT                                │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                         │
│   ┌──────────────┐    USB    ┌──────────────────┐   WSS   ┌──────────┐  │
│   │  USB Webcam  │◄─────────►│ Edge Device (Pi) │◄───────►│  Ingest  │  │
│   │  (UVC Class) │  UVC/V4L2 │  usb_camera_     │  JSON+  │  Server  │  │
│   │              │           │  uploader.py     │  JPEG   │          │  │
│   └──────────────┘           │  [systemd svc]   │         └──────────┘  │
│                              └──────────────────┘                       │
│                                      │                                  │
│                                      │ Health Reports (every 60s)       │
│                                      │ {cpu_temp, wifi_signal, ...}     │
│                                      ▼                                  │
│                              ┌──────────────────┐                       │
│                              │  Monitoring/     │                       │
│                              │  Alerting Stack  │                       │
│                              └──────────────────┘                       │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
```

### 3.2 Design Goals

| Goal | Priority | How Achieved |
|------|----------|--------------|
| **Hardware Agnostic** | P0 | OpenCV VideoCapture abstracts camera hardware |
| **Always-On** | P0 | systemd service + infinite reconnect loop |
| **Low Latency** | P1 | MJPEG camera mode, buffer_size=1, async I/O |
| **Self-Healing** | P1 | Exponential backoff reconnect, health telemetry |
| **Resource Efficient** | P2 | Thread pool for blocking ops, PIL optimize |
| **Observable** | P2 | Structured logging, health reports, frame counters |

---

## 4. Module Decomposition

### 4.1 Module Hierarchy

```
usb_camera_uploader.py
│
├── MODULE 1: Configuration Layer (Global Constants)
│   ├── Environment Variable Loader
│   ├── Default Value Fallbacks
│   └── Type Coercion (str → int for USB_CAMERA_INDEX)
│
├── MODULE 2: Camera Interface (OpenCV Wrapper)
│   ├── init_camera()
│   │   ├── Backend Discovery (V4L2 → ANY)
│   │   ├── Property Configuration (res, fps, fourcc, buffer)
│   │   ├── Warm-up Sequence (discard N frames)
│   │   └── Capability Verification
│   └── capture_jpeg_frame()
│       ├── Async Thread Pool Wrapper
│       ├── BGR→RGB Conversion
│       └── PIL JPEG Encoding
│
├── MODULE 3: Network Transport (WebSocket Client)
│   └── upload_loop()
│       ├── Connection Manager (async context)
│       ├── Registration Handshake
│       ├── Frame Transmission Loop
│       ├── Health Report Scheduler (60s)
│       ├── Timing Controller (FPS maintenance)
│       └── Reconnection Handler (exponential backoff)
│
├── MODULE 4: System Telemetry (Linux-specific)
│   ├── get_cpu_temp()      → /sys/class/thermal
│   ├── get_cpu_percent()   → psutil
│   ├── get_memory_percent() → psutil
│   └── get_wifi_signal()   → iwconfig / proc/net/wireless
│
└── MODULE 5: Bootstrap & Orchestration
    ├── Logging Configuration
    ├── init_camera() Invocation
    └── asyncio.run(upload_loop())
```

### 4.2 Module Responsibility Matrix

| Module | Responsibility | Lines of Code | Complexity |
|--------|---------------|---------------|------------|
| Configuration | Centralize all tunables | ~15 | Low |
| Camera Interface | Abstract USB camera ops | ~80 | Medium |
| Network Transport | Maintain persistent WSS | ~120 | High |
| System Telemetry | Read Linux system stats | ~60 | Low |
| Bootstrap | Wire modules together | ~10 | Low |

---

## 5. Detailed Module Design

### 5.1 MODULE 1: Configuration Layer

#### 5.1.1 Interface Specification

```python
# No functions — pure constants and environment loading
```

#### 5.1.2 Configuration Schema

| Constant | Env Var | Type | Default | Validation |
|----------|---------|------|---------|------------|
| `PI_ID` | `PI_ID` | `str` | `"pi-default"` | Non-empty |
| `INGEST_URL` | `INGEST_URL` | `str` | `"wss://..."` | Valid URI scheme |
| `USB_CAMERA_INDEX` | `USB_CAMERA_INDEX` | `int` | `0` | `>= 0` |
| `RESOLUTION` | — | `tuple[int,int]` | `(1280, 720)` | Width multiple of 16 |
| `FPS` | — | `int` | `15` | `1 <= FPS <= 60` |
| `JPEG_QUALITY` | — | `int` | `85` | `1 <= q <= 100` |
| `CHUNK_SIZE` | — | `int` | `8192` | Power of 2 |
| `RECONNECT_BASE` | — | `int` | `1` | Seconds, `> 0` |
| `RECONNECT_MAX` | — | `int` | `60` | Seconds, `> base` |

#### 5.1.3 Global State Variables

```python
cap: cv2.VideoCapture | None = None          # Camera handle (None until init)
connection_attempts: int = 0                 # Consecutive failure counter
frames_sent: int = 0                         # Cumulative counter (lifetime)
bytes_sent: int = 0                          # Cumulative bytes (lifetime)
last_health_report: float = time.time()      # Unix timestamp of last health tx
```

**Thread Safety**: These globals are mutated only within the single `asyncio` event loop thread (except `cap` which is accessed from the thread pool). No locks required.

---

### 5.2 MODULE 2: Camera Interface

#### 5.2.1 Function: `init_camera()`

**Signature**: `def init_camera() -> cv2.VideoCapture`

**Preconditions**: USB camera connected and accessible (`/dev/videoN` exists).

**Postconditions**: Camera is open, configured, warmed up, and ready for `cap.read()`.

**Algorithm**:

```
1. FOR each (index, backend) in [(0, V4L2), (0, ANY)]:
       cap = cv2.VideoCapture(index, backend)
       IF cap.isOpened():
           LOG success
           BREAK
   ELSE:
       RAISE RuntimeError("Failed to open USB camera")

2. SET cap properties:
       CAP_PROP_FRAME_WIDTH  = 1280
       CAP_PROP_FRAME_HEIGHT = 720
       CAP_PROP_FPS          = 15
       CAP_PROP_BUFFERSIZE   = 1
       CAP_PROP_FOURCC       = MJPG

3. FOR i = 1 to 5:
       ret, _ = cap.read()
       IF NOT ret: LOG warning

4. READ actual properties back from driver
   LOG actual_width x actual_height @ actual_fps

5. RETURN cap
```

**Error Handling**:

| Error | Cause | Action |
|-------|-------|--------|
| `RuntimeError` | No backend opens camera | Crash to systemd; service restarts |
| Warm-up failure | Camera busy or unplugged | Log warning; continue (may recover) |
| Property mismatch | Driver ignores request | Log actual values; continue |

#### 5.2.2 Function: `capture_jpeg_frame()`

**Signature**: `async def capture_jpeg_frame() -> bytes`

**Preconditions**: `cap` is initialized and `cap.isOpened() == True`.

**Postconditions**: Returns JPEG byte string, `len(result) > 0`.

**Algorithm**:

```
1. loop = asyncio.get_event_loop()
2. ret, frame = await loop.run_in_executor(None, cap.read)
   // Offloads blocking V4L2 ioctl/read to default ThreadPoolExecutor

3. IF NOT ret OR frame is None:
       RAISE RuntimeError("Frame capture failed")

4. rgb_frame = cv2.cvtColor(frame, COLOR_BGR2RGB)
   // OpenCV native BGR → RGB for PIL compatibility
   // Cost: ~2ms for 1280x720 on ARM Cortex-A72

5. pil_image = Image.fromarray(rgb_frame)

6. buf = io.BytesIO()
   pil_image.save(buf, format="JPEG", quality=85, optimize=True, progressive=False)

7. RETURN buf.getvalue()
```

**Performance Budget**:

| Step | Time (Pi 4, 720p) | Time (x86, 720p) |
|------|-------------------|------------------|
| `cap.read()` | 5-15 ms | 3-8 ms |
| `cvtColor` | 2-4 ms | 1-2 ms |
| PIL encode | 10-25 ms | 5-12 ms |
| **Total** | **20-45 ms** | **10-25 ms** |
| Budget (15 FPS) | 66.6 ms | 66.6 ms |
| **Headroom** | **21-46 ms** | **41-56 ms** |

**Error Handling**:

| Error | Cause | Action |
|-------|-------|--------|
| `RuntimeError` | Camera unplugged / buffer underrun | Propagate to `upload_loop` → reconnect |

---

### 5.3 MODULE 3: Network Transport

#### 5.3.1 Function: `upload_loop()`

**Signature**: `async def upload_loop() -> None`  *(never returns)*

**Preconditions**: Camera initialized, network interface up (not required to have internet).

**Postconditions**: Infinite loop; only exits on unrecoverable error or SIGTERM.

**State Machine**:

```
                    ┌─────────────┐
         ┌─────────►│   START     │◄────────────────┐
         │          └──────┬──────┘                 │
         │                 │ init_camera()          │
         │                 ▼                        │
         │          ┌─────────────┐                 │
         │          │  CONNECTING │                 │
         │          │  (async)    │                 │
         │          └──────┬──────┘                 │
         │                 │ success                │
         │                 ▼                        │
         │          ┌─────────────┐    exception    │
         │    ┌────►│  STREAMING  │─────────────────┘
         │    │      │  (frame loop)│
         │    │      └──────┬──────┘
         │    │             │ health_timer >= 60s
         │    │             ▼
         │    │      ┌─────────────┐
         │    │      │  HEALTH_TX  │
         │    │      │  (send JSON)│
         │    │      └──────┬──────┘
         │    │             │
         │    └─────────────┘ (return to STREAMING)
         │
         │    exception / connection lost
         │
         ▼
   ┌─────────────┐
   │  BACKOFF    │─── sleep(delay) ───► CONNECTING
   │  (calculate │
   │   delay)    │
   └─────────────┘
```

**Reconnection Backoff Formula**:

```python
delay = min(RECONNECT_BASE * (2 ** connection_attempts), RECONNECT_MAX)
```

| Attempt | Delay (seconds) | Cumulative Wait |
|---------|----------------|-----------------|
| 1 | 2 | 2s |
| 2 | 4 | 6s |
| 3 | 8 | 14s |
| 4 | 16 | 30s |
| 5 | 32 | 62s |
| 6 | 60 | 122s |
| 7+ | 60 | ... |

**Frame Timing Algorithm**:

```python
frame_interval = 1.0 / FPS          # 0.0666... seconds
next_frame_time = time.time()       # Anchor to monotonic clock

while True:
    # ... capture and send ...

    next_frame_time += frame_interval
    sleep_time = next_frame_time - time.time()

    if sleep_time > 0:
        await asyncio.sleep(sleep_time)   # Precise wait
    else:
        # Behind schedule — frame processing exceeded budget
        logging.warning(f"Behind by {-sleep_time:.3f}s")
        next_frame_time = time.time()      # Reset anchor (catch-up mode)
```

**Why not `asyncio.sleep(1/FPS)`?**  
Because capture+encode+send time varies per frame. Fixed sleep causes drift. The anchor-based approach maintains long-term average FPS exactly at target.

#### 5.3.2 Wire Protocol Specification

**Connection URI**: `wss://{INGEST_URL}/ws/pi/{PI_ID}`

**Message Types**:

| Type | Direction | Trigger | Payload |
|------|-----------|---------|---------|
| `register` | Client → Server | On connect | Metadata JSON |
| `frame` | Client → Server | Every 1/FPS seconds | Header JSON + JPEG binary |
| `health` | Client → Server | Every 60 seconds | System stats JSON |
| `ping` | Bidirectional | Every 20 seconds | WebSocket native |
| `pong` | Bidirectional | Response to ping | WebSocket native |

**Frame Message Structure**:

```
0                   1                   2                   3
0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|                    JSON HEADER (UTF-8 text)                   |
|                                                               |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|       \n (0x0A)       |                                       |
+-+-+-+-+-+-+-+-+-+-+-+-|                                       +
|                    BINARY JPEG PAYLOAD                        |
|                                                               |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
```

**Header JSON Schema (Frame)**:

```json
{
  "type": "frame",
  "pi_id": "string",          // Max 64 chars, [a-z0-9-]
  "frame_num": "integer",     // Monotonic, 1-indexed, uint64
  "timestamp": "number",      // Unix epoch with microsecond precision
  "size": "integer",          // JPEG byte length, uint32
  "resolution": [1280, 720],  // [width, height]
  "fps_actual": 15            // Nominal FPS (may differ from actual)
}
```

**Registration JSON Schema**:

```json
{
  "type": "register",
  "pi_id": "string",
  "resolution": [1280, 720],
  "fps": 15,
  "jpeg_quality": 85,
  "timestamp": 1717500000.123,
  "firmware_version": "2.1.0-usb",
  "boot_time": 3600.5,
  "camera_type": "usb_opencv",
  "camera_index": 0
}
```

**Health JSON Schema**:

```json
{
  "type": "health",
  "pi_id": "string",
  "frames_sent": 54000,
  "bytes_sent": 2630000000,
  "cpu_temp": 42.3,
  "cpu_percent": 23.5,
  "memory_percent": 45.2,
  "wifi_signal": -42
}
```

#### 5.3.3 WebSocket Client Configuration

| Parameter | Value | Rationale |
|-----------|-------|-----------|
| `ping_interval` | 20s | Detect dead connections before TCP timeout |
| `ping_timeout` | 10s | Fast fail if server unresponsive |
| `close_timeout` | 5s | Don't block on graceful close |
| `compression` | `None` | JPEG is already compressed; CPU waste |
| `max_size` | `None` | Allow large frames (720p JPEG ~100KB) |
| `max_queue` | 128 | Backpressure: drop old frames if network lags |

---

### 5.4 MODULE 4: System Telemetry

#### 5.4.1 Interface Summary

| Function | Return Type | Source | Latency | Failure Value |
|----------|-------------|--------|---------|---------------|
| `get_cpu_temp()` | `float` | `/sys/class/thermal/thermal_zone0/temp` | <1ms | `0.0` |
| `get_cpu_percent()` | `float` | `psutil.cpu_percent(interval=1)` | 1000ms | N/A (blocks) |
| `get_memory_percent()` | `float` | `psutil.virtual_memory().percent` | <1ms | N/A |
| `get_wifi_signal()` | `int` | `iwconfig wlan0` or `/proc/net/wireless` | ~50ms | `-100` |

#### 5.4.2 `get_wifi_signal()` Parsing Logic

```
1. TRY iwconfig wlan0:
       EXECUTE: iwconfig wlan0
       PARSE: "Signal level=-42 dBm"  → return -42
       PARSE: "Signal level=42/100"  → return -100 (relative quality)
   EXCEPT: continue

2. TRY /proc/net/wireless:
       READ lines[2:] (skip header)
       FOR each line:
           SPLIT by whitespace
           IF len(parts) >= 4:
               RETURN int(parts[3].replace(".", ""))  // e.g., "-42." → -42
   EXCEPT: continue

3. RETURN -100  // Unknown
```

**Note**: `get_cpu_percent()` blocks for 1 second (psutil samples over interval). This is called from the main async loop during health reporting only (every 60s), so the 1s block is acceptable. Do NOT call this every frame.

---

### 5.5 MODULE 5: Bootstrap & Orchestration

#### 5.5.1 Execution Flow

```
main()
│
├── 1. logging.basicConfig()
│       └── level=INFO, format='%(asctime)s [%(levelname)s] %(message)s'
│
├── 2. init_camera()
│       └── May raise RuntimeError → systemd restart
│
└── 3. asyncio.run(upload_loop())
        └── Enters event loop
            ├── upload_loop() never returns (infinite while True)
            └── SIGTERM triggers graceful exit via asyncio cancellation
```

#### 5.5.2 Signal Handling

| Signal | Handler | Behavior |
|--------|---------|----------|
| `SIGTERM` | Python default + asyncio | Cancel `upload_loop`, close camera, exit 0 |
| `SIGINT` (Ctrl+C) | Python default | Same as SIGTERM |
| `SIGKILL` | Kernel | Hard kill; camera handle leaked (driver cleans up) |

**Graceful Shutdown Sequence**:

```python
# Implicit via asyncio.run() cancellation
try:
    await upload_loop()
except asyncio.CancelledError:
    if cap and cap.isOpened():
        cap.release()       # Release V4L2 device
    logging.info("[{PI_ID}] Graceful shutdown complete")
    raise
```

---

## 6. Data Flow Diagrams

### 6.1 Frame Capture & Encode Pipeline

```
┌─────────────┐    ioctl/read     ┌─────────────┐    numpy    ┌─────────────┐
│  USB Camera │──────────────────►│ OpenCV cap  │────────────►│  BGR Frame  │
│  (V4L2)     │   ~5-15ms         │  .read()    │   zero-copy  │  (720p)     │
└─────────────┘                   └─────────────┘              └──────┬──────┘
                                                                      │
                                                                      │ cv2.cvtColor
                                                                      │ COLOR_BGR2RGB
                                                                      │ ~2ms
                                                                      ▼
                                                               ┌─────────────┐
                                                               │  RGB Frame  │
                                                               │  (720p)     │
                                                               └──────┬──────┘
                                                                      │
                                                                      │ Image.fromarray
                                                                      │ ~1ms
                                                                      ▼
                                                               ┌─────────────┐
                                                               │  PIL Image  │
                                                               │  (RGB)      │
                                                               └──────┬──────┘
                                                                      │
                                                                      │ .save(buf, JPEG)
                                                                      │ quality=85
                                                                      │ optimize=True
                                                                      │ ~10-25ms
                                                                      ▼
                                                               ┌─────────────┐
                                                               │ JPEG Bytes  │
                                                               │ ~50-120KB   │
                                                               └─────────────┘
```

### 6.2 Network Transmission Sequence

```
Client (Edge Device)                           Server (Ingest)
────────────────────                           ───────────────
       │                                              │
       │────── WSS CONNECT /ws/pi/pi-0427 ───────────►│
       │                                              │
       │────── {"type":"register",...} ──────────────►│
       │◄───── 101 Switching Protocols ──────────────►│
       │                                              │
       │  [Every 66ms]                                │
       │────── {"type":"frame",...}\n[JPEG] ─────────►│
       │────── {"type":"frame",...}\n[JPEG] ─────────►│
       │────── {"type":"frame",...}\n[JPEG] ─────────►│
       │              ...                             │
       │                                              │
       │  [Every 20s]                                 │
       │────── WS PING ──────────────────────────────►│
       │◄───── WS PONG ──────────────────────────────►│
       │                                              │
       │  [Every 60s]                                 │
       │────── {"type":"health",...} ────────────────►│
       │                                              │
       │  [Connection Lost]                           │
       │  (TCP timeout / WiFi drop / server restart)  │
       │                                              │
       │  [Reconnect after backoff]                   │
       │────── WSS CONNECT ──────────────────────────►│
       │────── {"type":"register",...} ──────────────►│
       │              ...                             │
```

---

## 7. Error Handling & Recovery

### 7.1 Error Taxonomy

| Code | Category | Example | Severity | Recovery |
|------|----------|---------|----------|----------|
| E001 | Camera Hardware | USB unplugged | Critical | systemd restart |
| E002 | Camera Timeout | `cap.read()` returns False | Warning | Skip frame, continue |
| E003 | Network Connect | DNS failure, server down | Warning | Exponential backoff |
| E004 | Network Transmit | TCP reset mid-send | Warning | Exponential backoff |
| E005 | WebSocket Protocol | Invalid pong, ping timeout | Warning | Exponential backoff |
| E006 | System Telemetry | `/sys/class/thermal` missing | Info | Return 0.0 |
| E007 | System Telemetry | `iwconfig` not installed | Info | Fallback to `/proc/net/wireless` |

### 7.2 Recovery Matrix

| Scenario | Immediate Action | Delay | Next State |
|----------|-----------------|-------|------------|
| Camera open fails | Raise RuntimeError | 0 | systemd restart (5s) |
| Frame capture fails | Log warning, skip frame | 0 | Continue streaming |
| 3 consecutive frame fails | Treat as camera dead | 0 | systemd restart |
| WebSocket connect fails | Log error | Backoff delay | Reconnect attempt |
| WebSocket send fails | Close connection | Backoff delay | Reconnect attempt |
| Ping timeout | Close connection | Backoff delay | Reconnect attempt |
| Health report fails | Log warning | 0 | Continue streaming |

---

## 8. Performance & Resource Budget

### 8.1 CPU Budget (Raspberry Pi 4, 1.5 GHz)

| Task | CPU % | Notes |
|------|-------|-------|
| OpenCV capture | 5-10% | V4L2 in kernel space |
| BGR→RGB | 3-5% | NumPy vectorized |
| PIL JPEG encode | 15-25% | Quality=85, optimize |
| WebSocket send | 2-5% | asyncio, non-blocking |
| Health reporting | 1% | Every 60s, amortized |
| **Total** | **25-45%** | Leaves headroom for OS |

### 8.2 Memory Budget

| Allocation | Size | Lifetime |
|------------|------|----------|
| OpenCV frame buffer | 1280×720×3 = 2.7 MB | Per capture (freed by GC) |
| PIL image buffer | 1280×720×3 = 2.7 MB | Per encode (freed by GC) |
| JPEG output buffer | ~80 KB | Per send (freed by GC) |
| WebSocket queue (128 frames) | ~10 MB | Persistent |
| Python interpreter + imports | ~50 MB | Process lifetime |
| **Steady-state RSS** | **~70-100 MB** | — |

### 8.3 Network Budget

| Parameter | Value |
|-----------|-------|
| Frame size | 50-120 KB (scene dependent) |
| Frame rate | 15 FPS |
| Raw bitrate | 6-14.4 Mbps |
| With JPEG overhead + headers | 6.5-15 Mbps |
| Health report | ~200 bytes / 60s (negligible) |
| WebSocket overhead | ~2-6 bytes/frame (negligible) |
| **Recommended uplink** | **20 Mbps minimum** |

---

## 9. Testing Strategy

### 9.1 Unit Test Matrix

| Module | Test Case | Mock/Stub |
|--------|-----------|-----------|
| `init_camera` | Opens with V4L2 backend | Mock `cv2.VideoCapture` |
| `init_camera` | Falls back to ANY backend | Mock failing V4L2 |
| `init_camera` | Raises on total failure | Mock all backends fail |
| `capture_jpeg_frame` | Returns valid JPEG | Mock `cap.read()` with test image |
| `capture_jpeg_frame` | Raises on capture fail | Mock `cap.read()` returns `(False, None)` |
| `upload_loop` | Sends registration on connect | Mock `websockets.connect` |
| `upload_loop` | Sends frame with correct header | Mock WS, verify JSON schema |
| `upload_loop` | Backs off on disconnect | Mock WS raise Exception |
| `get_cpu_temp` | Parses millidegrees correctly | Mock `/sys/class/thermal` file |
| `get_wifi_signal` | Parses iwconfig output | Mock `subprocess.run` |
| `get_wifi_signal` | Fallback to proc/net/wireless | Mock missing iwconfig |

### 9.2 Integration Test Scenarios

| ID | Scenario | Expected Result |
|----|----------|-----------------|
| IT-01 | Start with camera connected | Registers within 5s, streams at 15 FPS |
| IT-02 | Unplug camera while streaming | Logs warning, continues attempting capture |
| IT-03 | Unplug WiFi while streaming | Detects ping timeout, reconnects with backoff |
| IT-04 | Server restart mid-stream | Detects close, reconnects, re-registers |
| IT-05 | 24-hour soak test | No memory leaks, stable FPS, health reports on time |
| IT-06 | Multiple cameras (indices 0,1) | Each instance connects with unique `PI_ID` |

### 9.3 Performance Tests

| ID | Metric | Target | Tool |
|----|--------|--------|------|
| PT-01 | End-to-end latency | < 150ms | Wireshark + server timestamp diff |
| PT-02 | CPU utilization | < 50% | `psutil` + `top` |
| PT-03 | Memory growth | 0 MB / hour | `memory_profiler` |
| PT-04 | Frame drop rate | < 1% | Compare `frame_num` vs expected |
| PT-05 | Reconnect time | < 5s (after backoff) | Server logs |

---

## 10. Deployment Configuration

### 10.1 systemd Service File

```ini
; /etc/systemd/system/usb-camera.service
[Unit]
Description=USB Camera WebSocket Streamer
Documentation=https://docs.nextflyt.aerospace/usb-camera
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=pi
Group=pi
WorkingDirectory=/opt/usb_camera

; Environment
Environment="PI_ID=pi-0427"
Environment="INGEST_URL=wss://ingest.nextflyt.aerospace"
Environment="USB_CAMERA_INDEX=0"
Environment="PYTHONUNBUFFERED=1"
Environment="PYTHONDONTWRITEBYTECODE=1"

; Execution
ExecStartPre=/bin/sh -c 'until ping -c1 ingest.nextflyt.aerospace; do sleep 1; done'
ExecStart=/usr/bin/python3 /opt/usb_camera/usb_camera_uploader.py
ExecStop=/bin/kill -TERM $MAINPID
TimeoutStopSec=10

; Restart policy
Restart=always
RestartSec=5
StartLimitInterval=60s
StartLimitBurst=3

; Resource limits
LimitNOFILE=65536
MemoryMax=256M
CPUQuota=80%

; Logging
StandardOutput=journal
StandardError=journal
SyslogIdentifier=usb-camera

[Install]
WantedBy=multi-user.target
```

### 10.2 Environment-Specific Overrides

```ini
; /etc/systemd/system/usb-camera.service.d/override.conf
[Service]
; For development server
Environment="INGEST_URL=wss://dev-ingest.nextflyt.aerospace"
Environment="PI_ID=dev-pi-01"

; For secondary camera
Environment="USB_CAMERA_INDEX=1"
Environment="PI_ID=pi-0427-cam2"
```

### 10.3 Log Rotation

```bash
# /etc/logrotate.d/usb-camera
/var/log/usb-camera/*.log {
    daily
    rotate 7
    compress
    delaycompress
    missingok
    notifempty
    create 0644 pi pi
}
```

---

## 11. Security Considerations

| Threat | Mitigation | Implementation |
|--------|-----------|----------------|
| Camera feed interception | WSS (TLS) | `wss://` URI scheme |
| Device impersonation | Unique `PI_ID` + server auth | Enforced by ingest server |
| Resource exhaustion | Memory/CPU limits | systemd `MemoryMax`, `CPUQuota` |
| Camera permission denial | `video` group membership | `sudo usermod -a -G video pi` |
| Log injection | Structured JSON logging | No user input in log format |
| Secrets in code | Environment variables only | No hardcoded URLs/IDs |

---

## 12. Migration Guide: PiCamera2 → USB Camera

### 12.1 Code Changes

| Aspect | PiCamera2 | USB/OpenCV | Rationale |
|--------|-----------|------------|-----------|
| Import | `from picamera2 import Picamera2` | `import cv2` | Cross-platform |
| Init | `Picamera2()` + `create_video_configuration()` | `cv2.VideoCapture(index, backend)` | Standard UVC |
| Capture | `capture_array("main")` | `cap.read()` | Blocking I/O |
| Color | RGB888 | BGR → `cvtColor` to RGB | OpenCV default |
| Encode | Hardware MJPEG | PIL `Image.save()` | Software fallback |
| Buffer | 6 hardware buffers | `CAP_PROP_BUFFERSIZE=1` | Latency vs stability |
| Backend | libcamera | V4L2 / DirectShow / MediaFoundation | OS-specific |

### 12.2 Performance Impact

| Metric | PiCamera2 | USB Camera | Delta |
|--------|-----------|------------|-------|
| Capture latency | 2-5 ms | 5-15 ms | +5-10 ms |
| Encode latency | 1-3 ms (HW) | 10-25 ms (SW) | +9-22 ms |
| Total per frame | 10-20 ms | 20-45 ms | +10-25 ms |
| CPU usage | 10-20% | 25-45% | +15-25% |
| Memory usage | 40-60 MB | 70-100 MB | +30-40 MB |

**Verdict**: USB camera adds ~15ms latency and ~20% CPU overhead but gains universal hardware compatibility.

---

## 13. Appendices

### Appendix A: OpenCV Backend Reference

| Platform | Backend Code | String | Use Case |
|----------|-------------|--------|----------|
| Linux | `cv2.CAP_V4L2` | `"V4L2"` | Native V4L2 (recommended) |
| Linux | `cv2.CAP_GSTREAMER` | `"GSTREAMER"` | Jetson, custom pipelines |
| Windows | `cv2.CAP_DSHOW` | `"DSHOW"` | DirectShow |
| Windows | `cv2.CAP_MSMF` | `"MSMF"` | Media Foundation |
| macOS | `cv2.CAP_AVFOUNDATION` | `"AVFOUNDATION"` | macOS native |
| Any | `cv2.CAP_ANY` | `"ANY"` | Auto-detect |

### Appendix B: V4L2 Pixel Formats

| FourCC | Format | Compression | Bandwidth | CPU Decode |
|--------|--------|-------------|-----------|------------|
| `MJPG` | Motion JPEG | Camera HW | Low | Required |
| `YUYV` | YUV 4:2:2 | None | High | Required |
| `H264` | H.264 stream | Camera HW | Very Low | Required |
| `RGB3` | RGB24 | None | Very High | None |

**Recommendation**: `MJPG` for USB 2.0 cameras, `YUYV` for USB 3.0 with CPU headroom.

### Appendix C: Troubleshooting Decision Tree

```
Service won't start?
├── Check camera detection:
│   └── ls /dev/video* → No output?
│       └── Camera not connected or driver missing
│           └── dmesg | grep -i usb → Look for UVC errors
│   └── v4l2-ctl --list-devices → Shows camera?
│       └── Yes → Check permissions (user in 'video' group?)
│       └── No → USB cable/port issue
│
High CPU usage?
├── Check pixel format:
│   └── v4l2-ctl -d /dev/video0 --get-fmt-video
│       └── Not MJPG? → Set CAP_PROP_FOURCC to MJPG
├── Check resolution:
│   └── Higher than 720p? → Reduce RESOLUTION
└── Check FPS:
    └── Higher than 15? → Reduce FPS

Frame drops?
├── Check network:
│   └── iwconfig → Signal < -70 dBm?
│       └── Move antenna/closer to AP
├── Check buffer:
│   └── CAP_PROP_BUFFERSIZE = 1?
│       └── Increase to 2-3 for unstable networks
└── Check CPU throttling:
    └── vcgencmd measure_temp > 80°C?
        └── Add heatsink/fan
```

---

## 14. Glossary

| Term | Definition |
|------|------------|
| **UVC** | USB Video Class — standard protocol for USB webcams |
| **V4L2** | Video4Linux 2 — Linux kernel video capture API |
| **MJPEG** | Motion JPEG — intra-frame compression format |
| **WSS** | WebSocket Secure — TLS-encrypted WebSocket protocol |
| **Backpressure** | Mechanism to prevent overwhelming slow consumers |
| **Exponential Backoff** | Retry delay that doubles after each failure |
| **Systemd** | Linux system and service manager |
| **Event Loop** | Asyncio's core — schedules and runs coroutines |
| **ThreadPoolExecutor** | Pool of threads for blocking I/O offload |

---

*End of Low-Level Design Document*
