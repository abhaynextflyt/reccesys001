| Issue                                           | Severity  | Why It Matters                                                                                                                  |
| ----------------------------------------------- | --------- | ------------------------------------------------------------------------------------------------------------------------------- |
| **No authentication on WSS**                    | 🔴 High   | Anyone can connect to your ingest endpoint. Needs TLS client certs or token auth.                                               |
| **No frame ACK / backpressure handling**        | 🔴 High   | Server could be overwhelmed; client blindly sends. `max_queue` helps but isn't a protocol-level ACK.                            |
| **`get_cpu_percent(interval=None)`**            | 🟡 Medium | Returns instantaneous (since last call), not true 1s average. The docstring says "blocks for ~1 second" but the code doesn't.   |
| **No frame dropping on lag**                    | 🟡 Medium | If `sleep_time < 0`, it logs a warning but doesn't skip frames to catch up—could accumulate delay.                              |
| **JPEG encoding in main thread**                | 🟡 Medium | `Image.fromarray()` + `save()` are CPU-bound and run *after* the executor returns. At 1280x720/15fps this could stall the loop. |
| **No structured logging (JSON)**                | 🟡 Medium | Plain text logs are hard to parse in production log aggregation.                                                                |
| **Missing tests**                               | 🟡 Medium | No unit tests for telemetry fallbacks, reconnect logic, or camera init failure paths.                                           |
| **`_encode_frame_message` uses `\n` delimiter** | 🟢 Low    | Binary JPEG could theoretically contain `\n` (0x0A), though extremely unlikely in valid JPEG. Length-prefixed framing is safer. |
