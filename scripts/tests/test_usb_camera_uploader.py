"""Unit tests for ``usb_camera_uploader`` (LLD §9.1 matrix).

These tests mock every external dependency (OpenCV, websockets, psutil,
/sys, /proc, subprocess) so the suite is hermetic and runs anywhere with
plain ``pytest``.
"""

from __future__ import annotations

import asyncio
import io
import json
import sys
import types
from unittest import mock

import numpy as np
import pytest

# ---------------------------------------------------------------------------
# Lightweight stand-in for cv2 so the module can be imported without OpenCV
# installed on the test runner. The real cv2 is installed in production.
# ---------------------------------------------------------------------------
if "cv2" not in sys.modules:
    cv2_stub = types.ModuleType("cv2")
    cv2_stub.CAP_V4L2 = 200
    cv2_stub.CAP_ANY = 0
    cv2_stub.CAP_PROP_FRAME_WIDTH = 3
    cv2_stub.CAP_PROP_FRAME_HEIGHT = 4
    cv2_stub.CAP_PROP_FPS = 5
    cv2_stub.CAP_PROP_BUFFERSIZE = 38
    cv2_stub.CAP_PROP_FOURCC = 6
    cv2_stub.COLOR_BGR2RGB = 4

    def _fourcc(*chars):
        return sum((ord(c) << (8 * i)) for i, c in enumerate(chars))

    cv2_stub.VideoWriter_fourcc = _fourcc

    class _StubCapture:
        def __init__(self, *_a, **_kw):
            self._opened = True

        def isOpened(self):
            return self._opened

        def set(self, *_a, **_kw):
            return True

        def get(self, prop):
            return {3: 1280, 4: 720, 5: 15.0}.get(prop, 0.0)

        def read(self):
            frame = np.zeros((720, 1280, 3), dtype=np.uint8)
            return True, frame

        def release(self):
            self._opened = False

    cv2_stub.VideoCapture = _StubCapture

    def _cvt_color(frame, _code):
        # The stub doesn't reorder channels; it's only used for shape/dtype.
        return frame

    cv2_stub.cvtColor = _cvt_color
    sys.modules["cv2"] = cv2_stub

import usb_camera_uploader as ucu  # noqa: E402  (import after cv2 stub)


# ---------------------------------------------------------------------------
# Module 2: init_camera
# ---------------------------------------------------------------------------


class _MockCap:
    """Configurable mock for cv2.VideoCapture."""

    def __init__(self, opened: bool = True, read_ok: bool = True):
        self._opened = opened
        self._read_ok = read_ok
        self.props: dict = {}
        self.released = False

    def isOpened(self):
        return self._opened

    def set(self, prop, value):
        self.props[prop] = value
        return True

    def get(self, prop):
        return {
            ucu.cv2.CAP_PROP_FRAME_WIDTH: 1280,
            ucu.cv2.CAP_PROP_FRAME_HEIGHT: 720,
            ucu.cv2.CAP_PROP_FPS: 15.0,
        }.get(prop, 0.0)

    def read(self):
        if not self._read_ok:
            return False, None
        return True, np.zeros((720, 1280, 3), dtype=np.uint8)

    def release(self):
        self.released = True
        self._opened = False


def test_init_camera_opens_with_v4l2(monkeypatch):
    """§9.1: init_camera opens with V4L2 backend on first try."""
    cap = _MockCap(opened=True)
    factory = mock.Mock(return_value=cap)
    monkeypatch.setattr(ucu.cv2, "VideoCapture", factory)

    result = ucu.init_camera()

    assert result is cap
    # First call should use V4L2 backend.
    first_args = factory.call_args_list[0].args
    assert first_args[1] == ucu.cv2.CAP_V4L2
    # Properties were configured per LLD.
    assert cap.props[ucu.cv2.CAP_PROP_FRAME_WIDTH] == 1280
    assert cap.props[ucu.cv2.CAP_PROP_FRAME_HEIGHT] == 720
    assert cap.props[ucu.cv2.CAP_PROP_BUFFERSIZE] == 1


def test_init_camera_falls_back_to_any(monkeypatch):
    """§9.1: V4L2 fails, ANY succeeds."""
    bad = _MockCap(opened=False)
    good = _MockCap(opened=True)
    factory = mock.Mock(side_effect=[bad, good])
    monkeypatch.setattr(ucu.cv2, "VideoCapture", factory)

    result = ucu.init_camera()

    assert result is good
    assert factory.call_count == 2
    assert factory.call_args_list[1].args[1] == ucu.cv2.CAP_ANY


def test_init_camera_raises_when_all_backends_fail(monkeypatch):
    """§9.1: every backend fails -> RuntimeError E001."""
    factory = mock.Mock(return_value=_MockCap(opened=False))
    monkeypatch.setattr(ucu.cv2, "VideoCapture", factory)

    with pytest.raises(RuntimeError, match=r"E001"):
        ucu.init_camera()


# ---------------------------------------------------------------------------
# Module 2: capture_jpeg_frame
# ---------------------------------------------------------------------------


def test_capture_jpeg_frame_returns_valid_jpeg(monkeypatch):
    """§9.1: capture_jpeg_frame returns non-empty JPEG starting with FFD8."""
    monkeypatch.setattr(ucu, "cap", _MockCap(opened=True, read_ok=True))

    data = asyncio.run(ucu.capture_jpeg_frame())

    assert isinstance(data, bytes)
    assert len(data) > 0
    # JPEG SOI marker.
    assert data[:2] == b"\xff\xd8"


def test_capture_jpeg_frame_raises_on_read_failure(monkeypatch):
    """§9.1: cap.read() returns (False, None) -> RuntimeError E002."""
    monkeypatch.setattr(ucu, "cap", _MockCap(opened=True, read_ok=False))

    with pytest.raises(RuntimeError, match=r"E002"):
        asyncio.run(ucu.capture_jpeg_frame())


def test_capture_jpeg_frame_raises_when_camera_uninitialized(monkeypatch):
    """capture_jpeg_frame guards against missing cap."""
    monkeypatch.setattr(ucu, "cap", None)

    with pytest.raises(RuntimeError, match=r"not initialized"):
        asyncio.run(ucu.capture_jpeg_frame())


# ---------------------------------------------------------------------------
# Module 3: upload_loop helpers & framing
# ---------------------------------------------------------------------------


def test_build_ws_uri_concatenates_pi_id():
    uri = ucu._build_ws_uri()
    assert uri.endswith(f"/ws/pi/{ucu.PI_ID}")


def test_build_registration_schema():
    reg = ucu._build_registration()
    assert reg["type"] == "register"
    assert reg["pi_id"] == ucu.PI_ID
    assert reg["resolution"] == [1280, 720]
    assert reg["fps"] == ucu.FPS
    assert reg["firmware_version"] == ucu.FIRMWARE_VERSION
    assert reg["camera_type"] == "usb_opencv"


def test_build_frame_header_schema():
    hdr = ucu._build_frame_header(frame_num=42, size=12345)
    assert hdr["type"] == "frame"
    assert hdr["frame_num"] == 42
    assert hdr["size"] == 12345
    assert hdr["resolution"] == [1280, 720]


def test_encode_frame_message_layout():
    """§5.3.2 wire format: <JSON header>\\n<binary JPEG>."""
    hdr = {"type": "frame", "frame_num": 1, "size": 3}
    payload = b"\xff\xd8\xff"
    msg = ucu._encode_frame_message(hdr, payload)

    newline = msg.index(b"\n")
    decoded = json.loads(msg[:newline])
    assert decoded["frame_num"] == 1
    assert msg[newline + 1:] == payload


def test_backoff_delay_grows_then_caps():
    """§5.3.1 table: 2,4,8,16,32,60,60."""
    delays = [ucu._backoff_delay(n) for n in range(1, 8)]
    assert delays == [2, 4, 8, 16, 32, 60, 60]


# ---------------------------------------------------------------------------
# Module 3: upload_loop integration (mocked websockets)
# ---------------------------------------------------------------------------


class _FakeWS:
    """Async context manager that records sent payloads."""

    def __init__(self):
        self.sent: list = []

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_exc):
        return False

    async def send(self, payload):
        self.sent.append(payload)
        # After the first frame, simulate a clean disconnect so the test ends.
        if sum(1 for p in self.sent if isinstance(p, (bytes, bytearray))) >= 1:
            raise ucu.ConnectionClosed(None, None) if hasattr(
                ucu.ConnectionClosed, "__init__"
            ) else ucu.ConnectionClosed()


def test_upload_loop_registers_and_frames(monkeypatch):
    """§9.1: upload_loop sends registration then a frame with valid header."""
    fake = _FakeWS()

    def _connect(*_a, **_kw):
        return fake

    # Stub websockets module + a minimal ConnectionClosed exception.
    class _Closed(Exception):
        def __init__(self, *_a, **_kw):
            super().__init__("closed")

    monkeypatch.setattr(ucu, "websockets", types.SimpleNamespace(connect=_connect))
    monkeypatch.setattr(ucu, "ConnectionClosed", _Closed)
    monkeypatch.setattr(ucu, "WebSocketException", _Closed)

    # Stub capture_jpeg_frame so we don't depend on cv2 stub timing.
    async def _fake_capture():
        return b"\xff\xd8\xff\xd9"  # minimal JPEG

    monkeypatch.setattr(ucu, "capture_jpeg_frame", _fake_capture)

    # Run upload_loop briefly: the fake WS raises ConnectionClosed after
    # the first frame; cancel the backoff sleep to exit the outer loop.
    async def _runner():
        task = asyncio.create_task(ucu.upload_loop())
        await asyncio.sleep(0.05)
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

    ucu.connection_attempts = 0
    ucu.frames_sent = 0
    asyncio.run(_runner())

    # First message is the registration JSON.
    assert isinstance(fake.sent[0], str)
    reg = json.loads(fake.sent[0])
    assert reg["type"] == "register"

    # Second message is the frame: JSON header + \n + JPEG bytes.
    frame_msg = fake.sent[1]
    assert isinstance(frame_msg, (bytes, bytearray))
    nl = frame_msg.index(b"\n")
    hdr = json.loads(frame_msg[:nl])
    assert hdr["type"] == "frame"
    assert frame_msg[nl + 1:] == b"\xff\xd8\xff\xd9"


# ---------------------------------------------------------------------------
# Module 4: telemetry parsers
# ---------------------------------------------------------------------------


def test_get_cpu_temp_parses_millidegrees(monkeypatch):
    fake = mock.mock_open(read_data="54321\n")
    monkeypatch.setattr("builtins.open", fake)
    assert ucu.get_cpu_temp() == pytest.approx(54.321)


def test_get_cpu_temp_returns_zero_on_failure(monkeypatch):
    def _raise(*_a, **_kw):
        raise FileNotFoundError

    monkeypatch.setattr("builtins.open", _raise)
    assert ucu.get_cpu_temp() == 0.0


def test_get_wifi_signal_parses_iwconfig_dbm(monkeypatch):
    proc = types.SimpleNamespace(
        stdout="wlan0  IEEE 802.11  Signal level=-42 dBm  Noise level=-95 dBm"
    )
    monkeypatch.setattr(ucu.subprocess, "run", lambda *a, **kw: proc)
    assert ucu.get_wifi_signal() == -42


def test_get_wifi_signal_relative_quality_returns_sentinel(monkeypatch):
    proc = types.SimpleNamespace(stdout="wlan0  Signal level=42/100")
    monkeypatch.setattr(ucu.subprocess, "run", lambda *a, **kw: proc)
    assert ucu.get_wifi_signal() == -100


def test_get_wifi_signal_fallback_to_proc(monkeypatch):
    """§9.1: iwconfig missing -> parse /proc/net/wireless."""

    def _no_iwconfig(*_a, **_kw):
        raise FileNotFoundError

    monkeypatch.setattr(ucu.subprocess, "run", _no_iwconfig)

    proc_data = (
        "Inter-| sta-|   Quality        |        Discarded packets\n"
        " face | tus | link level noise |  nwid  crypt   frag  retry\n"
        " wlan0: 0000   70.  -55.  -256        0      0      0      0\n"
    )
    monkeypatch.setattr("builtins.open", mock.mock_open(read_data=proc_data))

    assert ucu.get_wifi_signal() == -55
