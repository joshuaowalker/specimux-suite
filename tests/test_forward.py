"""The HTTP event forwarder: every event, in order, at least once, resumable.

The log is the buffer, so an unreachable receiver costs only retries, a
restart resumes from the last acknowledged version, and shutdown never
loses an event (unacknowledged ones go next time).
"""

import json
import socket
import threading
import time
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path

from specimux_suite.events import EventLog
from specimux_suite.forward import ACK_FILENAME, EventForwarder


class Receiver:
    """Collects POSTed batches; can be told to fail for a while."""

    def __init__(self):
        with socket.socket() as s:
            s.bind(("127.0.0.1", 0))
            port = s.getsockname()[1]
        self.url = f"http://127.0.0.1:{port}/ingest"
        self.batches: list[dict] = []
        self.headers: list[dict] = []
        self.fail_until = 0.0
        self.lock = threading.Lock()
        rx = self

        class Handler(BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.1"

            def log_message(self, *a):
                pass

            def do_POST(self):
                n = int(self.headers.get("Content-Length", 0))
                body = json.loads(self.rfile.read(n) or b"{}")
                if time.monotonic() < rx.fail_until:
                    self.send_response(503)
                    self.send_header("Content-Length", "0")
                    self.end_headers()
                    return
                with rx.lock:
                    rx.batches.append(body)
                    rx.headers.append(dict(self.headers))
                self.send_response(200)
                self.send_header("Content-Length", "2")
                self.end_headers()
                self.wfile.write(b"{}")

        self.server = HTTPServer(("127.0.0.1", port), Handler)
        threading.Thread(target=self.server.serve_forever, daemon=True).start()

    def versions(self) -> list[int]:
        with self.lock:
            return [e["version"] for b in self.batches for e in b["events"]]

    def wait_for(self, version: int, timeout=10.0):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            v = self.versions()
            if v and max(v) >= version:
                return
            time.sleep(0.02)
        raise AssertionError(f"receiver never saw version {version}; got {self.versions()[-5:]}")


def _log(tmp_path, n) -> EventLog:
    log = EventLog(tmp_path / "events.jsonl")
    for i in range(n):
        log.emit("specimen.updated", {"specimen_id": f"S{i}", "total_reads": i})
    return log


def test_forwards_everything_in_order_in_batches(tmp_path):
    rx = Receiver()
    log = _log(tmp_path, 450)
    fwd = EventForwarder(rx.url, batch_max=100, flush_s=0.05,
                         headers={"Authorization": "Bearer t"}, extra={"run_id": "r1"})
    fwd.attach(log, tmp_path)
    rx.wait_for(450)
    # live events after the backlog
    log.emit("specimen.watched", {"specimen_id": "S1", "watched": True})
    rx.wait_for(451)
    fwd.shutdown()

    assert rx.versions() == list(range(1, 452))
    assert all(len(b["events"]) <= 100 for b in rx.batches)
    first = rx.batches[0]
    assert first["run_id"] == "r1"
    assert first["from_version"] == 1 and first["to_version"] == first["events"][-1]["version"]
    assert first["events"][0]["type"] == "specimen.updated"
    assert rx.headers[0]["Authorization"] == "Bearer t"
    assert "specimux-suite/" in rx.headers[0]["User-Agent"]
    ack = json.loads((tmp_path / ACK_FILENAME).read_text())
    assert ack == {"url": rx.url, "acked_version": 451}
    assert fwd.acked_version == 451 and fwd.last_error is None


def test_retries_until_the_receiver_is_back(tmp_path):
    rx = Receiver()
    rx.fail_until = time.monotonic() + 1.0
    log = _log(tmp_path, 5)
    fwd = EventForwarder(rx.url, flush_s=0.05, retry_max_s=0.2)
    fwd.attach(log, tmp_path)
    rx.wait_for(5)
    fwd.shutdown()
    assert rx.versions() == [1, 2, 3, 4, 5]  # once, in order, despite the 503s


def test_restart_resumes_from_the_ack_without_duplicates(tmp_path):
    rx = Receiver()
    log = _log(tmp_path, 30)
    fwd = EventForwarder(rx.url, batch_max=10, flush_s=0.05)
    fwd.attach(log, tmp_path)
    rx.wait_for(30)
    fwd.shutdown()
    # More events land while nothing forwards them; a new forwarder (a
    # restarted engine) picks up exactly where the ack says
    for i in range(3):
        log.emit("pipeline.error", {"message": str(i)})
    fwd2 = EventForwarder(rx.url, batch_max=10, flush_s=0.05)
    fwd2.attach(log, tmp_path)
    rx.wait_for(33)
    fwd2.shutdown()
    assert rx.versions() == list(range(1, 34))


def test_unreachable_endpoint_loses_nothing_on_shutdown(tmp_path):
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        dead = f"http://127.0.0.1:{s.getsockname()[1]}/ingest"
    log = _log(tmp_path, 3)
    fwd = EventForwarder(dead, flush_s=0.05, retry_max_s=0.2)
    fwd.attach(log, tmp_path)
    deadline = time.monotonic() + 5
    while fwd.last_error is None and time.monotonic() < deadline:
        time.sleep(0.02)
    assert fwd.last_error
    t0 = time.monotonic()
    fwd.shutdown()
    assert time.monotonic() - t0 < 3  # stop is prompt even mid-backoff
    assert fwd.acked_version == 0 and not (tmp_path / ACK_FILENAME).exists()
    # the receiver comes up later: everything arrives
    rx = Receiver()
    fwd2 = EventForwarder(rx.url, flush_s=0.05)
    fwd2.attach(log, tmp_path)
    rx.wait_for(3)
    fwd2.shutdown()
    assert rx.versions() == [1, 2, 3]


def test_ack_for_another_url_is_ignored(tmp_path):
    rx = Receiver()
    log = _log(tmp_path, 4)
    (tmp_path / ACK_FILENAME).write_text(json.dumps({"url": "http://elsewhere/", "acked_version": 4}))
    fwd = EventForwarder(rx.url, flush_s=0.05)
    fwd.attach(log, tmp_path)
    rx.wait_for(4)
    fwd.shutdown()
    assert rx.versions() == [1, 2, 3, 4]
