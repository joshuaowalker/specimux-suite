"""Tests for the event log."""

import json
import threading
import time

from specimux_suite.events import EventLog, Event


def test_emit_and_replay(tmp_output):
    log = EventLog(tmp_output / "events.jsonl")

    e1 = log.emit("pipeline.started", {"mode": "batch"})
    e2 = log.emit("file.detected", {"path": "/tmp/test.fastq", "size_bytes": 1000})

    assert e1.version == 1
    assert e2.version == 2
    assert e1.type == "pipeline.started"
    assert e2.data["path"] == "/tmp/test.fastq"

    # Replay
    events = list(log.replay())
    assert len(events) == 2
    assert events[0].type == "pipeline.started"
    assert events[1].type == "file.detected"


def test_version_recovery(tmp_output):
    path = tmp_output / "events.jsonl"

    log1 = EventLog(path)
    log1.emit("a", {})
    log1.emit("b", {})
    assert log1.version == 2

    # New instance should recover version
    log2 = EventLog(path)
    assert log2.version == 2

    e = log2.emit("c", {})
    assert e.version == 3


def test_tail_yields_existing_events(tmp_output):
    log = EventLog(tmp_output / "events.jsonl")
    log.emit("a", {"x": 1})
    log.emit("b", {"x": 2})

    events = list(log.tail(after_version=0, timeout=0.1))
    assert len(events) == 2
    assert events[0].type == "a"


def test_tail_yields_new_events(tmp_output):
    log = EventLog(tmp_output / "events.jsonl")
    log.emit("a", {})

    results = []

    def tailer():
        for event in log.tail(after_version=1, timeout=2.0):
            results.append(event)
            if len(results) >= 1:
                return

    t = threading.Thread(target=tailer)
    t.start()

    time.sleep(0.2)
    log.emit("b", {"new": True})

    t.join(timeout=5.0)
    assert len(results) == 1
    assert results[0].type == "b"


def test_event_jsonl_format(tmp_output):
    log = EventLog(tmp_output / "events.jsonl")
    log.emit("test.event", {"key": "value"})

    line = (tmp_output / "events.jsonl").read_text().strip()
    data = json.loads(line)
    assert data["v"] == 1
    assert data["version"] == 1
    assert data["type"] == "test.event"
    assert data["data"]["key"] == "value"
    assert "ts" in data


def test_log_rotation(tmp_output):
    """Event log rotates when it exceeds max_bytes."""
    path = tmp_output / "events.jsonl"
    # Use a tiny max to force rotation quickly
    log = EventLog(path, max_bytes=200)

    # Emit enough events to trigger rotation
    for i in range(20):
        log.emit("test", {"i": i})

    # Should have created archive file(s)
    archives = sorted(tmp_output.glob("events.*.jsonl"))
    assert len(archives) >= 1

    # Replay should yield all events in order
    events = list(log.replay())
    assert len(events) == 20
    assert events[0].version == 1
    assert events[-1].version == 20

    # Version recovery across rotation
    log2 = EventLog(path, max_bytes=200)
    assert log2.version == 20
