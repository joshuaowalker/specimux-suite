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


def test_listener_receives_emitted_events(tmp_output):
    """Registered listeners see every event, in version order."""
    log = EventLog(tmp_output / "events.jsonl")
    seen = []
    log.add_listener(seen.append)

    log.emit("pipeline.started", {"mode": "batch"})
    log.emit("specimen.updated", {"specimen_id": "A", "total_reads": 5})

    assert [e.type for e in seen] == ["pipeline.started", "specimen.updated"]
    assert [e.version for e in seen] == [1, 2]


def test_listener_keeps_state_incrementally_in_sync(tmp_output):
    """A PipelineState registered as a listener matches a full rebuild."""
    from specimux_suite.state import PipelineState, SpecimenStatus

    log = EventLog(tmp_output / "events.jsonl")
    live = PipelineState()
    live.rebuild(log)
    log.add_listener(live.apply)

    log.emit("specimux.completed", {
        "job_id": "j1", "exit_code": 0,
        "specimens": {"specimen_A": 100},
    })
    log.emit("consensus.started", {"specimen_id": "specimen_A", "job_id": "c1"})
    log.emit("consensus.completed", {
        "specimen_id": "specimen_A", "job_id": "c1",
        "clusters": [{"name": "specimen_A-c0", "size": 90}],
    })

    rebuilt = PipelineState()
    rebuilt.rebuild(log)

    assert live.version == rebuilt.version == 3
    assert live.specimens["specimen_A"].status == SpecimenStatus.CONSENSUS_DONE
    assert live.to_dict() == rebuilt.to_dict()


def test_failing_listener_does_not_break_emit(tmp_output):
    """An exception in one listener must not prevent the emit or other listeners."""
    log = EventLog(tmp_output / "events.jsonl")
    seen = []

    def bad_listener(event):
        raise RuntimeError("boom")

    log.add_listener(bad_listener)
    log.add_listener(seen.append)

    event = log.emit("pipeline.started", {"mode": "batch"})
    assert event.version == 1
    assert len(seen) == 1
    assert len(list(log.replay())) == 1
