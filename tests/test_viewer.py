"""The viewer app factory: the dashboard's read side without a pipeline.

Built from an event source, a state and a run directory, so a service can
host many runs in one process and a fixture can be served with no
processing. Invariants that matter to those hosts:

- two apps in one process never share state (the old module-global server
  could only serve one run);
- a live run replays faithfully (``heal=False``) while a finished one heals;
- a sequence the log has announced but the disk lacks is "not yet
  available" (503 + Retry-After), distinct from an unknown one (404);
- SSE ids are log versions and a client catches up from ``after_version``.
"""

import socket
import threading
import time

import httpx
import pytest
from fastapi.testclient import TestClient

from specimux_suite.events import EventLog
from specimux_suite.state import PipelineState, SpecimenStatus
from specimux_suite.web.viewer import create_viewer_app, load_run, serve_in_thread

CLUSTERS = [{"name": "S1-c0", "size": 30}]


def _write_run(log: EventLog):
    log.emit("pipeline.started", {"mode": "batch", "config_summary": {}})
    log.emit("specimux.completed", {"specimens": {"S1": 40, "S2": 12}})
    log.emit("consensus.completed", {"specimen_id": "S1", "clusters": CLUSTERS})
    log.emit("consensus.started", {"specimen_id": "S2"})  # interrupted here


def _app(tmp_path, heal=True, **kw):
    log = EventLog(tmp_path / "events.jsonl")
    _write_run(log)
    event_log, state = load_run(tmp_path / "events.jsonl", heal=heal)
    return create_viewer_app(event_log, state, tmp_path, **kw), event_log, state


def test_load_run_heals_only_when_asked(tmp_path):
    log = EventLog(tmp_path / "events.jsonl")
    _write_run(log)
    _, healed = load_run(tmp_path / "events.jsonl", heal=True)
    _, faithful = load_run(tmp_path / "events.jsonl", heal=False)
    assert healed.specimens["S2"].status == SpecimenStatus.WAITING
    assert faithful.specimens["S2"].status == SpecimenStatus.CONSENSUS_RUNNING


def test_load_run_keeps_state_current(tmp_path):
    log = EventLog(tmp_path / "events.jsonl")
    _write_run(log)
    event_log, state = load_run(tmp_path / "events.jsonl")
    event_log.emit("specimen.watched", {"specimen_id": "S1", "watched": True})
    assert state.specimens["S1"].watched
    assert state.version == event_log.version


def test_state_snapshot_and_extras(tmp_path):
    app, _, state = _app(tmp_path, config_summary={"min_reads": 7},
                         share={"url": "http://10.0.0.5:8077", "max_clients": 3})
    c = TestClient(app)
    snap = c.get("/api/state").json()
    assert snap["version"] == state.version
    assert set(snap["specimens"]) == {"S1", "S2"}
    assert snap["config_summary"] == {"min_reads": 7}
    assert snap["share"]["url"] == "http://10.0.0.5:8077"
    assert snap["sse_clients"] == 0
    assert c.get("/api/viewers").json() == {"sse_clients": 0}
    ids = {s["specimen_id"] for s in c.get("/api/specimens").json()}
    assert ids == {"S1", "S2"}


def test_snapshot_omits_extras_when_not_given(tmp_path):
    app, _, _ = _app(tmp_path)
    snap = TestClient(app).get("/api/state").json()
    assert "config_summary" not in snap
    assert "share" not in snap


def test_pages_and_static_served(tmp_path):
    app, _, _ = _app(tmp_path)
    c = TestClient(app)
    assert "<html" in c.get("/").text.lower()
    assert "<html" in c.get("/present").text.lower()
    assert c.get("/static/derived.js").status_code == 200
    (tmp_path / "inat_photos" / "123_large.jpg").write_bytes(b"jpeg")
    r = c.get("/photos/123_large.jpg")
    assert r.status_code == 200
    assert "immutable" in r.headers["cache-control"]
    # no command route on the viewer itself
    assert c.post("/api/commands", json={"command": "watch", "specimen_id": "S1"}).status_code in (404, 405)


def test_sequence_not_yet_available_vs_unknown(tmp_path):
    app, _, _ = _app(tmp_path)
    c = TestClient(app)
    # The log announced S1-c0; its file has not landed
    r = c.get("/api/sequence/S1/S1-c0")
    assert r.status_code == 503
    assert r.headers["retry-after"] == "1"
    assert r.json()["retry"] is True
    # Nothing announced this one
    assert c.get("/api/sequence/S1/S1-c9").status_code == 404
    assert c.get("/api/sequence/nope/S1-c0").status_code == 404
    assert c.get("/api/sequence/../x/S1-c0").status_code in (400, 404)
    # File lands: contract is JSON with the extracted sequence
    d = tmp_path / "consensus" / "S1"
    d.mkdir(parents=True)
    (d / "S1-all.fasta").write_text(">S1-c0 size=30\nACGT\nTTGA\n>S1-c1 size=2\nGG\n")
    assert c.get("/api/sequence/S1/S1-c0").json() == {"sequence": "ACGTTTGA"}


def test_variant_sequence_from_summary_dir(tmp_path):
    app, event_log, _ = _app(tmp_path)
    event_log.emit("summarize.completed", {
        "specimen_id": "S1", "consensus_version": 1,
        "variants": [{"name": "S1-1.v1", "size": 30}],
    })
    c = TestClient(app)
    assert c.get("/api/sequence/S1/S1-1.v1").status_code == 503
    (tmp_path / "summary").mkdir()
    (tmp_path / "summary" / "S1-1.v1-RiC30.fasta").write_text(">S1-1.v1 size=30\nAC\nGT\n")
    assert c.get("/api/sequence/S1/S1-1.v1").json() == {"sequence": "ACGT"}


def test_two_apps_one_process_are_independent(tmp_path):
    a, _, _ = _app(tmp_path / "a")
    b_log = EventLog(tmp_path / "b" / "events.jsonl")
    b_log.emit("specimux.completed", {"specimens": {"OTHER": 5}})
    b_src, b_state = load_run(tmp_path / "b" / "events.jsonl")
    b = create_viewer_app(b_src, b_state, tmp_path / "b")
    assert set(TestClient(a).get("/api/state").json()["specimens"]) == {"S1", "S2"}
    assert set(TestClient(b).get("/api/state").json()["specimens"]) == {"OTHER"}


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def _wait_up(url: str, timeout=10.0):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            httpx.get(url, timeout=1.0)
            return
        except httpx.HTTPError:
            time.sleep(0.05)
    raise RuntimeError("server did not come up")


@pytest.fixture
def served(tmp_path):
    app, event_log, state = _app(tmp_path)
    port = _free_port()
    serve_in_thread(app, "127.0.0.1", port)
    base = f"http://127.0.0.1:{port}"
    _wait_up(base + "/api/viewers")
    return base, event_log, state


def _sse_ids(base, after_version, want, emit_after_connect=None, timeout=10.0):
    """Ids of the first `want` SSE events after after_version."""
    ids = []
    with httpx.stream("GET", f"{base}/events?after_version={after_version}",
                      timeout=timeout) as r:
        assert r.status_code == 200
        if emit_after_connect:
            threading.Timer(0.2, emit_after_connect).start()
        for line in r.iter_lines():
            if line.startswith("id:"):
                ids.append(int(line[3:].strip()))
                if len(ids) >= want:
                    break
    return ids


def test_sse_backlog_then_live(served):
    base, event_log, state = served
    v = state.version
    assert _sse_ids(base, after_version=2, want=v - 2) == list(range(3, v + 1))
    # Live: connect at the current version, then emit
    emit = lambda: event_log.emit("specimen.watched", {"specimen_id": "S1", "watched": True})
    assert _sse_ids(base, after_version=v, want=1, emit_after_connect=emit) == [v + 1]


def test_sse_client_count_and_cap(tmp_path):
    app, _, _ = _app(tmp_path, max_clients=1)
    port = _free_port()
    serve_in_thread(app, "127.0.0.1", port)
    base = f"http://127.0.0.1:{port}"
    _wait_up(base + "/api/viewers")
    with httpx.stream("GET", f"{base}/events?after_version=0", timeout=10.0) as r:
        lines = r.iter_lines()  # keep the iterator alive: dropping it closes the stream
        next(lines)
        deadline = time.monotonic() + 5
        while httpx.get(base + "/api/viewers").json()["sse_clients"] < 1:
            assert time.monotonic() < deadline
            time.sleep(0.05)
        assert httpx.get(base + "/events?after_version=0").status_code == 503
