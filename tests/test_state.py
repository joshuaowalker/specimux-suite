"""Tests for pipeline state materialization."""

from specimux_suite.events import EventLog
from specimux_suite.state import PipelineState, SpecimenStatus


def test_rebuild_from_events(tmp_output):
    log = EventLog(tmp_output / "events.jsonl")

    log.emit("pipeline.started", {"mode": "batch"})
    log.emit("specimux.completed", {
        "job_id": "abc",
        "exit_code": 0,
        "specimens": {"specimen_A": 150, "specimen_B": 50},
    })
    log.emit("specimen.updated", {
        "specimen_id": "specimen_A",
        "pool": "pool1",
        "total_reads": 150,
    })
    log.emit("consensus.started", {
        "specimen_id": "specimen_A",
        "job_id": "con1",
        "read_count": 150,
    })
    log.emit("consensus.completed", {
        "specimen_id": "specimen_A",
        "job_id": "con1",
        "clusters": [
            {"name": "specimen_A-c0", "size": 120, "ric": 100, "rid": 95.5},
            {"name": "specimen_A-c1", "size": 30, "ric": 30, "rid": 92.1},
        ],
    })
    log.emit("identification.completed", {
        "specimen_id": "specimen_A",
        "matches": [
            {
                "cluster": "specimen_A-c0",
                "top_hits": [
                    {"ref_id": "Russula_emetica", "name": "Russula emetica", "identity": 0.98},
                ],
            },
        ],
    })

    state = PipelineState()
    state.rebuild(log)

    assert state.mode == "batch"
    assert state.version == 6

    spec_a = state.specimens["specimen_A"]
    assert spec_a.total_reads == 150
    assert spec_a.pool == "pool1"
    assert spec_a.status == SpecimenStatus.IDENTIFIED
    assert spec_a.consensus_version == 1
    assert len(spec_a.clusters) == 2
    assert spec_a.clusters[0].name == "specimen_A-c0"
    assert spec_a.clusters[0].size == 120
    assert len(spec_a.identification) == 1

    spec_b = state.specimens["specimen_B"]
    assert spec_b.total_reads == 50
    assert spec_b.status == SpecimenStatus.WAITING


def test_error_state(tmp_output):
    log = EventLog(tmp_output / "events.jsonl")

    log.emit("specimen.updated", {"specimen_id": "s1", "pool": "p1", "total_reads": 100})
    log.emit("pipeline.error", {
        "component": "speconsense",
        "specimen_id": "s1",
        "message": "crashed",
    })

    state = PipelineState()
    state.rebuild(log)

    assert state.specimens["s1"].status == SpecimenStatus.ERROR
    assert len(state.errors) == 1


def test_to_dict(tmp_output):
    log = EventLog(tmp_output / "events.jsonl")
    log.emit("pipeline.started", {"mode": "batch"})
    log.emit("specimen.updated", {"specimen_id": "s1", "pool": "p1", "total_reads": 42})

    state = PipelineState()
    state.rebuild(log)

    d = state.to_dict()
    assert d["mode"] == "batch"
    assert "s1" in d["specimens"]
    assert d["specimens"]["s1"]["total_reads"] == 42
