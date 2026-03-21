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


def test_specimens_loaded(tmp_output):
    """specimens.loaded should pre-populate specimens with 0 reads."""
    log = EventLog(tmp_output / "events.jsonl")

    log.emit("specimens.loaded", {
        "specimens": [
            {"specimen_id": "A", "pool": "ITS"},
            {"specimen_id": "B", "pool": "ITS"},
        ],
    })

    state = PipelineState()
    state.rebuild(log)

    assert len(state.specimens) == 2
    assert state.specimens["A"].pool == "ITS"
    assert state.specimens["A"].total_reads == 0
    assert state.specimens["A"].status == SpecimenStatus.WAITING
    assert state.specimens["B"].pool == "ITS"


def test_read_totals(tmp_output):
    """specimux.completed events should accumulate input/matched read totals."""
    log = EventLog(tmp_output / "events.jsonl")

    log.emit("specimux.completed", {
        "job_id": "a", "exit_code": 0,
        "specimens": {"s1": 100, "s2": 50},
        "input_reads": 500, "matched_reads": 150,
    })
    log.emit("specimux.completed", {
        "job_id": "b", "exit_code": 0,
        "specimens": {"s1": 200, "s2": 80},
        "input_reads": 600, "matched_reads": 280,
    })

    state = PipelineState()
    state.rebuild(log)

    assert state.total_input_reads == 1100
    # matched_reads is recomputed from specimen totals (not accumulated)
    # After second event: s1=200, s2=80 → total=280
    assert state.total_matched_reads == 280
    d = state.to_dict()
    assert d["total_input_reads"] == 1100
    assert d["total_matched_reads"] == 280


def test_no_match_status(tmp_output):
    """identification.completed with no hits should set NO_MATCH status."""
    log = EventLog(tmp_output / "events.jsonl")

    log.emit("specimen.updated", {"specimen_id": "s1", "pool": "p1", "total_reads": 100})
    log.emit("consensus.started", {"specimen_id": "s1", "job_id": "j1", "read_count": 100})
    log.emit("consensus.completed", {
        "specimen_id": "s1", "job_id": "j1",
        "clusters": [{"name": "s1-c0", "size": 80}],
    })
    log.emit("identification.completed", {
        "specimen_id": "s1",
        "matches": [{"cluster": "s1-c0", "top_hits": []}],
    })

    state = PipelineState()
    state.rebuild(log)

    assert state.specimens["s1"].status == SpecimenStatus.NO_MATCH


def test_summarize_state(tmp_output):
    """summarize events should update specimen status and variants."""
    log = EventLog(tmp_output / "events.jsonl")

    log.emit("specimen.updated", {"specimen_id": "s1", "pool": "p1", "total_reads": 100})
    log.emit("consensus.started", {"specimen_id": "s1", "job_id": "j1", "read_count": 100})
    log.emit("consensus.completed", {
        "specimen_id": "s1", "job_id": "j1",
        "clusters": [{"name": "s1-c0", "size": 80, "ric": 60}],
    })
    log.emit("identification.completed", {
        "specimen_id": "s1",
        "matches": [{"cluster": "s1-c0", "top_hits": [
            {"ref_id": "Sp_x", "name": "Species x", "identity": 0.99},
        ]}],
    })
    log.emit("summarize.started", {
        "specimen_id": "s1",
        "job_id": "sum1",
    })
    log.emit("summarize.completed", {
        "specimen_id": "s1",
        "job_id": "sum1",
        "variant_count": 2,
        "variants": [
            {"name": "s1-1.v1", "ric": 50, "size": 60, "length": 700},
            {"name": "s1-1.v2", "ric": 10, "size": 20, "length": 695},
        ],
    })

    state = PipelineState()
    state.rebuild(log)

    spec = state.specimens["s1"]
    assert spec.status == SpecimenStatus.SUMMARIZED
    assert spec.summarize_version == 1
    assert len(spec.variants) == 2
    assert spec.variants[0]["name"] == "s1-1.v1"

    # Check serialization
    d = state.to_dict()
    assert d["specimens"]["s1"]["summarize_version"] == 1
    assert len(d["specimens"]["s1"]["variants"]) == 2


def test_specimen_watched(tmp_output):
    """specimen.watched should toggle watched field on the specimen."""
    log = EventLog(tmp_output / "events.jsonl")

    log.emit("specimen.updated", {"specimen_id": "s1", "pool": "p1", "total_reads": 100})
    log.emit("specimen.watched", {"specimen_id": "s1", "watched": True})

    state = PipelineState()
    state.rebuild(log)

    assert state.specimens["s1"].watched is True

    # Toggle off
    log.emit("specimen.watched", {"specimen_id": "s1", "watched": False})
    state2 = PipelineState()
    state2.rebuild(log)

    assert state2.specimens["s1"].watched is False

    # Check serialization
    d = state2.to_dict()
    assert d["specimens"]["s1"]["watched"] is False


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
