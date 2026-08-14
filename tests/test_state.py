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


def test_stale_identification_is_dropped(tmp_output):
    """identification.completed from a superseded consensus generation is ignored."""
    log = EventLog(tmp_output / "events.jsonl")

    log.emit("consensus.completed", {
        "specimen_id": "specimen_A", "job_id": "c1",
        "clusters": [{"name": "specimen_A-0.v1", "size": 90}],
    })
    # Re-consensus: generation 2
    log.emit("consensus.completed", {
        "specimen_id": "specimen_A", "job_id": "c2",
        "clusters": [{"name": "specimen_A-0.v1", "size": 150}],
    })
    # Identification launched against generation 1 lands late — must be dropped
    log.emit("identification.completed", {
        "specimen_id": "specimen_A",
        "consensus_version": 1,
        "matches": [{"cluster": "specimen_A-0.v1",
                     "top_hits": [{"ref_id": "stale", "name": "Stale hit",
                                   "identity": 0.9, "adjusted_identity": 0.9}]}],
    })

    state = PipelineState()
    state.rebuild(log)
    spec = state.specimens["specimen_A"]
    assert spec.consensus_version == 2
    assert spec.identification == []
    assert spec.status == SpecimenStatus.CONSENSUS_DONE

    # Identification for the current generation is applied
    log.emit("identification.completed", {
        "specimen_id": "specimen_A",
        "consensus_version": 2,
        "matches": [{"cluster": "specimen_A-0.v1",
                     "top_hits": [{"ref_id": "fresh", "name": "Fresh hit",
                                   "identity": 0.99, "adjusted_identity": 0.99}]}],
    })
    state = PipelineState()
    state.rebuild(log)
    spec = state.specimens["specimen_A"]
    assert spec.status == SpecimenStatus.IDENTIFIED
    assert spec.identification[0].top_hits[0]["ref_id"] == "fresh"


def test_unversioned_identification_still_applies(tmp_output):
    """Events without consensus_version (older logs) are applied as before."""
    log = EventLog(tmp_output / "events.jsonl")
    log.emit("consensus.completed", {
        "specimen_id": "specimen_A", "job_id": "c1",
        "clusters": [{"name": "specimen_A-c0", "size": 90}],
    })
    log.emit("identification.completed", {
        "specimen_id": "specimen_A",
        "matches": [{"cluster": "specimen_A-c0",
                     "top_hits": [{"ref_id": "r1", "name": "Some hit",
                                   "identity": 0.98, "adjusted_identity": 0.98}]}],
    })
    state = PipelineState()
    state.rebuild(log)
    assert state.specimens["specimen_A"].status == SpecimenStatus.IDENTIFIED


def test_file_processed_requires_demux(tmp_output):
    """A stable-but-never-demuxed file must not read as processed (live-mode
    restart seeds the watcher from `processed`, so it gets picked up again)."""
    log = EventLog(tmp_output / "events.jsonl")
    log.emit("file.detected", {"path": "/w/a.fastq", "size_bytes": 10})
    log.emit("file.stable", {"path": "/w/a.fastq", "size_bytes": 10})

    state = PipelineState()
    state.rebuild(log)
    f = state.files["/w/a.fastq"]
    assert f.stable and not f.processed

    log.emit("specimux.completed", {
        "job_id": "j1", "exit_code": 0, "specimens": {},
        "file_path": "/w/a.fastq",
    })
    state = PipelineState()
    state.rebuild(log)
    assert state.files["/w/a.fastq"].processed


def test_demux_history_snapshots(tmp_output):
    """Successful demux runs append forecast snapshots with the event ts;
    failed runs don't; to_dict carries the history for the dashboard bootstrap."""
    log = EventLog(tmp_output / "events.jsonl")
    log.emit("specimux.completed", {
        "job_id": "j1", "exit_code": 0, "input_reads": 5000,
        "matched_reads": 2700, "specimens": {"A": 2000, "B": 700},
    })
    log.emit("specimux.completed", {  # failed run — no snapshot
        "job_id": "j2", "exit_code": 1, "specimens": {},
    })
    log.emit("specimux.completed", {
        "job_id": "j3", "exit_code": 0, "input_reads": 5000,
        "matched_reads": 5600, "specimens": {"A": 4100, "B": 1500},
    })

    state = PipelineState()
    state.rebuild(log)
    h = state.demux_history
    assert len(h) == 2
    assert h[0]["matched_cum"] == 2700 and h[1]["matched_cum"] == 5600
    assert h[1]["input_cum"] == 10000
    assert h[0]["counts"] == {"A": 2000, "B": 700}
    assert h[0]["ts"]  # event timestamp captured
    assert state.to_dict()["demux_history"][1]["counts"]["A"] == 4100


def test_demux_history_decimation(tmp_output):
    """History is halved beyond the cap, always keeping the newest entry."""
    log = EventLog(tmp_output / "events.jsonl")
    n = PipelineState._DEMUX_HISTORY_MAX + 1
    for i in range(1, n + 1):
        log.emit("specimux.completed", {
            "job_id": f"j{i}", "exit_code": 0, "input_reads": 10,
            "matched_reads": i, "specimens": {"A": i},
        })
    state = PipelineState()
    state.rebuild(log)
    h = state.demux_history
    assert len(h) <= PipelineState._DEMUX_HISTORY_MAX
    assert h[-1]["matched_cum"] == n  # newest always retained
    assert h[0]["matched_cum"] == 1


def test_cluster_chimera_captured(tmp_path):
    """chimera= flag flows from consensus.completed into state and to_dict."""
    from specimux_suite.events import EventLog

    log = EventLog(tmp_path / "events.jsonl")
    log.emit("consensus.started", {"specimen_id": "A", "job_id": "j1", "read_count": 50})
    log.emit("consensus.completed", {"specimen_id": "A", "job_id": "j1", "clusters": [
        {"name": "A-1.v1", "size": 40, "ric": 38},
        {"name": "A-1.v2", "size": 10, "ric": 9, "chimera": "v0+v1"},
    ]})

    state = PipelineState()
    state.rebuild(log)
    clusters = state.specimens["A"].clusters
    assert clusters[0].chimera is None
    assert clusters[1].chimera == "v0+v1"
    d = state.to_dict()["specimens"]["A"]["clusters"]
    assert d[1]["chimera"] == "v0+v1"
