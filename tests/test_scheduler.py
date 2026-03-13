"""Tests for the scheduler."""

from specimux_suite.config import PipelineConfig
from specimux_suite.events import EventLog
from specimux_suite.state import PipelineState, SpecimenStatus
from specimux_suite.scheduler import Scheduler


def _make_config(tmp_path, **kwargs):
    defaults = dict(
        primers_file=tmp_path / "primers.fasta",
        specimens_file=tmp_path / "specimens.tsv",
        min_reads=30,
        reprocess_ratio=0.5,
        max_concurrent_consensus=2,
    )
    defaults.update(kwargs)
    return PipelineConfig(**defaults)


def test_schedule_never_processed(tmp_path):
    config = _make_config(tmp_path)
    log = EventLog(tmp_path / "events.jsonl")

    log.emit("specimen.updated", {"specimen_id": "A", "pool": "p1", "total_reads": 100})
    log.emit("specimen.updated", {"specimen_id": "B", "pool": "p1", "total_reads": 50})
    log.emit("specimen.updated", {"specimen_id": "C", "pool": "p1", "total_reads": 10})  # below min

    state = PipelineState()
    state.rebuild(log)

    scheduler = Scheduler(config, state)
    jobs = scheduler.get_ready_jobs(max_jobs=10)

    # A and B should be scheduled (above min_reads), C should not
    assert len(jobs) == 2
    # A first (more reads = higher priority)
    assert jobs[0].specimen_id == "A"
    assert jobs[1].specimen_id == "B"


def test_reprocess_ratio(tmp_path):
    config = _make_config(tmp_path, reprocess_ratio=0.5)
    log = EventLog(tmp_path / "events.jsonl")

    # Specimen with previous consensus
    log.emit("specimen.updated", {"specimen_id": "A", "pool": "p1", "total_reads": 100})
    log.emit("consensus.started", {"specimen_id": "A", "job_id": "j1", "read_count": 100})
    log.emit("consensus.completed", {"specimen_id": "A", "job_id": "j1", "clusters": []})
    # Now has 160 reads (60 new / 100 old = 0.6 > 0.5 threshold)
    log.emit("specimen.updated", {"specimen_id": "A", "pool": "p1", "total_reads": 160})

    # Specimen with too few new reads
    log.emit("specimen.updated", {"specimen_id": "B", "pool": "p1", "total_reads": 100})
    log.emit("consensus.started", {"specimen_id": "B", "job_id": "j2", "read_count": 100})
    log.emit("consensus.completed", {"specimen_id": "B", "job_id": "j2", "clusters": []})
    # Only 120 reads (20 new / 100 old = 0.2 < 0.5)
    log.emit("specimen.updated", {"specimen_id": "B", "pool": "p1", "total_reads": 120})

    state = PipelineState()
    state.rebuild(log)

    scheduler = Scheduler(config, state)
    jobs = scheduler.get_ready_jobs()

    assert len(jobs) == 1
    assert jobs[0].specimen_id == "A"


def test_skip_running(tmp_path):
    config = _make_config(tmp_path)
    log = EventLog(tmp_path / "events.jsonl")

    log.emit("specimen.updated", {"specimen_id": "A", "pool": "p1", "total_reads": 100})
    log.emit("consensus.started", {"specimen_id": "A", "job_id": "j1", "read_count": 100})
    # A is now CONSENSUS_RUNNING

    state = PipelineState()
    state.rebuild(log)

    scheduler = Scheduler(config, state)
    jobs = scheduler.get_ready_jobs()

    assert len(jobs) == 0


def test_available_slots(tmp_path):
    config = _make_config(tmp_path, max_concurrent_consensus=2)
    log = EventLog(tmp_path / "events.jsonl")

    log.emit("specimen.updated", {"specimen_id": "A", "pool": "p1", "total_reads": 100})
    log.emit("consensus.started", {"specimen_id": "A", "job_id": "j1", "read_count": 100})

    state = PipelineState()
    state.rebuild(log)

    scheduler = Scheduler(config, state)
    assert scheduler.available_slots() == 1
