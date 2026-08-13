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
        workers=2,
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


def test_get_all_eligible_jobs_ignores_reprocess_ratio(tmp_path):
    """get_all_eligible_jobs returns specimens that get_ready_jobs skips due to reprocess_ratio."""
    config = _make_config(tmp_path, reprocess_ratio=0.5)
    log = EventLog(tmp_path / "events.jsonl")

    # Specimen A: previously processed, only 20% new reads (below 0.5 ratio)
    log.emit("specimen.updated", {"specimen_id": "A", "pool": "p1", "total_reads": 100})
    log.emit("consensus.started", {"specimen_id": "A", "job_id": "j1", "read_count": 100})
    log.emit("consensus.completed", {"specimen_id": "A", "job_id": "j1", "clusters": []})
    log.emit("specimen.updated", {"specimen_id": "A", "pool": "p1", "total_reads": 120})

    # Specimen B: never processed, above min_reads
    log.emit("specimen.updated", {"specimen_id": "B", "pool": "p1", "total_reads": 50})

    # Specimen C: below min_reads
    log.emit("specimen.updated", {"specimen_id": "C", "pool": "p1", "total_reads": 10})

    state = PipelineState()
    state.rebuild(log)

    scheduler = Scheduler(config, state)

    # get_ready_jobs should skip A (ratio 0.2 < 0.5)
    ready = scheduler.get_ready_jobs(max_jobs=10)
    assert len(ready) == 1
    assert ready[0].specimen_id == "B"

    # get_all_eligible_jobs should include both A and B
    eligible = scheduler.get_all_eligible_jobs()
    assert len(eligible) == 2
    sids = [j.specimen_id for j in eligible]
    assert "A" in sids
    assert "B" in sids
    # B (never-processed) should have higher priority than A (previously processed)
    assert eligible[0].specimen_id == "B"
    assert eligible[1].specimen_id == "A"


def test_get_all_eligible_jobs_skips_no_new_reads(tmp_path):
    """get_all_eligible_jobs skips specimens with no new reads since last consensus."""
    config = _make_config(tmp_path)
    log = EventLog(tmp_path / "events.jsonl")

    # Specimen A: processed, no new reads (total_reads == reads_at_last_consensus)
    log.emit("specimen.updated", {"specimen_id": "A", "pool": "p1", "total_reads": 100})
    log.emit("consensus.started", {"specimen_id": "A", "job_id": "j1", "read_count": 100})
    log.emit("consensus.completed", {"specimen_id": "A", "job_id": "j1", "clusters": [{"name": "c1", "size": 80}]})

    # Specimen B: processed, has new reads
    log.emit("specimen.updated", {"specimen_id": "B", "pool": "p1", "total_reads": 100})
    log.emit("consensus.started", {"specimen_id": "B", "job_id": "j2", "read_count": 100})
    log.emit("consensus.completed", {"specimen_id": "B", "job_id": "j2", "clusters": [{"name": "c1", "size": 80}]})
    log.emit("specimen.updated", {"specimen_id": "B", "pool": "p1", "total_reads": 110})

    state = PipelineState()
    state.rebuild(log)

    scheduler = Scheduler(config, state)
    eligible = scheduler.get_all_eligible_jobs()

    assert len(eligible) == 1
    assert eligible[0].specimen_id == "B"


def test_available_slots(tmp_path):
    config = _make_config(tmp_path, workers=2)
    log = EventLog(tmp_path / "events.jsonl")

    log.emit("specimen.updated", {"specimen_id": "A", "pool": "p1", "total_reads": 100})
    log.emit("consensus.started", {"specimen_id": "A", "job_id": "j1", "read_count": 100})

    state = PipelineState()
    state.rebuild(log)

    scheduler = Scheduler(config, state)
    assert scheduler.available_slots() == 1


# --- Confidence-driven reprocess prioritization ---

def _hit(name, identity):
    return {"ref_id": name.replace(" ", "_"), "name": name,
            "identity": identity, "adjusted_identity": identity}


def _processed(log, sid, *, clusters, matches, reads_before=100, reads_after=160,
               taxon=None):
    """Emit events for a specimen with one completed consensus + identification."""
    log.emit("specimen.updated", {"specimen_id": sid, "pool": "p1", "total_reads": reads_before})
    if taxon:
        log.emit("specimens.taxa", {"taxa": {sid: {"name": taxon, "genus": taxon.split()[0]}}})
    log.emit("consensus.started", {"specimen_id": sid, "job_id": f"j-{sid}", "read_count": reads_before})
    log.emit("consensus.completed", {"specimen_id": sid, "job_id": f"j-{sid}", "clusters": clusters})
    log.emit("identification.completed", {
        "specimen_id": sid, "consensus_version": 1, "matches": matches,
    })
    log.emit("specimen.updated", {"specimen_id": sid, "pool": "p1", "total_reads": reads_after})


def test_confidence_band_ordering(tmp_path):
    """Reprocess candidates order: no_match > low_identity > off_target > marginal > confident."""
    config = _make_config(tmp_path, reprocess_ratio=0.5)
    log = EventLog(tmp_path / "events.jsonl")
    c1 = [{"name": "c1", "size": 80}]

    _processed(log, "NOMATCH", clusters=c1,
               matches=[{"cluster": "c1", "top_hits": []}])
    _processed(log, "LOWID", clusters=c1,
               matches=[{"cluster": "c1", "top_hits": [_hit("Amanita muscaria", 0.85)]}],
               taxon="Amanita muscaria")
    _processed(log, "OFFTARGET", clusters=c1,
               matches=[{"cluster": "c1", "top_hits": [_hit("Saccharomyces cerevisiae", 0.99)]}],
               taxon="Amanita muscaria")
    _processed(log, "MARGINAL", clusters=c1,
               matches=[{"cluster": "c1", "top_hits": [_hit("Amanita muscaria", 0.95)]}],
               taxon="Amanita muscaria")
    _processed(log, "CONFIDENT", clusters=c1,
               matches=[{"cluster": "c1", "top_hits": [_hit("Amanita muscaria", 0.995)]}],
               taxon="Amanita muscaria")

    state = PipelineState()
    state.rebuild(log)
    jobs = Scheduler(config, state).get_ready_jobs()

    assert [j.specimen_id for j in jobs] == [
        "NOMATCH", "LOWID", "OFFTARGET", "MARGINAL", "CONFIDENT"]
    assert [j.reason for j in jobs] == [
        "no_match", "low_identity", "off_target", "marginal", "confident"]


def test_uncertain_bands_soften_reprocess_gate(tmp_path):
    """Bands 1-3 re-enter the queue at half the reprocess_ratio; band 5 does not."""
    config = _make_config(tmp_path, reprocess_ratio=0.5)
    log = EventLog(tmp_path / "events.jsonl")
    c1 = [{"name": "c1", "size": 80}]

    # Both at ratio 0.3: above 0.25 (softened) but below 0.5 (full gate)
    _processed(log, "NOMATCH", clusters=c1, reads_after=130,
               matches=[{"cluster": "c1", "top_hits": []}])
    _processed(log, "CONFIDENT", clusters=c1, reads_after=130,
               matches=[{"cluster": "c1", "top_hits": [_hit("Amanita muscaria", 0.995)]}],
               taxon="Amanita muscaria")

    state = PipelineState()
    state.rebuild(log)
    jobs = Scheduler(config, state).get_ready_jobs()

    assert [j.specimen_id for j in jobs] == ["NOMATCH"]


def test_minority_on_target_boosted(tmp_path):
    """Community genus present only in a minority cluster => band 3 (depth may recover it)."""
    from specimux_suite.scheduler import confidence_band

    config = _make_config(tmp_path, reprocess_ratio=0.5)
    log = EventLog(tmp_path / "events.jsonl")

    _processed(log, "HOSTPARA",
               clusters=[{"name": "c1", "size": 80}, {"name": "c2", "size": 10}],
               matches=[
                   {"cluster": "c1", "top_hits": [_hit("Saccharomyces cerevisiae", 0.99)]},
                   {"cluster": "c2", "top_hits": [_hit("Amanita muscaria", 0.99)]},
               ],
               taxon="Amanita muscaria")

    state = PipelineState()
    state.rebuild(log)
    band, reason = confidence_band(state.specimens["HOSTPARA"])
    assert (band, reason) == (3, "minority_on_target")

    jobs = Scheduler(config, state).get_ready_jobs()
    assert jobs[0].reason == "minority_on_target"


def test_pending_identification_is_neutral(tmp_path):
    """Consensus done but identification not yet landed: neutral band, full gate."""
    config = _make_config(tmp_path, reprocess_ratio=0.5)
    log = EventLog(tmp_path / "events.jsonl")
    c1 = [{"name": "c1", "size": 80}]

    # No identification.completed event: status stays CONSENSUS_DONE
    log.emit("specimen.updated", {"specimen_id": "PENDING", "pool": "p1", "total_reads": 100})
    log.emit("consensus.started", {"specimen_id": "PENDING", "job_id": "j1", "read_count": 100})
    log.emit("consensus.completed", {"specimen_id": "PENDING", "job_id": "j1", "clusters": c1})
    log.emit("specimen.updated", {"specimen_id": "PENDING", "pool": "p1", "total_reads": 130})

    state = PipelineState()
    state.rebuild(log)

    # ratio 0.3 < full gate 0.5: not eligible (pending must not get the softened gate)
    assert Scheduler(config, state).get_ready_jobs() == []

    # At ratio 0.6 it becomes eligible with the neutral reason
    log.emit("specimen.updated", {"specimen_id": "PENDING", "pool": "p1", "total_reads": 160})
    state = PipelineState()
    state.rebuild(log)
    jobs = Scheduler(config, state).get_ready_jobs()
    assert [j.reason for j in jobs] == ["pending"]


def test_new_specimens_and_watched_outrank_bands(tmp_path):
    """Tier order preserved: watched > never-processed > any reprocess band."""
    config = _make_config(tmp_path, reprocess_ratio=0.5)
    log = EventLog(tmp_path / "events.jsonl")
    c1 = [{"name": "c1", "size": 80}]

    _processed(log, "NOMATCH", clusters=c1,
               matches=[{"cluster": "c1", "top_hits": []}])
    log.emit("specimen.updated", {"specimen_id": "NEW", "pool": "p1", "total_reads": 40})
    _processed(log, "STARRED", clusters=c1,
               matches=[{"cluster": "c1", "top_hits": [_hit("Amanita muscaria", 0.995)]}],
               taxon="Amanita muscaria")
    log.emit("specimen.watched", {"specimen_id": "STARRED", "watched": True})

    state = PipelineState()
    state.rebuild(log)
    jobs = Scheduler(config, state).get_ready_jobs()

    assert [j.specimen_id for j in jobs] == ["STARRED", "NEW", "NOMATCH"]
