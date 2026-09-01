"""Restart recovery: a rebuilt state must re-enter the pipeline cleanly.

Every stage transition is driven by in-process callbacks, so a killed run
strands specimens in whatever status its events reached. Three healing
paths cover the three stranding classes:

1. ``PipelineState.rebuild`` demotes phantom CONSENSUS_RUNNING specimens
   (killed mid-consensus) to the status their data implies — both
   scheduler paths skip "running", so a phantom would otherwise be stuck
   forever and displayed as processing.
2. ``_submit_stranded_identifications`` (live startup + batch rounds)
   identifies CONSENSUS_DONE specimens whose identification never ran.
3. The incremental summarize lane is seeded at startup with identified/
   no_match specimens whose summary is missing or stale.
"""

from unittest.mock import MagicMock, patch

from specimux_suite.config import PipelineConfig
from specimux_suite.events import EventLog
from specimux_suite.state import PipelineState, SpecimenStatus


def _make_config(tmp_path, **kwargs):
    defaults = dict(
        primers_file=tmp_path / "primers.fasta",
        specimens_file=tmp_path / "specimens.tsv",
        reads_file=tmp_path / "reads.fastq",
        output_dir=tmp_path / "output",
    )
    defaults.update(kwargs)
    return PipelineConfig(**defaults)


CLUSTERS = [{"name": "c1", "size": 30}]
HITS = [{"cluster": "c1", "top_hits": [
    {"ref_id": "Amanita x", "name": "Amanita x",
     "identity": 0.99, "adjusted_identity": 0.99}]}]
NO_HITS = [{"cluster": "c1", "top_hits": []}]


def _emit_interrupted_run(log: EventLog):
    """A run killed with specimens in every mid-flight shape."""
    # Killed during first-ever consensus: no completed generation exists
    log.emit("specimux.completed", {"specimens": {"fresh": 40}})
    log.emit("consensus.started", {"specimen_id": "fresh"})
    # Reprocess killed mid-consensus; previous generation was identified
    log.emit("specimux.completed", {"specimens": {"reident": 50}})
    log.emit("consensus.completed", {"specimen_id": "reident", "clusters": CLUSTERS})
    log.emit("identification.completed",
             {"specimen_id": "reident", "matches": HITS, "consensus_version": 1})
    log.emit("consensus.started", {"specimen_id": "reident"})
    # Same, but previous identification had no hits
    log.emit("specimux.completed", {"specimens": {"nomatch": 50}})
    log.emit("consensus.completed", {"specimen_id": "nomatch", "clusters": CLUSTERS})
    log.emit("identification.completed",
             {"specimen_id": "nomatch", "matches": NO_HITS, "consensus_version": 1})
    log.emit("consensus.started", {"specimen_id": "nomatch"})
    # Same, but previous generation was fully summarized
    log.emit("specimux.completed", {"specimens": {"summ": 50}})
    log.emit("consensus.completed", {"specimen_id": "summ", "clusters": CLUSTERS})
    log.emit("identification.completed",
             {"specimen_id": "summ", "matches": HITS, "consensus_version": 1})
    log.emit("summarize.completed",
             {"specimen_id": "summ", "variants": [], "consensus_version": 1})
    log.emit("consensus.started", {"specimen_id": "summ"})
    # Killed between consensus and identification (class B)
    log.emit("specimux.completed", {"specimens": {"stranded_ident": 60}})
    log.emit("consensus.completed",
             {"specimen_id": "stranded_ident", "clusters": CLUSTERS})
    # Identified but killed before its incremental summarize (class C)
    log.emit("specimux.completed", {"specimens": {"stranded_summ": 60}})
    log.emit("consensus.completed",
             {"specimen_id": "stranded_summ", "clusters": CLUSTERS})
    log.emit("identification.completed",
             {"specimen_id": "stranded_summ", "matches": HITS,
              "consensus_version": 1})
    # A healthy completed specimen — must be untouched by healing
    log.emit("specimux.completed", {"specimens": {"healthy": 70}})
    log.emit("consensus.completed", {"specimen_id": "healthy", "clusters": CLUSTERS})
    log.emit("identification.completed",
             {"specimen_id": "healthy", "matches": HITS, "consensus_version": 1})
    log.emit("summarize.completed",
             {"specimen_id": "healthy", "variants": [], "consensus_version": 1})


def test_rebuild_heals_phantom_consensus_running(tmp_path):
    log = EventLog(tmp_path / "events.jsonl")
    _emit_interrupted_run(log)

    state = PipelineState()
    state.rebuild(log)

    assert state.specimens["fresh"].status == SpecimenStatus.WAITING
    assert state.specimens["reident"].status == SpecimenStatus.IDENTIFIED
    assert state.specimens["nomatch"].status == SpecimenStatus.NO_MATCH
    assert state.specimens["summ"].status == SpecimenStatus.SUMMARIZED
    assert state.specimens["healthy"].status == SpecimenStatus.SUMMARIZED
    assert all(s.status != SpecimenStatus.CONSENSUS_RUNNING
               for s in state.specimens.values())
    # The healed fresh specimen is schedulable again
    from specimux_suite.scheduler import Scheduler
    config = _make_config(tmp_path, min_reads=10)
    jobs = Scheduler(config, state).get_ready_jobs()
    assert "fresh" in {j.specimen_id for j in jobs}


def test_normalization_only_runs_on_rebuild(tmp_path):
    """During live operation consensus.started must still mean running."""
    log = EventLog(tmp_path / "events.jsonl")
    state = PipelineState()
    state.rebuild(log)
    log.add_listener(state.apply)
    log.emit("specimux.completed", {"specimens": {"A": 40}})
    log.emit("consensus.started", {"specimen_id": "A"})
    assert state.specimens["A"].status == SpecimenStatus.CONSENSUS_RUNNING


def test_startup_sweep_identifies_stranded_specimens(tmp_path):
    config = _make_config(tmp_path, reference_db=tmp_path / "refs.fasta")
    log = EventLog(config.event_log_path)
    _emit_interrupted_run(log)

    with patch("specimux_suite.pipeline.SpecimuxRunner"), \
         patch("specimux_suite.pipeline.SpeconsenseRunner"), \
         patch("specimux_suite.pipeline.IdentifyRunner"), \
         patch("specimux_suite.pipeline.SummarizeRunner"), \
         patch("specimux_suite.pipeline._check_tool_on_path", return_value=True):
        from specimux_suite.pipeline import Pipeline
        pipeline = Pipeline(config)
        try:
            pipeline.speconsense.get_consensus_fasta = MagicMock(
                return_value=tmp_path / "consensus.fasta")
            pipeline._submit_stranded_identifications()
            assert "stranded_ident" in pipeline._id_futures
            # Specimens with identification (any generation) are not re-run
            assert "stranded_summ" not in pipeline._id_futures
            assert "healthy" not in pipeline._id_futures
        finally:
            pipeline._shutdown.set()
            pipeline._executor.shutdown(wait=False)


def test_summarize_lane_seeded_from_rebuilt_state(tmp_path):
    config = _make_config(tmp_path)
    log = EventLog(config.event_log_path)
    _emit_interrupted_run(log)

    with patch("specimux_suite.pipeline.SpecimuxRunner"), \
         patch("specimux_suite.pipeline.SpeconsenseRunner"), \
         patch("specimux_suite.pipeline.SummarizeRunner"), \
         patch("specimux_suite.pipeline._check_tool_on_path", return_value=True), \
         patch("specimux_suite.pipeline.Pipeline._maybe_queue_incremental_summarize") as queued:
        from specimux_suite.pipeline import Pipeline
        pipeline = Pipeline(config)
        try:
            seeded = {c.args[0] for c in queued.call_args_list}
            # Identified/no_match with missing or stale summaries are seeded
            # (_maybe_queue itself filters on status, exercised elsewhere);
            # fully-summarized specimens are not
            assert "stranded_summ" in seeded
            assert "reident" in seeded
            assert "healthy" not in seeded
            assert "summ" not in seeded
        finally:
            pipeline._shutdown.set()
            pipeline._executor.shutdown(wait=False)


def test_no_seeding_when_incremental_disabled(tmp_path):
    config = _make_config(tmp_path, incremental_summarize=False)
    log = EventLog(config.event_log_path)
    _emit_interrupted_run(log)

    with patch("specimux_suite.pipeline.SpecimuxRunner"), \
         patch("specimux_suite.pipeline.SpeconsenseRunner"), \
         patch("specimux_suite.pipeline.SummarizeRunner"), \
         patch("specimux_suite.pipeline._check_tool_on_path", return_value=True), \
         patch("specimux_suite.pipeline.Pipeline._maybe_queue_incremental_summarize") as queued:
        from specimux_suite.pipeline import Pipeline
        pipeline = Pipeline(config)
        try:
            assert not queued.called
        finally:
            pipeline._shutdown.set()
            pipeline._executor.shutdown(wait=False)
