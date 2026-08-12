"""Tests for pipeline orchestration (unit-level, mocked runners)."""

from pathlib import Path
from unittest.mock import patch, MagicMock

from specimux_suite.config import PipelineConfig
from specimux_suite.events import EventLog
from specimux_suite.scheduler import ConsensusJob
from specimux_suite.state import PipelineState


def _make_config(tmp_path, **kwargs):
    defaults = dict(
        primers_file=tmp_path / "primers.fasta",
        specimens_file=tmp_path / "specimens.tsv",
        reads_file=tmp_path / "reads.fastq",
        output_dir=tmp_path / "output",
    )
    defaults.update(kwargs)
    return PipelineConfig(**defaults)


def test_batch_pipeline_emits_started_event(tmp_path):
    """Verify batch pipeline emits pipeline.started event."""
    config = _make_config(tmp_path)
    log = EventLog(config.event_log_path)

    # Create dummy reads file
    (tmp_path / "reads.fastq").write_text("@r1\nACGT\n+\nIIII\n")

    with patch("specimux_suite.pipeline.SpecimuxRunner") as MockRunner, \
         patch("specimux_suite.pipeline._check_tool_on_path", return_value=True):
        mock_instance = MockRunner.return_value
        mock_instance.run.return_value = {}

        from specimux_suite.pipeline import Pipeline
        pipeline = Pipeline(config)
        pipeline.specimux = mock_instance
        pipeline.run_batch()

    events = list(log.replay())
    assert any(e.type == "pipeline.started" for e in events)
    started = next(e for e in events if e.type == "pipeline.started")
    assert started.data["mode"] == "batch"


def test_submit_consensus_skips_inflight(tmp_path):
    """_submit_consensus should not resubmit a specimen already in _futures."""
    config = _make_config(tmp_path)
    (tmp_path / "reads.fastq").write_text("@r1\nACGT\n+\nIIII\n")

    with patch("specimux_suite.pipeline.SpecimuxRunner"), \
         patch("specimux_suite.pipeline.SpeconsenseRunner"), \
         patch("specimux_suite.pipeline._check_tool_on_path", return_value=True):
        from specimux_suite.pipeline import Pipeline
        pipeline = Pipeline(config)

        # Set up a specimen in state
        pipeline.state.get_specimen("A").pool = "p1"
        pipeline.state.get_specimen("A").total_reads = 100

        # Create a dummy fastq so _find_specimen_fastq works
        full_dir = config.specimux_output_dir / "full" / "p1"
        full_dir.mkdir(parents=True)
        (full_dir / "A.fastq").write_text("@r1\nACGT\n+\nIIII\n")

        # First submit should work
        pipeline._submit_consensus("A")
        assert "A" in pipeline._futures
        first_future = pipeline._futures["A"]

        # Second submit should be a no-op (same future retained)
        pipeline._submit_consensus("A")
        assert pipeline._futures["A"] is first_future

        pipeline._executor.shutdown(wait=False)


def test_validate_tools_reports_missing(tmp_path):
    """validate_tools should report tools not on PATH."""
    config = _make_config(tmp_path)

    from specimux_suite.pipeline import Pipeline
    pipeline = Pipeline(config)

    with patch("specimux_suite.pipeline._check_tool_on_path", return_value=False):
        missing = pipeline.validate_tools()
    assert "specimux" in missing
    assert "speconsense" in missing


def test_validate_tools_includes_vsearch_when_ref_db(tmp_path):
    """validate_tools should check vsearch when reference_db is set."""
    config = _make_config(tmp_path, reference_db=tmp_path / "refs.fasta")

    from specimux_suite.pipeline import Pipeline
    pipeline = Pipeline(config)

    with patch("specimux_suite.pipeline._check_tool_on_path", return_value=False):
        missing = pipeline.validate_tools()
    assert "vsearch" in missing


def test_finalization_calls_get_all_eligible_jobs(tmp_path):
    """_run_finalization uses get_all_eligible_jobs, not get_ready_jobs."""
    config = _make_config(tmp_path)

    with patch("specimux_suite.pipeline.SpecimuxRunner"), \
         patch("specimux_suite.pipeline.SpeconsenseRunner"), \
         patch("specimux_suite.pipeline._check_tool_on_path", return_value=True):
        from specimux_suite.pipeline import Pipeline
        pipeline = Pipeline(config)

        # Mock scheduler to track which method is called
        pipeline.scheduler.get_all_eligible_jobs = MagicMock(return_value=[])
        pipeline.scheduler.get_ready_jobs = MagicMock(return_value=[])

        pipeline._run_finalization()

        pipeline.scheduler.get_all_eligible_jobs.assert_called_once_with(max_jobs=None, min_reads=0)
        pipeline.scheduler.get_ready_jobs.assert_not_called()

        pipeline._executor.shutdown(wait=False)


def test_summarize_round_waits_for_inflight_identification(tmp_path):
    """Specimens whose identification is still running when the consensus round
    ends must not be skipped by the summarize round (regression: the last
    specimens to finish consensus were silently excluded from summarization)."""
    import time

    config = _make_config(tmp_path)
    (tmp_path / "reads.fastq").write_text("@r1\nACGT\n+\nIIII\n")

    with patch("specimux_suite.pipeline._check_tool_on_path", return_value=True):
        from specimux_suite.pipeline import Pipeline
        pipeline = Pipeline(config)

    log = pipeline.event_log

    # Specimen finished consensus; identification still in flight
    log.emit("consensus.completed", {
        "specimen_id": "specimen_A", "job_id": "c1",
        "clusters": [{"name": "specimen_A-c0", "size": 90}],
    })

    def slow_identify():
        time.sleep(0.3)
        log.emit("identification.completed", {
            "specimen_id": "specimen_A",
            "consensus_version": 1,
            "matches": [{"cluster": "specimen_A-c0",
                         "top_hits": [{"ref_id": "r1", "name": "Hit",
                                       "identity": 0.99, "adjusted_identity": 0.99}]}],
        })

    fut = pipeline._executor.submit(slow_identify)
    pipeline._id_futures["specimen_A"] = fut
    fut.add_done_callback(
        lambda f: pipeline._on_identification_done("specimen_A", f))

    summarized = []
    pipeline.summarize = MagicMock()
    pipeline.summarize.run.side_effect = lambda sid: (
        summarized.append(sid),
        log.emit("summarize.completed", {"specimen_id": sid, "variants": []}),
    ) and []

    pipeline._run_summarize_round()

    assert summarized == ["specimen_A"]
    pipeline.summarize.run_aggregate.assert_called_once()
    pipeline._executor.shutdown(wait=True)


def _quiet_pipeline(tmp_path):
    """Construct a Pipeline with tool checks patched out."""
    config = _make_config(tmp_path)
    (tmp_path / "reads.fastq").write_text("@r1\nACGT\n+\nIIII\n")
    with patch("specimux_suite.pipeline._check_tool_on_path", return_value=True):
        from specimux_suite.pipeline import Pipeline
        return Pipeline(config)


def test_quit_is_responsive_during_long_job(tmp_path):
    """[Q] must be processed within ~a tick even while a job runs for minutes
    (regression: cmd queue was only drained after a future completed)."""
    import time

    pipeline = _quiet_pipeline(tmp_path)
    stop = __import__("threading").Event()
    pipeline._futures["slow_specimen"] = pipeline._executor.submit(stop.wait, 30)

    pipeline.cmd_queue.put("quit")
    start = time.monotonic()
    pipeline._wait_for_any_future()
    elapsed = time.monotonic() - start

    assert pipeline._shutdown.is_set()
    assert elapsed < 5  # not the 30s the job would take
    stop.set()
    pipeline._executor.shutdown(wait=True)


def test_wait_all_futures_returns_early_on_quit(tmp_path):
    """Drain-before-specimux must abort promptly when quit is pressed."""
    import time

    pipeline = _quiet_pipeline(tmp_path)
    stop = __import__("threading").Event()
    pipeline._futures["slow_specimen"] = pipeline._executor.submit(stop.wait, 30)

    pipeline.cmd_queue.put("quit")
    start = time.monotonic()
    completed = pipeline._wait_all_futures()
    elapsed = time.monotonic() - start

    assert pipeline._shutdown.is_set()
    assert elapsed < 5
    assert completed == []
    # Unfinished job stays tracked so it cannot be double-submitted
    assert "slow_specimen" in pipeline._futures
    stop.set()
    pipeline._executor.shutdown(wait=True)


def test_summarize_round_skipped_after_quit(tmp_path):
    """After quit, no summarize work (including aggregate) should start."""
    pipeline = _quiet_pipeline(tmp_path)
    pipeline.event_log.emit("consensus.completed", {
        "specimen_id": "specimen_A", "job_id": "c1",
        "clusters": [{"name": "specimen_A-c0", "size": 90}],
    })
    pipeline.event_log.emit("identification.completed", {
        "specimen_id": "specimen_A", "consensus_version": 1,
        "matches": [{"cluster": "specimen_A-c0",
                     "top_hits": [{"ref_id": "r", "name": "n",
                                   "identity": 0.99, "adjusted_identity": 0.99}]}],
    })
    pipeline.summarize = MagicMock()
    pipeline._shutdown.set()

    pipeline._run_summarize_round()

    pipeline.summarize.run.assert_not_called()
    pipeline.summarize.run_aggregate.assert_not_called()
    pipeline._executor.shutdown(wait=True)


def test_drain_cmd_queue_requeues_finalize(tmp_path):
    """finalize must survive _drain_cmd_queue for the live main loop to handle."""
    pipeline = _quiet_pipeline(tmp_path)
    pipeline.cmd_queue.put("finalize")
    pipeline.cmd_queue.put("quit")

    pipeline._drain_cmd_queue()

    assert pipeline._shutdown.is_set()
    assert pipeline.cmd_queue.get_nowait() == "finalize"
    pipeline._executor.shutdown(wait=True)
