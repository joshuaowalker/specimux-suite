"""Tests for pipeline orchestration (unit-level, mocked runners)."""

from pathlib import Path
from unittest.mock import patch, MagicMock

from specimux_suite.config import PipelineConfig
from specimux_suite.events import EventLog
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

    with patch("specimux_suite.pipeline.SpecimuxRunner") as MockRunner:
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
