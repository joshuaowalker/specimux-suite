"""Plugins: loaded by name, started with the run, shut down with it."""

from unittest.mock import patch

import pytest

from specimux_suite.config import PipelineConfig
from specimux_suite.plugins import PluginContext, load_plugin, parse_options, resolve_factory


class Recorder:
    """A plugin that records what it was given."""
    instances = []

    def __init__(self, options):
        self.options = options
        self.context = None
        self.stopped = False
        Recorder.instances.append(self)

    def start(self, context):
        self.context = context

    def shutdown(self):
        self.stopped = True


def recorder_factory(options):
    return Recorder(options)


class Broken:
    def __init__(self, options):
        pass

    def start(self, context):
        raise RuntimeError("boom")

    def shutdown(self):
        raise RuntimeError("boom again")


def test_load_by_module_path_and_options():
    plugin = load_plugin("test_plugins:recorder_factory", {"a": "1"})
    assert isinstance(plugin, Recorder) and plugin.options == {"a": "1"}
    with pytest.raises(ValueError, match="no 'nope'"):
        resolve_factory("test_plugins:nope")
    with pytest.raises(ValueError, match="Unknown plugin"):
        resolve_factory("not-registered-anywhere")


def test_load_by_entry_point():
    """The suite registers its own forwarder under the group."""
    from specimux_suite.forward import EventForwarder
    plugin = load_plugin("forward", {"forward_url": "http://127.0.0.1:1/ingest",
                                     "forward_header": "Authorization: Bearer x"})
    assert isinstance(plugin, EventForwarder)
    assert plugin.headers == {"Authorization": "Bearer x"}
    with pytest.raises(ValueError, match="forward_url"):
        load_plugin("forward", {})


def test_plugin_must_have_the_hooks():
    with pytest.raises(ValueError, match="no start"):
        load_plugin("builtins:dict", {})


def test_parse_options():
    assert parse_options(["a=1", "b=x=y", "c="]) == {"a": "1", "b": "x=y", "c": ""}
    assert parse_options(None) == {}
    with pytest.raises(ValueError):
        parse_options(["novalue"])


def _config(tmp_path):
    return PipelineConfig(primers_file=tmp_path / "primers.fasta",
                          specimens_file=tmp_path / "specimens.tsv",
                          reads_file=tmp_path / "reads.fastq",
                          output_dir=tmp_path / "output", workers=1)


def test_pipeline_starts_and_stops_plugins_around_a_run(tmp_path):
    config = _config(tmp_path)
    (tmp_path / "reads.fastq").write_text("@r1\nACGT\n+\nIIII\n")
    Recorder.instances.clear()
    with patch("specimux_suite.pipeline.SpecimuxRunner") as MockRunner, \
         patch("specimux_suite.pipeline._check_tool_on_path", return_value=True):
        MockRunner.return_value.run.return_value = {}
        from specimux_suite.pipeline import Pipeline
        pipeline = Pipeline(config)
        pipeline.specimux = MockRunner.return_value
        good = load_plugin("test_plugins:recorder_factory", {"k": "v"})
        pipeline.attach_plugin(Broken({}))   # a failing plugin never takes the run down
        pipeline.attach_plugin(good)
        pipeline.run_batch()

    ctx = good.context
    assert isinstance(ctx, PluginContext)
    assert ctx.event_log is pipeline.event_log
    assert ctx.state is pipeline.state
    assert ctx.commands is pipeline.commands
    assert ctx.config is config and ctx.output_dir == config.output_dir
    assert good.stopped
    assert pipeline._plugins_started == []
    # the plugin was running before pipeline.started was emitted
    assert any(e.type == "pipeline.started" for e in pipeline.event_log.replay())
