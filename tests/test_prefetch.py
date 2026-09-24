"""Blocking iNat startup prefetch (default) vs the background fetch path."""

import threading
import time
from unittest.mock import patch

from specimux_suite.config import PipelineConfig
from specimux_suite.events import EventLog


def _make_config(tmp_path, **kwargs):
    defaults = dict(
        primers_file=tmp_path / "primers.fasta",
        specimens_file=tmp_path / "specimens.tsv",
        reads_file=tmp_path / "reads.fastq",
        output_dir=tmp_path / "output",
    )
    defaults.update(kwargs)
    return PipelineConfig(**defaults)


def _write_specimens(tmp_path):
    (tmp_path / "specimens.tsv").write_text(
        "SampleID\tPrimerPool\n"
        "specA--iNat111\tp1\n"
        "specB--iNat222\tp1\n"
    )


def _make_pipeline(config):
    with patch("specimux_suite.pipeline.SpecimuxRunner"), \
         patch("specimux_suite.pipeline.SpeconsenseRunner"), \
         patch("specimux_suite.pipeline.SummarizeRunner"), \
         patch("specimux_suite.pipeline._check_tool_on_path", return_value=True):
        from specimux_suite.pipeline import Pipeline
        return Pipeline(config)


def test_blocking_prefetch_fetches_synchronously(tmp_path):
    """Default mode: prefetch_inat runs taxa + audit inline, hands photos to
    a background thread (the UIs fall back to iNat URLs for cache misses),
    and _load_specimens never spawns the background fetch thread."""
    _write_specimens(tmp_path)
    config = _make_config(tmp_path)
    assert config.inat_blocking  # the default
    pipeline = _make_pipeline(config)
    try:
        taxa = {"specA--iNat111": {"name": "Amanita x", "genus": "Amanita"}}
        photos_called = threading.Event()
        with patch("specimux_suite.pipeline.fetch_community_taxa",
                   return_value=taxa) as fetch, \
             patch("specimux_suite.pipeline.run_inat_check") as check, \
             patch("specimux_suite.pipeline.prefetch_photos",
                   side_effect=lambda *a, **k: photos_called.set()):
            pipeline.prefetch_inat(show_progress=False)
            assert photos_called.wait(timeout=5.0)
        assert fetch.call_count == 1
        assert fetch.call_args.args[0] == {"specA--iNat111": "111",
                                           "specB--iNat222": "222"}
        assert check.called
        events = [e.type for e in pipeline.event_log.replay()]
        assert events.count("specimens.loaded") == 1
        assert "specimens.taxa" in events
        # run_batch/run_live re-invoke _load_specimens — must be a no-op
        pipeline._load_specimens()
        events = [e.type for e in pipeline.event_log.replay()]
        assert events.count("specimens.loaded") == 1
    finally:
        pipeline._shutdown.set()
        pipeline._executor.shutdown(wait=False)


def test_blocking_prefetch_applies_corrections(tmp_path):
    """The prefetch queries admin-corrected observation ids, same as the
    background path — a restart must not resurrect a mistyped id."""
    _write_specimens(tmp_path)
    config = _make_config(tmp_path)
    pipeline = _make_pipeline(config)
    try:
        pipeline.event_log.emit("inat.correction", {
            "specimen_id": "specA--iNat111", "old_obs_id": "111",
            "new_obs_id": "999"})
        with patch("specimux_suite.pipeline.fetch_community_taxa",
                   return_value={}) as fetch:
            pipeline.prefetch_inat(show_progress=False)
        assert fetch.call_args.args[0]["specA--iNat111"] == "999"
    finally:
        pipeline._shutdown.set()
        pipeline._executor.shutdown(wait=False)


def test_blocking_prefetch_resolves_restart_lineages(tmp_path):
    """Restart-seeded genera are resolved synchronously (taxa.lineage
    emitted) before the background lineage thread starts."""
    config = _make_config(tmp_path, reference_db=tmp_path / "refs.fasta")
    log = EventLog(config.event_log_path)
    log.emit("consensus.completed", {"specimen_id": "A",
                                     "clusters": [{"name": "c1", "size": 30}]})
    log.emit("identification.completed", {
        "specimen_id": "A", "consensus_version": 1,
        "matches": [{"cluster": "c1", "top_hits": [
            {"ref_id": "Russula x", "name": "Russula x",
             "identity": 0.99, "adjusted_identity": 0.99}]}]})

    with patch("specimux_suite.pipeline.IdentifyRunner"):
        pipeline = _make_pipeline(config)
    try:
        assert not pipeline._lineage_thread_started
        lineages = {"Russula": [{"id": 1, "rank": "genus", "name": "Russula"}]}
        with patch("specimux_suite.pipeline.fetch_community_taxa", return_value={}), \
             patch("specimux_suite.pipeline.fetch_genus_lineages",
                   return_value=lineages) as fetch:
            pipeline.prefetch_inat(show_progress=False)
        assert fetch.call_args.args[0] == ["Russula"]
        assert pipeline._lineage_queue.empty()
        assert pipeline._lineage_thread_started
        assert any(e.type == "taxa.lineage" for e in pipeline.event_log.replay())
    finally:
        pipeline._shutdown.set()
        pipeline._executor.shutdown(wait=False)


def test_background_mode_spawns_fetch_thread(tmp_path):
    """--inat-background restores the fetch-while-running behavior."""
    _write_specimens(tmp_path)
    config = _make_config(tmp_path, inat_blocking=False)
    pipeline = _make_pipeline(config)
    try:
        fetched = threading.Event()
        with patch.object(type(pipeline), "_fetch_inat_taxa",
                          side_effect=lambda ids: fetched.set()):
            pipeline._load_specimens()
            assert fetched.wait(timeout=5.0)
    finally:
        pipeline._shutdown.set()
        pipeline._executor.shutdown(wait=False)


def test_no_photo_cache_fetches_taxa_but_no_photos(tmp_path):
    """--no-photo-cache (a hosted dashboard): field IDs are fetched as
    usual, photos are never downloaded (blocking or background path), no
    cache link is made in the mirror, and config_summary tells the pages."""
    _write_specimens(tmp_path)
    for blocking in (True, False):
        out = tmp_path / f"out-{blocking}"
        config = _make_config(tmp_path, output_dir=out, mirror_dir=tmp_path / f"mirror-{blocking}",
                              inat_blocking=blocking, photo_cache=False)
        assert config.summary()["photo_cache"] is False
        pipeline = _make_pipeline(config)
        try:
            taxa = {"specA--iNat111": {"name": "Amanita x", "genus": "Amanita"}}
            with patch("specimux_suite.pipeline.fetch_community_taxa", return_value=taxa), \
                 patch("specimux_suite.pipeline.run_inat_check"), \
                 patch("specimux_suite.pipeline.prefetch_photos") as photos:
                if blocking:
                    pipeline.prefetch_inat(show_progress=False)
                else:
                    pipeline._fetch_inat_taxa({"specA--iNat111": "111"})
                time.sleep(0.2)                      # a stray photo thread would have run
            # (other tests' photo threads may still be running and call the
            # patched function for their own output dirs: only ours count)
            assert not [c for c in photos.call_args_list if str(out) in str(c.args[1:2])]
            assert "specimens.taxa" in [e.type for e in pipeline.event_log.replay()]
            assert not (out / "inat_photos").exists()
        finally:
            pipeline._shutdown.set()
            pipeline._executor.shutdown(wait=False)
    assert _make_config(tmp_path).summary()["photo_cache"] is True    # the default
