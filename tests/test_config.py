"""Tests for PipelineConfig, focused on summarize-filter threshold resolution."""

import importlib.util
from pathlib import Path

import pytest

from specimux_suite.config import (
    PipelineConfig,
    SUMMARIZE_DEFAULT_MAX_ERR_FACTOR,
    SUMMARIZE_DEFAULT_MIN_CER_FACTOR,
)


def _make_config(tmp_path, **kwargs):
    defaults = dict(
        primers_file=tmp_path / "primers.fasta",
        specimens_file=tmp_path / "specimens.tsv",
        output_dir=tmp_path / "output",
    )
    defaults.update(kwargs)
    return PipelineConfig(**defaults)


def test_summarize_thresholds_default(tmp_path):
    cfg = _make_config(tmp_path)
    t = cfg.resolve_summarize_thresholds()
    assert t["min_cer_factor"] == SUMMARIZE_DEFAULT_MIN_CER_FACTOR
    assert t["max_err_factor"] == SUMMARIZE_DEFAULT_MAX_ERR_FACTOR


def test_summarize_thresholds_from_overrides(tmp_path):
    cfg = _make_config(
        tmp_path,
        summarize_overrides={"min-cer-factor": 0, "max-err-factor": 2.5},
    )
    t = cfg.resolve_summarize_thresholds()
    assert t["min_cer_factor"] == 0.0
    assert t["max_err_factor"] == 2.5


def test_summarize_thresholds_from_overrides_underscore_keys(tmp_path):
    cfg = _make_config(tmp_path, summarize_overrides={"min_cer_factor": 1.2})
    t = cfg.resolve_summarize_thresholds()
    assert t["min_cer_factor"] == 1.2


def test_summarize_thresholds_args_override_everything(tmp_path):
    # --summarize-args is the highest-precedence escape hatch.
    cfg = _make_config(
        tmp_path,
        summarize_overrides={"min-cer-factor": 1.0},
        summarize_args=["--min-cer-factor", "0", "--max-err-factor=3.0"],
    )
    t = cfg.resolve_summarize_thresholds()
    assert t["min_cer_factor"] == 0.0
    assert t["max_err_factor"] == 3.0


def test_summarize_thresholds_in_config_summary(tmp_path):
    cfg = _make_config(tmp_path)
    summary = cfg.summary()
    assert "summarize_filter" in summary
    assert summary["summarize_filter"]["min_cer_factor"] == SUMMARIZE_DEFAULT_MIN_CER_FACTOR


def _herbarium_profile_available() -> bool:
    if (Path.home() / ".config" / "speconsense" / "profiles" / "herbarium.yaml").is_file():
        return True
    spec = importlib.util.find_spec("speconsense")
    if spec and spec.submodule_search_locations:
        for loc in spec.submodule_search_locations:
            if (Path(loc) / "profiles" / "herbarium.yaml").is_file():
                return True
    return False


@pytest.mark.skipif(
    not _herbarium_profile_available(),
    reason="speconsense herbarium profile not installed",
)
def test_summarize_thresholds_from_profile(tmp_path):
    # The bundled herbarium profile disables both filters (sets them to 0).
    cfg = _make_config(tmp_path, summarize_profile="herbarium")
    t = cfg.resolve_summarize_thresholds()
    assert t["min_cer_factor"] == 0.0
    assert t["max_err_factor"] == 0.0


def test_summarize_thresholds_unknown_profile_falls_back(tmp_path):
    cfg = _make_config(tmp_path, summarize_profile="no-such-profile-xyz")
    t = cfg.resolve_summarize_thresholds()
    assert t["min_cer_factor"] == SUMMARIZE_DEFAULT_MIN_CER_FACTOR
    assert t["max_err_factor"] == SUMMARIZE_DEFAULT_MAX_ERR_FACTOR


def test_filter_chimeras_default_off(tmp_path):
    cfg = _make_config(tmp_path)
    assert cfg.resolve_summarize_thresholds()["filter_chimeras"] is False


def test_filter_chimeras_from_overrides_and_args(tmp_path):
    cfg = _make_config(tmp_path, summarize_overrides={"filter-chimeras": True})
    assert cfg.resolve_summarize_thresholds()["filter_chimeras"] is True

    cfg = _make_config(tmp_path, summarize_args=["--filter-chimeras"])
    assert cfg.resolve_summarize_thresholds()["filter_chimeras"] is True
