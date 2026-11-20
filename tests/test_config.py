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


def test_run_settings_are_read_per_tool():
    import pytest
    from specimux_suite.config import parse_tool_settings

    assert parse_tool_settings([]) == {"specimux": {}, "speconsense": {}, "summarize": {}}
    assert parse_tool_settings([
        "summarize.merge-position-count=5", "summarize.fasta-fields=ric,snp",
        "summarize.select-max-variants=-1", "speconsense.presample=500",
        "specimux.index-edit-distance=3", "speconsense.--disable-position-phasing=true",
        "summarize.merge-position-count=4",          # the later one wins
    ]) == {
        "specimux": {"index-edit-distance": "3"},
        "speconsense": {"presample": "500", "disable-position-phasing": True},
        "summarize": {"merge-position-count": "4", "fasta-fields": "ric,snp", "select-max-variants": "-1"},
    }
    for bad in ("min-len=150", "summarize.min-len", "summarize.=3", "identify.vsearch-min-identity=0.9",
                "summarize.min-len=", "summarize.min-len= "):
        with pytest.raises(ValueError):
            parse_tool_settings([bad])


def test_set_on_the_command_line_goes_over_the_profile(tmp_path, monkeypatch):
    """--set is one run's own settings: over the profile's, under the
    pass-through arguments."""
    import sys
    from unittest.mock import patch
    from specimux_suite import cli

    monkeypatch.setenv("XDG_CONFIG_HOME", str(tmp_path / "config"))
    monkeypatch.setattr("specimux_suite.profiles.PROFILES_DIR", tmp_path / "profiles")
    (tmp_path / "profiles").mkdir()
    (tmp_path / "profiles" / "lab.yaml").write_text(
        "specimux-suite-version: '*'\n"
        "summarize:\n  profile: herbarium\n  merge-position-count: 2\n  min-len: 150\n"
        "speconsense:\n  profile: herbarium\n  presample: 1000\n")
    monkeypatch.setattr(sys, "argv", [
        "specimux-suite", "batch", "p.fasta", "s.tsv", "r.fastq", "-o", str(tmp_path / "out"),
        "--no-web", "--inat-background", "-p", "lab",
        "--set", "summarize.merge-position-count=5", "--set", "speconsense.presample=500",
        "--set", "specimux.index-edit-distance=3", "--set", "summarize.profile=default"])
    with patch("specimux_suite.pipeline.Pipeline") as MockPipeline:
        cli.main()
    config = MockPipeline.call_args[0][0]
    assert config.summarize_overrides == {"merge-position-count": "5", "min-len": 150}
    assert config.speconsense_overrides == {"presample": "500"}
    assert config.specimux_overrides == {"index-edit-distance": "3"}
    # a tool's profile is its -p, as in the suite profile, never a second
    # --profile override: the NS/LQ preview reads summarize_profile
    assert config.summarize_profile == "default"
    assert config.speconsense_profile == "herbarium"

    monkeypatch.setattr(sys, "argv", ["specimux-suite", "batch", "p.fasta", "s.tsv", "r.fastq",
                                      "--set", "summarize.min-len"])
    import pytest
    with pytest.raises(SystemExit):
        cli.main()
