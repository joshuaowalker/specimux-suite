"""Tests for runner command construction."""

from pathlib import Path

from specimux_suite.config import PipelineConfig
from specimux_suite.events import EventLog
from specimux_suite.runners.specimux_runner import SpecimuxRunner
from specimux_suite.runners.speconsense_runner import SpeconsenseRunner, CLUSTER_HEADER_RE


def _make_config(tmp_path, **kwargs):
    defaults = dict(
        primers_file=tmp_path / "primers.fasta",
        specimens_file=tmp_path / "specimens.tsv",
        output_dir=tmp_path / "output",
    )
    defaults.update(kwargs)
    return PipelineConfig(**defaults)


def test_parse_specimens_file(tmp_path):
    """parse_specimens_file should extract SampleID and PrimerPool."""
    from specimux_suite.util import parse_specimens_file

    index = tmp_path / "Index.txt"
    index.write_text(
        "SampleID\tPrimerPool\tFwIndex\tFwPrimer\tRvIndex\tRvPrimer\n"
        "ONT01.01-A01--iNat233404001\tITS\tAGC\tITS1F\tAAC\tITS4\n"
        "ONT01.02-B01--iNat233404080\tITS\tAGC\tITS1F\tACT\tITS4\n"
    )

    specimens = parse_specimens_file(index)
    assert len(specimens) == 2
    assert specimens[0]["specimen_id"] == "ONT01.01-A01--iNat233404001"
    assert specimens[0]["pool"] == "ITS"
    assert specimens[1]["specimen_id"] == "ONT01.02-B01--iNat233404080"


def test_workers_default_auto(tmp_path):
    """workers=0 should auto-resolve to cpu_count // 2."""
    import os
    config = _make_config(tmp_path, workers=0)
    assert config.workers == max(1, os.cpu_count() // 2)


def test_specimux_command_construction(tmp_path):
    config = _make_config(tmp_path)
    log = EventLog(tmp_path / "events.jsonl")
    runner = SpecimuxRunner(config, log)

    cmd = runner._build_command(
        Path("/data/reads.fastq"),
        config.specimux_output_dir,
    )

    assert cmd[0] == "specimux"
    assert str(config.primers_file) in cmd
    assert str(config.specimens_file) in cmd
    assert "/data/reads.fastq" in cmd
    assert "-F" in cmd
    assert "-O" in cmd


def test_specimux_passthrough_args(tmp_path):
    config = _make_config(tmp_path, specimux_args=["-e", "2", "--trim", "PRIMERS"])
    log = EventLog(tmp_path / "events.jsonl")
    runner = SpecimuxRunner(config, log)

    cmd = runner._build_command(
        Path("/data/reads.fastq"),
        config.specimux_output_dir,
    )

    assert "-e" in cmd
    assert "2" in cmd
    assert "--trim" in cmd
    assert "PRIMERS" in cmd


def test_speconsense_command_construction(tmp_path):
    config = _make_config(tmp_path)
    log = EventLog(tmp_path / "events.jsonl")
    runner = SpeconsenseRunner(config, log)

    cmd = runner._build_command(
        Path("/data/specimen_A.fastq"),
        config.consensus_output_dir / "specimen_A",
    )

    assert cmd[0] == "speconsense"
    assert "/data/specimen_A.fastq" in cmd
    assert "-O" in cmd


def test_cluster_header_parsing():
    """Test regex parsing of speconsense FASTA headers."""
    headers = [
        ">sample-c0 size=120 ric=100 rid=95.5",
        ">sample-c1 size=30 ric=30 rid=92.1 primers=ITS3,ITS4",
        ">sample-c2 size=10",
    ]

    m = CLUSTER_HEADER_RE.match(headers[0])
    assert m
    assert m.group(1) == "sample-c0"
    assert m.group(2) == "120"
    assert m.group(3) == "100"
    assert m.group(4) == "95.5"

    m = CLUSTER_HEADER_RE.match(headers[1])
    assert m
    assert m.group(1) == "sample-c1"
    assert m.group(2) == "30"

    m = CLUSTER_HEADER_RE.match(headers[2])
    assert m
    assert m.group(1) == "sample-c2"
    assert m.group(2) == "10"
    assert m.group(3) is None  # no ric
    assert m.group(4) is None  # no rid


def test_specimux_thread_flag(tmp_path):
    """specimux command should include -t with workers count."""
    config = _make_config(tmp_path, workers=4)
    log = EventLog(tmp_path / "events.jsonl")
    runner = SpecimuxRunner(config, log)

    cmd = runner._build_command(
        Path("/data/reads.fastq"),
        config.specimux_output_dir,
    )

    idx = cmd.index("-t")
    assert cmd[idx + 1] == "4"


def test_name_lookup_from_fasta(tmp_path):
    """IdentifyRunner should parse name= fields from reference FASTA."""
    from specimux_suite.runners.identify_runner import IdentifyRunner

    ref = tmp_path / "refs.fasta"
    ref.write_text(
        '>iNaturalist_28125417_Coprinellus_sp size=1 name="Coprinellus sp. \'radians IN02\'"\n'
        "ACGTACGT\n"
        ">iNaturalist_99999_Russula_emetica size=1\n"
        "TGCATGCA\n"
        '>iNaturalist_189757404_Gymnopilus_sp_IN03 name="= Gymnopilus "sp-IN03""\n'
        "GGGGAAAA\n"
        '>iNaturalist_30000660_thompsonii_IN01 name=""thompsonii-IN01""\n'
        "CCCCTTTT\n"
        '>MycoMap_106681_Agaricus_sp_IN02 name="Agaricus \\"sp-IN02\\""\n'
        "AAAACCCC\n"
    )

    config = _make_config(tmp_path, reference_db=ref)
    log = EventLog(tmp_path / "events.jsonl")
    runner = IdentifyRunner(config, log)
    runner._load_name_lookup()

    assert runner._name_lookup["iNaturalist_28125417_Coprinellus_sp"] == "Coprinellus sp. 'radians IN02'"
    assert "iNaturalist_99999_Russula_emetica" not in runner._name_lookup
    # Embedded double quotes: greedy match captures from first " to last " on line
    assert runner._name_lookup["iNaturalist_189757404_Gymnopilus_sp_IN03"] == '= Gymnopilus "sp-IN03"'
    assert runner._name_lookup["iNaturalist_30000660_thompsonii_IN01"] == '"thompsonii-IN01"'
    # Backslash-escaped quotes are unescaped
    assert runner._name_lookup["MycoMap_106681_Agaricus_sp_IN02"] == 'Agaricus "sp-IN02"'


def test_parse_clusters_from_fasta(tmp_path):
    """Test parsing cluster info from a mock -all.fasta file."""
    config = _make_config(tmp_path)
    log = EventLog(tmp_path / "events.jsonl")
    runner = SpeconsenseRunner(config, log)

    # Create mock output
    output_dir = config.consensus_output_dir / "specimen_A"
    output_dir.mkdir(parents=True)
    fasta = output_dir / "specimen_A-all.fasta"
    fasta.write_text(
        ">specimen_A-c0 size=120 ric=100 rid=95.5\n"
        "ACGTACGT\n"
        ">specimen_A-c1 size=30 ric=30 rid=92.1\n"
        "TGCATGCA\n"
    )

    clusters = runner._parse_clusters(output_dir, "specimen_A")
    assert len(clusters) == 2
    assert clusters[0]["name"] == "specimen_A-c0"
    assert clusters[0]["size"] == 120
    assert clusters[0]["ric"] == 100
    assert clusters[0]["rid"] == 95.5
    assert clusters[1]["size"] == 30
