"""Tests for runner command construction."""

from pathlib import Path

from specimux_suite.config import PipelineConfig
from specimux_suite.events import EventLog
from specimux_suite.runners.specimux_runner import SpecimuxRunner
from specimux_suite.runners.speconsense_runner import SpeconsenseRunner, parse_cluster_header


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


def test_cluster_header_parsing_legacy_0_7():
    """0.7.x headers (-c{n} naming, no CER/err fields) parse correctly."""
    c0 = parse_cluster_header(">sample-c0 size=120 ric=100 rid=95.5")
    assert c0["name"] == "sample-c0"
    assert c0["size"] == 120
    assert c0["ric"] == 100
    assert c0["rid"] == 95.5
    # Fields absent from the header are simply not present.
    assert "cer_factor" not in c0
    assert "gid" not in c0

    c1 = parse_cluster_header(">sample-c1 size=30 ric=30 rid=92.1 rid_min=88.0 primers=ITS3,ITS4 ambig=2")
    assert c1["name"] == "sample-c1"
    assert c1["rid_min"] == 88.0
    assert c1["primers"] == "ITS3,ITS4"
    assert c1["ambig"] == 2

    c2 = parse_cluster_header(">sample-c2 size=10")
    assert c2 == {"name": "sample-c2", "size": 10}


def test_cluster_header_parsing_0_8():
    """0.8.x headers carry gid/vid naming plus cer_factor/err_factor fields."""
    c = parse_cluster_header(
        ">sample-1.v2 size=120 ric=100 rid=95.5 rid_min=90.1 "
        "primers=ITS1F,ITS4 ambig=1 cer_factor=1.234 err_factor=1.100 gid=1 vid=2"
    )
    assert c["name"] == "sample-1.v2"
    assert c["size"] == 120
    assert c["cer_factor"] == 1.234
    assert c["err_factor"] == 1.100
    assert c["gid"] == 1
    assert c["vid"] == 2


def test_cluster_header_inf_cer_factor_normalized():
    """cer_factor=inf (always-pass anchors) is normalized to None for JSON-safety."""
    c = parse_cluster_header(">sample-1.v1 size=200 ric=100 cer_factor=inf err_factor=1.0 gid=1 vid=1")
    assert c["cer_factor"] is None
    assert c["err_factor"] == 1.0


def test_cluster_header_non_defline_returns_none():
    assert parse_cluster_header("ACGTACGT") is None
    assert parse_cluster_header(">") is None


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


def test_extract_reference_seqs_via_offset_index(tmp_path):
    """Sequence extraction should use header offsets, handling multi-line and last entries."""
    from specimux_suite.runners.identify_runner import IdentifyRunner

    ref = tmp_path / "refs.fasta"
    ref.write_text(
        '>ref_A name="Alpha one"\n'
        "ACGT\nACGT\n"
        ">ref_B\n"
        "TTTT\n"
        '>ref_C name="Gamma three"\n'
        "GGGG\nCCCC\nAAAA\n"
    )

    config = _make_config(tmp_path, reference_db=ref)
    log = EventLog(tmp_path / "events.jsonl")
    runner = IdentifyRunner(config, log)
    runner._load_name_lookup()

    seqs = runner._extract_reference_seqs({"ref_A", "ref_C", "ref_missing"})
    assert seqs == {"ref_A": "ACGTACGT", "ref_C": "GGGGCCCCAAAA"}
    # Lazy index build when called before _load_name_lookup
    runner2 = IdentifyRunner(config, log)
    assert runner2._extract_reference_seqs({"ref_B"}) == {"ref_B": "TTTT"}


def test_speconsense_timeout_kills_and_reports(tmp_path, monkeypatch):
    """A hung speconsense must be killed at job_timeout and leave the specimen in ERROR."""
    import os
    import stat
    import time
    from specimux_suite.state import PipelineState, SpecimenStatus

    # Fake speconsense that hangs far longer than the timeout
    fake_bin = tmp_path / "bin"
    fake_bin.mkdir()
    fake_tool = fake_bin / "speconsense"
    fake_tool.write_text("#!/bin/sh\nsleep 30\n")
    fake_tool.chmod(fake_tool.stat().st_mode | stat.S_IEXEC)
    monkeypatch.setenv("PATH", f"{fake_bin}{os.pathsep}{os.environ['PATH']}")

    config = _make_config(tmp_path)
    config.job_timeout = 0.5
    log = EventLog(tmp_path / "events.jsonl")
    runner = SpeconsenseRunner(config, log)

    fastq = tmp_path / "specimen_A.fastq"
    fastq.write_text("@r1\nACGT\n+\nIIII\n")

    start = time.monotonic()
    clusters = runner.run("specimen_A", fastq)
    elapsed = time.monotonic() - start

    assert clusters == []
    assert elapsed < 10  # killed at ~0.5s, not the full 30s sleep

    events = list(log.replay())
    types = [e.type for e in events]
    # consensus.completed precedes pipeline.error so replayed status is ERROR
    assert types.index("consensus.completed") < types.index("pipeline.error")
    err = next(e for e in events if e.type == "pipeline.error")
    assert "timed out" in err.data["message"]

    state = PipelineState()
    state.rebuild(log)
    spec = state.specimens["specimen_A"]
    assert spec.status == SpecimenStatus.ERROR
    assert spec.consensus_version == 1  # failed run still counts, no retry loop


def test_identification_tsv_written(tmp_path):
    """run() results are also written to identification/{name}.tsv."""
    from specimux_suite.runners.identify_runner import IdentifyRunner

    config = _make_config(tmp_path)
    log = EventLog(tmp_path / "events.jsonl")
    runner = IdentifyRunner(config, log)

    matches = [{"cluster": "spec_A-c0", "top_hits": [
        {"ref_id": "r1", "name": "Russula emetica", "identity": 0.97,
         "adjusted_identity": 0.985, "coverage": 0.91},
        {"ref_id": "r2", "name": "Russula sp.", "identity": 0.95,
         "adjusted_identity": 0.96},
    ]}]
    runner._write_tsv("spec_A", matches)

    tsv = (config.identification_output_dir / "spec_A.tsv").read_text()
    lines = tsv.splitlines()
    assert lines[0].split("\t") == ["cluster", "ref_id", "name", "identity",
                                    "adjusted_identity", "coverage"]
    assert lines[1].split("\t") == ["spec_A-c0", "r1", "Russula emetica",
                                    "0.9700", "0.9850", "0.9100"]
    assert lines[2].split("\t")[5] == ""  # missing coverage -> empty field


def test_scan_specimen_reads_incremental_cache(tmp_path):
    """Cached scans must count only appended bytes and detect shrinkage."""
    from specimux_suite.util import scan_specimen_reads

    pool = tmp_path / "specimux" / "full" / "ITS"
    pool.mkdir(parents=True)
    fq = pool / "spec_A.fastq"
    record = "@r{}\nACGT\n+\nIIII\n"
    fq.write_text("".join(record.format(i) for i in range(3)))

    cache = {}
    out = scan_specimen_reads(tmp_path / "specimux", cache=cache)
    assert out["spec_A"]["reads"] == 3

    # Unchanged size -> cached count reused
    out = scan_specimen_reads(tmp_path / "specimux", cache=cache)
    assert out["spec_A"]["reads"] == 3

    # Append two records -> incremental count from prior EOF
    with open(fq, "a") as f:
        f.write("".join(record.format(i) for i in range(3, 5)))
    out = scan_specimen_reads(tmp_path / "specimux", cache=cache)
    assert out["spec_A"]["reads"] == 5

    # Shrunken file (unexpected) -> full recount, not garbage
    fq.write_text(record.format(0))
    out = scan_specimen_reads(tmp_path / "specimux", cache=cache)
    assert out["spec_A"]["reads"] == 1

    # No-cache call still works and matches
    assert scan_specimen_reads(tmp_path / "specimux")["spec_A"]["reads"] == 1
