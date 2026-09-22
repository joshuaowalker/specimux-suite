"""--mirror-dir: a run on local disk, served from shared storage.

A hosted engine works on the instance's local disk (demux appends and
speconsense's debug files are thousands of small writes that a network
filesystem makes ~100x slower) and mirrors only what a dashboard reads:
the event log, consensus/<id>/<id>-all.fasta, summary/<name>-RiC*.fasta
and the photo cache.
"""

from pathlib import Path

from specimux_suite.config import PipelineConfig
from specimux_suite.events import EventLog
from specimux_suite.pipeline import _link_photo_cache
from specimux_suite.photos import photo_cache_dir
from specimux_suite.runners.speconsense_runner import SpeconsenseRunner
from specimux_suite.runners.summarize_runner import SummarizeRunner
from specimux_suite.util import mirror_files

from test_publish import _fake_tools


def _config(tmp_path):
    return PipelineConfig(primers_file=tmp_path / "p", specimens_file=tmp_path / "s",
                          output_dir=tmp_path / "local", mirror_dir=tmp_path / "shared", workers=1)


def _files(root: Path) -> set:
    return {p.relative_to(root).as_posix() for p in root.rglob("*") if p.is_file()}


def test_the_event_log_lives_in_the_mirror(tmp_path):
    config = _config(tmp_path)
    assert config.event_log_path == tmp_path / "shared" / "events.jsonl"
    explicit = PipelineConfig(primers_file=tmp_path / "p", specimens_file=tmp_path / "s",
                              output_dir=tmp_path / "local", mirror_dir=tmp_path / "shared",
                              event_log_path=tmp_path / "elsewhere.jsonl")
    assert explicit.event_log_path == tmp_path / "elsewhere.jsonl"
    plain = PipelineConfig(primers_file=tmp_path / "p", specimens_file=tmp_path / "s", output_dir=tmp_path / "o")
    assert plain.event_log_path == tmp_path / "o" / "events.jsonl" and plain.mirror_dir is None


def test_runners_mirror_only_what_the_dashboard_reads(tmp_path, monkeypatch):
    _fake_tools(tmp_path, monkeypatch)
    config = _config(tmp_path)
    log = EventLog(config.event_log_path)
    shared = tmp_path / "shared"
    # the served file is in the mirror before the event that announces it
    seen = []
    log.add_listener(lambda e: seen.append(
        (e.type, (shared / "consensus" / "S1" / "S1-all.fasta").exists())) if e.type == "consensus.completed" else None)
    cons, summ = SpeconsenseRunner(config, log), SummarizeRunner(config, log)
    fastq = tmp_path / "S1.fastq"
    other = tmp_path / "S10.fastq"
    fastq.write_text("@r1\nA\n+\nI\n" * 3)
    other.write_text("@r1\nA\n+\nI\n" * 2)
    for sid, fq in (("S1", fastq), ("S10", other)):
        cons.run(sid, fq)
        summ.run(sid, consensus_version=1)
    assert seen and all(present for _, present in seen)
    assert _files(shared) == {"events.jsonl",
                              "consensus/S1/S1-all.fasta", "consensus/S10/S10-all.fasta",
                              "summary/S1-1.v1-RiC3.fasta", "summary/S10-1.v1-RiC2.fasta"}
    # the rest stays local: debug reads, variants
    local = _files(tmp_path / "local")
    assert "consensus/S1/cluster_debug/S1-1.v1-RiC3-final.fastq" in local
    assert "summary/variants/S1-1.v1-RiC3.fasta" in local

    # a new generation replaces S1's files in the mirror and leaves S10's alone
    fastq.write_text("@r1\nA\n+\nI\n" * 5)
    cons.run("S1", fastq)
    summ.run("S1", consensus_version=2)
    assert "ric=5" in (shared / "consensus" / "S1" / "S1-all.fasta").read_text()
    assert _files(shared / "summary") == {"S1-1.v1-RiC5.fasta", "S10-1.v1-RiC2.fasta"}
    assert not [p for p in shared.rglob("*.tmp")]


def test_mirror_files_replaces_whole_files_and_prunes_what_it_claims(tmp_path):
    root, mirror = tmp_path / "root", tmp_path / "mirror"
    (root / "a").mkdir(parents=True)
    (root / "a" / "x.fasta").write_text("new")
    (mirror / "a").mkdir(parents=True)
    (mirror / "a" / "x.fasta").write_text("old")
    (mirror / "a" / "stale.fasta").write_text("old")
    (mirror / "b.fasta").write_text("keep")
    mirror_files(root, [Path("a/x.fasta")], mirror, prune=lambda rel: rel.parts[0] == "a")
    assert _files(mirror) == {"a/x.fasta", "b.fasta"}
    assert (mirror / "a" / "x.fasta").read_text() == "new"


def test_the_photo_cache_is_a_link_into_the_mirror(tmp_path):
    local, shared = tmp_path / "local", tmp_path / "shared"
    _link_photo_cache(local, shared)
    link = photo_cache_dir(local)
    assert link.is_symlink() and link.resolve() == photo_cache_dir(shared).resolve()
    (link / "1_medium.jpg").write_bytes(b"jpg")
    assert (photo_cache_dir(shared) / "1_medium.jpg").read_bytes() == b"jpg"
    _link_photo_cache(local, shared)          # a restart: the link stays
    assert link.is_symlink()
