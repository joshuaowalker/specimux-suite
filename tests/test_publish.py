"""Atomic publication of served artifacts.

The dashboard reads consensus/<id>/<id>-all.fasta and summary/<name>-RiC*.fasta
while the engine is still writing them, and both tools rewrite those files
in place. The runners now point the tools at a staging dir and publish the
result with util.publish_tree: per-file atomic replace, then the previous
generation's leftovers removed, so a reader sees a file whole-old or
whole-new and never empty or half-written.
"""

import os
import stat
import threading
import time

from specimux_suite.config import PipelineConfig
from specimux_suite.events import EventLog
from specimux_suite.runners.speconsense_runner import SpeconsenseRunner
from specimux_suite.runners.summarize_runner import SummarizeRunner
from specimux_suite.util import USER_AGENT, owned_by, publish_tree


def test_publish_moves_files_prunes_leftovers_and_removes_staging(tmp_path):
    staging = tmp_path / "stage"
    final = tmp_path / "final"
    (staging / "sub").mkdir(parents=True)
    (staging / "S1-all.fasta").write_text("new")
    (staging / "sub" / "S1-c0.fastq").write_text("reads")
    final.mkdir()
    (final / "S1-all.fasta").write_text("old")
    (final / "S1-c9.fasta").write_text("stale")      # previous generation
    (final / "S10-all.fasta").write_text("other")     # another specimen

    published = publish_tree(staging, final, prune=owned_by("S1"))

    assert sorted(str(p) for p in published) == ["S1-all.fasta", "sub/S1-c0.fastq"]
    assert (final / "S1-all.fasta").read_text() == "new"
    assert (final / "sub" / "S1-c0.fastq").read_text() == "reads"
    assert not (final / "S1-c9.fasta").exists()
    assert (final / "S10-all.fasta").read_text() == "other"
    assert not staging.exists()


def test_owned_by_matches_the_tools_naming_only():
    p = owned_by("S1")
    from pathlib import Path
    assert p(Path("S1")) and p(Path("S1-1.v2-RiC30.fasta")) and p(Path("variants/S1.ITS-RiC3.fasta"))
    assert not p(Path("S10-all.fasta")) and not p(Path("summary.fasta")) and not p(Path("S1x"))


def test_reader_never_sees_a_partial_file(tmp_path):
    """Publish generations while a reader hammers the served file."""
    final = tmp_path / "final"
    final.mkdir()
    payloads = [chr(65 + i) * 200_000 for i in range(6)]  # 200 KB each
    (final / "S1-all.fasta").write_text(payloads[0])
    seen, bad, stop = set(), [], threading.Event()

    def reader():
        while not stop.is_set():
            try:
                text = (final / "S1-all.fasta").read_text()
            except FileNotFoundError:
                bad.append("missing")
                continue
            if text not in payloads:
                bad.append(f"partial: {len(text)} bytes")
            else:
                seen.add(text[0])

    t = threading.Thread(target=reader)
    t.start()
    for i, payload in enumerate(payloads[1:], 1):
        staging = tmp_path / f"stage{i}"
        staging.mkdir()
        (staging / "S1-all.fasta").write_text(payload)
        publish_tree(staging, final, prune=lambda rel: True)
        time.sleep(0.02)
    stop.set()
    t.join()
    assert not bad, bad[:5]
    assert len(seen) >= 2


FAKE_SPECONSENSE = """#!/usr/bin/env python3
import sys
from pathlib import Path
args = sys.argv[1:]
out = Path(args[args.index("-O") + 1]); out.mkdir(parents=True, exist_ok=True)
sid = Path(args[0]).stem
gen = Path(args[0]).read_text().count("@")
(out / f"{sid}-all.fasta").write_text(f">{sid}-1.v1 size={gen} ric={gen}\\nACGT\\n")
(out / "cluster_debug").mkdir(exist_ok=True)
(out / "cluster_debug" / f"{sid}-1.v1-RiC{gen}-final.fastq").write_text("@r\\nA\\n+\\nI\\n")
"""

FAKE_SUMMARIZE = """#!/usr/bin/env python3
import json, sys
from pathlib import Path
args = sys.argv[1:]
summary = Path(args[args.index("--summary-dir") + 1])
sid = args[args.index("--specimen") + 1]
source = Path(args[args.index("--source") + 1])
ric = (source / f"{sid}-all.fasta").read_text().split("ric=")[1].split()[0]
summary.mkdir(parents=True, exist_ok=True)
(summary / f"{sid}-1.v1-RiC{ric}.fasta").write_text(f">{sid}-1.v1\\nACGT\\n")
(summary / "variants").mkdir(exist_ok=True)
(summary / "variants" / f"{sid}-1.v1-RiC{ric}.fasta").write_text("x")
print(json.dumps({"specimen_id": sid, "variant_count": 1,
                  "variants": [{"name": f"{sid}-1.v1", "ric": int(ric), "size": int(ric), "length": 4}]}))
"""


def _fake_tools(tmp_path, monkeypatch):
    fake_bin = tmp_path / "bin"
    fake_bin.mkdir()
    for name, body in (("speconsense", FAKE_SPECONSENSE), ("speconsense-summarize", FAKE_SUMMARIZE)):
        t = fake_bin / name
        t.write_text(body)
        t.chmod(t.stat().st_mode | stat.S_IEXEC)
    monkeypatch.setenv("PATH", f"{fake_bin}{os.pathsep}{os.environ['PATH']}")


def _config(tmp_path):
    return PipelineConfig(primers_file=tmp_path / "p", specimens_file=tmp_path / "s",
                          output_dir=tmp_path / "output", workers=1)


def test_runners_stage_then_publish(tmp_path, monkeypatch):
    _fake_tools(tmp_path, monkeypatch)
    config = _config(tmp_path)
    log = EventLog(config.event_log_path)
    fastq = tmp_path / "S1.fastq"
    fastq.write_text("@r1\nA\n+\nI\n" * 3)
    cons = SpeconsenseRunner(config, log)
    summ = SummarizeRunner(config, log)

    clusters = cons.run("S1", fastq)
    assert clusters == [{"name": "S1-1.v1", "size": 3, "ric": 3}]
    all_fasta = config.consensus_output_dir / "S1" / "S1-all.fasta"
    assert "ric=3" in all_fasta.read_text()
    assert (config.consensus_output_dir / "S1" / "cluster_debug" / "S1-1.v1-RiC3-final.fastq").exists()
    variants = summ.run("S1", consensus_version=1)
    assert variants[0]["name"] == "S1-1.v1"
    assert (config.summarize_output_dir / "S1-1.v1-RiC3.fasta").exists()
    assert not any(f.is_file() for f in config.staging_dir.rglob("*"))  # nothing left staged

    # A second generation with more reads: new files replace, old RiC leftovers go
    fastq.write_text("@r1\nA\n+\nI\n" * 5)
    cons.run("S1", fastq)
    assert "ric=5" in all_fasta.read_text()
    assert not (config.consensus_output_dir / "S1" / "cluster_debug" / "S1-1.v1-RiC3-final.fastq").exists()
    assert (config.consensus_output_dir / "S1" / "cluster_debug" / "S1-1.v1-RiC5-final.fastq").exists()
    summ.run("S1", consensus_version=2)
    assert (config.summarize_output_dir / "S1-1.v1-RiC5.fasta").exists()
    assert not (config.summarize_output_dir / "S1-1.v1-RiC3.fasta").exists()
    assert not (config.summarize_output_dir / "variants" / "S1-1.v1-RiC3.fasta").exists()
    assert (config.summarize_output_dir / "variants" / "S1-1.v1-RiC5.fasta").exists()
    # the speconsense command pointed the tool at staging, not the served dir
    cmd = cons._build_command(fastq, config.staging_dir / "consensus" / "S1.x")
    assert str(config.staging_dir) in cmd[cmd.index("-O") + 1]


def test_failed_tool_leaves_the_published_generation_alone(tmp_path, monkeypatch):
    _fake_tools(tmp_path, monkeypatch)
    config = _config(tmp_path)
    log = EventLog(config.event_log_path)
    fastq = tmp_path / "S1.fastq"
    fastq.write_text("@r1\nA\n+\nI\n")
    cons = SpeconsenseRunner(config, log)
    cons.run("S1", fastq)
    served = config.consensus_output_dir / "S1" / "S1-all.fasta"
    before = served.read_text()
    (tmp_path / "bin" / "speconsense").write_text("#!/bin/sh\nexit 3\n")
    assert cons.run("S1", fastq) == []
    assert served.read_text() == before
    assert not any(f.is_file() for f in config.staging_dir.rglob("*"))


def test_user_agent_carries_a_contact_url():
    assert USER_AGENT.startswith("specimux-suite/")
    assert "(+https://github.com/joshuaowalker/specimux-suite)" in USER_AGENT


def test_the_prune_only_examines_the_specimens_own_names(tmp_path):
    """A shared summary dir grows to tens of thousands of files; the prune
    looks only at names starting with the specimen id (owned_by implies
    that prefix), so publishing stays cheap late in a run."""
    final, staging = tmp_path / "summary", tmp_path / "staging"
    (final / "variants").mkdir(parents=True)
    for i in range(300):
        (final / f"S{i}-1.v1-RiC5.fasta").write_text("x")
        (final / "variants" / f"S{i}-1.v1-RiC5.fasta").write_text("x")
    (final / "S7-1.v1-RiC3.fasta").write_text("stale")
    (final / "variants" / "S7-1.v1-RiC3.fasta").write_text("stale")
    staging.mkdir()
    (staging / "S7-1.v1-RiC9.fasta").write_text("new")
    seen = []
    owned = owned_by("S7")
    publish_tree(staging, final, prune=lambda rel: seen.append(rel) or owned(rel), prefix="S7")
    assert all(r.name.startswith("S7") for r in seen)          # S70..S79 are candidates, not S1..
    assert not (final / "S7-1.v1-RiC3.fasta").exists() and not (final / "variants" / "S7-1.v1-RiC3.fasta").exists()
    assert not (final / "S7-1.v1-RiC5.fasta").exists()         # S7's previous generation
    assert (final / "S70-1.v1-RiC5.fasta").exists()            # S70 is not S7's
    assert (final / "S7-1.v1-RiC9.fasta").read_text() == "new"
    assert not (final / "variants" / "S7-1.v1-RiC5.fasta").exists()
    # 600 + 2 stale, less S7's four old files, plus the new one
    assert sum(1 for p in final.rglob("*") if p.is_file()) == 599



def test_publish_from_a_missing_staging_dir_prunes_the_specimen(tmp_path):
    """speconsense-summarize --specimen creates no --summary-dir when every
    cluster is below --min-ric: nothing is published, and the specimen's
    previous summary files go (it has no variants now); others stay."""
    from specimux_suite.util import owned_by, publish_tree
    final = tmp_path / "summary"
    final.mkdir()
    (final / "S1-1.v1-RiC5.fasta").write_text("old")
    (final / "S2-1.v1-RiC5.fasta").write_text("other")
    assert publish_tree(tmp_path / "never-created", final, prune=owned_by("S1"), prefix="S1") == []
    assert sorted(p.name for p in final.iterdir()) == ["S2-1.v1-RiC5.fasta"]
