"""The demux commit boundary: a demux that dies after writing is undone.

specimux appends reads to per-specimen FASTQs and specimux.completed is
emitted afterwards. A run killed in between (power loss, a Spot reclaim,
OOM) leaves those reads on disk while the log says the file was never
processed, so a restart would demux it again and double every read. The
runner records each output file's length before a demux and a restart
rolls the outputs back to that manifest (``recover_interrupted``), which
``Pipeline.__init__`` does before anything reads the specimen files.

The acceptance test is the one CLOUD.md names for milestone 1: kill the
process during demux after outputs are written but before the completion
event, restart, assert no duplicated reads.
"""

import json
import os
import signal
import stat
import subprocess
import sys
import textwrap
import time
from pathlib import Path

from specimux_suite.config import PipelineConfig
from specimux_suite.events import EventLog
from specimux_suite.runners.specimux_runner import INFLIGHT_FILENAME, SpecimuxRunner
from specimux_suite.util import count_fastq_reads_fast

# A stand-in specimux: appends every input read to full/ITS/S1.fastq and,
# every other read, to full/ITS/S2.fastq; writes a stats file. With
# FAKE_SPECIMUX_HANG set it touches a marker after writing and then hangs
# — the window the real tool is in when a run gets killed.
FAKE_SPECIMUX = textwrap.dedent('''
    #!/usr/bin/env python3
    import os, sys, time
    from pathlib import Path
    args = sys.argv[1:]
    out = Path(args[args.index("-O") + 1])
    fastq = Path(args[2])
    pool = out / "full" / "ITS"
    pool.mkdir(parents=True, exist_ok=True)
    lines = fastq.read_text().splitlines()
    reads = [lines[i:i + 4] for i in range(0, len(lines), 4)]
    with open(pool / "S1.fastq", "a") as f1, open(pool / "S2.fastq", "a") as f2:
        for i, r in enumerate(reads):
            f1.write("\\n".join(r) + "\\n")
            if i % 2:
                f2.write("\\n".join(r) + "\\n")
    (out / "stats.txt").write_text(f"{len(reads)} reads\\n")
    if os.environ.get("FAKE_SPECIMUX_HANG"):
        Path(os.environ["FAKE_SPECIMUX_HANG"]).touch()
        time.sleep(120)
''').lstrip()


def _fake_specimux(tmp_path, monkeypatch):
    fake_bin = tmp_path / "bin"
    fake_bin.mkdir(exist_ok=True)
    tool = fake_bin / "specimux"
    tool.write_text(FAKE_SPECIMUX)
    tool.chmod(tool.stat().st_mode | stat.S_IEXEC)
    monkeypatch.setenv("PATH", f"{fake_bin}{os.pathsep}{os.environ['PATH']}")
    return fake_bin


def _config(tmp_path):
    return PipelineConfig(primers_file=tmp_path / "primers.fasta",
                          specimens_file=tmp_path / "specimens.tsv",
                          output_dir=tmp_path / "output", workers=1)


def _reads(n):
    return "".join(f"@r{i}\nACGT\n+\nIIII\n" for i in range(n))


def test_rollback_restores_lengths_and_removes_new_files(tmp_path):
    config = _config(tmp_path)
    runner = SpecimuxRunner(config, EventLog(config.event_log_path))
    pool = config.specimux_output_dir / "full" / "ITS"
    pool.mkdir(parents=True)
    (pool / "S1.fastq").write_text(_reads(3))
    (pool / "S2.fastq").write_text(_reads(1))
    runner.record_inflight("job1", Path("/in/chunk.fastq"))
    manifest = json.loads((config.output_dir / INFLIGHT_FILENAME).read_text())
    assert manifest["lengths"] == {"full/ITS/S1.fastq": len(_reads(3)),
                                   "full/ITS/S2.fastq": len(_reads(1))}
    # the interrupted demux appends to both, creates a new specimen and a stats file
    with open(pool / "S1.fastq", "a") as f:
        f.write(_reads(2))
    with open(pool / "S2.fastq", "a") as f:
        f.write(_reads(5))
    (pool / "S3.fastq").write_text(_reads(4))
    (config.specimux_output_dir / "stats.txt").write_text("x")

    summary = runner.recover_interrupted()

    assert summary["file_path"] == "/in/chunk.fastq"
    assert sorted(summary["truncated"]) == ["full/ITS/S1.fastq", "full/ITS/S2.fastq"]
    assert sorted(summary["removed"]) == ["full/ITS/S3.fastq", "stats.txt"]
    assert (pool / "S1.fastq").read_text() == _reads(3)
    assert (pool / "S2.fastq").read_text() == _reads(1)
    assert not (pool / "S3.fastq").exists()
    assert not (config.output_dir / INFLIGHT_FILENAME).exists()
    assert runner.recover_interrupted() is None  # idempotent, nothing left


def test_successful_demux_clears_the_manifest(tmp_path, monkeypatch):
    _fake_specimux(tmp_path, monkeypatch)
    config = _config(tmp_path)
    log = EventLog(config.event_log_path)
    fastq = tmp_path / "chunk.fastq"
    fastq.write_text(_reads(10))
    result = SpecimuxRunner(config, log).run(fastq)
    assert result["S1"]["reads"] == 10 and result["S2"]["reads"] == 5
    assert not (config.output_dir / INFLIGHT_FILENAME).exists()
    types = [e.type for e in log.replay()]
    assert types[-1] == "specimen.updated" and "specimux.completed" in types


DRIVER = textwrap.dedent('''
    import sys
    from pathlib import Path
    from specimux_suite.config import PipelineConfig
    from specimux_suite.events import EventLog
    from specimux_suite.runners.specimux_runner import SpecimuxRunner
    tmp = Path(sys.argv[1])
    config = PipelineConfig(primers_file=tmp / "primers.fasta",
                            specimens_file=tmp / "specimens.tsv",
                            output_dir=tmp / "output", workers=1)
    SpecimuxRunner(config, EventLog(config.event_log_path)).run(tmp / "chunk.fastq")
''').lstrip()


def test_kill_during_demux_then_restart_does_not_duplicate_reads(tmp_path, monkeypatch):
    """Milestone 1 acceptance: SIGKILL the engine after specimux has
    appended but before specimux.completed; restart; no duplicated reads."""
    _fake_specimux(tmp_path, monkeypatch)
    config = _config(tmp_path)
    fastq = tmp_path / "chunk.fastq"
    fastq.write_text(_reads(10))
    # An earlier, completed demux of another chunk: its reads must survive
    first = tmp_path / "first.fastq"
    first.write_text(_reads(4))
    SpecimuxRunner(config, EventLog(config.event_log_path)).run(first)
    pool = config.specimux_output_dir / "full" / "ITS"
    assert count_fastq_reads_fast(pool / "S1.fastq") == 4

    driver = tmp_path / "driver.py"
    driver.write_text(DRIVER)
    marker = tmp_path / "appended.marker"
    env = {**os.environ, "FAKE_SPECIMUX_HANG": str(marker)}
    proc = subprocess.Popen([sys.executable, str(driver), str(tmp_path)],
                            env=env, start_new_session=True)
    try:
        deadline = time.monotonic() + 20
        while not marker.exists():
            assert proc.poll() is None, "driver exited early"
            assert time.monotonic() < deadline, "fake specimux never wrote"
            time.sleep(0.05)
        # Outputs written, completion event not emitted: the window
        assert count_fastq_reads_fast(pool / "S1.fastq") == 14
        log = EventLog(config.event_log_path)
        types = [e.type for e in log.replay()]
        assert types.count("specimux.started") == 2 and types.count("specimux.completed") == 1
        assert (config.output_dir / INFLIGHT_FILENAME).exists()
    finally:
        os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
        proc.wait(timeout=10)

    # Restart: the pipeline rolls back before anything reads the files
    from specimux_suite.pipeline import Pipeline
    pipeline = Pipeline(config)
    try:
        assert count_fastq_reads_fast(pool / "S1.fastq") == 4
        assert count_fastq_reads_fast(pool / "S2.fastq") == 2
        assert not (config.output_dir / INFLIGHT_FILENAME).exists()
        # and the file is still unprocessed, so the restart demuxes it again
        assert not pipeline.state.files.get(str(fastq), None) or not pipeline.state.files[str(fastq)].processed
        result = pipeline.specimux.run(fastq)
    finally:
        pipeline._executor.shutdown(wait=True)
    assert result["S1"]["reads"] == 14 and result["S2"]["reads"] == 7
    assert count_fastq_reads_fast(pool / "S1.fastq") == 14  # 4 + 10, not 4 + 10 + 10
