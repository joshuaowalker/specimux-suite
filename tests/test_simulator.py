"""Tests for FASTQ replay."""

import gzip
from pathlib import Path

from specimux_suite.simulator import simulate
from specimux_suite.util import count_fastq_reads_fast


def test_simulate_splits_fastq(tmp_path, tiny_fastq):
    """Replay should split FASTQ into chunks."""
    output_dir = tmp_path / "simulated"

    simulate(
        source_fastq=tiny_fastq,
        output_dir=output_dir,
        reads_per_file=2,
        delay=0,  # no delay for tests
    )

    files = sorted(output_dir.glob("*.fastq"))
    assert len(files) == 2  # 4 reads / 2 per file = 2 files

    # Check MinKNOW naming
    assert "FAX00000_pass_00000000_0.fastq" == files[0].name
    assert "FAX00000_pass_00000001_0.fastq" == files[1].name

    # Check read counts
    assert count_fastq_reads_fast(files[0]) == 2
    assert count_fastq_reads_fast(files[1]) == 2


def test_simulate_single_chunk(tmp_path, tiny_fastq):
    """All reads in one file when reads_per_file > total reads."""
    output_dir = tmp_path / "simulated"

    simulate(
        source_fastq=tiny_fastq,
        output_dir=output_dir,
        reads_per_file=100,
        delay=0,
    )

    files = sorted(output_dir.glob("*.fastq"))
    assert len(files) == 1
    assert count_fastq_reads_fast(files[0]) == 4


def test_simulate_gzip(tmp_path, tiny_fastq):
    """Replay with compress=True should produce .fastq.gz files."""
    output_dir = tmp_path / "simulated"

    simulate(
        source_fastq=tiny_fastq,
        output_dir=output_dir,
        reads_per_file=2,
        delay=0,
        compress=True,
    )

    files = sorted(output_dir.glob("*.fastq.gz"))
    assert len(files) == 2
    assert files[0].name == "FAX00000_pass_00000000_0.fastq.gz"

    # Verify content is valid gzip with correct reads
    content = gzip.decompress(files[0].read_bytes()).decode()
    # Count FASTQ records (lines starting with @)
    reads = sum(1 for i, line in enumerate(content.splitlines()) if i % 4 == 0 and line.startswith("@"))
    assert reads == 2
