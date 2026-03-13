"""MinION output simulator — splits a FASTQ into chunks with configurable delays."""

import logging
import os
import time
from pathlib import Path

from .util import atomic_write

logger = logging.getLogger(__name__)


def simulate(
    source_fastq: Path,
    output_dir: Path,
    reads_per_file: int = 4000,
    delay: float = 30.0,
) -> None:
    """Split source FASTQ into chunks, writing to output_dir with delays.

    Files are named in MinKNOW style: FAX00000_pass_00000000_0.fastq
    Written atomically (temp + rename) to avoid partial-file processing.
    """
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    file_num = 0
    read_buf = []
    lines_in_read = 0

    logger.info(f"Simulating MinION output from {source_fastq}")
    logger.info(f"  reads_per_file={reads_per_file}, delay={delay}s, output={output_dir}")

    with open(source_fastq) as f:
        for line in f:
            read_buf.append(line)
            lines_in_read += 1

            if lines_in_read == 4:
                lines_in_read = 0

                if len(read_buf) // 4 >= reads_per_file:
                    _write_chunk(output_dir, file_num, read_buf)
                    file_num += 1
                    read_buf = []

                    if delay > 0:
                        logger.info(f"  Waiting {delay}s before next chunk...")
                        time.sleep(delay)

    # Write remaining reads
    if read_buf:
        _write_chunk(output_dir, file_num, read_buf)


def _write_chunk(output_dir: Path, file_num: int, lines: list[str]) -> None:
    """Write a chunk of FASTQ lines atomically."""
    filename = f"FAX00000_pass_{file_num:08d}_0.fastq"
    path = output_dir / filename
    content = "".join(lines).encode()
    atomic_write(path, content)
    read_count = len(lines) // 4
    logger.info(f"  Wrote {filename} ({read_count} reads)")
