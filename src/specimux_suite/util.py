"""Shared helpers: FASTQ read counting, file utilities."""

import os
from pathlib import Path


def count_fastq_reads(path: Path) -> int:
    """Count reads in a FASTQ file (4 lines per record)."""
    count = 0
    with open(path) as f:
        for line in f:
            if line.startswith("@"):
                count += 1
                # Skip next 3 lines
                for _ in range(3):
                    next(f, None)
    return count


def count_fastq_reads_fast(path: Path) -> int:
    """Fast read count — counts lines and divides by 4."""
    line_count = 0
    with open(path, "rb") as f:
        for _ in f:
            line_count += 1
    return line_count // 4


def scan_specimen_reads(specimux_output_dir: Path) -> dict[str, dict]:
    """Scan specimux output for specimen read counts.

    Returns {specimen_id: {"pool": pool_name, "reads": count, "path": fastq_path}}
    Looks in full/{pool}/{specimen}.fastq
    """
    results = {}
    full_dir = specimux_output_dir / "full"
    if not full_dir.exists():
        return results

    for pool_dir in sorted(full_dir.iterdir()):
        if not pool_dir.is_dir():
            continue
        pool_name = pool_dir.name
        for fastq_file in sorted(pool_dir.glob("*.fastq")):
            specimen_id = fastq_file.stem
            reads = count_fastq_reads_fast(fastq_file)
            results[specimen_id] = {
                "pool": pool_name,
                "reads": reads,
                "path": str(fastq_file),
            }

    return results


def atomic_write(path: Path, content: bytes) -> None:
    """Write content atomically via temp file + rename."""
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_bytes(content)
    os.replace(tmp, path)
