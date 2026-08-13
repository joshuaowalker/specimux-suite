"""Shared helpers: FASTQ read counting, file utilities."""

import logging
import os
import shutil
import subprocess
import sys
from pathlib import Path

logger = logging.getLogger(__name__)


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


def parse_specimens_file(path: Path) -> list[dict]:
    """Parse a specimens TSV file (specimux index format).

    Returns list of {"specimen_id": str, "pool": str} dicts.
    Expects a header row with at least SampleID and PrimerPool columns.
    """
    specimens = []
    if not path.exists():
        return specimens
    with open(path) as f:
        header = f.readline().strip().split("\t")
        try:
            id_col = header.index("SampleID")
            pool_col = header.index("PrimerPool")
        except ValueError:
            # Fallback: assume first two columns
            id_col, pool_col = 0, 1
        for line in f:
            parts = line.strip().split("\t")
            if len(parts) > max(id_col, pool_col):
                specimens.append({
                    "specimen_id": parts[id_col],
                    "pool": parts[pool_col],
                })
    return specimens


def atomic_write(path: Path, content: bytes) -> None:
    """Write content atomically via temp file + rename."""
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_bytes(content)
    os.replace(tmp, path)


def clone_or_copy(src: Path, dst: Path) -> None:
    """Copy src to dst, using filesystem cloning (copy-on-write) when available.

    On APFS (macOS) and reflink-capable Linux filesystems (btrfs, XFS) the
    clone is O(1) regardless of file size; elsewhere this degrades to a
    regular copy. Any existing dst is replaced.
    """
    dst.parent.mkdir(parents=True, exist_ok=True)
    dst.unlink(missing_ok=True)

    if sys.platform == "darwin":
        cmd = ["cp", "-c", str(src), str(dst)]
    elif sys.platform.startswith("linux"):
        cmd = ["cp", "--reflink=auto", str(src), str(dst)]
    else:
        cmd = None

    if cmd is not None:
        result = subprocess.run(cmd, capture_output=True)
        if result.returncode == 0:
            return
        logger.debug(f"clone copy failed ({result.stderr!r}), falling back to plain copy")

    shutil.copy2(src, dst)
