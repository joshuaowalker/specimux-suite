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
    """Fast read count — counts lines and divides by 4.

    Transparently decompresses .gz input (MinKNOW commonly emits
    .fastq.gz); counting raw lines of a compressed stream would return
    a meaningless number.
    """
    line_count = 0
    if path.name.endswith(".gz"):
        import gzip
        opener = gzip.open(path, "rb")
    else:
        opener = open(path, "rb")
    with opener as f:
        for _ in f:
            line_count += 1
    return line_count // 4


def _count_reads_from_offset(path: Path, offset: int) -> int:
    """Count FASTQ records in the region of a file starting at offset.

    Valid only when offset is a record boundary — true for the append-only
    per-specimen files specimux writes, where offset is a previous EOF.
    """
    count = 0
    with open(path, "rb") as f:
        f.seek(offset)
        while True:
            chunk = f.read(1 << 20)
            if not chunk:
                break
            count += chunk.count(b"\n")
    return count // 4


def scan_specimen_reads(specimux_output_dir: Path, cache: dict | None = None) -> dict[str, dict]:
    """Scan specimux output for specimen read counts.

    Returns {specimen_id: {"pool": pool_name, "reads": count, "path": fastq_path}}
    Looks in full/{pool}/{specimen}.fastq

    Args:
        cache: optional {path: (size_bytes, reads)} from a previous scan.
            Specimux only appends to these files, so an unchanged size means
            an unchanged count, and growth means counting just the new bytes
            — O(new data) per scan instead of O(all data). A shrunken file
            (unexpected) falls back to a full recount. Updated in place.
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
            path_key = str(fastq_file)
            size = fastq_file.stat().st_size

            cached = cache.get(path_key) if cache is not None else None
            if cached is not None and size == cached[0]:
                reads = cached[1]
            elif cached is not None and size > cached[0]:
                reads = cached[1] + _count_reads_from_offset(fastq_file, cached[0])
            else:
                reads = count_fastq_reads_fast(fastq_file)

            if cache is not None:
                cache[path_key] = (size, reads)
            results[specimen_id] = {
                "pool": pool_name,
                "reads": reads,
                "path": path_key,
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
    with open(path, encoding="utf-8") as f:
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
        # OSError (e.g. cp missing from PATH on minimal images) must fall
        # through to the portable copy — if it escaped, the caller would fall
        # back to reading the live file, defeating the snapshot's purpose.
        try:
            result = subprocess.run(cmd, capture_output=True)
            if result.returncode == 0:
                return
            logger.debug(f"clone copy failed ({result.stderr!r}), falling back to plain copy")
        except OSError as e:
            logger.debug(f"clone copy unavailable ({e}), falling back to plain copy")

    shutil.copy2(src, dst)
