"""Shared helpers: FASTQ read counting, file utilities."""

import logging
import os
import shutil
import threading
import subprocess
import sys
from pathlib import Path
from typing import Callable, Optional

from . import __version__

# Sent on every outbound HTTP request (iNaturalist, Mushroom Observer, photo
# hosts). The contact URL is what those APIs ask for, so an operator who sees
# unusual traffic can reach the project.
USER_AGENT = f"specimux-suite/{__version__} (+https://github.com/joshuaowalker/specimux-suite)"

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


def publish_tree(staging: Path, final: Path,
                 prune: Optional[Callable[[Path], bool]] = None) -> list[Path]:
    """Publish a tool's output directory without ever exposing a partial file.

    A tool that writes straight into a directory the dashboard serves
    (``consensus/<id>/``, ``summary/``) truncates and rewrites files in
    place, so a reader can see an empty or half-written FASTA. Instead the
    tool writes into ``staging`` and this moves every file into ``final``
    with a per-file atomic replace (rename within one filesystem), then
    removes the files under ``final`` that ``prune`` claims for this
    publication but were not just written (a previous generation's
    leftovers), and removes ``staging``. Readers see each file either
    whole-old or whole-new, and the directory never disappears.

    Returns the published paths (relative to ``final``).
    """
    staging, final = Path(staging), Path(final)
    published: list[Path] = []
    for src in sorted(p for p in staging.rglob("*") if p.is_file()):
        rel = src.relative_to(staging)
        dst = final / rel
        dst.parent.mkdir(parents=True, exist_ok=True)
        os.replace(src, dst)
        published.append(rel)
    if prune is not None and final.exists():
        keep = set(published)
        for f in sorted(p for p in final.rglob("*") if p.is_file()):
            rel = f.relative_to(final)
            if rel not in keep and prune(rel):
                f.unlink(missing_ok=True)
    shutil.rmtree(staging, ignore_errors=True)
    return published


def mirror_files(root: Path, rels: list[Path], mirror_root: Path,
                 prune: Optional[Callable[[Path], bool]] = None) -> None:
    """Copy ``root/<rel>`` to ``mirror_root/<rel>`` for each rel, so a
    reader of the mirror sees each file whole-old or whole-new.

    The mirror is typically on another filesystem (``--mirror-dir``: the
    shared storage a hosted dashboard reads, while the run works on local
    disk), so each file is copied to a temporary name beside its target and
    renamed there. ``prune`` then removes mirror files it claims that were
    not just copied (a previous generation's leftovers)."""
    root, mirror_root = Path(root), Path(mirror_root)
    copied = set()
    for rel in rels:
        rel = Path(rel)
        dst = mirror_root / rel
        dst.parent.mkdir(parents=True, exist_ok=True)
        tmp = dst.with_name(f".{dst.name}.{os.getpid()}.{threading.get_ident()}.tmp")
        shutil.copyfile(root / rel, tmp)
        os.replace(tmp, dst)
        copied.add(rel)
    if prune is not None and mirror_root.exists():
        for f in sorted(p for p in mirror_root.rglob("*") if p.is_file()):
            rel = f.relative_to(mirror_root)
            if rel not in copied and not f.name.endswith(".tmp") and prune(rel):
                f.unlink(missing_ok=True)


def owned_by(specimen_id: str) -> Callable[[Path], bool]:
    """Prune predicate: files named for this specimen (``<id>``, ``<id>-...``,
    ``<id>....``), the shape speconsense-summarize itself cleans."""
    def prune(rel: Path) -> bool:
        name = rel.name
        return name == specimen_id or (
            name.startswith(specimen_id) and name[len(specimen_id)] in "-.")
    return prune


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
