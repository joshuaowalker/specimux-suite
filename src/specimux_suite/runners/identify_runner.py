"""Identification runner: vsearch + adjusted-identity scoring."""

import logging
import re
import subprocess
import tempfile
from pathlib import Path

from adjusted_identity import AdjustmentParams, align_and_score

from ..config import PipelineConfig
from ..events import EventLog

logger = logging.getLogger(__name__)


class IdentifyRunner:
    """Identifies consensus sequences against a reference database."""

    def __init__(self, config: PipelineConfig, event_log: EventLog):
        self.config = config
        self.event_log = event_log
        self._udb_path: Path | None = None
        self._name_lookup: dict[str, str] = {}
        # ref_id -> byte offset of its header line in the reference FASTA,
        # so sequences can be extracted with a seek instead of a full scan.
        self._seq_offsets: dict[str, int] = {}

    def ensure_db(self) -> bool:
        """Build UDB from reference FASTA if needed. Returns True on success."""
        if not self.config.reference_db:
            logger.warning("No reference database configured")
            return False

        ref = self.config.reference_db
        if not ref.exists():
            logger.error(f"Reference database not found: {ref}")
            return False

        # Check if UDB already exists and is up-to-date
        udb = ref.with_suffix(".udb")
        if udb.exists() and udb.stat().st_mtime >= ref.stat().st_mtime:
            self._udb_path = udb
            self._load_name_lookup()
            return True
        if udb.exists():
            logger.info(f"Reference FASTA is newer than UDB, rebuilding")

        # Build UDB
        logger.info(f"Building vsearch UDB from {ref}")
        try:
            subprocess.run(
                ["vsearch", "--makeudb_usearch", str(ref), "--output", str(udb)],
                capture_output=True,
                text=True,
                check=True,
                timeout=self.config.job_timeout,
            )
            self._udb_path = udb
            return True
        except (subprocess.CalledProcessError, FileNotFoundError,
                subprocess.TimeoutExpired) as e:
            logger.error(f"Failed to build UDB: {e}")
            # Fall back to using FASTA directly
            self._udb_path = ref
            return True
        finally:
            self._load_name_lookup()

    def _load_name_lookup(self) -> None:
        """Scan reference FASTA headers once: name="..." fields and seek offsets."""
        ref = self.config.reference_db
        if not ref or not ref.exists():
            return
        # Greedy on purpose: name="..." is the last field on the header line
        # (both MycoMap and iNaturalist DBs), and real iNaturalist headers
        # contain unescaped embedded quotes (name="= Gymnopilus "sp-IN03"").
        # Matching to the last quote on the line handles both embedded and
        # backslash-escaped quotes; a non-greedy match truncates such names.
        name_re = re.compile(rb'name="(.+)"')
        offset = 0
        with open(ref, "rb") as f:
            for line in f:
                if line.startswith(b">"):
                    seq_id = line[1:].strip().split()[0].decode("utf-8", "replace")
                    self._seq_offsets[seq_id] = offset
                    m = name_re.search(line)
                    if m:
                        name = m.group(1).decode("utf-8", "replace")
                        self._name_lookup[seq_id] = name.replace('\\"', '"')
                offset += len(line)

    def run(self, specimen_id: str, consensus_fasta: Path,
            consensus_version: int | None = None) -> list[dict]:
        """Identify consensus clusters against reference DB.

        Args:
            consensus_version: The specimen's consensus generation these
                queries came from. Stamped into the event so stale results
                (landing after a re-consensus) can be discarded on apply.

        Returns list of {cluster, top_hits: [{ref_id, name, identity, adjusted_identity}]}
        """
        if not self._udb_path:
            if not self.ensure_db():
                return []

        # Step 1: vsearch search
        vsearch_hits = self._run_vsearch(consensus_fasta)

        # Step 2: adjusted-identity scoring for candidates
        matches = self._score_hits(consensus_fasta, vsearch_hits)

        event_data = {
            "specimen_id": specimen_id,
            "matches": matches,
        }
        if consensus_version is not None:
            event_data["consensus_version"] = consensus_version
        self.event_log.emit("identification.completed", event_data)

        return matches

    def _run_vsearch(self, query_fasta: Path) -> dict[str, list[dict]]:
        """Run vsearch --usearch_global, return hits grouped by query."""
        db = self._udb_path or self.config.reference_db
        cmd = [
            "vsearch",
            "--usearch_global", str(query_fasta),
            "--db", str(db),
            "--userout", "/dev/stdout",
            "--userfields", "query+target+id+alnlen+qcov",
            "--id", str(self.config.vsearch_min_identity),
            "--maxaccepts", str(self.config.vsearch_max_accepts),
            "--threads", "1",
            "--output_no_hits",
        ]

        try:
            result = subprocess.run(cmd, capture_output=True, text=True, check=False,
                                    timeout=self.config.job_timeout)
        except FileNotFoundError:
            logger.error("vsearch not found on PATH")
            self.event_log.emit("pipeline.error", {
                "component": "vsearch",
                "message": "vsearch not found on PATH",
            })
            return {}
        except subprocess.TimeoutExpired:
            logger.error(f"vsearch timed out after {self.config.job_timeout}s")
            self.event_log.emit("pipeline.error", {
                "component": "vsearch",
                "message": f"vsearch timed out after {self.config.job_timeout}s",
            })
            return {}

        if result.returncode != 0:
            logger.error(f"vsearch failed (exit {result.returncode}): {result.stderr}")
            self.event_log.emit("pipeline.error", {
                "component": "vsearch",
                "message": f"vsearch exited with code {result.returncode}",
                "details": result.stderr[-2000:] if result.stderr else "",
            })
            return {}

        hits: dict[str, list[dict]] = {}
        for line in result.stdout.splitlines():
            parts = line.strip().split("\t")
            if len(parts) < 5:
                continue
            query, target, identity, alnlen, qcov = parts[:5]
            if target == "*":  # no hit
                continue
            hits.setdefault(query, []).append({
                "ref_id": target,
                "name": self._name_lookup.get(target, _extract_species_name(target)),
                "identity": float(identity) / 100.0 if float(identity) > 1 else float(identity),
                "alnlen": int(alnlen),
                "qcov": float(qcov) / 100.0 if float(qcov) > 1 else float(qcov),
            })

        return hits

    def _score_hits(self, consensus_fasta: Path, vsearch_hits: dict) -> list[dict]:
        """Re-score top vsearch hits with adjusted-identity and filter by coverage."""
        query_seqs = _read_fasta(consensus_fasta)

        ref_ids_needed = set()
        for hits in vsearch_hits.values():
            for hit in hits[:5]:  # top 5 per cluster
                ref_ids_needed.add(hit["ref_id"])

        ref_seqs = self._extract_reference_seqs(ref_ids_needed)
        params = AdjustmentParams()
        min_cov = self.config.identify_min_coverage

        matches = []
        for cluster_name, hits in vsearch_hits.items():
            query_seq = query_seqs.get(cluster_name, "")
            scored_hits = []
            for hit in hits[:5]:
                ref_seq = ref_seqs.get(hit["ref_id"], "")
                if query_seq and ref_seq:
                    try:
                        ar = align_and_score(query_seq, ref_seq, adjustment_params=params)
                        coverage = max(ar.seq1_coverage, ar.seq2_coverage)
                        if coverage < min_cov:
                            logger.debug(
                                f"Filtered {cluster_name} vs {hit['ref_id']}: "
                                f"coverage {coverage:.2f} < {min_cov}"
                            )
                            continue
                        scored_hits.append({
                            "ref_id": hit["ref_id"],
                            "name": hit["name"],
                            "identity": hit["identity"],
                            "adjusted_identity": ar.identity,
                            "coverage": coverage,
                        })
                    except Exception as e:
                        logger.debug(f"adjusted-identity failed for {cluster_name} vs {hit['ref_id']}: {e}")
                        scored_hits.append({
                            "ref_id": hit["ref_id"],
                            "name": hit["name"],
                            "identity": hit["identity"],
                            "adjusted_identity": hit["identity"],
                        })
                else:
                    scored_hits.append({
                        "ref_id": hit["ref_id"],
                        "name": hit["name"],
                        "identity": hit["identity"],
                        "adjusted_identity": hit["identity"],
                    })

            scored_hits.sort(key=lambda h: h["adjusted_identity"], reverse=True)
            matches.append({"cluster": cluster_name, "top_hits": scored_hits})

        return matches

    def _extract_reference_seqs(self, ref_ids: set[str]) -> dict[str, str]:
        """Extract specific sequences from the reference database.

        Uses the header offset index built by _load_name_lookup to seek
        directly to each entry instead of scanning the whole file (the
        reference DB can be hundreds of MB and this runs per specimen).
        """
        if not ref_ids:
            return {}

        ref_path = self.config.reference_db
        if not ref_path or not ref_path.exists():
            return {}

        if not self._seq_offsets:
            self._load_name_lookup()

        seqs = {}
        with open(ref_path, "rb") as f:
            for ref_id in ref_ids:
                offset = self._seq_offsets.get(ref_id)
                if offset is None:
                    continue
                f.seek(offset)
                f.readline()  # skip the header line
                parts = []
                for line in f:
                    if line.startswith(b">"):
                        break
                    parts.append(line.strip())
                if parts:
                    seqs[ref_id] = b"".join(parts).decode("utf-8", "replace")

        return seqs


def _extract_species_name(ref_id: str) -> str:
    """Extract a readable species name from a reference ID.

    Handles formats like: 'Genus_species_authority' or 'Genus species "code"'
    """
    # Replace underscores with spaces for display
    name = ref_id.replace("_", " ")
    # Take first two words as genus + species
    parts = name.split()
    if len(parts) >= 2:
        return f"{parts[0]} {parts[1]}"
    return name


def _read_fasta(path: Path) -> dict[str, str]:
    """Read a FASTA file into {header_id: sequence} dict."""
    seqs = {}
    current_id = None
    current_seq = []
    with open(path) as f:
        for line in f:
            if line.startswith(">"):
                if current_id:
                    seqs[current_id] = "".join(current_seq)
                current_id = line[1:].strip().split()[0]
                current_seq = []
            else:
                current_seq.append(line.strip())
        if current_id:
            seqs[current_id] = "".join(current_seq)
    return seqs
