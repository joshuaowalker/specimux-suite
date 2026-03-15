"""Identification runner: vsearch + adjusted-identity scoring."""

import logging
import re
import subprocess
import tempfile
from pathlib import Path

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
            )
            self._udb_path = udb
            return True
        except (subprocess.CalledProcessError, FileNotFoundError) as e:
            logger.error(f"Failed to build UDB: {e}")
            # Fall back to using FASTA directly
            self._udb_path = ref
            return True
        finally:
            self._load_name_lookup()

    def _load_name_lookup(self) -> None:
        """Scan reference FASTA headers for name="..." fields."""
        ref = self.config.reference_db
        if not ref or not ref.exists():
            return
        name_re = re.compile(r'name="(.+)"')
        with open(ref) as f:
            for line in f:
                if line.startswith(">"):
                    seq_id = line[1:].strip().split()[0]
                    m = name_re.search(line)
                    if m:
                        self._name_lookup[seq_id] = m.group(1).replace('\\"', '"')

    def run(self, specimen_id: str, consensus_fasta: Path) -> list[dict]:
        """Identify consensus clusters against reference DB.

        Returns list of {cluster, top_hits: [{ref_id, name, identity, adjusted_identity}]}
        """
        if not self._udb_path:
            if not self.ensure_db():
                return []

        # Step 1: vsearch search
        vsearch_hits = self._run_vsearch(consensus_fasta)

        # Step 2: adjusted-identity scoring for candidates
        matches = self._score_hits(consensus_fasta, vsearch_hits)

        self.event_log.emit("identification.completed", {
            "specimen_id": specimen_id,
            "matches": matches,
        })

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
            result = subprocess.run(cmd, capture_output=True, text=True, check=False)
        except FileNotFoundError:
            logger.error("vsearch not found on PATH")
            self.event_log.emit("pipeline.error", {
                "component": "vsearch",
                "message": "vsearch not found on PATH",
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
        """Re-score top vsearch hits with adjusted-identity."""
        try:
            from adjusted_identity import align_and_score, AdjustmentParams
        except ImportError:
            logger.warning("adjusted-identity not installed, using vsearch identity only")
            return self._format_matches_without_adjustment(vsearch_hits)

        # Read consensus sequences
        query_seqs = _read_fasta(consensus_fasta)

        # Read reference sequences for candidates (need to extract from DB)
        # For efficiency, only score top hits per cluster
        ref_ids_needed = set()
        for hits in vsearch_hits.values():
            for hit in hits[:5]:  # top 5 per cluster
                ref_ids_needed.add(hit["ref_id"])

        ref_seqs = self._extract_reference_seqs(ref_ids_needed)

        params = AdjustmentParams()

        matches = []
        for cluster_name, hits in vsearch_hits.items():
            query_seq = query_seqs.get(cluster_name, "")
            scored_hits = []
            for hit in hits[:5]:
                ref_seq = ref_seqs.get(hit["ref_id"], "")
                if query_seq and ref_seq:
                    try:
                        ar = align_and_score(query_seq, ref_seq, adjustment_params=params)
                        hit["adjusted_identity"] = ar.identity
                    except Exception as e:
                        logger.debug(f"adjusted-identity failed for {cluster_name} vs {hit['ref_id']}: {e}")
                        hit["adjusted_identity"] = hit["identity"]
                else:
                    hit["adjusted_identity"] = hit["identity"]
                scored_hits.append({
                    "ref_id": hit["ref_id"],
                    "name": hit["name"],
                    "identity": hit["identity"],
                    "adjusted_identity": hit["adjusted_identity"],
                })

            # Sort by adjusted identity descending
            scored_hits.sort(key=lambda h: h["adjusted_identity"], reverse=True)
            matches.append({"cluster": cluster_name, "top_hits": scored_hits})

        return matches

    def _format_matches_without_adjustment(self, vsearch_hits: dict) -> list[dict]:
        """Format matches when adjusted-identity is not available."""
        matches = []
        for cluster_name, hits in vsearch_hits.items():
            top_hits = [
                {
                    "ref_id": h["ref_id"],
                    "name": h["name"],
                    "identity": h["identity"],
                }
                for h in hits[:5]
            ]
            matches.append({"cluster": cluster_name, "top_hits": top_hits})
        return matches

    def _extract_reference_seqs(self, ref_ids: set[str]) -> dict[str, str]:
        """Extract specific sequences from the reference database."""
        if not ref_ids:
            return {}

        ref_path = self.config.reference_db
        if not ref_path or not ref_path.exists():
            return {}

        seqs = {}
        current_id = None
        current_seq = []
        with open(ref_path) as f:
            for line in f:
                if line.startswith(">"):
                    if current_id and current_id in ref_ids:
                        seqs[current_id] = "".join(current_seq)
                    header = line[1:].strip().split()[0]
                    current_id = header
                    current_seq = []
                else:
                    current_seq.append(line.strip())
            if current_id and current_id in ref_ids:
                seqs[current_id] = "".join(current_seq)

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
