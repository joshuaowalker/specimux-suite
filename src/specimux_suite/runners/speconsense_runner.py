"""Subprocess wrapper for speconsense consensus generation."""

import logging
import math
import subprocess
import uuid
from pathlib import Path

from ..config import PipelineConfig
from ..events import EventLog

logger = logging.getLogger(__name__)

# Typed cluster-header fields (key -> converter). Numeric values are parsed
# defensively; anything that fails conversion is dropped rather than raising.
_INT_FIELDS = ("size", "ric", "ambig", "gid", "vid")
_FLOAT_FIELDS = ("rid", "rid_min", "cer_factor", "err_factor")
_STR_FIELDS = ("primers",)


def parse_cluster_header(line: str) -> dict | None:
    """Parse a speconsense consensus defline into a cluster dict.

    Handles both 0.7.x headers (``>sample-c0 size=.. ric=.. rid=.. rid_min=..
    primers=.. ambig=..``) and 0.8.x headers, which additionally carry
    ``cer_factor=``, ``err_factor=``, ``gid=`` and ``vid=`` and name the record
    ``sample-{gid}.v{vid}`` instead of ``sample-c{n}``. The first whitespace
    token is the record name; remaining ``key=value`` tokens are captured by
    type. Unknown keys are ignored. Missing keys are simply absent.

    ``cer_factor`` is emitted by core as ``inf`` for always-pass clusters
    (anchors / no comparison peer); since JSON has no infinity literal that
    browsers accept, non-finite factors are normalized to ``None`` — which the
    routing logic already treats as "passes the filter".
    """
    line = line.strip()
    if not line.startswith(">"):
        return None
    parts = line[1:].split()
    if not parts:
        return None

    cluster: dict = {"name": parts[0]}
    for token in parts[1:]:
        if "=" not in token:
            continue
        key, value = token.split("=", 1)
        if key in _INT_FIELDS:
            try:
                cluster[key] = int(value)
            except ValueError:
                continue
        elif key in _FLOAT_FIELDS:
            try:
                num = float(value)  # accepts "inf" / "nan"
            except ValueError:
                continue
            cluster[key] = num if math.isfinite(num) else None
        elif key in _STR_FIELDS:
            cluster[key] = value
    return cluster


class SpeconsenseRunner:
    """Runs speconsense as a subprocess and parses cluster output."""

    def __init__(self, config: PipelineConfig, event_log: EventLog):
        self.config = config
        self.event_log = event_log

    def run(self, specimen_id: str, specimen_fastq: Path, presample: int = 0) -> list[dict]:
        """Run speconsense on a specimen FASTQ.

        Args:
            presample: If > 0, pass --presample N to limit reads considered.

        Returns list of cluster dicts. Always carries ``name`` and ``size``;
        ``ric``, ``rid``, ``rid_min``, ``primers``, ``ambig`` and (0.8.x only)
        ``cer_factor``, ``err_factor``, ``gid``, ``vid`` are present when the
        header supplies them.
        """
        job_id = str(uuid.uuid4())[:8]
        from ..util import count_fastq_reads_fast
        read_count = count_fastq_reads_fast(specimen_fastq)

        output_dir = self.config.consensus_output_dir / specimen_id

        self.event_log.emit("consensus.started", {
            "specimen_id": specimen_id,
            "job_id": job_id,
            "read_count": read_count,
        })

        cmd = self._build_command(specimen_fastq, output_dir, presample=presample)
        logger.info(f"Running speconsense for {specimen_id}: {' '.join(str(c) for c in cmd)}")

        try:
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                check=False,
                timeout=self.config.job_timeout,
            )

            if result.returncode != 0:
                logger.error(f"speconsense failed for {specimen_id}: {result.stderr}")
                self._emit_failure(
                    specimen_id, job_id,
                    f"speconsense exited with code {result.returncode}",
                    result.stderr,
                )
                return []

            # Parse the -all.fasta output
            clusters = self._parse_clusters(output_dir, specimen_id)

            self.event_log.emit("consensus.completed", {
                "specimen_id": specimen_id,
                "job_id": job_id,
                "read_count": read_count,
                "clusters": clusters,
            })

            # Emit explicit specimen update with consensus version info
            self.event_log.emit("specimen.updated", {
                "specimen_id": specimen_id,
                "total_reads": read_count,
                "consensus_version": None,  # incremented by state handler
            })

            return clusters

        except subprocess.TimeoutExpired:
            msg = f"speconsense timed out after {self.config.job_timeout}s (killed)"
            logger.error(f"{msg} for {specimen_id}")
            self._emit_failure(specimen_id, job_id, msg)
            return []
        except FileNotFoundError:
            msg = "speconsense not found on PATH"
            logger.error(msg)
            self.event_log.emit("pipeline.error", {
                "component": "speconsense",
                "specimen_id": specimen_id,
                "message": msg,
            })
            return []

    def _emit_failure(self, specimen_id: str, job_id: str, message: str,
                      stderr: str = "") -> None:
        """Emit a failed-run event pair for a specimen.

        consensus.completed is emitted first so that state replay applies
        pipeline.error last — otherwise the specimen displays CONSENSUS_DONE
        instead of ERROR. The completed event still bumps consensus_version
        and reads_at_last_consensus, preventing an immediate retry loop.
        """
        self.event_log.emit("consensus.completed", {
            "specimen_id": specimen_id,
            "job_id": job_id,
            "clusters": [],
        })
        self.event_log.emit("pipeline.error", {
            "component": "speconsense",
            "specimen_id": specimen_id,
            "message": message,
            "details": stderr[-2000:] if stderr else "",
        })

    def _build_command(self, specimen_fastq: Path, output_dir: Path, presample: int = 0) -> list[str]:
        """Build the speconsense command line."""
        cmd = [
            "speconsense",
            str(specimen_fastq),
            "-O", str(output_dir),
        ]
        if presample > 0:
            cmd.extend(["--presample", str(presample)])
        # Profile
        if self.config.speconsense_profile:
            cmd.extend(["-p", self.config.speconsense_profile])
        # Overrides from suite profile
        for key, value in self.config.speconsense_overrides.items():
            if isinstance(value, bool):
                if value:
                    cmd.append(f"--{key}")
            else:
                cmd.extend([f"--{key}", str(value)])
        # Escape hatch (highest precedence)
        cmd.extend(self.config.speconsense_args)
        return cmd

    def _parse_clusters(self, output_dir: Path, specimen_id: str) -> list[dict]:
        """Parse the -all.fasta file for cluster metadata."""
        all_fasta = output_dir / f"{specimen_id}-all.fasta"
        if not all_fasta.exists():
            logger.warning(f"No cluster output found: {all_fasta}")
            return []

        clusters = []
        with open(all_fasta) as f:
            for line in f:
                if not line.startswith(">"):
                    continue
                cluster = parse_cluster_header(line)
                if cluster and "size" in cluster:
                    clusters.append(cluster)

        return clusters

    def get_consensus_fasta(self, specimen_id: str) -> Path | None:
        """Return path to the consensus FASTA for a specimen, if it exists."""
        fasta = self.config.consensus_output_dir / specimen_id / f"{specimen_id}-all.fasta"
        return fasta if fasta.exists() else None
