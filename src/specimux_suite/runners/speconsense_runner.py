"""Subprocess wrapper for speconsense consensus generation."""

import logging
import re
import subprocess
import uuid
from pathlib import Path

from ..config import PipelineConfig
from ..events import EventLog

logger = logging.getLogger(__name__)

# Parse FASTA header: >specimen-c0 size=123 ric=100 rid=95.2 ...
CLUSTER_HEADER_RE = re.compile(
    r">(\S+)\s+"
    r"size=(\d+)"
    r"(?:\s+ric=(\d+))?"
    r"(?:\s+rid=([\d.]+))?"
)


class SpeconsenseRunner:
    """Runs speconsense as a subprocess and parses cluster output."""

    def __init__(self, config: PipelineConfig, event_log: EventLog):
        self.config = config
        self.event_log = event_log

    def run(self, specimen_id: str, specimen_fastq: Path, presample: int = 0) -> list[dict]:
        """Run speconsense on a specimen FASTQ.

        Args:
            presample: If > 0, pass --presample N to limit reads considered.

        Returns list of cluster dicts: [{name, size, ric, rid}, ...]
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
            )

            if result.returncode != 0:
                logger.error(f"speconsense failed for {specimen_id}: {result.stderr}")
                self.event_log.emit("pipeline.error", {
                    "component": "speconsense",
                    "specimen_id": specimen_id,
                    "message": f"speconsense exited with code {result.returncode}",
                    "details": result.stderr[-2000:] if result.stderr else "",
                })
                self.event_log.emit("consensus.completed", {
                    "specimen_id": specimen_id,
                    "job_id": job_id,
                    "clusters": [],
                })
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

        except FileNotFoundError:
            msg = "speconsense not found on PATH"
            logger.error(msg)
            self.event_log.emit("pipeline.error", {
                "component": "speconsense",
                "specimen_id": specimen_id,
                "message": msg,
            })
            return []

    def _build_command(self, specimen_fastq: Path, output_dir: Path, presample: int = 0) -> list[str]:
        """Build the speconsense command line."""
        cmd = [
            "speconsense",
            str(specimen_fastq),
            "-O", str(output_dir),
        ]
        if presample > 0:
            cmd.extend(["--presample", str(presample)])
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
                m = CLUSTER_HEADER_RE.match(line.strip())
                if m:
                    name, size, ric, rid = m.groups()
                    cluster = {
                        "name": name,
                        "size": int(size),
                    }
                    if ric is not None:
                        cluster["ric"] = int(ric)
                    if rid is not None:
                        cluster["rid"] = float(rid)
                    clusters.append(cluster)

        return clusters

    def get_consensus_fasta(self, specimen_id: str) -> Path | None:
        """Return path to the consensus FASTA for a specimen, if it exists."""
        fasta = self.config.consensus_output_dir / specimen_id / f"{specimen_id}-all.fasta"
        return fasta if fasta.exists() else None
