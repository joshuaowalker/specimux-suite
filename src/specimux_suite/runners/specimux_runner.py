"""Subprocess wrapper for specimux demultiplexing."""

import logging
import subprocess
import uuid
from pathlib import Path

from ..config import PipelineConfig
from ..events import EventLog
from ..util import count_fastq_reads_fast, scan_specimen_reads

logger = logging.getLogger(__name__)


class SpecimuxRunner:
    """Runs specimux as a subprocess and reports results via events."""

    def __init__(self, config: PipelineConfig, event_log: EventLog):
        self.config = config
        self.event_log = event_log

    def run(self, fastq_path: Path) -> dict[str, dict]:
        """Run specimux on a FASTQ file.

        Returns specimen read counts: {specimen_id: {"pool": str, "reads": int, "path": str}}
        """
        job_id = str(uuid.uuid4())[:8]
        output_dir = self.config.specimux_output_dir
        input_reads = count_fastq_reads_fast(fastq_path)

        self.event_log.emit("specimux.started", {
            "job_id": job_id,
            "file_path": str(fastq_path),
            "input_reads": input_reads,
        })

        cmd = self._build_command(fastq_path, output_dir)
        logger.info(f"Running specimux: {' '.join(str(c) for c in cmd)}")

        try:
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                check=False,
            )

            if result.returncode != 0:
                logger.error(f"specimux failed (exit {result.returncode}): {result.stderr}")
                self.event_log.emit("pipeline.error", {
                    "component": "specimux",
                    "message": f"specimux exited with code {result.returncode}",
                    "details": result.stderr[-2000:] if result.stderr else "",
                })
                self.event_log.emit("specimux.completed", {
                    "job_id": job_id,
                    "exit_code": result.returncode,
                    "specimens": {},
                    "file_path": str(fastq_path),
                })
                return {}

            # Scan output directory for specimen read counts
            specimens = scan_specimen_reads(output_dir)
            specimen_counts = {sid: info["reads"] for sid, info in specimens.items()}
            matched_reads = sum(specimen_counts.values())

            self.event_log.emit("specimux.completed", {
                "job_id": job_id,
                "exit_code": 0,
                "specimens": specimen_counts,
                "file_path": str(fastq_path),
                "input_reads": input_reads,
                "matched_reads": matched_reads,
            })

            # Emit per-specimen updates
            for sid, info in specimens.items():
                self.event_log.emit("specimen.updated", {
                    "specimen_id": sid,
                    "pool": info["pool"],
                    "total_reads": info["reads"],
                    "new_reads": info["reads"],
                })

            return specimens

        except FileNotFoundError:
            msg = "specimux not found on PATH"
            logger.error(msg)
            self.event_log.emit("pipeline.error", {
                "component": "specimux",
                "message": msg,
            })
            return {}

    def _build_command(self, fastq_path: Path, output_dir: Path) -> list[str]:
        """Build the specimux command line."""
        cmd = [
            "specimux",
            str(self.config.primers_file),
            str(self.config.specimens_file),
            str(fastq_path),
            "-F",  # output to files
            "-O", str(output_dir),
            "-t", str(self.config.workers),
        ]
        cmd.extend(self.config.specimux_args)
        return cmd
