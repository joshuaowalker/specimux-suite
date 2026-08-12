"""Subprocess wrapper for speconsense-summarize variant summarization."""

import json
import logging
import uuid
import subprocess
from pathlib import Path

from ..config import PipelineConfig
from ..events import EventLog

logger = logging.getLogger(__name__)


class SummarizeRunner:
    """Runs speconsense-summarize as a subprocess for per-specimen variant summarization."""

    def __init__(self, config: PipelineConfig, event_log: EventLog):
        self.config = config
        self.event_log = event_log

    def run(self, specimen_id: str) -> list[dict]:
        """Run speconsense-summarize in single-specimen mode.

        Returns list of variant dicts from stdout JSON.
        """
        job_id = str(uuid.uuid4())[:8]

        self.event_log.emit("summarize.started", {
            "specimen_id": specimen_id,
            "job_id": job_id,
        })

        source_dir = self.config.consensus_output_dir / specimen_id
        summary_dir = self.config.summarize_output_dir

        cmd = self._build_command(source_dir, summary_dir, specimen_id)
        logger.info(f"Running summarize for {specimen_id}: {' '.join(str(c) for c in cmd)}")

        try:
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                check=False,
                timeout=self.config.job_timeout,
            )

            if result.returncode != 0:
                logger.error(f"speconsense-summarize failed for {specimen_id}: {result.stderr}")
                self.event_log.emit("pipeline.error", {
                    "component": "summarize",
                    "specimen_id": specimen_id,
                    "message": f"speconsense-summarize exited with code {result.returncode}",
                    "details": result.stderr[-2000:] if result.stderr else "",
                })
                return []

            # Parse JSON from stdout
            variants = self._parse_output(result.stdout, specimen_id)

            self.event_log.emit("summarize.completed", {
                "specimen_id": specimen_id,
                "job_id": job_id,
                "variant_count": len(variants),
                "variants": variants,
            })

            return variants

        except subprocess.TimeoutExpired:
            msg = f"speconsense-summarize timed out after {self.config.job_timeout}s (killed)"
            logger.error(f"{msg} for {specimen_id}")
            self.event_log.emit("pipeline.error", {
                "component": "summarize",
                "specimen_id": specimen_id,
                "message": msg,
            })
            return []
        except FileNotFoundError:
            msg = "speconsense-summarize not found on PATH"
            logger.error(msg)
            self.event_log.emit("pipeline.error", {
                "component": "summarize",
                "specimen_id": specimen_id,
                "message": msg,
            })
            return []

    def run_aggregate(self) -> None:
        """Run speconsense-summarize in aggregate-only mode to generate summary files."""
        source_dir = self.config.consensus_output_dir
        summary_dir = self.config.summarize_output_dir

        cmd = self._build_command(source_dir, summary_dir, aggregate_only=True)
        logger.info(f"Running summarize aggregate: {' '.join(str(c) for c in cmd)}")

        try:
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                check=False,
                timeout=self.config.job_timeout,
            )

            if result.returncode != 0:
                logger.error(f"speconsense-summarize aggregate failed: {result.stderr}")
                self.event_log.emit("pipeline.error", {
                    "component": "summarize",
                    "message": f"speconsense-summarize aggregate exited with code {result.returncode}",
                    "details": result.stderr[-2000:] if result.stderr else "",
                })
                return

            self.event_log.emit("summarize.aggregate_completed", {})
            logger.info("Summarize aggregate complete")

        except subprocess.TimeoutExpired:
            msg = f"speconsense-summarize aggregate timed out after {self.config.job_timeout}s (killed)"
            logger.error(msg)
            self.event_log.emit("pipeline.error", {
                "component": "summarize",
                "message": msg,
            })
        except FileNotFoundError:
            logger.error("speconsense-summarize not found on PATH")

    def _build_command(
        self,
        source_dir: Path,
        summary_dir: Path,
        specimen_id: str | None = None,
        aggregate_only: bool = False,
    ) -> list[str]:
        """Build the speconsense-summarize command line."""
        cmd = [
            "speconsense-summarize",
            "--source", str(source_dir),
            "--summary-dir", str(summary_dir),
        ]
        if specimen_id:
            cmd.extend(["--specimen", specimen_id])
        if aggregate_only:
            cmd.append("--aggregate-only")
        # Profile
        if self.config.summarize_profile:
            cmd.extend(["-p", self.config.summarize_profile])
        # Overrides from suite profile
        for key, value in self.config.summarize_overrides.items():
            if isinstance(value, bool):
                if value:
                    cmd.append(f"--{key}")
            else:
                cmd.extend([f"--{key}", str(value)])
        # Escape hatch (highest precedence)
        cmd.extend(self.config.summarize_args)
        return cmd

    def _parse_output(self, stdout: str, specimen_id: str) -> list[dict]:
        """Parse the JSON output from single-specimen mode."""
        # stdout may contain log lines before the JSON; find the JSON line
        for line in stdout.strip().splitlines():
            line = line.strip()
            if line.startswith("{"):
                try:
                    data = json.loads(line)
                    return data.get("variants", [])
                except json.JSONDecodeError:
                    continue
        logger.warning(f"No JSON output found from summarize for {specimen_id}")
        return []
