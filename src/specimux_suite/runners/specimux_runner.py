"""Subprocess wrapper for specimux demultiplexing."""

import json
import logging
import subprocess
import tempfile
import threading
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

        # Create temp file for progress reporting
        progress_file = tempfile.NamedTemporaryFile(
            mode='w', suffix='.jsonl', prefix='specimux-progress-',
            dir=self.config.output_dir, delete=False
        )
        progress_path = Path(progress_file.name)
        progress_file.close()

        cmd = self._build_command(fastq_path, output_dir)
        cmd.extend(["--progress-file", str(progress_path)])
        logger.info(f"Running specimux: {' '.join(str(c) for c in cmd)}")

        try:
            proc = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
            )

            # Monitor progress in background thread
            stop_event = threading.Event()
            monitor = threading.Thread(
                target=self._monitor_progress,
                args=(progress_path, job_id, stop_event),
                daemon=True,
            )
            monitor.start()

            # Wait for completion, killing the process if it exceeds job_timeout
            try:
                stdout, stderr = proc.communicate(timeout=self.config.job_timeout)
            except subprocess.TimeoutExpired:
                proc.kill()
                stdout, stderr = proc.communicate()
                msg = f"specimux timed out after {self.config.job_timeout}s (killed)"
                logger.error(msg)
                self.event_log.emit("pipeline.error", {
                    "component": "specimux",
                    "message": msg,
                })
                self.event_log.emit("specimux.completed", {
                    "job_id": job_id,
                    "exit_code": -1,
                    "specimens": {},
                    "file_path": str(fastq_path),
                })
                return {}
            finally:
                # Stop monitor
                stop_event.set()
                monitor.join(timeout=2.0)

            if proc.returncode != 0:
                logger.error(f"specimux failed (exit {proc.returncode}): {stderr}")
                self.event_log.emit("pipeline.error", {
                    "component": "specimux",
                    "message": f"specimux exited with code {proc.returncode}",
                    "details": stderr[-2000:] if stderr else "",
                })
                self.event_log.emit("specimux.completed", {
                    "job_id": job_id,
                    "exit_code": proc.returncode,
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
        finally:
            # Clean up progress file
            try:
                progress_path.unlink(missing_ok=True)
            except OSError:
                pass

    def _monitor_progress(self, progress_path: Path, job_id: str, stop_event: threading.Event):
        """Tail the progress file and emit events."""
        # Wait briefly for the file to be created
        for _ in range(10):
            if progress_path.exists():
                break
            if stop_event.wait(0.5):
                return

        try:
            with open(progress_path, 'r') as f:
                while not stop_event.is_set():
                    line = f.readline()
                    if line:
                        line = line.strip()
                        if line:
                            try:
                                data = json.loads(line)
                                if data.get("type") == "progress":
                                    self.event_log.emit("specimux.progress", {
                                        "job_id": job_id,
                                        "processed": data.get("processed", 0),
                                        "matched": data.get("matched", 0),
                                        "total_est": data.get("total_est", 0),
                                        "rate": data.get("rate", 0.0),
                                    })
                            except json.JSONDecodeError:
                                pass
                    else:
                        # No new data, wait a bit
                        stop_event.wait(0.3)
        except (OSError, IOError):
            pass

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
        # Profile
        if self.config.specimux_profile:
            cmd.extend(["-p", self.config.specimux_profile])
        # Overrides from suite profile
        for key, value in self.config.specimux_overrides.items():
            if isinstance(value, bool):
                if value:
                    cmd.append(f"--{key}")
            else:
                cmd.extend([f"--{key}", str(value)])
        # Escape hatch (highest precedence)
        cmd.extend(self.config.specimux_args)
        return cmd
