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
from ..util import atomic_write, count_fastq_reads_fast, scan_specimen_reads

logger = logging.getLogger(__name__)

# The demux commit boundary. specimux appends reads to per-specimen FASTQs
# and the runner emits specimux.completed afterwards; a run that dies in
# between leaves appended reads that a restart (which re-runs any file not
# marked processed) would append again. So before each demux the runner
# records every output file's length in this manifest, and a restart that
# finds it truncates the outputs back (appends only grow files, so this is
# exact) and removes files the interrupted demux created. The manifest is
# removed once the completion event is out.
INFLIGHT_FILENAME = "specimux-inflight.json"


class SpecimuxRunner:
    """Runs specimux as a subprocess and reports results via events."""

    def __init__(self, config: PipelineConfig, event_log: EventLog):
        self.config = config
        self.event_log = event_log
        # Per-file (size, reads) from previous scans; makes each post-demux
        # scan O(new data) instead of recounting every specimen file.
        self._scan_cache: dict[str, tuple[int, int]] = {}

    @property
    def inflight_path(self) -> Path:
        return self.config.output_dir / INFLIGHT_FILENAME

    def record_inflight(self, job_id: str, fastq_path: Path) -> None:
        """Write the manifest of output-file lengths before a demux starts."""
        output_dir = self.config.specimux_output_dir
        lengths = {}
        if output_dir.exists():
            for f in output_dir.rglob("*"):
                if f.is_file():
                    lengths[str(f.relative_to(output_dir))] = f.stat().st_size
        self.config.output_dir.mkdir(parents=True, exist_ok=True)
        atomic_write(self.inflight_path, json.dumps({
            "job_id": job_id,
            "file_path": str(fastq_path),
            "lengths": lengths,
        }).encode("utf-8"))

    def clear_inflight(self) -> None:
        try:
            self.inflight_path.unlink(missing_ok=True)
        except OSError as e:
            logger.warning(f"Could not remove {self.inflight_path}: {e}")

    def recover_interrupted(self) -> dict | None:
        """Roll the specimux output back to the state before an interrupted demux.

        Call at startup, before anything reads the per-specimen files. Does
        nothing without a manifest. Returns a summary of what was undone.
        """
        try:
            manifest = json.loads(self.inflight_path.read_text(encoding="utf-8"))
        except FileNotFoundError:
            return None
        except (OSError, ValueError) as e:
            logger.warning(f"Unreadable demux manifest {self.inflight_path}: {e}")
            return None

        output_dir = self.config.specimux_output_dir
        lengths: dict[str, int] = manifest.get("lengths", {})
        truncated, removed = [], []
        for rel, size in lengths.items():
            f = output_dir / rel
            try:
                current = f.stat().st_size
            except FileNotFoundError:
                continue
            if current > size:
                with open(f, "r+b") as fh:
                    fh.truncate(size)
                truncated.append(rel)
            elif current < size:
                logger.warning(f"{f} is shorter ({current}) than the demux manifest "
                               f"recorded ({size}); leaving it")
        if output_dir.exists():
            for f in sorted(output_dir.rglob("*")):
                if f.is_file() and str(f.relative_to(output_dir)) not in lengths:
                    f.unlink()
                    removed.append(str(f.relative_to(output_dir)))
        self.clear_inflight()
        self._scan_cache.clear()
        summary = {"file_path": manifest.get("file_path"), "job_id": manifest.get("job_id"),
                   "truncated": truncated, "removed": removed}
        logger.warning(
            f"Rolled back an interrupted demux of {manifest.get('file_path')}: "
            f"{len(truncated)} file(s) truncated, {len(removed)} removed; "
            "it will be demuxed again")
        return summary

    def run(self, fastq_path: Path, threads: int | None = None) -> dict[str, dict]:
        """Run specimux on a FASTQ file.

        Args:
            threads: Worker-thread override; defaults to config.workers.

        Returns specimen read counts: {specimen_id: {"pool": str, "reads": int, "path": str}}
        """
        job_id = str(uuid.uuid4())[:8]
        output_dir = self.config.specimux_output_dir
        input_reads = count_fastq_reads_fast(fastq_path)

        # A manifest here means the previous demux never reached its
        # completion event (startup recovery should have handled it; this
        # is the same rollback, idempotent)
        self.recover_interrupted()
        self.record_inflight(job_id, fastq_path)

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

        cmd = self._build_command(fastq_path, output_dir, threads=threads)
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
                self.clear_inflight()
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
                self.clear_inflight()
                return {}

            # Scan output directory for specimen read counts
            specimens = scan_specimen_reads(output_dir, cache=self._scan_cache)
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
            # The commit point: the event is durable, the appends are final
            self.clear_inflight()

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
            self.clear_inflight()  # nothing ran, nothing to roll back
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

    def _build_command(self, fastq_path: Path, output_dir: Path,
                       threads: int | None = None) -> list[str]:
        """Build the specimux command line."""
        cmd = [
            "specimux",
            str(self.config.primers_file),
            str(self.config.specimens_file),
            str(fastq_path),
            "-F",  # output to files
            "-O", str(output_dir),
            "-t", str(threads if threads else self.config.workers),
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
