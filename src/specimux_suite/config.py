"""Pipeline configuration."""

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional


@dataclass
class PipelineConfig:
    """Configuration for a pipeline run."""

    # Required inputs
    primers_file: Path
    specimens_file: Path

    # Mode-specific: batch gets reads_file, live gets watch_dir
    reads_file: Optional[Path] = None
    watch_dir: Optional[Path] = None

    # Output
    output_dir: Path = Path("specimux-suite-output")
    event_log_path: Optional[Path] = None  # defaults to output_dir/events.jsonl

    # Identification
    reference_db: Optional[Path] = None

    # Scheduler tuning
    min_reads: int = 30
    reprocess_ratio: float = 0.5
    workers: int = 0  # 0 = auto (total cores / 2)

    # Watcher tuning (live mode)
    settle_time: float = 30.0
    watch_pattern: str = "*.fastq"

    # Specimux passthrough args
    specimux_args: list[str] = field(default_factory=list)

    # Speconsense passthrough args
    speconsense_args: list[str] = field(default_factory=list)

    # vsearch settings
    vsearch_min_identity: float = 0.85
    vsearch_max_accepts: int = 10

    # Job timeout (seconds) — kill subprocess if it exceeds this
    job_timeout: float = 3600.0  # 1 hour default

    # Web server
    web_host: str = "0.0.0.0"
    web_port: int = 8077

    def __post_init__(self):
        self.primers_file = Path(self.primers_file)
        self.specimens_file = Path(self.specimens_file)
        if self.reads_file:
            self.reads_file = Path(self.reads_file)
        if self.watch_dir:
            self.watch_dir = Path(self.watch_dir)
        self.output_dir = Path(self.output_dir)
        if self.reference_db:
            self.reference_db = Path(self.reference_db)
        if self.event_log_path is None:
            self.event_log_path = self.output_dir / "events.jsonl"
        if self.workers <= 0:
            self.workers = max(1, os.cpu_count() // 2)

    @property
    def specimux_output_dir(self) -> Path:
        return self.output_dir / "specimux"

    @property
    def consensus_output_dir(self) -> Path:
        return self.output_dir / "consensus"

    @property
    def identification_output_dir(self) -> Path:
        return self.output_dir / "identification"

    def summary(self) -> dict:
        """Return a JSON-serializable config summary for events."""
        return {
            "primers_file": str(self.primers_file),
            "specimens_file": str(self.specimens_file),
            "reads_file": str(self.reads_file) if self.reads_file else None,
            "watch_dir": str(self.watch_dir) if self.watch_dir else None,
            "output_dir": str(self.output_dir),
            "reference_db": str(self.reference_db) if self.reference_db else None,
            "min_reads": self.min_reads,
            "reprocess_ratio": self.reprocess_ratio,
            "workers": self.workers,
        }
