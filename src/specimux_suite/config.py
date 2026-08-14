"""Pipeline configuration."""

import importlib.util
import logging
import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)

# speconsense-summarize built-in defaults for the variant-routing filters.
# These mirror the `--min-cer-factor` / `--max-err-factor` argparse defaults in
# speconsense.summarize.cli; a value of 0 disables the corresponding filter.
# Kept here so the dashboard can preview which clusters summarize will route to
# its `.ns` (CER-not-significant) / `.lq` (low-quality) tracks.
SUMMARIZE_DEFAULT_MIN_CER_FACTOR = 1.0
SUMMARIZE_DEFAULT_MAX_ERR_FACTOR = 1.5


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

    # Profile-based tool configuration
    specimux_profile: Optional[str] = None
    speconsense_profile: Optional[str] = None
    specimux_overrides: dict = field(default_factory=dict)
    speconsense_overrides: dict = field(default_factory=dict)

    # Specimux passthrough args
    specimux_args: list[str] = field(default_factory=list)

    # Speconsense passthrough args
    speconsense_args: list[str] = field(default_factory=list)

    # Summarize settings
    summarize_profile: Optional[str] = None
    summarize_overrides: dict = field(default_factory=dict)
    summarize_args: list[str] = field(default_factory=list)

    # Live mode: subsample reads for incremental consensus (0 = no limit)
    live_presample: int = 100

    # vsearch settings
    vsearch_min_identity: float = 0.85
    vsearch_max_accepts: int = 10
    identify_min_coverage: float = 0.5

    # Job timeout (seconds) — kill subprocess if it exceeds this
    job_timeout: float = 3600.0  # 1 hour default

    # Web server
    web_host: str = "127.0.0.1"
    web_port: int = 8077
    share_url: Optional[str] = None  # set by --share; the URL for QR code
    share_max_clients: int = 0  # max concurrent SSE clients; 0 = unlimited

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
            # sched_getaffinity respects cgroup/taskset/SLURM CPU limits on
            # Linux; os.cpu_count() reports host cores (macOS has no affinity).
            try:
                available = len(os.sched_getaffinity(0))
            except AttributeError:
                available = os.cpu_count() or 2
            self.workers = max(1, available // 2)

    @property
    def specimux_output_dir(self) -> Path:
        return self.output_dir / "specimux"

    @property
    def consensus_output_dir(self) -> Path:
        return self.output_dir / "consensus"

    @property
    def identification_output_dir(self) -> Path:
        return self.output_dir / "identification"

    @property
    def summarize_output_dir(self) -> Path:
        return self.output_dir / "summary"

    def resolve_summarize_thresholds(self) -> dict:
        """Resolve the effective ``--min-cer-factor`` / ``--max-err-factor``.

        These determine how speconsense-summarize routes clusters to its
        ``.ns`` / ``.lq`` tracks. We resolve them from the same inputs the suite
        passes to the summarize subprocess, in the same precedence order the
        summarize CLI applies (lowest to highest): tool defaults < ``-p``
        profile < ``summarize.*`` overrides < ``--summarize-args`` escape hatch.

        Returns ``{"min_cer_factor": float, "max_err_factor": float}``.
        Profile resolution is best-effort: an unreadable/unknown profile leaves
        the lower-precedence value in place (debug-logged), never raising.
        """
        min_cer = SUMMARIZE_DEFAULT_MIN_CER_FACTOR
        max_err = SUMMARIZE_DEFAULT_MAX_ERR_FACTOR

        # 1. Named summarize profile (best-effort YAML read).
        if self.summarize_profile:
            prof = _read_summarize_profile_filters(self.summarize_profile)
            if "min-cer-factor" in prof:
                min_cer = _as_float(prof["min-cer-factor"], min_cer)
            if "max-err-factor" in prof:
                max_err = _as_float(prof["max-err-factor"], max_err)

        # 2. summarize.* overrides from the suite profile.
        for key in ("min-cer-factor", "min_cer_factor"):
            if key in self.summarize_overrides:
                min_cer = _as_float(self.summarize_overrides[key], min_cer)
        for key in ("max-err-factor", "max_err_factor"):
            if key in self.summarize_overrides:
                max_err = _as_float(self.summarize_overrides[key], max_err)

        # 3. --summarize-args escape hatch (highest precedence).
        min_cer = _scan_flag_value(self.summarize_args, "--min-cer-factor", min_cer)
        max_err = _scan_flag_value(self.summarize_args, "--max-err-factor", max_err)

        return {"min_cer_factor": min_cer, "max_err_factor": max_err}

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
            "summarize_filter": self.resolve_summarize_thresholds(),
        }


def _as_float(value, fallback: float) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return fallback


def _scan_flag_value(args: list[str], flag: str, fallback: float) -> float:
    """Return the float value of ``flag`` in ``args`` (``--flag V`` or ``--flag=V``)."""
    result = fallback
    for i, arg in enumerate(args):
        if arg == flag and i + 1 < len(args):
            result = _as_float(args[i + 1], result)
        elif arg.startswith(flag + "="):
            result = _as_float(arg.split("=", 1)[1], result)
    return result


def _read_summarize_profile_filters(name: str) -> dict:
    """Best-effort read of a speconsense profile's ``speconsense-summarize`` keys.

    Mirrors speconsense's profile search order (user dir, then the installed
    package's bundled profiles) without importing speconsense's API. Returns an
    empty dict on any failure so threshold resolution degrades to defaults.
    """
    import yaml

    candidates = [Path.home() / ".config" / "speconsense" / "profiles" / f"{name}.yaml"]
    try:
        spec = importlib.util.find_spec("speconsense")
        if spec and spec.submodule_search_locations:
            for loc in spec.submodule_search_locations:
                candidates.append(Path(loc) / "profiles" / f"{name}.yaml")
    except (ImportError, ValueError):
        pass

    for path in candidates:
        try:
            if not path.is_file():
                continue
            with open(path, encoding="utf-8") as f:
                data = yaml.safe_load(f) or {}
            return data.get("speconsense-summarize", {}) or {}
        except (OSError, yaml.YAMLError) as e:
            logger.debug(f"Could not read summarize profile '{name}' at {path}: {e}")
            return {}

    logger.debug(f"Summarize profile '{name}' not found; using filter defaults")
    return {}
