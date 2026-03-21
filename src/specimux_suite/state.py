"""Pipeline state — in-memory materialized view rebuilt from events."""

import threading
from dataclasses import dataclass, field
from enum import Enum
from typing import Optional

from .events import Event, EventLog


class SpecimenStatus(str, Enum):
    WAITING = "waiting"
    CONSENSUS_RUNNING = "consensus_running"
    CONSENSUS_DONE = "consensus_done"
    IDENTIFIED = "identified"
    NO_MATCH = "no_match"
    SUMMARIZED = "summarized"
    ERROR = "error"


@dataclass
class ClusterInfo:
    name: str
    size: int
    ric: Optional[int] = None
    rid: Optional[float] = None


@dataclass
class IdentificationMatch:
    cluster: str
    top_hits: list[dict] = field(default_factory=list)
    # Each hit: {ref_id, name, identity, adjusted_identity}


@dataclass
class SpecimenState:
    specimen_id: str
    pool: str = ""
    community_taxon: str = ""
    community_genus: str = ""
    community_iconic_taxon: str = ""
    total_reads: int = 0
    reads_at_last_consensus: int = 0
    consensus_version: int = 0
    status: SpecimenStatus = SpecimenStatus.WAITING
    clusters: list[ClusterInfo] = field(default_factory=list)
    identification: list[IdentificationMatch] = field(default_factory=list)
    summarize_version: int = 0
    variants: list[dict] = field(default_factory=list)
    active_job_id: Optional[str] = None


@dataclass
class FileState:
    path: str
    size_bytes: int = 0
    stable: bool = False
    processed: bool = False


class PipelineState:
    """In-memory materialized view of the pipeline, rebuilt from events."""

    def __init__(self):
        self._lock = threading.Lock()
        self.specimens: dict[str, SpecimenState] = {}
        self.files: dict[str, FileState] = {}
        self.version: int = 0
        self.mode: Optional[str] = None
        self.errors: list[dict] = []
        self.total_input_reads: int = 0
        self.total_matched_reads: int = 0
        self.specimux_runs: int = 0

    def apply(self, event: Event) -> None:
        """Apply a single event to update state (thread-safe)."""
        with self._lock:
            self.version = event.version
            handler = self._handlers.get(event.type)
            if handler:
                handler(self, event.data)

    def rebuild(self, event_log: EventLog) -> None:
        """Rebuild state by replaying all events."""
        with self._lock:
            for event in event_log.replay():
                self.version = event.version
                handler = self._handlers.get(event.type)
                if handler:
                    handler(self, event.data)

    def get_specimen(self, specimen_id: str) -> SpecimenState:
        if specimen_id not in self.specimens:
            self.specimens[specimen_id] = SpecimenState(specimen_id=specimen_id)
        return self.specimens[specimen_id]

    def to_dict(self) -> dict:
        """Serialize full state for API (thread-safe snapshot)."""
        with self._lock:
            return {
                "version": self.version,
                "mode": self.mode,
                "total_input_reads": self.total_input_reads,
                "total_matched_reads": self.total_matched_reads,
                "specimens": {
                    sid: _specimen_to_dict(s) for sid, s in self.specimens.items()
                },
                "errors": list(self.errors),
            }

    # --- Event handlers ---

    def _on_pipeline_started(self, data: dict):
        self.mode = data.get("mode")

    def _on_specimens_loaded(self, data: dict):
        for s in data.get("specimens", []):
            spec = self.get_specimen(s["specimen_id"])
            if s.get("pool"):
                spec.pool = s["pool"]

    def _on_specimens_taxa(self, data: dict):
        for specimen_id, taxon in data.get("taxa", {}).items():
            spec = self.get_specimen(specimen_id)
            if isinstance(taxon, dict):
                spec.community_taxon = taxon.get("name", "")
                spec.community_genus = taxon.get("genus", "")
                spec.community_iconic_taxon = taxon.get("iconic_taxon", "")
            else:
                # Legacy string format
                spec.community_taxon = taxon
                spec.community_genus = taxon.split()[0] if taxon else ""

    def _on_file_detected(self, data: dict):
        path = data["path"]
        self.files[path] = FileState(path=path, size_bytes=data.get("size_bytes", 0))

    def _on_file_stable(self, data: dict):
        path = data["path"]
        if path in self.files:
            self.files[path].stable = True
            self.files[path].size_bytes = data.get("size_bytes", self.files[path].size_bytes)

    def _on_specimux_started(self, data: dict):
        pass  # tracked for logging, no state change needed

    def _on_specimux_completed(self, data: dict):
        self.specimux_runs += 1
        specimens = data.get("specimens", {})
        for name, read_count in specimens.items():
            spec = self.get_specimen(name)
            spec.total_reads = read_count
            # Pool info comes from specimen name convention or data
            if "pool" in data:
                spec.pool = data["pool"]
        file_path = data.get("file_path")
        if file_path and file_path in self.files:
            self.files[file_path].processed = True
        self.total_input_reads += data.get("input_reads", 0)
        # Recompute matched reads from specimen totals (scan_specimen_reads
        # returns cumulative counts, so accumulating would double-count)
        self.total_matched_reads = sum(s.total_reads for s in self.specimens.values())

    def _on_specimen_updated(self, data: dict):
        spec = self.get_specimen(data["specimen_id"])
        if "pool" in data:
            spec.pool = data["pool"]
        spec.total_reads = data.get("total_reads", spec.total_reads)
        if "consensus_version" in data and data["consensus_version"] is not None:
            spec.consensus_version = data["consensus_version"]

    def _on_consensus_started(self, data: dict):
        spec = self.get_specimen(data["specimen_id"])
        spec.status = SpecimenStatus.CONSENSUS_RUNNING
        spec.active_job_id = data.get("job_id")

    def _on_consensus_completed(self, data: dict):
        spec = self.get_specimen(data["specimen_id"])
        spec.status = SpecimenStatus.CONSENSUS_DONE
        spec.active_job_id = None
        spec.reads_at_last_consensus = spec.total_reads
        spec.consensus_version += 1
        spec.identification = []  # Clear stale identification for re-identification
        clusters = []
        for c in data.get("clusters", []):
            clusters.append(ClusterInfo(
                name=c["name"],
                size=c["size"],
                ric=c.get("ric"),
                rid=c.get("rid"),
            ))
        spec.clusters = clusters

    def _on_identification_completed(self, data: dict):
        spec = self.get_specimen(data["specimen_id"])
        matches = []
        for m in data.get("matches", []):
            matches.append(IdentificationMatch(
                cluster=m["cluster"],
                top_hits=m.get("top_hits", []),
            ))
        # Merge: keep existing identifications for clusters not in the new batch
        new_clusters = {m.cluster for m in matches}
        kept = [m for m in spec.identification if m.cluster not in new_clusters]
        spec.identification = kept + matches
        has_hits = any(m.top_hits for m in matches)
        if spec.status != SpecimenStatus.SUMMARIZED:
            spec.status = SpecimenStatus.IDENTIFIED if has_hits else SpecimenStatus.NO_MATCH

    def _on_summarize_started(self, data: dict):
        spec = self.get_specimen(data["specimen_id"])
        spec.active_job_id = data.get("job_id")

    def _on_summarize_completed(self, data: dict):
        spec = self.get_specimen(data["specimen_id"])
        spec.status = SpecimenStatus.SUMMARIZED
        spec.active_job_id = None
        spec.summarize_version += 1
        spec.variants = data.get("variants", [])

    def _on_pipeline_error(self, data: dict):
        self.errors.append(data)
        # If it's a specimen-level error, mark it
        if "specimen_id" in data:
            spec = self.get_specimen(data["specimen_id"])
            spec.status = SpecimenStatus.ERROR
            spec.active_job_id = None

    _handlers = {
        "pipeline.started": _on_pipeline_started,
        "specimens.loaded": _on_specimens_loaded,
        "specimens.taxa": _on_specimens_taxa,
        "file.detected": _on_file_detected,
        "file.stable": _on_file_stable,
        "specimux.started": _on_specimux_started,
        "specimux.completed": _on_specimux_completed,
        "specimen.updated": _on_specimen_updated,
        "consensus.started": _on_consensus_started,
        "consensus.completed": _on_consensus_completed,
        "identification.completed": _on_identification_completed,
        "summarize.started": _on_summarize_started,
        "summarize.completed": _on_summarize_completed,
        "pipeline.error": _on_pipeline_error,
    }


def _specimen_to_dict(s: SpecimenState) -> dict:
    result = {
        "specimen_id": s.specimen_id,
        "pool": s.pool,
        "community_taxon": s.community_taxon,
        "community_genus": s.community_genus,
        "community_iconic_taxon": s.community_iconic_taxon,
        "total_reads": s.total_reads,
        "reads_at_last_consensus": s.reads_at_last_consensus,
        "consensus_version": s.consensus_version,
        "status": s.status.value,
        "clusters": [
            {"name": c.name, "size": c.size, "ric": c.ric, "rid": c.rid}
            for c in s.clusters
        ],
        "identification": [
            {"cluster": m.cluster, "top_hits": m.top_hits}
            for m in s.identification
        ],
        "summarize_version": s.summarize_version,
        "variants": s.variants,
    }
    return result
