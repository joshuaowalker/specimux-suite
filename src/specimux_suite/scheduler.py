"""Priority queue and job dispatch for specimen consensus jobs."""

import logging
from dataclasses import dataclass
from typing import Optional

from .config import PipelineConfig
from .state import PipelineState, SpecimenState, SpecimenStatus

logger = logging.getLogger(__name__)

# Priority bases per confidence band for reprocessing candidates (band 1 =
# most worth revisiting). Bases sit below the never-processed tier (1M+) so
# first answers always beat refinements; the growth ratio is added within a
# band (clamped under the 100k band gap).
_BAND_BASES = {1: 400_000, 2: 300_000, 3: 200_000, 4: 100_000, 5: 0}
# Uncertain results (bands 1-3) re-enter the reprocess queue at half the
# configured reprocess_ratio, so additional depth reaches them sooner.
_UNCERTAIN_BANDS = {1, 2, 3}


def _hit_genus(hit: dict) -> str:
    name = hit.get("name") or hit.get("ref_id") or ""
    parts = name.split()
    return parts[0].lower() if parts else ""


def confidence_band(spec: SpecimenState) -> tuple[int, str]:
    """Classify a previously-processed specimen by downstream confidence.

    Bands (1 = most urgent to revisit with more reads):
      1 no_match           consensus exists but nothing hit the reference DB
      2 low_identity       best hit < 90% adjusted identity
      3 off_target         no hit matches the community genus (possible
                           parasite/contaminant dominating the target), or the
                           community genus appears only in a minority cluster
      4 marginal/pending   identity 90-98%, ambiguous consensus bases, or
                           identification not yet available (neutral)
      5 confident          >=98% identity, on-target (or nothing to compare)

    Mirrored client-side in the dashboard (reprocessBand in index.html).
    """
    if not any(m.top_hits for m in spec.identification):
        if spec.status == SpecimenStatus.NO_MATCH:
            return 1, "no_match"
        # Identification pending (or no reference DB configured): neutral
        return 4, "pending"

    community_genus = spec.community_genus
    if not community_genus and spec.community_taxon:
        community_genus = spec.community_taxon.split()[0]
    community_genus = community_genus.lower()

    sizes = {c.name: c.size for c in spec.clusters}
    # Headline hit: on-target top-hit first, then largest cluster — mirrors
    # the dashboard's _findTopMatch. on_target_anywhere scans all hits like
    # the dashboard's target indicator.
    best_hit = None
    best_on = False
    best_size = -1
    dominant_hit = None
    dominant_size = -1
    on_target_anywhere = False
    for m in spec.identification:
        if not m.top_hits:
            continue
        if sizes and m.cluster not in sizes:
            continue
        top = m.top_hits[0]
        size = sizes.get(m.cluster, 0)
        if size > dominant_size:
            dominant_size = size
            dominant_hit = top
        if community_genus and any(_hit_genus(h) == community_genus for h in m.top_hits):
            on_target_anywhere = True
        on = bool(community_genus) and _hit_genus(top) == community_genus
        if best_hit is None or (on and not best_on) or (on == best_on and size > best_size):
            best_hit, best_on, best_size = top, on, size

    if best_hit is None:
        return 4, "pending"

    identity = best_hit.get("adjusted_identity") or best_hit.get("identity") or 0.0
    if identity < 0.90:
        return 2, "low_identity"
    if community_genus:
        if not on_target_anywhere:
            return 3, "off_target"
        if dominant_hit is not None and _hit_genus(dominant_hit) != community_genus:
            return 3, "minority_on_target"
    dominant_cluster = max(spec.clusters, key=lambda c: c.size, default=None)
    if identity < 0.98 or (dominant_cluster and (
            (dominant_cluster.ambig or 0) > 0 or dominant_cluster.chimera)):
        return 4, "marginal"
    return 5, "confident"


@dataclass
class ConsensusJob:
    """A pending consensus job."""
    specimen_id: str
    read_count: int
    priority: float  # higher = more urgent
    reason: str = ""  # why it was scheduled (band reason, "new", "no_clusters")


class Scheduler:
    """Decides which specimens need consensus and in what order."""

    def __init__(self, config: PipelineConfig, state: PipelineState):
        self.config = config
        self.state = state

    def get_ready_jobs(self, max_jobs: Optional[int] = None, min_reads: Optional[int] = None) -> list[ConsensusJob]:
        """Return prioritized list of specimens ready for consensus.

        Prioritization:
        - Specimens with no clusters: highest priority (by read count desc)
        - Specimens with clusters: reprocess if new_reads / reads_at_last_consensus
          exceeds the gate — reprocess_ratio, halved for uncertain results
          (bands 1-3) — ordered by confidence band, then growth ratio
        - Never-processed specimens are always eligible if >= min_reads

        Confidence only reorders and softens the gate; it never removes work.
        Finalization (get_all_eligible_jobs) is unaffected.

        Args:
            min_reads: Override config.min_reads threshold. Use 0 to process all.
        """
        threshold = min_reads if min_reads is not None else self.config.min_reads

        jobs = []

        for sid, spec in self.state.specimens.items():
            # Skip if already running
            if spec.status == SpecimenStatus.CONSENSUS_RUNNING:
                continue

            if spec.total_reads < threshold:
                continue

            has_clusters = len(spec.clusters) > 0

            if spec.consensus_version == 0:
                # Never processed — highest priority tier
                jobs.append(ConsensusJob(
                    specimen_id=sid,
                    read_count=spec.total_reads,
                    priority=1_000_000 + spec.total_reads,
                    reason="new",
                ))
            else:
                # Previously processed — check if enough new reads
                new_reads = spec.total_reads - spec.reads_at_last_consensus
                if spec.reads_at_last_consensus > 0:
                    ratio = new_reads / spec.reads_at_last_consensus
                else:
                    ratio = float("inf")

                if not has_clusters:
                    # No clusters yet — same tier as never-processed
                    if ratio > self.config.reprocess_ratio:
                        jobs.append(ConsensusJob(
                            specimen_id=sid,
                            read_count=spec.total_reads,
                            priority=1_000_000 + spec.total_reads,
                            reason="no_clusters",
                        ))
                else:
                    band, reason = confidence_band(spec)
                    gate = self.config.reprocess_ratio
                    if band in _UNCERTAIN_BANDS:
                        gate = gate / 2
                    if ratio > gate:
                        jobs.append(ConsensusJob(
                            specimen_id=sid,
                            read_count=spec.total_reads,
                            priority=_BAND_BASES[band] + min(ratio, 99_999.0),
                            reason=reason,
                        ))

        # Watched specimens get highest priority boost
        for job in jobs:
            spec = self.state.specimens[job.specimen_id]
            if spec.watched:
                job.priority += 10_000_000

        # Sort by priority descending
        jobs.sort(key=lambda j: j.priority, reverse=True)

        return jobs[:max_jobs]

    def get_all_eligible_jobs(self, max_jobs: Optional[int] = None, min_reads: Optional[int] = None) -> list[ConsensusJob]:
        """Return ALL specimens eligible for consensus, ignoring reprocess_ratio.

        Like get_ready_jobs() but skips the reprocess_ratio check — any specimen
        with total_reads >= min_reads that isn't currently running is returned.
        Used for finalization when the operator knows no more data is coming.

        Args:
            min_reads: Override config.min_reads threshold. Use 0 to process all.
        """
        threshold = min_reads if min_reads is not None else self.config.min_reads
        jobs = []

        for sid, spec in self.state.specimens.items():
            if spec.status == SpecimenStatus.CONSENSUS_RUNNING:
                continue

            if spec.total_reads < threshold:
                continue

            if spec.consensus_version == 0:
                # Never processed — highest priority tier
                priority = 1_000_000 + spec.total_reads
            elif spec.total_reads > spec.reads_at_last_consensus:
                # Has new reads since last consensus — worth reprocessing
                priority = spec.total_reads
            else:
                # No new reads — skip, reprocessing would be redundant
                continue

            jobs.append(ConsensusJob(
                specimen_id=sid,
                read_count=spec.total_reads,
                priority=priority,
            ))

        # Watched specimens get highest priority boost
        for job in jobs:
            spec = self.state.specimens[job.specimen_id]
            if spec.watched:
                job.priority += 10_000_000

        jobs.sort(key=lambda j: j.priority, reverse=True)

        if max_jobs is not None:
            return jobs[:max_jobs]
        return jobs

    def count_running(self) -> int:
        """Count specimens currently running consensus."""
        return sum(
            1 for spec in self.state.specimens.values()
            if spec.status == SpecimenStatus.CONSENSUS_RUNNING
        )

    def available_slots(self) -> int:
        """How many more consensus jobs can we start."""
        return max(0, self.config.workers - self.count_running())
