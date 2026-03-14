"""Priority queue and job dispatch for specimen consensus jobs."""

import logging
from dataclasses import dataclass
from typing import Optional

from .config import PipelineConfig
from .state import PipelineState, SpecimenStatus

logger = logging.getLogger(__name__)


@dataclass
class ConsensusJob:
    """A pending consensus job."""
    specimen_id: str
    read_count: int
    priority: float  # higher = more urgent


class Scheduler:
    """Decides which specimens need consensus and in what order."""

    def __init__(self, config: PipelineConfig, state: PipelineState):
        self.config = config
        self.state = state

    def get_ready_jobs(self, max_jobs: Optional[int] = None) -> list[ConsensusJob]:
        """Return prioritized list of specimens ready for consensus.

        Prioritization:
        - Specimens with no clusters: highest priority (by read count desc)
        - Specimens with clusters: reprocess if new_reads / reads_at_last_consensus > reprocess_ratio
        - Never-processed specimens are always eligible if >= min_reads
        """
        if max_jobs is None:
            max_jobs = self.config.workers

        jobs = []

        for sid, spec in self.state.specimens.items():
            # Skip if already running
            if spec.status == SpecimenStatus.CONSENSUS_RUNNING:
                continue

            if spec.total_reads < self.config.min_reads:
                continue

            has_clusters = len(spec.clusters) > 0

            if spec.consensus_version == 0:
                # Never processed — highest priority tier
                jobs.append(ConsensusJob(
                    specimen_id=sid,
                    read_count=spec.total_reads,
                    priority=1_000_000 + spec.total_reads,
                ))
            else:
                # Previously processed — check if enough new reads
                new_reads = spec.total_reads - spec.reads_at_last_consensus
                if spec.reads_at_last_consensus > 0:
                    ratio = new_reads / spec.reads_at_last_consensus
                else:
                    ratio = float("inf")

                if ratio > self.config.reprocess_ratio:
                    if has_clusters:
                        # Has results — lower priority tier
                        priority = ratio
                    else:
                        # No clusters yet — same tier as never-processed
                        priority = 1_000_000 + spec.total_reads
                    jobs.append(ConsensusJob(
                        specimen_id=sid,
                        read_count=spec.total_reads,
                        priority=priority,
                    ))

        # Sort by priority descending
        jobs.sort(key=lambda j: j.priority, reverse=True)

        return jobs[:max_jobs]

    def count_running(self) -> int:
        """Count specimens currently running consensus."""
        return sum(
            1 for spec in self.state.specimens.values()
            if spec.status == SpecimenStatus.CONSENSUS_RUNNING
        )

    def available_slots(self) -> int:
        """How many more consensus jobs can we start."""
        return max(0, self.config.workers - self.count_running())
