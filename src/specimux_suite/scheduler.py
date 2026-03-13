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
        - Never-processed specimens with >= min_reads: highest priority (by read count desc)
        - Previously-processed: scored by new_reads / reads_at_last_consensus
        - Only reprocess if ratio > reprocess_ratio
        """
        if max_jobs is None:
            max_jobs = self.config.max_concurrent_consensus

        jobs = []

        for sid, spec in self.state.specimens.items():
            # Skip if already running or errored
            if spec.status == SpecimenStatus.CONSENSUS_RUNNING:
                continue

            if spec.total_reads < self.config.min_reads:
                continue

            if spec.consensus_version == 0:
                # Never processed — highest priority
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
                    jobs.append(ConsensusJob(
                        specimen_id=sid,
                        read_count=spec.total_reads,
                        priority=ratio,
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
        return max(0, self.config.max_concurrent_consensus - self.count_running())
