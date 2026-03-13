"""Pipeline orchestrator: wires components together for batch and live modes."""

import logging
import threading
from concurrent.futures import ThreadPoolExecutor, Future
from pathlib import Path

from .config import PipelineConfig
from .events import EventLog
from .state import PipelineState
from .scheduler import Scheduler
from .runners.specimux_runner import SpecimuxRunner
from .runners.speconsense_runner import SpeconsenseRunner
from .runners.identify_runner import IdentifyRunner

logger = logging.getLogger(__name__)


class Pipeline:
    """Main pipeline orchestrator."""

    def __init__(self, config: PipelineConfig):
        self.config = config
        self.event_log = EventLog(config.event_log_path)
        self.state = PipelineState()
        self.scheduler = Scheduler(config, self.state)

        self.specimux = SpecimuxRunner(config, self.event_log)
        self.speconsense = SpeconsenseRunner(config, self.event_log)
        self.identify = IdentifyRunner(config, self.event_log) if config.reference_db else None

        self._executor = ThreadPoolExecutor(max_workers=config.max_concurrent_consensus)
        self._futures: dict[str, Future] = {}
        self._shutdown = threading.Event()

    def run_batch(self) -> None:
        """Run the full batch pipeline: specimux → consensus → identification."""
        # Rebuild state from any existing events
        self.state.rebuild(self.event_log)

        self.event_log.emit("pipeline.started", {
            "mode": "batch",
            "config_summary": self.config.summary(),
        })

        # Ensure output dirs exist
        self.config.output_dir.mkdir(parents=True, exist_ok=True)
        self.config.specimux_output_dir.mkdir(parents=True, exist_ok=True)
        self.config.consensus_output_dir.mkdir(parents=True, exist_ok=True)

        # Build identification DB if needed
        if self.identify:
            self.identify.ensure_db()

        # Step 1: Run specimux
        logger.info(f"Running specimux on {self.config.reads_file}")
        specimens = self.specimux.run(self.config.reads_file)

        if not specimens:
            logger.warning("No specimens found after specimux")
            return

        # Rebuild state after specimux events
        self.state = PipelineState()
        self.state.rebuild(self.event_log)

        # Step 2: Schedule and run consensus jobs
        logger.info(f"Found {len(specimens)} specimens, scheduling consensus")
        self._run_consensus_round()

        # Step 3: Run identification on completed specimens
        if self.identify:
            self._run_identification()

        self._executor.shutdown(wait=True)
        logger.info("Batch pipeline complete")

    def run_live(self) -> None:
        """Run the live pipeline with file watching."""
        from .watcher import FileWatcher

        self.state.rebuild(self.event_log)

        self.event_log.emit("pipeline.started", {
            "mode": "live",
            "config_summary": self.config.summary(),
        })

        self.config.output_dir.mkdir(parents=True, exist_ok=True)
        self.config.specimux_output_dir.mkdir(parents=True, exist_ok=True)
        self.config.consensus_output_dir.mkdir(parents=True, exist_ok=True)

        if self.identify:
            self.identify.ensure_db()

        watcher = FileWatcher(
            watch_dir=self.config.watch_dir,
            pattern=self.config.watch_pattern,
            settle_time=self.config.settle_time,
            on_file_stable=self._on_file_stable,
            event_log=self.event_log,
        )

        watcher.start()
        logger.info(f"Watching {self.config.watch_dir} for new FASTQ files")

        try:
            while not self._shutdown.is_set():
                self._shutdown.wait(timeout=2.0)
                self._check_completed_futures()
        except KeyboardInterrupt:
            logger.info("Shutting down live pipeline")
        finally:
            watcher.stop()
            self._executor.shutdown(wait=True)

    def shutdown(self) -> None:
        """Signal the pipeline to shut down."""
        self._shutdown.set()

    def _on_file_stable(self, file_path: Path) -> None:
        """Called by watcher when a new stable FASTQ is detected."""
        logger.info(f"Processing new file: {file_path}")
        specimens = self.specimux.run(file_path)

        # Rebuild state and check for work
        self.state = PipelineState()
        self.state.rebuild(self.event_log)
        self._schedule_consensus()

    def _run_consensus_round(self) -> None:
        """Run consensus on all ready specimens, respecting concurrency."""
        jobs = self.scheduler.get_ready_jobs(max_jobs=None)  # get all ready
        if not jobs:
            logger.info("No specimens ready for consensus")
            return

        logger.info(f"Running consensus for {len(jobs)} specimens")

        # Submit in batches respecting concurrency
        pending = list(jobs)
        while pending:
            slots = self.scheduler.available_slots()
            if slots <= 0:
                # Wait for a future to complete
                self._wait_for_any_future()
                # Rebuild state
                self.state = PipelineState()
                self.state.rebuild(self.event_log)
                continue

            batch = pending[:slots]
            pending = pending[slots:]

            for job in batch:
                self._submit_consensus(job.specimen_id)

        # Wait for all remaining
        self._wait_all_futures()

    def _schedule_consensus(self) -> None:
        """Check scheduler and submit consensus jobs for available slots."""
        slots = self.scheduler.available_slots()
        if slots <= 0:
            return
        jobs = self.scheduler.get_ready_jobs(max_jobs=slots)
        for job in jobs:
            self._submit_consensus(job.specimen_id)

    def _submit_consensus(self, specimen_id: str) -> None:
        """Submit a consensus job to the thread pool."""
        spec = self.state.get_specimen(specimen_id)
        specimen_fastq = self._find_specimen_fastq(specimen_id, spec.pool)
        if not specimen_fastq:
            logger.warning(f"No FASTQ found for specimen {specimen_id}")
            return

        future = self._executor.submit(self._run_consensus_job, specimen_id, specimen_fastq)
        self._futures[specimen_id] = future

    def _run_consensus_job(self, specimen_id: str, specimen_fastq: Path) -> list[dict]:
        """Run consensus for a single specimen (executed in thread pool)."""
        return self.speconsense.run(specimen_id, specimen_fastq)

    def _run_identification(self) -> None:
        """Run identification on all specimens with completed consensus."""
        self.state = PipelineState()
        self.state.rebuild(self.event_log)

        for sid, spec in self.state.specimens.items():
            if spec.clusters and not spec.identification:
                consensus_fasta = self.speconsense.get_consensus_fasta(sid)
                if consensus_fasta:
                    logger.info(f"Identifying {sid}")
                    self.identify.run(sid, consensus_fasta)

    def _find_specimen_fastq(self, specimen_id: str, pool: str) -> Path | None:
        """Find the accumulated FASTQ for a specimen in specimux output."""
        # Look in full/{pool}/{specimen}.fastq
        if pool:
            path = self.config.specimux_output_dir / "full" / pool / f"{specimen_id}.fastq"
            if path.exists():
                return path

        # Fallback: search all pools
        full_dir = self.config.specimux_output_dir / "full"
        if full_dir.exists():
            for pool_dir in full_dir.iterdir():
                if pool_dir.is_dir():
                    path = pool_dir / f"{specimen_id}.fastq"
                    if path.exists():
                        return path
        return None

    def _wait_for_any_future(self) -> None:
        """Wait for at least one future to complete."""
        from concurrent.futures import as_completed
        if self._futures:
            done = next(as_completed(self._futures.values()))
            # Find and remove the completed one
            for sid, fut in list(self._futures.items()):
                if fut.done():
                    del self._futures[sid]
                    try:
                        fut.result()
                    except Exception as e:
                        logger.error(f"Consensus job failed for {sid}: {e}")

    def _wait_all_futures(self) -> None:
        """Wait for all pending futures."""
        for sid, fut in list(self._futures.items()):
            try:
                fut.result()
            except Exception as e:
                logger.error(f"Consensus job failed for {sid}: {e}")
        self._futures.clear()

    def _check_completed_futures(self) -> None:
        """Check for completed futures and trigger identification."""
        completed = []
        for sid, fut in list(self._futures.items()):
            if fut.done():
                completed.append(sid)
                try:
                    fut.result()
                except Exception as e:
                    logger.error(f"Consensus job failed for {sid}: {e}")

        for sid in completed:
            del self._futures[sid]

            # Trigger identification if available
            if self.identify:
                self.state = PipelineState()
                self.state.rebuild(self.event_log)
                consensus_fasta = self.speconsense.get_consensus_fasta(sid)
                if consensus_fasta:
                    self._executor.submit(self.identify.run, sid, consensus_fasta)

        # Check for new consensus work
        if completed:
            self.state = PipelineState()
            self.state.rebuild(self.event_log)
            self._schedule_consensus()
