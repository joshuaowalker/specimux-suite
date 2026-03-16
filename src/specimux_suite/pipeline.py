"""Pipeline orchestrator: wires components together for batch and live modes."""

import logging
import queue
import sys
import threading
from concurrent.futures import ThreadPoolExecutor, Future, TimeoutError
from pathlib import Path

from .config import PipelineConfig
from .console import ConsoleUI
from .events import EventLog
from .state import PipelineState
from .scheduler import Scheduler
from .inat import extract_inat_ids, fetch_community_taxa
from .util import parse_specimens_file
from .runners.specimux_runner import SpecimuxRunner
from .runners.speconsense_runner import SpeconsenseRunner
from .runners.identify_runner import IdentifyRunner

logger = logging.getLogger(__name__)


def _check_tool_on_path(name: str) -> bool:
    """Check if an external tool is available on PATH."""
    import shutil
    return shutil.which(name) is not None


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

        self._executor = ThreadPoolExecutor(max_workers=config.workers)
        self._futures: dict[str, Future] = {}
        self._shutdown = threading.Event()
        self._draining = False  # True while waiting for specimux to run
        self.cmd_queue: queue.Queue = queue.Queue()
        self._console: ConsoleUI | None = None

    def _rebuild_state(self) -> None:
        """Rebuild state from event log and update scheduler reference."""
        self.state = PipelineState()
        self.state.rebuild(self.event_log)
        self.scheduler.state = self.state

    def _load_specimens(self) -> None:
        """Parse the specimens file and emit specimens.loaded event."""
        specimens = parse_specimens_file(self.config.specimens_file)
        if specimens:
            self.event_log.emit("specimens.loaded", {
                "specimens": specimens,
            })
            logger.info(f"Loaded {len(specimens)} specimens from index file")

            # Fetch community taxon from iNaturalist asynchronously
            inat_ids = extract_inat_ids(specimens)
            if inat_ids:
                self._executor.submit(self._fetch_inat_taxa, inat_ids)

    def _fetch_inat_taxa(self, inat_ids: dict[str, str]) -> None:
        """Fetch community taxon from iNaturalist and emit event (runs in thread pool)."""
        try:
            taxa = fetch_community_taxa(inat_ids, cache_dir=self.config.output_dir)
            if taxa:
                self.event_log.emit("specimens.taxa", {"taxa": taxa})
                logger.info(f"Fetched community taxon for {len(taxa)} specimens")
        except Exception as e:
            logger.warning(f"Failed to fetch iNaturalist taxa: {e}")

    def validate_tools(self) -> list[str]:
        """Check that required external tools are on PATH. Returns list of missing tools."""
        required = ["specimux", "speconsense"]
        if self.config.reference_db:
            required.append("vsearch")
        return [t for t in required if not _check_tool_on_path(t)]

    def run_batch(self) -> None:
        """Run the full batch pipeline: specimux → consensus → identification."""
        missing = self.validate_tools()
        if missing:
            logger.error(f"Required tools not found on PATH: {', '.join(missing)}")
            raise RuntimeError(f"Missing required tools: {', '.join(missing)}")

        # Rebuild state from any existing events
        self._rebuild_state()

        self.event_log.emit("pipeline.started", {
            "mode": "batch",
            "config_summary": self.config.summary(),
        })
        self._load_specimens()

        # Ensure output dirs exist
        self.config.output_dir.mkdir(parents=True, exist_ok=True)
        self.config.specimux_output_dir.mkdir(parents=True, exist_ok=True)
        self.config.consensus_output_dir.mkdir(parents=True, exist_ok=True)

        # Build identification DB if needed
        if self.identify:
            self.identify.ensure_db()

        with ConsoleUI("batch", self.state, self.cmd_queue) as console:
            self._console = console

            # Step 1: Run specimux
            logger.info(f"Running specimux on {self.config.reads_file}")
            specimens = self.specimux.run(self.config.reads_file)

            if not specimens:
                logger.warning("No specimens found after specimux")
                self._console = None
                return

            # Rebuild state after specimux events
            self._rebuild_state()
            console.state = self.state
            console.redraw()

            # Step 2: Run consensus → identification, interleaved per-specimen
            logger.info(f"Found {len(specimens)} specimens, scheduling consensus")
            self._run_consensus_round(min_reads=0)

            self._executor.shutdown(wait=True)
            self._console = None

        if not self._shutdown.is_set():
            logger.info("Batch pipeline complete")

    def run_live(self) -> None:
        """Run the live pipeline with file watching."""
        from .watcher import FileWatcher

        missing = self.validate_tools()
        if missing:
            logger.error(f"Required tools not found on PATH: {', '.join(missing)}")
            raise RuntimeError(f"Missing required tools: {', '.join(missing)}")

        self._rebuild_state()

        self.event_log.emit("pipeline.started", {
            "mode": "live",
            "config_summary": self.config.summary(),
        })
        self._load_specimens()

        self.config.output_dir.mkdir(parents=True, exist_ok=True)
        self.config.specimux_output_dir.mkdir(parents=True, exist_ok=True)
        self.config.consensus_output_dir.mkdir(parents=True, exist_ok=True)

        if self.identify:
            self.identify.ensure_db()

        watcher = FileWatcher(
            watch_dir=self.config.watch_dir,
            settle_time=self.config.settle_time,
            on_file_stable=self._on_file_stable,
            event_log=self.event_log,
        )

        # Seed tracker with files already processed in previous runs
        already_processed = [
            f.path for f in self.state.files.values() if f.stable
        ]
        if already_processed:
            watcher._tracker.seed(already_processed)
            logger.info(f"Restored {len(already_processed)} previously processed files from event log")

        watcher.start()
        logger.info(f"Watching {self.config.watch_dir} for new FASTQ files")

        # Schedule any work that's ready from rebuilt state (e.g. specimens
        # that gained reads before previous shutdown but never got consensus)
        self._schedule_consensus()

        try:
            with ConsoleUI("live", self.state, self.cmd_queue) as console:
                self._console = console
                while not self._shutdown.is_set():
                    self._shutdown.wait(timeout=2.0)
                    self._check_completed_futures()

                    # Drain command queue
                    while True:
                        try:
                            cmd = self.cmd_queue.get_nowait()
                        except queue.Empty:
                            break
                        if cmd == "finalize":
                            self._run_finalization()
                            self._rebuild_state()
                            self._schedule_consensus()
                        elif cmd == "quit":
                            logger.info("Quit command received, shutting down")
                            self._shutdown.set()
                            break

                    console.state = self.state
                    console.redraw()
        except KeyboardInterrupt:
            logger.info("Shutting down live pipeline")
        finally:
            watcher.stop()
            self._executor.shutdown(wait=True)

    def shutdown(self) -> None:
        """Signal the pipeline to shut down."""
        self._shutdown.set()

    def _drain_cmd_queue(self) -> None:
        """Process pending commands from the console UI."""
        while True:
            try:
                cmd = self.cmd_queue.get_nowait()
            except queue.Empty:
                break
            if cmd == "quit":
                logger.info("Quit command received, shutting down")
                self._shutdown.set()
            elif cmd == "finalize":
                # Finalize is handled in the live mode main loop, not here
                pass
        if self._console:
            self._console.state = self.state
            self._console.redraw()

    def _on_file_stable(self, file_path: Path) -> None:
        """Called by watcher when a new stable FASTQ is detected.

        Drains all in-flight jobs, runs specimux with all cores, then resumes scheduling.
        """
        logger.info(f"Processing new file: {file_path}")

        # 1. Stop scheduling new consensus jobs
        self._draining = True

        # 2. Wait for all in-flight futures to complete
        drained_sids = []
        if self._futures:
            logger.info(f"Draining {len(self._futures)} in-flight jobs before specimux")
            drained_sids = self._wait_all_futures()

        # 3. Run specimux with all cores
        specimens = self.specimux.run(file_path)

        # 4. Resume scheduling
        self._draining = False
        self._rebuild_state()

        # 5. Trigger identification for consensus jobs that completed during drain
        if self.identify and drained_sids:
            for sid in drained_sids:
                self._submit_identification(sid, deferred=True)

        self._schedule_consensus()

    def _run_finalization(self) -> None:
        """Reprocess all eligible specimens, ignoring reprocess_ratio.

        Each specimen goes through consensus → identification sequentially,
        so progress is visible in the UI specimen-by-specimen.
        """
        self._rebuild_state()
        jobs = self.scheduler.get_all_eligible_jobs(max_jobs=None, min_reads=0)

        # Filter out specimens already in-flight
        jobs = [j for j in jobs if j.specimen_id not in self._futures]

        if not jobs:
            logger.info("Finalize: no eligible specimens to process")
            return

        logger.info(f"Finalize: scheduling consensus for {len(jobs)} specimens")
        self.event_log.emit("finalization.started", {
            "specimen_count": len(jobs),
        })

        # Submit in batches respecting concurrency, running identification
        # on each specimen as its consensus completes (like live mode)
        pending = list(jobs)
        identified = set()
        while (pending or self._futures) and not self._shutdown.is_set():
            # Fill available slots
            while pending:
                slots = self.config.workers - len(self._futures)
                if slots <= 0:
                    break
                job = pending.pop(0)
                self._submit_consensus(job.specimen_id)

            # Wait for at least one to finish
            if self._futures:
                self._wait_for_any_future()
                self._rebuild_state()
                self._drain_cmd_queue()

                # Identify specimens that just completed consensus
                if self.identify:
                    for sid, spec in self.state.specimens.items():
                        if spec.clusters and sid not in identified:
                            self._submit_identification(sid)
                            identified.add(sid)

        self.event_log.emit("finalization.completed", {})
        logger.info("Finalize: complete")

    def _run_consensus_round(self, min_reads: int | None = None) -> None:
        """Run consensus on all ready specimens, interleaving identification.

        Args:
            min_reads: Override config.min_reads threshold. Use 0 to process all.
        """
        jobs = self.scheduler.get_ready_jobs(max_jobs=None, min_reads=min_reads)
        if not jobs:
            logger.info("No specimens ready for consensus")
            return

        logger.info(f"Running consensus for {len(jobs)} specimens")

        pending = list(jobs)
        identified = set()
        while (pending or self._futures) and not self._shutdown.is_set():
            # Fill available slots
            while pending:
                slots = self.config.workers - len(self._futures)
                if slots <= 0:
                    break
                job = pending.pop(0)
                self._submit_consensus(job.specimen_id)

            # Wait for at least one to finish
            if self._futures:
                self._wait_for_any_future()
                self._rebuild_state()
                self._drain_cmd_queue()

                # Identify specimens that just completed consensus
                if self.identify:
                    for sid, spec in self.state.specimens.items():
                        if spec.clusters and sid not in identified:
                            self._submit_identification(sid)
                            identified.add(sid)

    def _schedule_consensus(self) -> None:
        """Check scheduler and submit consensus jobs for available slots."""
        if self._draining:
            return
        # Use _futures as ground truth for in-flight work, since state may lag
        # behind actual submissions (consensus.started not yet emitted)
        slots = self.config.workers - len(self._futures)
        if slots <= 0:
            return
        jobs = self.scheduler.get_ready_jobs(max_jobs=slots)
        logger.info(f"Scheduler: {len(jobs)} specimens ready for consensus ({slots} slots available)")
        for job in jobs:
            self._submit_consensus(job.specimen_id, presample=self.config.live_presample)

    def _submit_consensus(self, specimen_id: str, presample: int = 0) -> None:
        """Submit a consensus job to the thread pool."""
        # Guard: don't submit if already in-flight (race between submit and
        # consensus.started event being written to the log)
        if specimen_id in self._futures:
            logger.debug(f"Skipping {specimen_id}: already in-flight")
            return

        spec = self.state.get_specimen(specimen_id)
        specimen_fastq = self._find_specimen_fastq(specimen_id, spec.pool)
        if not specimen_fastq:
            logger.warning(f"No FASTQ found for specimen {specimen_id}")
            return

        logger.info(f"Submitting consensus job for {specimen_id} ({spec.total_reads} reads)")
        future = self._executor.submit(self._run_consensus_job, specimen_id, specimen_fastq, presample)
        self._futures[specimen_id] = future

    def _run_consensus_job(self, specimen_id: str, specimen_fastq: Path, presample: int = 0) -> list[dict]:
        """Run consensus for a single specimen (executed in thread pool)."""
        return self.speconsense.run(specimen_id, specimen_fastq, presample=presample)

    def _submit_identification(self, specimen_id: str, deferred: bool = False) -> None:
        """Submit identification for a specimen if it has clusters."""
        spec = self.state.get_specimen(specimen_id)
        if not spec or not spec.clusters:
            return
        consensus_fasta = self.speconsense.get_consensus_fasta(specimen_id)
        if not consensus_fasta:
            return
        label = f" (deferred from drain)" if deferred else ""
        logger.info(f"Identifying {specimen_id}{label}")
        self._executor.submit(self.identify.run, specimen_id, consensus_fasta)

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
        """Wait for at least one future to complete, with timeout."""
        from concurrent.futures import as_completed, TimeoutError
        if self._futures:
            timeout = self.config.job_timeout
            try:
                done_iter = as_completed(self._futures.values(), timeout=timeout)
                next(done_iter)
            except (StopIteration, TimeoutError):
                pass
            # Find and remove completed (or timed-out) futures
            for sid, fut in list(self._futures.items()):
                if fut.done():
                    del self._futures[sid]
                    try:
                        fut.result()
                    except Exception as e:
                        logger.error(f"Job failed for {sid}: {e}")
                        self.event_log.emit("pipeline.error", {
                            "component": "pipeline",
                            "specimen_id": sid,
                            "message": str(e),
                        })

    def _wait_all_futures(self) -> list[str]:
        """Wait for all pending futures, with timeout.

        Returns list of specimen IDs whose consensus completed successfully.
        """
        completed = []
        timeout = self.config.job_timeout
        for sid, fut in list(self._futures.items()):
            try:
                fut.result(timeout=timeout)
                completed.append(sid)
            except TimeoutError:
                logger.error(f"Job timed out for {sid} after {timeout}s")
                self.event_log.emit("pipeline.error", {
                    "component": "pipeline",
                    "specimen_id": sid,
                    "message": f"Job timed out after {timeout}s",
                })
            except Exception as e:
                logger.error(f"Job failed for {sid}: {e}")
                self.event_log.emit("pipeline.error", {
                    "component": "pipeline",
                    "specimen_id": sid,
                    "message": str(e),
                })
        self._futures.clear()
        return completed

    def _check_completed_futures(self) -> None:
        """Check for completed futures and trigger identification."""
        completed = []
        errored = []
        for sid, fut in list(self._futures.items()):
            if fut.done():
                try:
                    fut.result()
                    completed.append(sid)
                except Exception as e:
                    logger.error(f"Job failed for {sid}: {e}")
                    self.event_log.emit("pipeline.error", {
                        "component": "pipeline",
                        "specimen_id": sid,
                        "message": str(e),
                    })
                    errored.append(sid)

        for sid in completed + errored:
            del self._futures[sid]

        for sid in completed:
            if self.identify:
                self._rebuild_state()
                self._submit_identification(sid)

        # Check for new consensus work
        if completed or errored:
            self._rebuild_state()
            self._schedule_consensus()
