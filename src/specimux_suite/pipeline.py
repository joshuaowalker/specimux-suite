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
from .state import PipelineState, SpecimenStatus
from .scheduler import Scheduler
from .inat import extract_inat_ids, fetch_community_taxa
from .util import parse_specimens_file
from .runners.specimux_runner import SpecimuxRunner
from .runners.speconsense_runner import SpeconsenseRunner
from .runners.identify_runner import IdentifyRunner
from .runners.summarize_runner import SummarizeRunner

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
        self.summarize = SummarizeRunner(config, self.event_log)

        self._executor = ThreadPoolExecutor(max_workers=config.workers)
        self._futures: dict[str, Future] = {}
        self._identifying: set[str] = set()  # specimen IDs with in-flight identification
        self._shutdown = threading.Event()
        self._draining = False  # True while waiting for specimux to run
        self.cmd_queue: queue.Queue = queue.Queue()
        self._file_queue: queue.Queue[Path] = queue.Queue()
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
        required = ["specimux", "speconsense", "speconsense-summarize"]
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
        self.config.summarize_output_dir.mkdir(parents=True, exist_ok=True)

        # Build identification DB if needed
        if self.identify:
            self.identify.ensure_db()

        with ConsoleUI("batch", self.state, self.cmd_queue) as console:
            self._console = console

            # Step 1: Run specimux (skip if already completed in a prior run)
            if self.state.specimux_runs > 0:
                logger.info("Specimux already completed in prior run, skipping demux")
            else:
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
            logger.info(f"Found {len(self.state.specimens)} specimens, scheduling consensus")
            self._run_consensus_round(min_reads=0)

            # Step 3: Run summarize for all identified/no_match specimens
            self._run_summarize_round()

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
        self.config.summarize_output_dir.mkdir(parents=True, exist_ok=True)

        if self.identify:
            self.identify.ensure_db()

        watcher = FileWatcher(
            watch_dir=self.config.watch_dir,
            settle_time=self.config.settle_time,
            on_file_stable=self._on_file_stable,
            event_log=self.event_log,
            stable_queue=self._file_queue,
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

                    # Process any stable files (drain once, run all back-to-back)
                    self._process_stable_files()

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
        """Legacy callback — only used if watcher has no queue."""
        self._file_queue.put(file_path)
        self._process_stable_files()

    def _has_unsettled_files(self) -> bool:
        """Check if any detected files haven't stabilized yet."""
        return any(not f.stable for f in self.state.files.values())

    def _drain_file_queue(self) -> None:
        """Process all queued stable files without scheduling consensus afterward."""
        files: list[Path] = []
        while True:
            try:
                files.append(self._file_queue.get_nowait())
            except queue.Empty:
                break

        if not files:
            return

        logger.info(f"Draining {len(files)} stable file(s): {', '.join(f.name for f in files)}")

        self._draining = True

        if self._futures:
            logger.info(f"Draining {len(self._futures)} in-flight jobs before specimux")
            self._wait_all_futures()

        for file_path in files:
            logger.info(f"Running specimux on {file_path.name}")
            try:
                self.specimux.run(file_path)
            except Exception as e:
                logger.error(f"Error running specimux on {file_path}: {e}")
                self.event_log.emit("pipeline.error", {
                    "component": "specimux",
                    "message": f"Error processing {file_path}: {e}",
                })

        self._draining = False
        self._rebuild_state()

    def _drain_all_files(self) -> None:
        """Drain queued files and wait for any settling files to stabilize."""
        self._drain_file_queue()
        self._rebuild_state()
        if self._has_unsettled_files():
            wait = self.config.settle_time + 5
            logger.info(f"Waiting up to {wait}s for {sum(1 for f in self.state.files.values() if not f.stable)} settling file(s)")
            self._shutdown.wait(timeout=wait)
            self._drain_file_queue()

    def _process_stable_files(self) -> None:
        """Process all queued stable files back-to-back with a single drain cycle."""
        # Collect all ready files
        files: list[Path] = []
        while True:
            try:
                files.append(self._file_queue.get_nowait())
            except queue.Empty:
                break

        if not files:
            return

        logger.info(f"Processing {len(files)} stable file(s): {', '.join(f.name for f in files)}")

        # 1. Stop scheduling new consensus jobs
        self._draining = True

        # 2. Drain in-flight jobs once (not per-file)
        drained_sids = []
        if self._futures:
            logger.info(f"Draining {len(self._futures)} in-flight jobs before specimux")
            drained_sids = self._wait_all_futures()

        # 3. Run specimux on each file back-to-back
        for file_path in files:
            logger.info(f"Running specimux on {file_path.name}")
            try:
                self.specimux.run(file_path)
            except Exception as e:
                logger.error(f"Error running specimux on {file_path}: {e}")
                self.event_log.emit("pipeline.error", {
                    "component": "specimux",
                    "message": f"Error processing {file_path}: {e}",
                })

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
        # Drain any pending files first — finalization means "process everything"
        # Also wait for files that are detected but still settling
        self._drain_all_files()

        self._rebuild_state()
        jobs = self.scheduler.get_all_eligible_jobs(max_jobs=None, min_reads=0)

        # Filter out specimens already in-flight or with no reads (nothing to process)
        jobs = [j for j in jobs if j.specimen_id not in self._futures and j.read_count > 0]

        if not jobs:
            logger.info("Finalize: no eligible specimens to process")
            return

        logger.info(f"Finalize: scheduling consensus for {len(jobs)} specimens")
        self.event_log.emit("finalization.started", {
            "specimen_count": len(jobs),
            "specimen_ids": [j.specimen_id for j in jobs],
        })

        # Submit in batches respecting concurrency, running identification
        # on each specimen as its consensus completes (like live mode)
        pending = list(jobs)
        job_sids = {j.specimen_id for j in jobs}
        while not self._shutdown.is_set():
            # Drain any files that arrived since finalization started
            if not self._file_queue.empty() or self._has_unsettled_files():
                self._drain_all_files()
                # Check for newly-eligible specimens from the new reads
                new_jobs = self.scheduler.get_all_eligible_jobs(max_jobs=None, min_reads=0)
                for j in new_jobs:
                    if j.read_count > 0 and j.specimen_id not in job_sids and j.specimen_id not in self._futures:
                        pending.append(j)
                        job_sids.add(j.specimen_id)
                if new_jobs:
                    # Update finalization set so dashboard tracks new specimens
                    self.event_log.emit("finalization.started", {
                        "specimen_count": len(job_sids),
                        "specimen_ids": list(job_sids),
                    })

            if not pending and not self._futures:
                break

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
                # (consensus.completed sets CONSENSUS_DONE and clears identification)
                if self.identify:
                    for sid, spec in self.state.specimens.items():
                        if (sid in job_sids
                                and spec.status == SpecimenStatus.CONSENSUS_DONE
                                and spec.clusters
                                and not spec.identification):
                            self._submit_identification(sid)

        # Summarize all identified/no_match specimens, then aggregate
        self._run_summarize_round()

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
        job_sids = {j.specimen_id for j in jobs}
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
                # (consensus.completed sets CONSENSUS_DONE and clears identification)
                if self.identify:
                    for sid, spec in self.state.specimens.items():
                        if (sid in job_sids
                                and spec.status == SpecimenStatus.CONSENSUS_DONE
                                and spec.clusters
                                and not spec.identification):
                            self._submit_identification(sid)

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
        if specimen_id in self._identifying:
            return
        spec = self.state.get_specimen(specimen_id)
        if not spec or not spec.clusters:
            return
        consensus_fasta = self.speconsense.get_consensus_fasta(specimen_id)
        if not consensus_fasta:
            return
        label = f" (deferred from drain)" if deferred else ""
        logger.info(f"Identifying {specimen_id}{label}")
        self._identifying.add(specimen_id)
        future = self._executor.submit(self.identify.run, specimen_id, consensus_fasta)
        future.add_done_callback(lambda _: self._identifying.discard(specimen_id))

    def _build_variant_fasta(self, specimen_id: str) -> Path | None:
        """Combine all variant FASTA files for a specimen into one file for identification."""
        self._rebuild_state()
        spec = self.state.get_specimen(specimen_id)
        if not spec or not spec.variants:
            return None

        summary_dir = self.config.summarize_output_dir
        combined = summary_dir / f"{specimen_id}-variants-combined.fasta"
        found_any = False

        with open(combined, "w") as out:
            for variant in spec.variants:
                vname = variant.get("name")
                if not vname:
                    continue
                matches = list(summary_dir.glob(f"{vname}-RiC*.fasta"))
                for fasta_path in matches:
                    out.write(fasta_path.read_text())
                    found_any = True

        if not found_any:
            combined.unlink(missing_ok=True)
            return None
        return combined

    def _submit_variant_identification(self, specimen_id: str) -> None:
        """Submit identification for variant sequences of a specimen."""
        combined_fasta = self._build_variant_fasta(specimen_id)
        if not combined_fasta:
            return
        logger.info(f"Identifying variants for {specimen_id}")
        future = self._executor.submit(self.identify.run, specimen_id, combined_fasta)
        self._futures[specimen_id] = future

    def _submit_summarize(self, specimen_id: str) -> None:
        """Submit a summarize job to the thread pool."""
        if specimen_id in self._futures:
            logger.debug(f"Skipping summarize for {specimen_id}: already in-flight")
            return
        logger.info(f"Submitting summarize job for {specimen_id}")
        future = self._executor.submit(self.summarize.run, specimen_id)
        self._futures[specimen_id] = future

    def _run_summarize_round(self) -> None:
        """Run summarize for all identified/no_match specimens, then aggregate."""
        self._rebuild_state()

        eligible = [
            sid for sid, spec in self.state.specimens.items()
            if spec.status in (SpecimenStatus.IDENTIFIED, SpecimenStatus.NO_MATCH)
            and spec.clusters  # must have consensus output
        ]

        if not eligible:
            logger.info("No specimens eligible for summarization")
            return

        logger.info(f"Running summarize for {len(eligible)} specimens")

        pending = list(eligible)
        while (pending or self._futures) and not self._shutdown.is_set():
            while pending:
                slots = self.config.workers - len(self._futures)
                if slots <= 0:
                    break
                sid = pending.pop(0)
                self._submit_summarize(sid)

            if self._futures:
                self._wait_for_any_future()
                self._rebuild_state()
                self._drain_cmd_queue()

        # Re-identify using variant sequences
        if self.identify:
            self._rebuild_state()
            for sid in eligible:
                self._submit_variant_identification(sid)
            while self._futures and not self._shutdown.is_set():
                self._wait_for_any_future()
                self._rebuild_state()
                self._drain_cmd_queue()

        # Run aggregate to generate summary.fasta etc.
        logger.info("Running summarize aggregate")
        self.summarize.run_aggregate()

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
