"""Pipeline orchestrator: wires components together for batch and live modes."""

import logging
import queue
import signal
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, Future, TimeoutError
from pathlib import Path

from .config import PipelineConfig
from .console import ConsoleUI
from .events import EventLog
from .state import PipelineState, SpecimenStatus
from .scheduler import Scheduler
from .inat import extract_inat_ids, fetch_community_taxa
from .util import clone_or_copy, parse_specimens_file
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

        # Single live state: replay history once, then stay current by
        # applying every event as it is emitted. This instance is shared
        # with the scheduler, console, and web server — never reassign it.
        self.state = PipelineState()
        self.state.rebuild(self.event_log)
        self.event_log.add_listener(self.state.apply)

        self.scheduler = Scheduler(config, self.state)

        self.specimux = SpecimuxRunner(config, self.event_log)
        self.speconsense = SpeconsenseRunner(config, self.event_log)
        self.identify = IdentifyRunner(config, self.event_log) if config.reference_db else None
        self.summarize = SummarizeRunner(config, self.event_log)

        self._executor = ThreadPoolExecutor(max_workers=config.workers)
        self._futures: dict[str, Future] = {}
        self._id_futures: dict[str, Future] = {}  # in-flight identification jobs

        # Cluster identifications are micro-batched: requests are collected
        # for a short window and served by a single vsearch call, amortizing
        # the reference-DB load cost (which dominates for few-query calls).
        self._id_requests: queue.Queue = queue.Queue()
        self._id_batcher_stop = threading.Event()
        self._id_batcher_thread: threading.Thread | None = None
        if self.identify:
            self._id_batcher_thread = threading.Thread(
                target=self._identification_batcher,
                name="identify-batcher", daemon=True,
            )
            self._id_batcher_thread.start()
        self._shutdown = threading.Event()
        self._draining = False  # True while waiting for specimux to run
        self.cmd_queue: queue.Queue = queue.Queue()
        self._file_queue: queue.Queue[Path] = queue.Queue()
        self._console: ConsoleUI | None = None
        self._exit_after_finalize = False
        self._sigint_count = 0
        self._old_sigint = None

    def _load_specimens(self) -> None:
        """Parse the specimens file and emit specimens.loaded event."""
        specimens = parse_specimens_file(self.config.specimens_file)
        if specimens:
            self.event_log.emit("specimens.loaded", {
                "specimens": specimens,
            })
            logger.info(f"Loaded {len(specimens)} specimens from index file")

            # Fetch community taxon from iNaturalist asynchronously. Runs on
            # a daemon thread, not the worker pool: a slow fetch must never
            # hold a consensus slot or block executor shutdown on exit.
            inat_ids = extract_inat_ids(specimens)
            if inat_ids:
                threading.Thread(
                    target=self._fetch_inat_taxa, args=(inat_ids,),
                    name="inat-fetch", daemon=True,
                ).start()

    def _fetch_inat_taxa(self, inat_ids: dict[str, str]) -> None:
        """Fetch community taxon from iNaturalist and emit event (daemon thread)."""
        try:
            taxa = fetch_community_taxa(
                inat_ids, cache_dir=self.config.output_dir, abort=self._shutdown,
            )
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

        self._install_sigint("batch")
        try:
            with ConsoleUI("batch", self.state, self.cmd_queue) as console:
                self._console = console

                # Step 1: Run specimux (skip if already completed in a prior run)
                if self.state.specimux_runs > 0:
                    logger.info("Specimux already completed in prior run, skipping demux")
                else:
                    logger.info(f"Running specimux on {self.config.reads_file}")
                    self._run_specimux_file(self.config.reads_file)

                    if not any(s.total_reads for s in self.state.specimens.values()):
                        logger.warning("No specimens found after specimux")
                        self._console = None
                        return

                console.redraw()

                # Step 2: Run consensus → identification, interleaved per-specimen
                if not self._shutdown.is_set():
                    logger.info(f"Found {len(self.state.specimens)} specimens, scheduling consensus")
                    self._run_consensus_round(min_reads=0)

                # Step 3: Run summarize for all identified/no_match specimens
                if not self._shutdown.is_set():
                    self._run_summarize_round()

                self._shutdown_executor()
                self._console = None
        finally:
            self._restore_sigint()

        if self._shutdown.is_set():
            logger.info("Batch pipeline stopped by user")
        else:
            logger.info("Batch pipeline complete")

    def run_live(self) -> None:
        """Run the live pipeline with file watching."""
        from .watcher import FileWatcher

        missing = self.validate_tools()
        if missing:
            logger.error(f"Required tools not found on PATH: {', '.join(missing)}")
            raise RuntimeError(f"Missing required tools: {', '.join(missing)}")

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

        # Seed tracker with files already processed in previous runs.
        # Keyed on processed (demuxed), not merely stable: a file that
        # stabilized right before a quit was never demuxed and must be
        # picked up again on restart.
        already_processed = [
            f.path for f in self.state.files.values() if f.processed
        ]
        if already_processed:
            watcher._tracker.seed(already_processed)
            logger.info(f"Restored {len(already_processed)} previously processed files from event log")

        watcher.start()
        logger.info(f"Watching {self.config.watch_dir} for new FASTQ files")

        # Schedule any work that's ready from rebuilt state (e.g. specimens
        # that gained reads before previous shutdown but never got consensus)
        self._schedule_consensus()

        self._install_sigint("live")
        try:
            with ConsoleUI("live", self.state, self.cmd_queue) as console:
                self._console = console
                while not self._shutdown.is_set():
                    self._shutdown.wait(timeout=self._TICK)
                    self._check_completed_futures()

                    # Process any stable files
                    self._process_stable_files()

                    # Drain command queue
                    while True:
                        try:
                            cmd = self.cmd_queue.get_nowait()
                        except queue.Empty:
                            break
                        if cmd == "finalize":
                            self._run_finalization()
                            if self._exit_after_finalize:
                                self._shutdown.set()
                            else:
                                self._schedule_consensus()
                        elif cmd == "quit":
                            logger.info("Quit command received, shutting down")
                            self._shutdown.set()
                            break

                    console.redraw()
        except KeyboardInterrupt:
            # Safety net — normally the SIGINT handler prevents this
            logger.info("Shutting down live pipeline")
        finally:
            self._restore_sigint()
            watcher.stop()
            self._shutdown_executor()

    def shutdown(self) -> None:
        """Signal the pipeline to shut down."""
        self._shutdown.set()

    def _install_sigint(self, mode: str) -> None:
        """Install a SIGINT handler for graceful Ctrl+C behavior.

        Live mode: first Ctrl+C finalizes then exits (as documented in the
        README); a second Ctrl+C aborts the finalization and shuts down.
        Batch mode: Ctrl+C behaves like [Q] — stop gracefully.

        Replacing the default handler also means no KeyboardInterrupt is
        raised mid-teardown, so the process exits 0 instead of 1/130.
        """
        def handler(signum, frame):
            self._sigint_count += 1
            if mode == "live" and self._sigint_count == 1:
                logger.info("Ctrl+C: finalizing and exiting — press Ctrl+C again to abort")
                self._exit_after_finalize = True
                self.cmd_queue.put("finalize")
            else:
                logger.info("Ctrl+C: shutting down")
                self._shutdown.set()

        try:
            self._old_sigint = signal.signal(signal.SIGINT, handler)
        except ValueError:
            # Not the main thread (e.g. under test harnesses) — skip
            self._old_sigint = None

    def _restore_sigint(self) -> None:
        if self._old_sigint is not None:
            try:
                signal.signal(signal.SIGINT, self._old_sigint)
            except ValueError:
                pass
            self._old_sigint = None

    @property
    def was_shutdown(self) -> bool:
        """True if the run ended via quit/shutdown rather than completing."""
        return self._shutdown.is_set()

    def _shutdown_executor(self) -> None:
        """Shut down the thread pool: cancel queued jobs, wait for running ones.

        Running subprocesses are allowed to finish (killing specimux mid-append
        could corrupt per-specimen FASTQs); queued-but-unstarted jobs are
        cancelled so quitting waits on at most `workers` jobs.
        """
        running = sum(
            1 for f in list(self._futures.values()) + list(self._id_futures.values())
            if f.running()
        )
        if running:
            logger.info(f"Waiting for {running} in-flight job(s) to finish; queued jobs cancelled")
        self._executor.shutdown(wait=True, cancel_futures=True)

        # Stop the identification batcher: cancel queued requests, then wait
        # for any batch already running (its vsearch child should not be
        # orphaned past process exit).
        self._id_batcher_stop.set()
        while True:
            try:
                _sid, _fasta, _cv, fut = self._id_requests.get_nowait()
            except queue.Empty:
                break
            fut.cancel()
        if self._id_batcher_thread is not None:
            self._id_batcher_thread.join(timeout=self.config.job_timeout)
            self._id_batcher_thread = None

    def _drain_cmd_queue(self) -> None:
        """Process pending commands from the console UI.

        Handles "quit" directly; "finalize" is owned by the live main loop,
        so it is re-queued (once) rather than dropped — this method is also
        called from wait loops that would otherwise swallow it.
        """
        finalize_seen = False
        while True:
            try:
                cmd = self.cmd_queue.get_nowait()
            except queue.Empty:
                break
            if cmd == "quit":
                logger.info("Quit command received, shutting down")
                self._shutdown.set()
            elif cmd == "finalize":
                finalize_seen = True
        if finalize_seen:
            self.cmd_queue.put("finalize")
        if self._console:
            self._console.redraw()

    def _run_specimux_file(self, fastq_path: Path, threads: int | None = None) -> None:
        """Run specimux in a worker thread, ticking console commands meanwhile.

        Demux itself is never interrupted (killing specimux mid-append could
        corrupt per-specimen FASTQs), but [Q]/Ctrl+C are acknowledged within
        ~_TICK and callers skip remaining work after it returns.
        """
        def target():
            try:
                self.specimux.run(fastq_path, threads=threads)
            except Exception as e:
                logger.error(f"Error running specimux on {fastq_path}: {e}")
                self.event_log.emit("pipeline.error", {
                    "component": "specimux",
                    "message": f"Error processing {fastq_path}: {e}",
                })

        t = threading.Thread(target=target, name="specimux-runner", daemon=True)
        t.start()
        while t.is_alive():
            t.join(timeout=self._TICK)
            self._drain_cmd_queue()

    def _on_file_stable(self, file_path: Path) -> None:
        """Legacy callback — only used if watcher has no queue."""
        self._file_queue.put(file_path)
        self._process_stable_files()

    def _has_unsettled_files(self) -> bool:
        """Check if any detected files haven't stabilized yet."""
        return any(not f.stable for f in self.state.files.values())

    def _drain_file_queue(self) -> None:
        """Process all queued stable files without scheduling consensus afterward.

        In-flight consensus jobs read copy-on-write snapshots of their input,
        so specimux can append to the live per-specimen FASTQs while they run
        — no need to wait for them. _draining only pauses NEW submissions so
        snapshots are never taken while specimux is appending.
        """
        files: list[Path] = []
        while True:
            try:
                files.append(self._file_queue.get_nowait())
            except queue.Empty:
                break

        if not files:
            return

        logger.info(f"Processing {len(files)} stable file(s): {', '.join(f.name for f in files)}")

        self._draining = True
        for file_path in files:
            if self._shutdown.is_set():
                logger.info("Shutdown requested; remaining file(s) will be picked up on restart")
                break
            threads = max(1, self.config.workers - len(self._futures))
            logger.info(f"Running specimux on {file_path.name} ({threads} threads)")
            self._run_specimux_file(file_path, threads=threads)
        self._draining = False

    def _drain_all_files(self) -> None:
        """Drain queued files and wait for any settling files to stabilize."""
        self._drain_file_queue()
        if self._has_unsettled_files():
            wait = self.config.settle_time + 5
            logger.info(f"Waiting up to {wait}s for {sum(1 for f in self.state.files.values() if not f.stable)} settling file(s)")
            self._shutdown.wait(timeout=wait)
            self._drain_file_queue()

    def _process_stable_files(self) -> None:
        """Demux all queued stable files, then schedule any newly-ready work.

        In-flight consensus jobs keep running throughout: they read snapshots
        taken at submission time, so specimux appending to the live specimen
        FASTQs cannot race them. Consensus jobs that complete during demux
        get their identification from the next _check_completed_futures tick.
        """
        self._drain_file_queue()
        self._schedule_consensus()

    def _run_finalization(self) -> None:
        """Reprocess all eligible specimens, ignoring reprocess_ratio.

        Each specimen goes through consensus → identification sequentially,
        so progress is visible in the UI specimen-by-specimen.
        """
        # Drain any pending files first — finalization means "process everything"
        # Also wait for files that are detected but still settling
        self._drain_all_files()

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
                self._submit_consensus(job.specimen_id, reason=job.reason)

            # Wait for at least one to finish
            if self._futures:
                self._wait_for_any_future()
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
                self._submit_consensus(job.specimen_id, reason=job.reason)

            # Wait for at least one to finish
            if self._futures:
                self._wait_for_any_future()
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
        if self._draining or self._shutdown.is_set():
            return
        # Use _futures as ground truth for in-flight work, since state may lag
        # behind actual submissions (consensus.started not yet emitted)
        slots = self.config.workers - len(self._futures)
        if slots <= 0:
            return
        jobs = self.scheduler.get_ready_jobs(max_jobs=slots)
        logger.info(f"Scheduler: {len(jobs)} specimens ready for consensus ({slots} slots available)")
        for job in jobs:
            self._submit_consensus(job.specimen_id, presample=self.config.live_presample, reason=job.reason)

    def _submit_consensus(self, specimen_id: str, presample: int = 0, reason: str = "") -> None:
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

        # Snapshot the input (copy-on-write where the filesystem supports it)
        # so specimux can append to the live file while this job runs. All
        # snapshots are taken on the orchestrator thread — the same thread
        # that runs specimux — so a snapshot never captures a half-written
        # record. The file must keep the specimen's name: speconsense derives
        # its output naming from the input file stem.
        snapshot = self.config.output_dir / "snapshots" / f"{specimen_id}.fastq"
        try:
            clone_or_copy(specimen_fastq, snapshot)
        except OSError as e:
            logger.warning(f"Snapshot failed for {specimen_id}, using live file: {e}")
            snapshot = specimen_fastq

        why = f", {reason}" if reason else ""
        logger.info(f"Submitting consensus job for {specimen_id} ({spec.total_reads} reads{why})")
        future = self._executor.submit(self._run_consensus_job, specimen_id, snapshot, presample)
        self._futures[specimen_id] = future

    def _run_consensus_job(self, specimen_id: str, specimen_fastq: Path, presample: int = 0) -> list[dict]:
        """Run consensus for a single specimen (executed in thread pool)."""
        try:
            return self.speconsense.run(specimen_id, specimen_fastq, presample=presample)
        finally:
            if specimen_fastq.parent.name == "snapshots":
                specimen_fastq.unlink(missing_ok=True)

    def _submit_identification(self, specimen_id: str) -> None:
        """Queue identification for a specimen if it has clusters.

        Requests go to the micro-batching thread; the returned future
        resolves when its batch's vsearch call completes.
        """
        if self.identify is None or specimen_id in self._id_futures:
            return
        spec = self.state.get_specimen(specimen_id)
        if not spec or not spec.clusters:
            return
        consensus_fasta = self.speconsense.get_consensus_fasta(specimen_id)
        if not consensus_fasta:
            return
        logger.info(f"Queueing identification for {specimen_id}")
        future: Future = Future()
        self._id_futures[specimen_id] = future
        future.add_done_callback(
            lambda fut, sid=specimen_id: self._on_identification_done(sid, fut)
        )
        self._id_requests.put(
            (specimen_id, consensus_fasta, spec.consensus_version, future)
        )

    # Collection window and size cap for one identification batch.
    _ID_BATCH_WINDOW = 1.0
    _ID_BATCH_MAX = 32

    def _identification_batcher(self) -> None:
        """Collect identification requests briefly, serve each batch with one
        vsearch call (runs on a dedicated daemon thread)."""
        while not self._id_batcher_stop.is_set():
            try:
                first = self._id_requests.get(timeout=self._TICK)
            except queue.Empty:
                continue

            batch = [first]
            deadline = time.monotonic() + self._ID_BATCH_WINDOW
            while len(batch) < self._ID_BATCH_MAX:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    break
                try:
                    batch.append(self._id_requests.get(timeout=min(remaining, 0.1)))
                except queue.Empty:
                    continue

            live = [r for r in batch if r[3].set_running_or_notify_cancel()]
            if not live:
                continue
            if len(live) > 1:
                logger.info(f"Identifying batch of {len(live)} specimens")
            try:
                results = self.identify.run_group(
                    [(sid, fasta, cv) for sid, fasta, cv, _fut in live]
                )
                for sid, _fasta, _cv, fut in live:
                    fut.set_result(results.get(sid, []))
            except Exception as e:
                for _sid, _fasta, _cv, fut in live:
                    if not fut.done():
                        fut.set_exception(e)

    def _on_identification_done(self, specimen_id: str, fut: Future) -> None:
        """Reap a finished identification future and surface any error."""
        self._id_futures.pop(specimen_id, None)
        if fut.cancelled():
            return
        exc = fut.exception()
        if exc is not None:
            logger.error(f"Identification failed for {specimen_id}: {exc}")
            self.event_log.emit("pipeline.error", {
                "component": "identify",
                "specimen_id": specimen_id,
                "message": str(exc),
            })

    def _drain_identifications(self) -> None:
        """Wait for all in-flight identification jobs to finish.

        Ticks so console commands keep working; returns early on shutdown.
        Failures are logged and reported by _on_identification_done.
        """
        from concurrent.futures import wait
        if not self._id_futures:
            return
        logger.info(f"Waiting for {len(self._id_futures)} in-flight identification(s)")
        deadline = time.monotonic() + self.config.job_timeout + 60
        while self._id_futures and not self._shutdown.is_set():
            _done, not_done = wait(list(self._id_futures.values()), timeout=self._TICK)
            self._drain_cmd_queue()
            if not not_done or time.monotonic() > deadline:
                break

    def _build_variant_fasta(self, specimen_id: str) -> Path | None:
        """Combine all variant FASTA files for a specimen into one file for identification."""
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
        spec = self.state.get_specimen(specimen_id)
        logger.info(f"Identifying variants for {specimen_id}")
        future = self._executor.submit(
            self.identify.run, specimen_id, combined_fasta,
            consensus_version=spec.consensus_version,
            output_name=f"{specimen_id}-variants",
        )
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
        if self._shutdown.is_set():
            return

        # Wait for in-flight identifications first: the last consensus jobs
        # submit identification right before the consensus round exits, and
        # those specimens are still CONSENSUS_DONE until identification lands.
        # Computing eligibility before that would silently skip them.
        self._drain_identifications()

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
        eligible_set = set(eligible)
        variant_id_submitted = set()
        while (pending or self._futures) and not self._shutdown.is_set():
            while pending:
                slots = self.config.workers - len(self._futures)
                if slots <= 0:
                    break
                sid = pending.pop(0)
                self._submit_summarize(sid)

            if self._futures:
                self._wait_for_any_future()
                self._drain_cmd_queue()

                # Identify specimens that just completed summarization
                if self.identify:
                    for sid, spec in self.state.specimens.items():
                        if (sid in eligible_set
                                and sid not in variant_id_submitted
                                and spec.status == SpecimenStatus.SUMMARIZED
                                and spec.variants
                                and sid not in self._futures):
                            self._submit_variant_identification(sid)
                            variant_id_submitted.add(sid)

        # Run aggregate to generate summary.fasta etc.
        if self._shutdown.is_set():
            logger.info("Skipping summarize aggregate (shutdown requested)")
            return
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

    # Interval between console-command checks while blocked on jobs.
    # Keeps [Q] responsive during long-running subprocess work.
    _TICK = 0.5

    def _wait_for_any_future(self) -> None:
        """Wait until at least one future completes, staying responsive.

        Polls in short ticks, processing console commands between ticks so
        [Q] is acknowledged within ~_TICK even while jobs run for minutes.
        Returns early (without reaping) if shutdown is requested. Runners
        enforce job_timeout at the subprocess level, so futures always finish
        within roughly that window; the deadline here is a backstop.
        """
        from concurrent.futures import wait, FIRST_COMPLETED
        if not self._futures:
            return
        deadline = time.monotonic() + self.config.job_timeout + 60
        while not self._shutdown.is_set():
            done, _ = wait(list(self._futures.values()),
                           timeout=self._TICK, return_when=FIRST_COMPLETED)
            self._drain_cmd_queue()
            if done:
                break
            if time.monotonic() > deadline:
                logger.warning(
                    f"No job finished within the backstop window; still in-flight: "
                    f"{', '.join(self._futures)}"
                )
                break
        self._reap_completed_futures()

    def _reap_completed_futures(self) -> None:
        """Remove finished futures from _futures, reporting failures."""
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
                self._submit_identification(sid)

        # Check for new consensus work
        if completed or errored:
            self._schedule_consensus()
