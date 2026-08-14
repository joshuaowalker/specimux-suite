"""File detection for live mode: watchdog + stability check."""

import logging
import os
import queue
import threading
import time
from pathlib import Path
from typing import Callable

from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler, FileCreatedEvent, FileModifiedEvent

from .events import EventLog

logger = logging.getLogger(__name__)

FASTQ_EXTENSIONS = {".fastq", ".fq", ".fastq.gz", ".fq.gz"}


def _has_fastq_extension(name: str) -> bool:
    """Check if a filename has a recognized FASTQ extension."""
    for ext in FASTQ_EXTENSIONS:
        if name.endswith(ext):
            return True
    return False


class FileStabilityChecker:
    """Wait for a file to stop growing before processing."""

    def __init__(self, settle_time: float = 30.0, check_interval: float = None):
        self.settle_time = settle_time
        # Default check interval: 1/3 of settle time, clamped to [1, 10] seconds
        if check_interval is None:
            self.check_interval = max(1.0, min(10.0, settle_time / 3))
        else:
            self.check_interval = check_interval

    def wait_for_stable(self, path: Path) -> bool:
        """Block until file size is stable. Returns True if stable, False if disappeared."""
        last_size = -1
        stable_since = None

        while True:
            try:
                size = path.stat().st_size
            except FileNotFoundError:
                return False

            if size == last_size:
                if stable_since is None:
                    stable_since = time.monotonic()
                elif time.monotonic() - stable_since >= self.settle_time:
                    return True
            else:
                stable_since = None
                last_size = size

            time.sleep(self.check_interval)


class ProcessedFilesTracker:
    """Track which files have been processed to avoid reprocessing."""

    def __init__(self):
        self._processed: set[str] = set()
        self._lock = threading.Lock()

    def is_processed(self, path: Path) -> bool:
        with self._lock:
            return str(path) in self._processed

    def mark_processed(self, path: Path) -> None:
        with self._lock:
            self._processed.add(str(path))

    def seed(self, paths) -> None:
        """Bulk-add paths already known to be processed (e.g. from event replay)."""
        with self._lock:
            for p in paths:
                self._processed.add(str(p))


class _FastqHandler(FileSystemEventHandler):
    """Watchdog handler that detects new FASTQ files."""

    def __init__(self, on_new_file: Callable[[Path], None]):
        self.on_new_file = on_new_file

    def on_created(self, event):
        if not event.is_directory and _has_fastq_extension(Path(event.src_path).name):
            self.on_new_file(Path(event.src_path))

    def on_modified(self, event):
        if not event.is_directory and _has_fastq_extension(Path(event.src_path).name):
            self.on_new_file(Path(event.src_path))


class FileWatcher:
    """Watch a directory for new FASTQ files, check stability, then queue for processing."""

    def __init__(
        self,
        watch_dir: Path,
        settle_time: float,
        on_file_stable: Callable[[Path], None],
        event_log: EventLog,
        stable_queue: "queue.Queue[Path] | None" = None,
    ):
        self.watch_dir = Path(watch_dir)
        self.settle_time = settle_time
        self.on_file_stable = on_file_stable
        self.event_log = event_log
        self._stable_queue = stable_queue

        self._checker = FileStabilityChecker(settle_time=settle_time)
        self._tracker = ProcessedFilesTracker()
        self._pending: set[str] = set()  # files currently in stability check
        self._pending_lock = threading.Lock()
        self._observer = Observer()
        self._stop_event = threading.Event()
        self._threads: list[threading.Thread] = []

    def start(self) -> None:
        """Start watching for files."""
        self.watch_dir.mkdir(parents=True, exist_ok=True)

        # Process any existing files first
        self._scan_existing()

        handler = _FastqHandler(
            on_new_file=self._handle_file,
        )
        # On Linux, inotify watch registration happens synchronously inside
        # start() and raises OSError (ENOSPC) when the per-user watch limit
        # is exhausted (fs.inotify.max_user_watches — common in containers).
        # The polling loop below is a complete substitute, so degrade to it
        # instead of aborting live mode.
        self._observer_started = False
        try:
            self._observer.schedule(handler, str(self.watch_dir), recursive=False)
            self._observer.start()
            self._observer_started = True
        except OSError as e:
            logger.warning(
                f"Filesystem watcher unavailable ({e}); falling back to "
                f"polling every {self._checker.check_interval}s"
            )

        # Start a polling thread as fallback for platforms where FSEvents
        # may coalesce or delay notifications (e.g. macOS)
        self._poll_thread = threading.Thread(target=self._poll_loop, daemon=True)
        self._poll_thread.start()

    def _scan_existing(self) -> None:
        """Scan for existing FASTQ files."""
        for path in sorted(self.watch_dir.iterdir()):
            if path.is_file() and _has_fastq_extension(path.name) and not self._tracker.is_processed(path):
                self._handle_file(path)

    def _poll_loop(self) -> None:
        """Periodic scan to catch files that watchdog may miss."""
        while not self._stop_event.is_set():
            self._stop_event.wait(timeout=self._checker.check_interval)
            if self._stop_event.is_set():
                break
            self._scan_existing()

    def stop(self) -> None:
        """Stop watching."""
        self._stop_event.set()
        if getattr(self, "_observer_started", False):
            self._observer.stop()
            self._observer.join()
        for t in self._threads:
            t.join(timeout=5.0)

    def _handle_file(self, path: Path) -> None:
        """Handle a detected file — check stability in a separate thread."""
        path_str = str(path)
        if self._tracker.is_processed(path):
            return

        with self._pending_lock:
            if path_str in self._pending:
                return  # already being checked
            self._pending.add(path_str)

        size = path.stat().st_size if path.exists() else 0
        logger.info(f"Detected new file: {path.name} ({size} bytes)")

        self.event_log.emit("file.detected", {
            "path": path_str,
            "size_bytes": size,
        })

        t = threading.Thread(
            target=self._stability_check,
            args=(path,),
            daemon=True,
        )
        t.start()
        self._threads.append(t)

    def _stability_check(self, path: Path) -> None:
        """Wait for file stability, then queue for processing."""
        if not self._checker.wait_for_stable(path):
            logger.warning(f"File disappeared: {path}")
            return

        if self._tracker.is_processed(path):
            return

        self._tracker.mark_processed(path)

        size = path.stat().st_size
        logger.info(f"File stable: {path.name} ({size} bytes)")

        self.event_log.emit("file.stable", {
            "path": str(path),
            "size_bytes": size,
        })

        if self._stable_queue is not None:
            self._stable_queue.put(path)
        else:
            # Fallback: direct callback (used when no queue provided)
            try:
                self.on_file_stable(path)
            except Exception as e:
                logger.error(f"Error processing {path}: {e}")
                self.event_log.emit("pipeline.error", {
                    "component": "watcher",
                    "message": f"Error processing {path}: {e}",
                })
