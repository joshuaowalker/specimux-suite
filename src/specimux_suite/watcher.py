"""File detection for live mode: watchdog + stability check."""

import fnmatch
import logging
import os
import threading
import time
from pathlib import Path
from typing import Callable

from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler, FileCreatedEvent, FileModifiedEvent

from .events import EventLog

logger = logging.getLogger(__name__)


class FileStabilityChecker:
    """Wait for a file to stop growing before processing."""

    def __init__(self, settle_time: float = 30.0, check_interval: float = 5.0):
        self.settle_time = settle_time
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


class _FastqHandler(FileSystemEventHandler):
    """Watchdog handler that detects new FASTQ files."""

    def __init__(self, pattern: str, on_new_file: Callable[[Path], None]):
        self.pattern = pattern
        self.on_new_file = on_new_file

    def on_created(self, event):
        if not event.is_directory and fnmatch.fnmatch(Path(event.src_path).name, self.pattern):
            self.on_new_file(Path(event.src_path))

    def on_modified(self, event):
        if not event.is_directory and fnmatch.fnmatch(Path(event.src_path).name, self.pattern):
            self.on_new_file(Path(event.src_path))


class FileWatcher:
    """Watch a directory for new FASTQ files, check stability, then invoke callback."""

    def __init__(
        self,
        watch_dir: Path,
        pattern: str,
        settle_time: float,
        on_file_stable: Callable[[Path], None],
        event_log: EventLog,
    ):
        self.watch_dir = Path(watch_dir)
        self.pattern = pattern
        self.settle_time = settle_time
        self.on_file_stable = on_file_stable
        self.event_log = event_log

        self._checker = FileStabilityChecker(settle_time=settle_time)
        self._tracker = ProcessedFilesTracker()
        self._observer = Observer()
        self._threads: list[threading.Thread] = []

    def start(self) -> None:
        """Start watching for files."""
        self.watch_dir.mkdir(parents=True, exist_ok=True)

        # Process any existing files first
        for path in sorted(self.watch_dir.glob(self.pattern)):
            if not self._tracker.is_processed(path):
                self._handle_file(path)

        handler = _FastqHandler(
            pattern=self.pattern,
            on_new_file=self._handle_file,
        )
        self._observer.schedule(handler, str(self.watch_dir), recursive=False)
        self._observer.start()

    def stop(self) -> None:
        """Stop watching."""
        self._observer.stop()
        self._observer.join()
        for t in self._threads:
            t.join(timeout=5.0)

    def _handle_file(self, path: Path) -> None:
        """Handle a detected file — check stability in a separate thread."""
        if self._tracker.is_processed(path):
            return

        self.event_log.emit("file.detected", {
            "path": str(path),
            "size_bytes": path.stat().st_size if path.exists() else 0,
        })

        t = threading.Thread(
            target=self._stability_check,
            args=(path,),
            daemon=True,
        )
        t.start()
        self._threads.append(t)

    def _stability_check(self, path: Path) -> None:
        """Wait for file stability, then invoke callback."""
        if not self._checker.wait_for_stable(path):
            logger.warning(f"File disappeared: {path}")
            return

        if self._tracker.is_processed(path):
            return

        self._tracker.mark_processed(path)

        self.event_log.emit("file.stable", {
            "path": str(path),
            "size_bytes": path.stat().st_size,
        })

        try:
            self.on_file_stable(path)
        except Exception as e:
            logger.error(f"Error processing {path}: {e}")
            self.event_log.emit("pipeline.error", {
                "component": "watcher",
                "message": f"Error processing {path}: {e}",
            })
