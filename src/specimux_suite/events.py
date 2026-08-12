"""Append-only JSONL event log — central data contract between orchestrator and web UX."""

import json
import logging
import os
import threading
import time
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Generator, Optional

logger = logging.getLogger(__name__)

# Default max log size before rotation (100 MB)
DEFAULT_MAX_LOG_BYTES = 100 * 1024 * 1024

# In-memory ring buffer size for tail() — avoids re-reading JSONL files.
# With typical events ~500 bytes each, 10K events ≈ 5 MB.
_TAIL_BUFFER_SIZE = 10_000


@dataclass
class Event:
    """A single pipeline event."""

    version: int
    type: str
    ts: str
    data: dict
    v: int = 1  # schema version


class EventLog:
    """Append-only JSONL event log with thread-safe write and tail support.

    Events are persisted to JSONL files on disk and also kept in a bounded
    in-memory ring buffer. The tail() method serves events from the buffer
    when possible, falling back to disk replay only when a client is too
    far behind.
    """

    def __init__(self, path: Path, max_bytes: int = DEFAULT_MAX_LOG_BYTES):
        self.path = Path(path)
        self._lock = threading.Lock()
        self._version = 0
        self._condition = threading.Condition(self._lock)
        self._max_bytes = max_bytes
        self._buffer: deque[Event] = deque(maxlen=_TAIL_BUFFER_SIZE)
        self._listeners: list[Callable[[Event], None]] = []

        # Ensure parent dir exists
        self.path.parent.mkdir(parents=True, exist_ok=True)

        # Recover version and populate buffer from existing log
        for log_path in self._all_log_paths():
            for line in log_path.read_text().splitlines():
                line = line.strip()
                if line:
                    self._version += 1
                    try:
                        self._buffer.append(_dict_to_event(json.loads(line)))
                    except (json.JSONDecodeError, KeyError):
                        pass

    def add_listener(self, fn: Callable[[Event], None]) -> None:
        """Register a callback invoked synchronously for every emitted event.

        Listeners run under the log lock so they observe events in version
        order; they must be fast and must never emit events themselves.
        """
        with self._lock:
            self._listeners.append(fn)

    def emit(self, event_type: str, data: Optional[dict] = None) -> Event:
        """Append an event and return it with its assigned version."""
        with self._condition:
            self._version += 1
            event = Event(
                version=self._version,
                type=event_type,
                ts=datetime.now(timezone.utc).isoformat(),
                data=data or {},
            )
            self._maybe_rotate()
            with open(self.path, "a") as f:
                f.write(json.dumps(_event_to_dict(event)) + "\n")
            self._buffer.append(event)
            for listener in self._listeners:
                try:
                    listener(event)
                except Exception:
                    logger.exception(f"Event listener failed for {event_type}")
            self._condition.notify_all()
            return event

    def replay(self) -> Generator[Event, None, None]:
        """Yield all events from all log files (archived + current) in order."""
        for log_path in self._all_log_paths():
            with open(log_path) as f:
                for line in f:
                    line = line.strip()
                    if line:
                        yield _dict_to_event(json.loads(line))

    def tail(self, after_version: int = 0, timeout: float = 30.0) -> Generator[Event, None, None]:
        """Yield events with version > after_version, blocking up to timeout for new ones.

        Serves from the in-memory buffer when possible. Falls back to disk
        replay only when the client needs events older than the buffer.
        Returns immediately after yielding any events.
        """
        events = self._get_buffered_events(after_version)
        if events is None:
            # Client needs events before buffer range — fall back to disk
            for event in self.replay():
                if event.version > after_version:
                    yield event
                    after_version = event.version
            return

        if events:
            yield from events
            return

        # No new events yet — wait for them
        deadline = time.monotonic() + timeout
        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return
            with self._condition:
                if self._version <= after_version:
                    self._condition.wait(timeout=min(remaining, 1.0))
                    if self._version <= after_version:
                        continue

            # New events available — serve from buffer
            events = self._get_buffered_events(after_version)
            if events is None:
                # Shouldn't happen (we just got notified), but handle gracefully
                for event in self.replay():
                    if event.version > after_version:
                        yield event
                        after_version = event.version
            elif events:
                yield from events
            return

    def _get_buffered_events(self, after_version: int) -> list[Event] | None:
        """Get events from the in-memory buffer after a given version.

        Returns:
            list[Event]: Events newer than after_version (may be empty if caught up)
            None: Client needs events before buffer range, caller should use replay()
        """
        with self._lock:
            if not self._buffer:
                # Buffer is empty; if events exist on disk, signal fallback
                return None if self._version > after_version else []

            buf_start = self._buffer[0].version
            buf_end = self._buffer[-1].version

            if after_version >= buf_end:
                return []  # fully caught up

            if after_version < buf_start - 1:
                return None  # too far behind, need disk replay

            # Compute offset: versions are sequential, so index math works
            start_idx = max(0, after_version - buf_start + 1)
            return [self._buffer[i] for i in range(start_idx, len(self._buffer))]

    @property
    def version(self) -> int:
        with self._lock:
            return self._version

    def _maybe_rotate(self) -> None:
        """Rotate the log file if it exceeds max_bytes. Must be called under lock."""
        if not self.path.exists():
            return
        try:
            size = self.path.stat().st_size
        except OSError:
            return
        if size < self._max_bytes:
            return

        # Find next archive number
        n = 1
        while self.path.with_suffix(f".{n}.jsonl").exists():
            n += 1
        archive = self.path.with_suffix(f".{n}.jsonl")
        os.replace(self.path, archive)
        logger.info(f"Rotated event log to {archive} ({size} bytes)")

    def _all_log_paths(self) -> list[Path]:
        """Return all log files in order: archives (1, 2, ...) then current."""
        paths = []
        n = 1
        while True:
            archive = self.path.with_suffix(f".{n}.jsonl")
            if archive.exists():
                paths.append(archive)
                n += 1
            else:
                break
        if self.path.exists():
            paths.append(self.path)
        return paths


def _event_to_dict(event: Event) -> dict:
    return {
        "v": event.v,
        "version": event.version,
        "type": event.type,
        "ts": event.ts,
        "data": event.data,
    }


def _dict_to_event(d: dict) -> Event:
    return Event(
        v=d.get("v", 1),
        version=d["version"],
        type=d["type"],
        ts=d["ts"],
        data=d.get("data", {}),
    )
