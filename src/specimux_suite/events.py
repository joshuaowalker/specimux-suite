"""Append-only JSONL event log — central data contract between orchestrator and web UX."""

import json
import logging
import os
import threading
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Generator, Optional

logger = logging.getLogger(__name__)

# Default max log size before rotation (100 MB)
DEFAULT_MAX_LOG_BYTES = 100 * 1024 * 1024


@dataclass
class Event:
    """A single pipeline event."""

    version: int
    type: str
    ts: str
    data: dict
    v: int = 1  # schema version


class EventLog:
    """Append-only JSONL event log with thread-safe write and tail support."""

    def __init__(self, path: Path, max_bytes: int = DEFAULT_MAX_LOG_BYTES):
        self.path = Path(path)
        self._lock = threading.Lock()
        self._version = 0
        self._condition = threading.Condition(self._lock)
        self._max_bytes = max_bytes

        # Ensure parent dir exists
        self.path.parent.mkdir(parents=True, exist_ok=True)

        # Recover version from existing log (and any rotated archives)
        for log_path in self._all_log_paths():
            for line in log_path.read_text().splitlines():
                if line.strip():
                    self._version += 1

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

        Returns immediately after yielding any buffered events. Only blocks
        (up to timeout) when no new events are available yet.
        """
        # First yield any already-written events
        yielded = False
        for event in self.replay():
            if event.version > after_version:
                yield event
                after_version = event.version
                yielded = True

        # If we yielded buffered events, return immediately so the SSE
        # endpoint can flush them to the client without waiting.
        if yielded:
            return

        # No buffered events — wait for new ones
        deadline = time.monotonic() + timeout
        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return
            with self._condition:
                current = self._version
                if current <= after_version:
                    self._condition.wait(timeout=min(remaining, 1.0))
                    if self._version <= after_version:
                        continue

            # New events available — read and yield them, then return
            for event in self.replay():
                if event.version > after_version:
                    yield event
                    after_version = event.version
            return

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
