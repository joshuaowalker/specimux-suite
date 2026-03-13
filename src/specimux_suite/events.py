"""Append-only JSONL event log — central data contract between orchestrator and web UX."""

import json
import threading
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Generator, Optional


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

    def __init__(self, path: Path):
        self.path = Path(path)
        self._lock = threading.Lock()
        self._version = 0
        self._condition = threading.Condition(self._lock)

        # Ensure parent dir exists
        self.path.parent.mkdir(parents=True, exist_ok=True)

        # Recover version from existing log
        if self.path.exists():
            for line in self.path.read_text().splitlines():
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
            with open(self.path, "a") as f:
                f.write(json.dumps(_event_to_dict(event)) + "\n")
            self._condition.notify_all()
            return event

    def replay(self) -> Generator[Event, None, None]:
        """Yield all events from the log file."""
        if not self.path.exists():
            return
        with open(self.path) as f:
            for line in f:
                line = line.strip()
                if line:
                    yield _dict_to_event(json.loads(line))

    def tail(self, after_version: int = 0, timeout: float = 30.0) -> Generator[Event, None, None]:
        """Yield events with version > after_version, blocking up to timeout for new ones."""
        # First yield any already-written events
        for event in self.replay():
            if event.version > after_version:
                yield event
                after_version = event.version

        # Then wait for new events
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

            # New events available — read them from file
            for event in self.replay():
                if event.version > after_version:
                    yield event
                    after_version = event.version

    @property
    def version(self) -> int:
        with self._lock:
            return self._version


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
