"""The HTTP event forwarder: mirror a run's events to a remote endpoint.

A plugin (``plugins.py``) that POSTs the event log to a URL in batches, in
version order, at least once. The log itself is the buffer: a thread tails
it from the last acknowledged version, so nothing is held in memory beyond
one batch, an unreachable endpoint costs nothing but retries, and a restart
resumes from the version the receiver last acknowledged (persisted beside
the log). The receiver dedupes by version.

Each POST is JSON: ``{"events": [...], "from_version": a, "to_version": b}``
plus any ``extra`` fields the caller adds (a run id, a fencing token). A
2xx acknowledges the whole batch; anything else is retried with backoff.

Mirroring a run to a read-only screen elsewhere (a foray's public
display, a hosted dashboard) is the use; ``--forward-events URL`` on the
command line attaches it.
"""

import json
import logging
import threading
import time
import urllib.error
import urllib.request
from pathlib import Path
from typing import Optional

from .events import EventLog, _event_to_dict
from .util import USER_AGENT, atomic_write

logger = logging.getLogger(__name__)

ACK_FILENAME = "forward-ack.json"


class EventForwarder:
    def __init__(self, url: str, *, headers: Optional[dict] = None,
                 extra: Optional[dict] = None, batch_max: int = 200,
                 flush_s: float = 0.5, retry_max_s: float = 30.0,
                 timeout_s: float = 20.0, ack_path: Optional[Path] = None):
        self.url = url
        self.headers = dict(headers or {})
        self.extra = dict(extra or {})
        self.batch_max = max(1, batch_max)
        self.flush_s = flush_s
        self.retry_max_s = retry_max_s
        self.timeout_s = timeout_s
        self.ack_path = ack_path
        self.event_log: Optional[EventLog] = None
        self.acked_version = 0
        self.sent_batches = 0
        self.last_error: Optional[str] = None
        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None

    @classmethod
    def from_options(cls, options: dict) -> "EventForwarder":
        """The entry-point factory: ``forward_url`` and optional
        ``forward_header`` ("Name: value")."""
        url = options.get("forward_url")
        if not url:
            raise ValueError("forward plugin needs forward_url=URL")
        headers = {}
        header = options.get("forward_header")
        if header:
            name, _, value = header.partition(":")
            headers[name.strip()] = value.strip()
        return cls(url, headers=headers)

    # --- plugin protocol ---

    def start(self, context) -> None:
        self.attach(context.event_log, context.output_dir)

    def attach(self, event_log: EventLog, output_dir: Path) -> None:
        """Start forwarding this log (the plugin hook without a context)."""
        self.event_log = event_log
        if self.ack_path is None:
            self.ack_path = Path(output_dir) / ACK_FILENAME
        self.acked_version = self._load_ack()
        self._stop.clear()
        self._thread = threading.Thread(target=self._loop, name="event-forward", daemon=True)
        self._thread.start()
        logger.info(f"Forwarding events to {self.url} from version {self.acked_version + 1}")

    def shutdown(self, flush_timeout: float = 5.0) -> None:
        """Stop, giving in-flight delivery a short grace to finish.

        Anything not acknowledged stays in the log and is sent on the next
        start, so a hard stop loses nothing.
        """
        self._stop.set()
        if self._thread:
            self._thread.join(timeout=flush_timeout)
            self._thread = None

    # --- delivery ---

    def _loop(self) -> None:
        version = self.acked_version
        while not self._stop.is_set():
            try:
                pending = list(self.event_log.tail(after_version=version, timeout=self.flush_s))
            except Exception:
                logger.exception("Event forwarder failed to read the log")
                time.sleep(1)
                continue
            for i in range(0, len(pending), self.batch_max):
                batch = pending[i:i + self.batch_max]
                if not self._deliver(batch):
                    return  # stopped mid-retry; resume from the ack next start
                version = batch[-1].version
                self.acked_version = version
                self._save_ack(version)

    def _deliver(self, batch: list) -> bool:
        """POST one batch until it is acknowledged or we are stopped."""
        body = json.dumps({
            **self.extra,
            "from_version": batch[0].version,
            "to_version": batch[-1].version,
            "events": [_event_to_dict(e) for e in batch],
        }).encode("utf-8")
        delay = 0.5
        while True:
            error = self._post(body)
            if error is None:
                self.sent_batches += 1
                self.last_error = None
                return True
            if self.last_error != error:
                logger.warning(f"Event forward to {self.url} failed ({error}); retrying")
            self.last_error = error
            if self._stop.wait(timeout=delay):
                return False
            delay = min(delay * 2, self.retry_max_s)

    def _post(self, body: bytes) -> Optional[str]:
        req = urllib.request.Request(self.url, data=body, method="POST", headers={
            "Content-Type": "application/json",
            "User-Agent": USER_AGENT,
            **self.headers,
        })
        try:
            with urllib.request.urlopen(req, timeout=self.timeout_s) as resp:
                if 200 <= resp.status < 300:
                    return None
                return f"HTTP {resp.status}"
        except urllib.error.HTTPError as e:
            return f"HTTP {e.code}"
        except (urllib.error.URLError, OSError, ValueError) as e:
            return str(e.reason if isinstance(e, urllib.error.URLError) else e)

    # --- the ack file ---

    def _load_ack(self) -> int:
        try:
            data = json.loads(self.ack_path.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            return 0
        # A different endpoint starts over; its acks mean nothing here
        if data.get("url") != self.url:
            return 0
        return int(data.get("acked_version", 0))

    def _save_ack(self, version: int) -> None:
        try:
            atomic_write(self.ack_path, json.dumps(
                {"url": self.url, "acked_version": version}).encode("utf-8"))
        except OSError as e:
            logger.warning(f"Could not persist forward ack: {e}")


def forwarder_plugin(options: dict) -> EventForwarder:
    """Entry point ``forward``."""
    return EventForwarder.from_options(options)
