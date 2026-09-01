"""Minimal tty-aware progress bars for blocking startup work (pure stdlib)."""

import sys
import time


class StageProgress:
    """One labeled progress bar on stderr, `\\r`-updated in place.

    Callback-compatible with the fetchers: pass `bar.update` as their
    `progress` argument — they call it as (done, total), so the bar learns
    its real total (uncached items) on the first callback. A stage that
    never calls back was fully cached; `finish()` says so.

    On a non-tty stderr nothing is drawn mid-stage — just a start line and
    a completion line, so logs stay readable.
    """

    WIDTH = 28

    def __init__(self, label: str, enabled: bool = True, stream=None):
        self.label = label
        self.stream = stream if stream is not None else sys.stderr
        self.enabled = enabled
        self.tty = self.enabled and self.stream.isatty()
        self.done = 0
        self.total = 0
        self.started = time.monotonic()
        self._last_draw = 0.0
        if self.enabled and not self.tty:
            print(f"{self.label}…", file=self.stream, flush=True)
        elif self.tty:
            # Draw the label right away: a stage whose first progress
            # callback is slow (or that has none) must not look like a hang
            self.update(0)

    def update(self, done: int, total: int | None = None) -> None:
        self.done = done
        if total is not None:
            self.total = total
        if not self.tty:
            return
        now = time.monotonic()
        # Redraw at most ~20/s so tight loops don't spend time painting
        if done < self.total and now - self._last_draw < 0.05:
            return
        self._last_draw = now
        if self.total > 0:
            filled = round(self.WIDTH * min(done, self.total) / self.total)
            bar = "█" * filled + "░" * (self.WIDTH - filled)
            line = f"\r{self.label:<26} {bar} {done}/{self.total}"
        else:
            line = f"\r{self.label:<26} …"
        print(f"{line}\x1b[K", end="", file=self.stream, flush=True)

    def finish(self, note: str = "") -> None:
        if not self.enabled:
            return
        elapsed = time.monotonic() - self.started
        if not note:
            if self.total == 0:
                note = "cached"
            elif self.done < self.total:
                note = f"{self.done}/{self.total} (interrupted)"
            else:
                note = f"{self.done} in {elapsed:.0f}s" if elapsed >= 1 else str(self.done)
        if self.tty:
            print(f"\r{self.label:<26} ✓ {note}\x1b[K", file=self.stream, flush=True)
        else:
            print(f"{self.label}: {note}", file=self.stream, flush=True)
