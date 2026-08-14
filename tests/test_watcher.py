"""Tests for FileWatcher platform-degradation behavior."""

import time
from unittest.mock import patch

from specimux_suite.events import EventLog
from specimux_suite.watcher import FileWatcher


def test_watcher_degrades_to_polling_on_inotify_failure(tmp_path):
    """OSError from the FS observer (e.g. inotify watch limit, ENOSPC on
    Linux) must degrade to the polling loop, not abort live mode."""
    watch_dir = tmp_path / "watch"
    log = EventLog(tmp_path / "events.jsonl")
    seen = []

    watcher = FileWatcher(
        watch_dir=watch_dir,
        settle_time=0.1,
        on_file_stable=lambda p: seen.append(p),
        event_log=log,
    )
    with patch.object(watcher._observer, "schedule",
                      side_effect=OSError(28, "No space left on device")):
        watcher.start()
    try:
        assert watcher._observer_started is False
        # Polling fallback still detects and settles a new file
        f = watch_dir / "reads.fastq"
        f.write_text("@r1\nACGT\n+\nIIII\n")
        deadline = time.time() + 5
        while not seen and time.time() < deadline:
            time.sleep(0.05)
        assert seen and seen[0].name == "reads.fastq"
    finally:
        # stop() must not raise on the never-started observer
        watcher.stop()
