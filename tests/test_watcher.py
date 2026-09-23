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


def test_a_file_seen_under_two_names_is_processed_once(tmp_path):
    """A watch dir reached through a symlink (macOS's /tmp and /var are
    links into /private) makes the filesystem events report a file under
    its real path and the directory scan under the given one; the watcher
    must treat both as one file, or its reads are demultiplexed twice."""
    real = tmp_path / "real"
    real.mkdir()
    link = tmp_path / "link"
    link.symlink_to(real)
    log = EventLog(tmp_path / "events.jsonl")
    seen = []
    watcher = FileWatcher(watch_dir=link, settle_time=0.1, on_file_stable=seen.append, event_log=log)
    (real / "reads.fastq").write_text("@r1\nACGT\n+\nIIII\n")
    watcher._handle_file(link / "reads.fastq")        # as the directory scan names it
    watcher._handle_file(real / "reads.fastq")        # as the filesystem events name it
    deadline = time.time() + 5
    while len(seen) < 1 and time.time() < deadline:
        time.sleep(0.05)
    time.sleep(0.5)                                   # a second one would have landed by now
    watcher.stop()
    assert len(seen) == 1
    assert sum(1 for e in log.replay() if e.type == "file.stable") == 1
    # a restart seeded with either name knows the file
    again = FileWatcher(watch_dir=link, settle_time=0.1, on_file_stable=seen.append, event_log=log)
    again._tracker.seed([str(link / "reads.fastq")])
    assert again._tracker.is_processed(real / "reads.fastq")
