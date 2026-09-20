#!/usr/bin/env python3
"""Serve any past run's dashboard from its events.jsonl, with no processing.

Builds the viewer app over the log and starts it — useful for developing the
dashboard/present pages against real run data, and for browser-level smoke
tests of client mirrors:

    python tests/tools/serve_fixture.py tests/fixtures/parity/ont98-corrections-mini.events.jsonl
    python tests/tools/serve_fixture.py ~/somewhere/events.jsonl --port 8899

The log is copied to a temp dir first so nothing can append to the fixture.
Pass --output-dir to serve the run's own sequences and photos; otherwise
those endpoints 404 harmlessly. Without --live the replay heals specimens
an interrupted run left mid-consensus, as a real restart would.
"""

import argparse
import shutil
import tempfile
import time
from pathlib import Path

from specimux_suite.web.viewer import create_viewer_app, load_run, serve_in_thread


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("events", type=Path)
    ap.add_argument("--port", type=int, default=8765)
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--output-dir", type=Path, default=None,
                    help="Run directory with consensus/, summary/ and inat_photos/")
    ap.add_argument("--live", action="store_true",
                    help="Replay faithfully (no interrupted-run healing)")
    args = ap.parse_args()

    tmp = Path(tempfile.mkdtemp(prefix="specimux-fixture-"))
    log_path = tmp / "events.jsonl"
    shutil.copy(args.events, log_path)

    event_log, state = load_run(log_path, heal=not args.live)
    app = create_viewer_app(event_log, state, args.output_dir or tmp)
    serve_in_thread(app, args.host, args.port)
    print(f"replayed {state.version} events ({len(state.specimens)} specimens)")
    print(f"dashboard: http://{args.host}:{args.port}/  (/present)")
    try:
        while True:
            time.sleep(3600)
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
