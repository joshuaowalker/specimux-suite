#!/usr/bin/env python3
"""Serve any past run's dashboard from its events.jsonl, with no processing.

Rebuilds PipelineState from the log and starts the web server against it —
useful for developing the dashboard/present/admin pages against real run
data, and for browser-level smoke tests of client mirrors:

    python tests/tools/serve_fixture.py tests/fixtures/parity/ont98-corrections-mini.events.jsonl
    python tests/tools/serve_fixture.py ~/somewhere/events.jsonl --port 8899

The log is copied to a temp dir first so web mutations (watch stars, admin
corrections) can never append to the fixture. Endpoints that read run files
(sequences, photos) 404 harmlessly.
"""

import argparse
import shutil
import tempfile
import time
from pathlib import Path

from specimux_suite.config import PipelineConfig
from specimux_suite.events import EventLog
from specimux_suite.state import PipelineState
from specimux_suite.web.server import start_web_server


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("events", type=Path)
    ap.add_argument("--port", type=int, default=8765)
    ap.add_argument("--host", default="127.0.0.1")
    args = ap.parse_args()

    tmp = Path(tempfile.mkdtemp(prefix="specimux-fixture-"))
    log_path = tmp / "events.jsonl"
    shutil.copy(args.events, log_path)

    event_log = EventLog(log_path)
    state = PipelineState()
    n = 0
    for event in event_log.replay():
        state.apply(event)
        n += 1

    config = PipelineConfig(
        primers_file=tmp / "primers.fasta",
        specimens_file=tmp / "specimens.txt",
        output_dir=tmp,
        web_host=args.host,
        web_port=args.port,
    )
    start_web_server(event_log, state, config)
    print(f"replayed {n} events ({len(state.specimens)} specimens)")
    print(f"dashboard: http://{args.host}:{args.port}/  (/present, /admin)")
    try:
        while True:
            time.sleep(3600)
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
