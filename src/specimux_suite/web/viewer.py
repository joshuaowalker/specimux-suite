"""Viewer app factory: the read side of the dashboard, with no pipeline.

``create_viewer_app`` builds a self-contained FastAPI app from an event
source, a ``PipelineState`` and a run directory. It serves the dashboard
contract the pages depend on — ``/api/state``, ``/events`` (SSE),
``/api/specimens``, ``/api/sequence/...``, ``/photos/...`` and the static
pages — and nothing that mutates the run. The local server builds one and
adds its command routes on top; a service that hosts many runs builds one
per run over a stored log; ``tests/tools/serve_fixture.py`` builds one over
a fixture.

The event source only has to provide ``tail(after_version, timeout)`` and
``version`` (see ``EventSource``); ``EventLog`` is the local implementation
and a remote ingest buffer can be another. The state is kept current by
whoever owns the source (the pipeline registers ``state.apply`` as a log
listener); the viewer never applies events itself.
"""

import asyncio
import json
import logging
import threading
import time
from pathlib import Path
from typing import Iterable, Optional, Protocol

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from sse_starlette.sse import EventSourceResponse

from ..events import Event, EventLog, _event_to_dict
from ..photos import photo_cache_dir
from ..state import PipelineState
from .pages import SAFE_NAME_RE, STATIC_DIR, render_page

logger = logging.getLogger(__name__)


class EventSource(Protocol):
    """What the viewer needs from an event source (``EventLog`` satisfies it)."""

    @property
    def version(self) -> int: ...

    def tail(self, after_version: int = 0, timeout: float = 30.0) -> Iterable[Event]: ...


def load_run(events_path: Path, heal: bool = True) -> tuple[EventLog, PipelineState]:
    """Open a run's event log and rebuild its state, kept current by a listener.

    ``heal`` is passed to ``PipelineState.rebuild``: True for a finished or
    restarted run, False to replay faithfully while the engine that writes
    the log is still alive.
    """
    event_log = EventLog(events_path)
    state = PipelineState()
    state.rebuild(event_log, heal=heal)
    event_log.add_listener(state.apply)
    return event_log, state


class RunPaths:
    """Where a run's served artifacts live, derived from its output dir.

    Mirrors the ``PipelineConfig`` properties so a viewer needs only the
    directory, not a config.
    """

    def __init__(self, output_dir: Path):
        self.output_dir = Path(output_dir)
        self.consensus_dir = self.output_dir / "consensus"
        self.summary_dir = self.output_dir / "summary"
        self.photos_dir = photo_cache_dir(self.output_dir)


# --- SSE fan-out ---
#
# One broadcaster thread per app tails the event source and pushes to
# per-client asyncio queues, so a connected viewer costs a queue rather
# than a thread. (Parking one executor thread per client in a blocking
# tail() starved event delivery for everyone once a roomful of --share
# viewers connected.)
_SUBSCRIBER_QUEUE_MAX = 1000


class Broadcaster:
    def __init__(self, source: EventSource):
        self._source = source
        self._lock = threading.Lock()
        self._subscribers: list[dict] = []
        self._started = False
        self.clients = 0

    def ensure_started(self) -> None:
        with self._lock:
            if self._started:
                return
            self._started = True
        threading.Thread(target=self._loop, name="sse-broadcast", daemon=True).start()

    def subscribe(self, sub: dict) -> None:
        with self._lock:
            self.clients += 1
            self._subscribers.append(sub)

    def unsubscribe(self, sub: dict) -> None:
        with self._lock:
            self.clients -= 1
            self._subscribers.remove(sub)

    def _loop(self) -> None:
        version = self._source.version
        while True:
            try:
                for event in self._source.tail(after_version=version, timeout=5.0):
                    version = event.version
                    with self._lock:
                        subs = list(self._subscribers)
                    for sub in subs:
                        def push(sub=sub, event=event):
                            try:
                                sub["queue"].put_nowait(event)
                            except asyncio.QueueFull:
                                # Too slow to drain: mark it; the generator
                                # closes and the client reconnects, catching
                                # up from its version via the backlog path.
                                sub["overflow"] = True
                        try:
                            sub["loop"].call_soon_threadsafe(push)
                        except RuntimeError:
                            pass  # client's loop already closed
            except Exception:
                logger.exception("SSE broadcaster error")
                time.sleep(1)


def _sse_message(event: Event) -> dict:
    return {
        "event": event.type,
        "id": str(event.version),
        "data": json.dumps(_event_to_dict(event)),
    }


# --- Sequence lookup ---

def _parse_fasta_entry(path: Path, entry_name: str) -> str | None:
    """Find a sequence by header ID in a FASTA file."""
    try:
        text = path.read_text(encoding="utf-8")
    except (OSError, IOError):
        return None
    current_id = None
    seq_lines = []
    for line in text.splitlines():
        if line.startswith(">"):
            if current_id == entry_name and seq_lines:
                return "".join(seq_lines)
            current_id = line[1:].split()[0]
            seq_lines = []
        else:
            seq_lines.append(line.strip())
    if current_id == entry_name and seq_lines:
        return "".join(seq_lines)
    return None


def _read_single_fasta(path: Path) -> str | None:
    """Read the first sequence from a single-sequence FASTA file."""
    try:
        text = path.read_text(encoding="utf-8")
    except (OSError, IOError):
        return None
    seq_lines = []
    for line in text.splitlines():
        if line.startswith(">"):
            if seq_lines:
                return "".join(seq_lines)
            continue
        seq_lines.append(line.strip())
    return "".join(seq_lines) if seq_lines else None


def is_safe_name(name: str) -> bool:
    """Specimen and sequence names as they appear in FASTA headers/filenames.

    Path params are interpolated into filesystem globs/paths, so anything
    else (separators, "..", null bytes, glob metacharacters) is rejected.
    """
    return bool(SAFE_NAME_RE.match(name)) and ".." not in name


def find_sequence(paths: RunPaths, specimen_id: str, sequence_name: str) -> str | None:
    """A sequence from the summary dir (variants) or the consensus dir (clusters)."""
    matches = list(paths.summary_dir.glob(f"{sequence_name}-RiC*.fasta"))
    if matches:
        seq = _read_single_fasta(matches[0])
        if seq:
            return seq
    consensus_fasta = paths.consensus_dir / specimen_id / f"{specimen_id}-all.fasta"
    if consensus_fasta.exists():
        seq = _parse_fasta_entry(consensus_fasta, sequence_name)
        if seq:
            return seq
    return None


def state_knows_sequence(state: PipelineState, specimen_id: str, sequence_name: str) -> bool:
    """True if an event has announced this cluster or variant.

    A sequence the state knows but the disk lacks is one whose file has not
    landed yet (the viewer reads the directory the engine is still writing)
    — that gets a retryable response rather than a 404.
    """
    spec = state.specimens.get(specimen_id)
    if spec is None:
        return False
    if any(c.name == sequence_name for c in spec.clusters):
        return True
    return any(v.get("name") == sequence_name for v in spec.variants)


NOT_YET_AVAILABLE = {"error": "Sequence not yet available", "retry": True}


# --- The factory ---

def create_viewer_app(
    event_log: EventSource,
    state: PipelineState,
    output_dir: Path,
    *,
    config_summary: Optional[dict] = None,
    share: Optional[dict] = None,
    max_clients: int = 0,
    runtime: Optional[dict] = None,
    allowed_origins: Iterable[str] = (),
    title: str = "specimux-suite",
) -> FastAPI:
    """Build the read-only dashboard app for one run.

    ``config_summary`` and ``share`` ride along in ``/api/state`` when given
    (the pages read thresholds from the former and draw a QR code from the
    latter); ``max_clients`` caps concurrent SSE connections (0 = no cap).
    ``runtime`` is injected into the pages this app serves (see
    ``pages.py``; the defaults point everything at this app's own origin).
    ``allowed_origins`` lists page origins that may call this API from a
    browser with credentials — pages hosted elsewhere and pointed here by
    their runtime config; empty means same-origin only.
    """
    paths = RunPaths(output_dir)
    app = FastAPI(title=title)
    origins = list(allowed_origins)
    if origins:
        app.add_middleware(
            CORSMiddleware, allow_origins=origins, allow_credentials=True,
            allow_methods=["GET", "POST"], allow_headers=["*"],
        )
    broadcaster = Broadcaster(event_log)
    app.state.viewer = {"event_log": event_log, "state": state, "paths": paths,
                        "broadcaster": broadcaster, "runtime": runtime}

    if STATIC_DIR.exists():
        app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")
    # Observation photos cached per run (photos.prefetch_photos fills it in
    # the background); the pages fall back to the provider URL on a miss.
    paths.photos_dir.mkdir(parents=True, exist_ok=True)
    app.mount("/photos", StaticFiles(directory=str(paths.photos_dir)), name="photos")

    @app.middleware("http")
    async def photo_cache_headers(request: Request, call_next):
        """Cached photos are keyed by immutable provider photo id — let
        every browser fetch each one exactly once."""
        response = await call_next(request)
        if request.url.path.startswith("/photos/") and response.status_code == 200:
            response.headers["Cache-Control"] = "public, max-age=31536000, immutable"
        return response

    @app.get("/")
    async def index():
        """Serve the dashboard."""
        return HTMLResponse(render_page("index.html", runtime))

    @app.get("/present")
    async def present():
        """Serve the audience highlights screen (projector mode)."""
        return HTMLResponse(render_page("present.html", runtime))

    @app.get("/api/state")
    async def get_state():
        """Full state snapshot.

        The owner of the state keeps it current by applying every event as
        it is emitted, so a snapshot is O(state), not O(event history).
        """
        result = state.to_dict()
        if config_summary is not None:
            result["config_summary"] = config_summary
        if share:
            result["share"] = dict(share)
        result["sse_clients"] = broadcaster.clients
        return result

    @app.get("/api/viewers")
    async def get_viewers():
        """Lightweight endpoint for viewer count polling."""
        return {"sse_clients": broadcaster.clients}

    @app.get("/api/specimens")
    async def get_specimens():
        """Specimen list with status."""
        return list(state.to_dict()["specimens"].values())

    @app.get("/events")
    async def event_stream(request: Request, after_version: int = 0):
        """SSE endpoint — streams events as they arrive, ids are log versions."""
        if max_clients > 0 and broadcaster.clients >= max_clients:
            return JSONResponse(
                status_code=503,
                content={"error": "Dashboard is full, try again later",
                         "max_clients": max_clients},
            )
        broadcaster.ensure_started()

        async def generate():
            sub = {
                "queue": asyncio.Queue(maxsize=_SUBSCRIBER_QUEUE_MAX),
                "loop": asyncio.get_running_loop(),
                "overflow": False,
            }
            # Subscribe before reading the backlog so no event can fall
            # between; the version check below dedupes the overlap.
            broadcaster.subscribe(sub)
            try:
                version = after_version
                for event in event_log.tail(after_version=version, timeout=0):
                    version = event.version
                    yield _sse_message(event)
                while True:
                    if sub["overflow"]:
                        return
                    try:
                        event = await asyncio.wait_for(sub["queue"].get(), timeout=5.0)
                    except asyncio.TimeoutError:
                        if await request.is_disconnected():
                            return
                        continue
                    if event.version <= version:
                        continue
                    version = event.version
                    yield _sse_message(event)
            finally:
                broadcaster.unsubscribe(sub)

        return EventSourceResponse(generate())

    @app.get("/api/sequence/{specimen_id}/{sequence_name}")
    async def get_sequence(specimen_id: str, sequence_name: str):
        """A nucleotide sequence from disk (summary or consensus FASTA).

        503 with ``Retry-After`` when the state has announced the sequence
        but its file has not landed yet; 404 when nothing knows it.
        """
        if not is_safe_name(specimen_id) or not is_safe_name(sequence_name):
            return JSONResponse(status_code=400, content={"error": "Invalid name"})
        seq = find_sequence(paths, specimen_id, sequence_name)
        if seq:
            return {"sequence": seq}
        if state_knows_sequence(state, specimen_id, sequence_name):
            return JSONResponse(status_code=503, content=NOT_YET_AVAILABLE,
                                headers={"Retry-After": "1"})
        return JSONResponse(status_code=404, content={"error": "Sequence not found"})

    return app


def serve_in_thread(app: FastAPI, host: str, port: int) -> threading.Thread:
    """Run an app under uvicorn in a daemon thread."""
    import uvicorn

    def run():
        uvicorn.run(app, host=host, port=port, log_level="warning")

    thread = threading.Thread(target=run, name="web-server", daemon=True)
    thread.start()
    return thread
