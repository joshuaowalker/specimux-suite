"""FastAPI server with SSE for live dashboard updates."""

import asyncio
import json
import logging
import threading
from pathlib import Path

from fastapi import FastAPI, Request
from fastapi.staticfiles import StaticFiles
from fastapi.responses import HTMLResponse, JSONResponse
from sse_starlette.sse import EventSourceResponse

import glob as globmod

from ..config import PipelineConfig
from ..events import EventLog, _event_to_dict
from ..state import PipelineState

logger = logging.getLogger(__name__)

app = FastAPI(title="specimux-suite")

# These get set by start_web_server
_event_log: EventLog = None
_state: PipelineState = None
_config: PipelineConfig = None
_sse_clients: int = 0
_sse_lock = threading.Lock()


def create_app(event_log: EventLog, state: PipelineState, config: PipelineConfig = None) -> FastAPI:
    """Create the FastAPI app with references to shared state."""
    global _event_log, _state, _config
    _event_log = event_log
    _state = state
    _config = config

    static_dir = Path(__file__).parent / "static"
    if static_dir.exists():
        app.mount("/static", StaticFiles(directory=str(static_dir)), name="static")

    return app


@app.get("/")
async def index():
    """Serve the dashboard."""
    static_dir = Path(__file__).parent / "static"
    index_file = static_dir / "index.html"
    if index_file.exists():
        return HTMLResponse(index_file.read_text())
    return HTMLResponse("<h1>specimux-suite dashboard</h1><p>Static files not found.</p>")


@app.get("/api/state")
async def get_state():
    """Full state snapshot.

    The pipeline keeps this state instance current by applying every event
    as it is emitted, so a snapshot is O(state), not O(event history).
    """
    if _state is None:
        return {"error": "State not initialized"}
    result = _state.to_dict()
    if _config:
        result["config_summary"] = _config.summary()
        if _config.share_url:
            result["share"] = {
                "url": _config.share_url,
                "max_clients": _config.share_max_clients,
            }
    with _sse_lock:
        result["sse_clients"] = _sse_clients
    return result


@app.get("/api/viewers")
async def get_viewers():
    """Lightweight endpoint for viewer count polling."""
    with _sse_lock:
        return {"sse_clients": _sse_clients}


@app.get("/api/specimens")
async def get_specimens():
    """Specimen list with status."""
    if _state is None:
        return []
    return list(_state.to_dict()["specimens"].values())


@app.get("/events")
async def event_stream(request: Request, after_version: int = 0):
    """SSE endpoint — streams events as they arrive."""
    global _sse_clients

    # Check connection limit
    if _config and _config.share_max_clients > 0:
        with _sse_lock:
            if _sse_clients >= _config.share_max_clients:
                return JSONResponse(
                    status_code=503,
                    content={"error": "Dashboard is full, try again later",
                             "max_clients": _config.share_max_clients},
                )

    async def generate():
        global _sse_clients
        with _sse_lock:
            _sse_clients += 1
        try:
            version = after_version
            loop = asyncio.get_event_loop()
            while True:
                if await request.is_disconnected():
                    return
                try:
                    events = await loop.run_in_executor(
                        None,
                        lambda: list(_event_log.tail(after_version=version, timeout=5.0))
                    )
                except RuntimeError:
                    # Executor shut down during server exit
                    return
                for event in events:
                    version = event.version
                    yield {
                        "event": event.type,
                        "id": str(event.version),
                        "data": json.dumps(_event_to_dict(event)),
                    }
        finally:
            with _sse_lock:
                _sse_clients -= 1

    return EventSourceResponse(generate())


def _parse_fasta_entry(path: Path, entry_name: str) -> str | None:
    """Find a sequence by header ID in a FASTA file."""
    try:
        text = path.read_text()
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
        text = path.read_text()
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


@app.get("/api/sequence/{specimen_id}/{sequence_name}")
async def get_sequence(specimen_id: str, sequence_name: str):
    """Return a nucleotide sequence from disk (summary or consensus FASTA)."""
    if _config is None:
        return JSONResponse(status_code=404, content={"error": "Config not initialized"})

    # Try summary dir first: glob for {sequence_name}-RiC*.fasta
    summary_dir = _config.summarize_output_dir
    matches = list(summary_dir.glob(f"{sequence_name}-RiC*.fasta"))
    if matches:
        seq = _read_single_fasta(matches[0])
        if seq:
            return {"sequence": seq}

    # Try consensus dir: parse {specimen_id}/{specimen_id}-all.fasta
    consensus_fasta = _config.consensus_output_dir / specimen_id / f"{specimen_id}-all.fasta"
    if consensus_fasta.exists():
        seq = _parse_fasta_entry(consensus_fasta, sequence_name)
        if seq:
            return {"sequence": seq}

    return JSONResponse(status_code=404, content={"error": "Sequence not found"})


@app.post("/api/watch/{specimen_id}")
async def toggle_watch(specimen_id: str):
    """Toggle watched state for a specimen."""
    if _event_log is None or _state is None:
        return JSONResponse(status_code=500, content={"error": "Not initialized"})
    spec = _state.specimens.get(specimen_id)
    if not spec:
        return JSONResponse(status_code=404, content={"error": "Specimen not found"})
    new_watched = not spec.watched
    _event_log.emit("specimen.watched", {
        "specimen_id": specimen_id,
        "watched": new_watched,
    })
    return {"specimen_id": specimen_id, "watched": new_watched}


def start_web_server(event_log: EventLog, state: PipelineState, config: PipelineConfig):
    """Start the web server in a background thread."""
    import uvicorn

    app_instance = create_app(event_log, state, config)

    def run():
        uvicorn.run(
            app_instance,
            host=config.web_host,
            port=config.web_port,
            log_level="warning",
        )

    thread = threading.Thread(target=run, daemon=True)
    thread.start()
    logger.info(f"Web dashboard at http://{config.web_host}:{config.web_port}")
