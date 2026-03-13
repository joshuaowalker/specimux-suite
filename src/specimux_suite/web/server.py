"""FastAPI server with SSE for live dashboard updates."""

import json
import logging
import threading
from pathlib import Path

from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from fastapi.responses import HTMLResponse
from sse_starlette.sse import EventSourceResponse

from ..config import PipelineConfig
from ..events import EventLog, _event_to_dict
from ..state import PipelineState

logger = logging.getLogger(__name__)

app = FastAPI(title="specimux-suite")

# These get set by start_web_server
_event_log: EventLog = None
_state: PipelineState = None


def create_app(event_log: EventLog, state: PipelineState) -> FastAPI:
    """Create the FastAPI app with references to shared state."""
    global _event_log, _state
    _event_log = event_log
    _state = state

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
    """Full state snapshot."""
    if _state is None:
        return {"error": "State not initialized"}
    # Rebuild from events for freshness
    fresh = PipelineState()
    fresh.rebuild(_event_log)
    return fresh.to_dict()


@app.get("/api/specimens")
async def get_specimens():
    """Specimen list with status."""
    if _state is None:
        return []
    fresh = PipelineState()
    fresh.rebuild(_event_log)
    return list(fresh.to_dict()["specimens"].values())


@app.get("/events")
async def event_stream(after_version: int = 0):
    """SSE endpoint — streams events as they arrive."""
    async def generate():
        version = after_version
        while True:
            for event in _event_log.tail(after_version=version, timeout=30.0):
                version = event.version
                yield {
                    "event": event.type,
                    "id": str(event.version),
                    "data": json.dumps(_event_to_dict(event)),
                }

    return EventSourceResponse(generate())


def start_web_server(event_log: EventLog, state: PipelineState, config: PipelineConfig):
    """Start the web server in a background thread."""
    import uvicorn

    app_instance = create_app(event_log, state)

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
