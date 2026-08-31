"""FastAPI server with SSE for live dashboard updates."""

import asyncio
import json
import logging
import re
import threading
from pathlib import Path

from fastapi import FastAPI, Request
from fastapi.staticfiles import StaticFiles
from fastapi.responses import HTMLResponse, JSONResponse
from sse_starlette.sse import EventSourceResponse

import glob as globmod

from ..config import PipelineConfig
from ..events import EventLog, _event_to_dict
from ..inat_check import run_inat_check
from ..photos import photo_cache_dir
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

    # Local iNat photo cache (filled in the background by photos.prefetch_photos).
    # The /present client falls back to iNat URLs for anything not cached yet.
    if config is not None:
        photos_dir = photo_cache_dir(config.output_dir)
        photos_dir.mkdir(parents=True, exist_ok=True)
        app.mount("/photos", StaticFiles(directory=str(photos_dir)), name="photos")

    return app


@app.get("/")
async def index():
    """Serve the dashboard."""
    static_dir = Path(__file__).parent / "static"
    index_file = static_dir / "index.html"
    if index_file.exists():
        return HTMLResponse(index_file.read_text(encoding="utf-8"))
    return HTMLResponse("<h1>specimux-suite dashboard</h1><p>Static files not found.</p>")


# --- Admin (localhost only) ---
#
# Admin actions mutate the run by emitting events, so they are gated to the
# machine running the pipeline: the operator at the laptop is the admin.
# There is no TLS, so this deliberately keeps credentials off the wire.
# Two bits of local-server hygiene on top of the client-IP check:
# - the Host header must be a localhost form, which defeats DNS rebinding
#   (a malicious page resolving its own domain to 127.0.0.1);
# - mutating requests must carry a custom header, which forces a CORS
#   preflight that browsers refuse cross-origin (CSRF).
# Note: fronting this server with a reverse proxy/tunnel would make every
# request look local — don't expose the port that way.

_LOCAL_CLIENTS = {"127.0.0.1", "::1"}
_LOCAL_HOSTNAMES = {"localhost", "127.0.0.1", "::1"}
_ADMIN_HEADER = "x-specimux-admin"


def _host_base(host_header: str | None) -> str:
    """Hostname from a Host header: 'localhost:8077' → 'localhost',
    '[::1]:8077' → '::1'."""
    h = host_header or ""
    if h.startswith("["):
        return h.partition("]")[0].lstrip("[")
    return h.rsplit(":", 1)[0] if ":" in h else h


def _admin_denial(client_host: str | None, host_header: str | None,
                  admin_header: str | None = "required-not-checked") -> str | None:
    """Why this request may not use admin routes, or None if it may."""
    if client_host not in _LOCAL_CLIENTS:
        return "Admin is only available from the machine running the pipeline"
    if _host_base(host_header) not in _LOCAL_HOSTNAMES:
        return "Admin pages must be opened via localhost"
    if admin_header is None:
        return "Missing admin request header"
    return None


def _check_admin(request: Request, mutating: bool = False):
    """Return a JSONResponse denial, or None if the request is admin-OK."""
    denial = _admin_denial(
        request.client.host if request.client else None,
        request.headers.get("host"),
        request.headers.get(_ADMIN_HEADER) if mutating else "not-required",
    )
    if denial:
        return JSONResponse(status_code=403, content={"error": denial})
    return None


@app.get("/admin")
async def admin(request: Request):
    """Serve the admin page (localhost only)."""
    denial = _admin_denial(request.client.host if request.client else None,
                           request.headers.get("host"))
    if denial:
        return HTMLResponse(f"<h1>403</h1><p>{denial}.</p>", status_code=403)
    static_dir = Path(__file__).parent / "static"
    admin_file = static_dir / "admin.html"
    if admin_file.exists():
        return HTMLResponse(admin_file.read_text(encoding="utf-8"))
    return HTMLResponse("<h1>admin.html not found</h1>", status_code=404)


@app.post("/api/admin/inat/correction")
async def admin_inat_correction(request: Request):
    """Accept an iNat ID correction: emits inat.correction (pipeline heals)."""
    deny = _check_admin(request, mutating=True)
    if deny:
        return deny
    if _event_log is None or _state is None:
        return JSONResponse(status_code=500, content={"error": "Not initialized"})
    try:
        body = await request.json()
    except Exception:
        return JSONResponse(status_code=400, content={"error": "Invalid JSON"})
    specimen_id = body.get("specimen_id") or ""
    new_obs_id = str(body.get("new_obs_id") or "")
    if specimen_id not in _state.specimens:
        return JSONResponse(status_code=404, content={"error": "Specimen not found"})
    if not new_obs_id.isdigit() or len(new_obs_id) > 12:
        return JSONResponse(status_code=400, content={"error": "Invalid observation id"})
    m = re.search(r"iNat(\d+)", specimen_id)
    _event_log.emit("inat.correction", {
        "specimen_id": specimen_id,
        "old_obs_id": m.group(1) if m else "",
        "new_obs_id": new_obs_id,
    })
    return {"specimen_id": specimen_id, "new_obs_id": new_obs_id}


@app.post("/api/admin/inat/dismiss")
async def admin_inat_dismiss(request: Request):
    """Mark a suggestion reviewed-no-change: emits inat.suggestion_dismissed."""
    deny = _check_admin(request, mutating=True)
    if deny:
        return deny
    if _event_log is None or _state is None:
        return JSONResponse(status_code=500, content={"error": "Not initialized"})
    try:
        body = await request.json()
    except Exception:
        return JSONResponse(status_code=400, content={"error": "Invalid JSON"})
    specimen_id = body.get("specimen_id") or ""
    if specimen_id not in _state.specimens:
        return JSONResponse(status_code=404, content={"error": "Specimen not found"})
    _event_log.emit("inat.suggestion_dismissed", {"specimen_id": specimen_id})
    return {"specimen_id": specimen_id, "dismissed": True}


@app.post("/api/admin/inat/rescan")
async def admin_inat_rescan(request: Request):
    """Re-run the iNat ID audit in the background (network)."""
    deny = _check_admin(request, mutating=True)
    if deny:
        return deny
    if _event_log is None or _state is None or _config is None:
        return JSONResponse(status_code=500, content={"error": "Not initialized"})

    def run():
        try:
            run_inat_check(_state, _event_log, _config.summarize_output_dir)
        except Exception:
            logger.exception("iNat rescan failed")

    threading.Thread(target=run, name="inat-rescan", daemon=True).start()
    return {"started": True}


@app.get("/present")
async def present():
    """Serve the audience highlights screen (projector mode)."""
    static_dir = Path(__file__).parent / "static"
    present_file = static_dir / "present.html"
    if present_file.exists():
        return HTMLResponse(present_file.read_text(encoding="utf-8"))
    return HTMLResponse("<h1>present.html not found</h1>", status_code=404)


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


# Specimen and sequence names as they appear in FASTA headers/filenames.
# Path params are interpolated into filesystem globs/paths, so anything
# else (separators, "..", null bytes, glob metacharacters) is rejected.
_SAFE_NAME_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]*$")


def _is_safe_name(name: str) -> bool:
    return bool(_SAFE_NAME_RE.match(name)) and ".." not in name


@app.get("/api/sequence/{specimen_id}/{sequence_name}")
async def get_sequence(specimen_id: str, sequence_name: str):
    """Return a nucleotide sequence from disk (summary or consensus FASTA)."""
    if _config is None:
        return JSONResponse(status_code=404, content={"error": "Config not initialized"})
    if not _is_safe_name(specimen_id) or not _is_safe_name(sequence_name):
        return JSONResponse(status_code=400, content={"error": "Invalid name"})

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
