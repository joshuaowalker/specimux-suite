"""The local web server: the viewer app plus the routes that mutate a run.

The read side (state, SSE, sequences, photos, pages) comes from the viewer
factory in ``viewer.py``; this module adds what only the machine running
the pipeline may do — toggling watches and the localhost-only admin page
and its actions — and starts uvicorn.
"""

import logging
import re
import threading

from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, JSONResponse

from ..config import PipelineConfig
from ..events import EventLog
from ..inat_check import run_inat_check
from ..state import PipelineState
from .pages import render_page
from .viewer import create_viewer_app, is_safe_name, serve_in_thread

logger = logging.getLogger(__name__)

_is_safe_name = is_safe_name  # kept for existing imports


def create_app(event_log: EventLog, state: PipelineState, config: PipelineConfig) -> FastAPI:
    """The viewer app for this run with the local mutation routes added."""
    share = None
    if config.share_url:
        share = {"url": config.share_url, "max_clients": config.share_max_clients}
    app = create_viewer_app(
        event_log, state, config.output_dir,
        config_summary=config.summary(),
        share=share,
        max_clients=config.share_max_clients,
    )
    _add_mutation_routes(app, event_log, state, config)
    return app


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


def _add_mutation_routes(app: FastAPI, event_log: EventLog, state: PipelineState,
                         config: PipelineConfig) -> None:

    @app.get("/admin")
    async def admin(request: Request):
        """Serve the admin page (localhost only)."""
        denial = _admin_denial(request.client.host if request.client else None,
                               request.headers.get("host"))
        if denial:
            return HTMLResponse(f"<h1>403</h1><p>{denial}.</p>", status_code=403)
        return HTMLResponse(render_page("admin.html"))

    @app.post("/api/admin/inat/correction")
    async def admin_inat_correction(request: Request):
        """Accept an iNat ID correction: emits inat.correction (pipeline heals)."""
        deny = _check_admin(request, mutating=True)
        if deny:
            return deny
        try:
            body = await request.json()
        except Exception:
            return JSONResponse(status_code=400, content={"error": "Invalid JSON"})
        specimen_id = body.get("specimen_id") or ""
        new_obs_id = str(body.get("new_obs_id") or "")
        if specimen_id not in state.specimens:
            return JSONResponse(status_code=404, content={"error": "Specimen not found"})
        if not new_obs_id.isdigit() or len(new_obs_id) > 12:
            return JSONResponse(status_code=400, content={"error": "Invalid observation id"})
        m = re.search(r"iNat(\d+)", specimen_id)
        event_log.emit("inat.correction", {
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
        try:
            body = await request.json()
        except Exception:
            return JSONResponse(status_code=400, content={"error": "Invalid JSON"})
        specimen_id = body.get("specimen_id") or ""
        if specimen_id not in state.specimens:
            return JSONResponse(status_code=404, content={"error": "Specimen not found"})
        event_log.emit("inat.suggestion_dismissed", {"specimen_id": specimen_id})
        return {"specimen_id": specimen_id, "dismissed": True}

    @app.post("/api/admin/inat/rescan")
    async def admin_inat_rescan(request: Request):
        """Re-run the iNat ID audit in the background (network)."""
        deny = _check_admin(request, mutating=True)
        if deny:
            return deny

        def run():
            try:
                run_inat_check(state, event_log, config.summarize_output_dir)
            except Exception:
                logger.exception("iNat rescan failed")

        threading.Thread(target=run, name="inat-rescan", daemon=True).start()
        return {"started": True}

    @app.post("/api/watch/{specimen_id}")
    async def toggle_watch(specimen_id: str):
        """Toggle watched state for a specimen."""
        spec = state.specimens.get(specimen_id)
        if not spec:
            return JSONResponse(status_code=404, content={"error": "Specimen not found"})
        new_watched = not spec.watched
        event_log.emit("specimen.watched", {
            "specimen_id": specimen_id,
            "watched": new_watched,
        })
        return {"specimen_id": specimen_id, "watched": new_watched}


def start_web_server(event_log: EventLog, state: PipelineState, config: PipelineConfig):
    """Start the web server in a background thread."""
    app = create_app(event_log, state, config)
    serve_in_thread(app, config.web_host, config.web_port)
    logger.info(f"Web dashboard at http://{config.web_host}:{config.web_port}")
