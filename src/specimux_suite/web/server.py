"""The local web server: the viewer app plus the routes that mutate a run.

The read side (state, SSE, sequences, photos, pages) comes from the viewer
factory in ``viewer.py``; this module adds the one route that acts on the
run, ``POST /api/commands`` over the commands facade (viewer commands open,
admin commands localhost-only), the admin page, and starts uvicorn.
"""

import logging

from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, JSONResponse

from ..commands import ALL_COMMANDS, VIEWER_COMMANDS, Commands
from ..config import PipelineConfig
from ..events import EventLog
from ..state import PipelineState
from .pages import render_page
from .viewer import create_viewer_app, is_safe_name, serve_in_thread

logger = logging.getLogger(__name__)

_is_safe_name = is_safe_name  # kept for existing imports


def create_app(event_log: EventLog, state: PipelineState, config: PipelineConfig,
               commands: Commands, allowed_origins=()) -> FastAPI:
    """The viewer app for this run with the local command routes added."""
    share = None
    if config.share_url:
        share = {"url": config.share_url, "max_clients": config.share_max_clients}
    app = create_viewer_app(
        event_log, state, config.output_dir,
        config_summary=config.summary(),
        share=share,
        max_clients=config.share_max_clients,
        allowed_origins=allowed_origins,
    )
    _add_mutation_routes(app, commands)
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


def _actor_for(request: Request) -> str:
    """Who is acting, as the local server can tell: the operator at the
    laptop, or a LAN viewer identified by address."""
    client = request.client.host if request.client else None
    if client in _LOCAL_CLIENTS:
        return "operator"
    return f"viewer:{client or 'unknown'}"


def _add_mutation_routes(app: FastAPI, commands: Commands) -> None:

    @app.get("/admin")
    async def admin(request: Request):
        """Serve the admin page (localhost only)."""
        denial = _admin_denial(request.client.host if request.client else None,
                               request.headers.get("host"))
        if denial:
            return HTMLResponse(f"<h1>403</h1><p>{denial}.</p>", status_code=403)
        return HTMLResponse(render_page("admin.html", app.state.viewer["runtime"]))

    @app.post("/api/commands")
    async def post_command(request: Request):
        """Act on the run: ``{"command": name, ...args}``.

        Viewer commands (watch, unwatch) are open to anyone who can see the
        dashboard; the rest are admin (localhost + marker header). The
        response is the facade's outcome; a rejected command is a 400 with
        the reason (as ``error`` too, for the pages).
        """
        try:
            body = await request.json()
            if not isinstance(body, dict):
                raise ValueError
        except Exception:
            return JSONResponse(status_code=400, content={"error": "Invalid JSON"})
        command = str(body.pop("command", "") or "")
        if command not in ALL_COMMANDS:
            return JSONResponse(status_code=400, content={"error": f"Unknown command: {command}"})
        if command not in VIEWER_COMMANDS:
            deny = _check_admin(request, mutating=True)
            if deny:
                return deny
        body.pop("actor", None)  # the server knows who is asking
        command_id = body.pop("command_id", None)
        result = commands.dispatch(command, body, actor=_actor_for(request),
                                   command_id=str(command_id) if command_id else None)
        payload = result.to_dict()
        if not result.ok:
            payload["error"] = result.reason
            return JSONResponse(status_code=400, content=payload)
        return payload


def start_web_server(event_log: EventLog, state: PipelineState, config: PipelineConfig,
                     commands: Commands):
    """Start the web server in a background thread."""
    app = create_app(event_log, state, config, commands)
    serve_in_thread(app, config.web_host, config.web_port)
    logger.info(f"Web dashboard at http://{config.web_host}:{config.web_port}")
