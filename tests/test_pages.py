"""The pages as served: runtime config injection and the foreign-origin test.

A host that proxies the dashboard onto its own origin points the pages at
a run API elsewhere through the injected runtime (``web/pages.py``). Two
things keep that honest:

- a source lint: no page may reference the API, an asset or another page
  by a root-relative URL — everything goes through the runtime;
- a browser test: the dashboard served from one origin, with its runtime
  pointing at a viewer app on another, loads state, streams events and
  posts a command there, and follows the session protocol when a token
  endpoint is configured. Needs Playwright's Chromium; skipped without.
"""

import json
import re
import socket
import threading
import time
from http.server import BaseHTTPRequestHandler, HTTPServer, ThreadingHTTPServer
from pathlib import Path

import httpx
import pytest

from specimux_suite.commands import Commands
from specimux_suite.config import PipelineConfig
from specimux_suite.events import EventLog
from specimux_suite.web.pages import DEFAULT_RUNTIME, STATIC_DIR, inject_runtime, render_page
from specimux_suite.web.server import create_app
from specimux_suite.web.viewer import load_run, serve_in_thread

PAGES = ["index.html", "present.html", "admin.html"]


# --- injection ---

def test_render_defaults_point_at_own_origin():
    html = render_page("index.html")
    assert "{{asset_base}}" not in html and "{{page_base}}" not in html
    assert 'src="/static/runtime.js"' in html
    assert 'href="/present"' in html
    m = re.search(r'<script id="specimux-runtime" type="application/json">(.*?)</script>', html)
    assert json.loads(m.group(1)) == DEFAULT_RUNTIME


def test_render_with_a_foreign_runtime():
    rt = {"apiBase": "https://runs.example/v1/runs/abc", "assetBase": "/runs/ui/1.2.3",
          "pageBase": "/runs/abc", "tokenEndpoint": "/api/runs/abc/token",
          "sessionEndpoint": "https://runs.example/v1/session"}
    html = render_page("index.html", rt)
    assert 'src="/runs/ui/1.2.3/static/derived.js"' in html
    assert 'href="/runs/abc/present"' in html
    m = re.search(r'<script id="specimux-runtime" type="application/json">(.*?)</script>', html)
    assert json.loads(m.group(1)) == rt


def test_inject_escapes_script_terminator_and_requires_tag():
    html = '<script id="specimux-runtime" type="application/json">{}</script>'
    out = inject_runtime(html, {"apiBase": "</script><script>alert(1)"})
    assert "</script><script>alert" not in out
    assert json.loads(re.search(r">(\{.*\})<", out).group(1))["apiBase"] == "</script><script>alert(1)"
    with pytest.raises(ValueError):
        inject_runtime("<html></html>", {})


@pytest.mark.parametrize("page", PAGES)
def test_no_root_relative_urls_in_pages(page):
    """Every API/asset/page reference must go through the runtime."""
    text = (STATIC_DIR / page).read_text(encoding="utf-8")
    offenders = []
    for m in re.finditer(r"""[`'"=]/(api|events|static|photos|present|admin)\b""", text):
        before = text[max(0, m.start() - 40):m.start()]
        if "SpecimuxRuntime." in before or before.endswith("{{asset_base}}") or before.endswith("{{page_base}}"):
            continue
        offenders.append(text[max(0, m.start() - 30):m.end() + 20].replace("\n", " "))
    assert not offenders, f"{page}: root-relative URLs bypass the runtime: {offenders}"
    assert 'id="specimux-runtime"' in text
    assert "{{asset_base}}/static/runtime.js" in text


# --- the foreign-origin browser test ---

def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def _wait_up(url, timeout=10.0):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            httpx.get(url, timeout=1.0)
            return
        except httpx.HTTPError:
            time.sleep(0.05)
    raise RuntimeError(f"{url} did not come up")


class _HostServer:
    """The page host: serves the dashboard with a runtime pointing at the
    API origin, plus a token endpoint and a fake session endpoint that
    record what the page sent."""

    def __init__(self, port: int, api_origin: str, with_session: bool):
        self.port = port
        self.origin = f"http://127.0.0.1:{self.port}"
        self.calls: list[tuple[str, str, dict]] = []
        runtime = {"apiBase": api_origin, "assetBase": api_origin, "pageBase": ""}
        if with_session:
            runtime["tokenEndpoint"] = "/token"
            runtime["sessionEndpoint"] = f"{self.origin}/session"
        self.html = render_page("index.html", runtime).encode()
        host = self

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *a):  # quiet
                pass

            def _send(self, code, body, ctype="application/json"):
                self.send_response(code)
                self.send_header("Content-Type", ctype)
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def do_GET(self):
                host.calls.append(("GET", self.path, dict(self.headers)))
                if self.path == "/":
                    self._send(200, host.html, "text/html; charset=utf-8")
                elif self.path == "/token":
                    self._send(200, json.dumps({"token": "tok-123", "expires_in": 600}).encode())
                else:
                    self._send(404, b"{}")

            def do_POST(self):
                host.calls.append(("POST", self.path, dict(self.headers)))
                if self.path == "/session":
                    # 200 with a body: Chromium aborts a bodiless 204 from
                    # this HTTP/1.0 stub, which would look like a page bug
                    self._send(200, b"{}")
                else:
                    self._send(404, b"{}")

        self.server = HTTPServer(("127.0.0.1", self.port), Handler)
        threading.Thread(target=self.server.serve_forever, daemon=True).start()


@pytest.fixture
def browser():
    try:
        from playwright.sync_api import sync_playwright
    except ImportError:
        pytest.skip("playwright not installed")
    with sync_playwright() as p:
        try:
            b = p.chromium.launch()
        except Exception as e:  # browser binaries missing
            pytest.skip(f"chromium unavailable: {e}")
        yield b
        b.close()


def _api(tmp_path, host_origin):
    log = EventLog(tmp_path / "events.jsonl")
    log.emit("pipeline.started", {"mode": "batch", "config_summary": {"min_reads": 10}})
    log.emit("specimux.completed", {"specimens": {"S1": 40, "S2": 12}})
    event_log, state = load_run(tmp_path / "events.jsonl")
    config = PipelineConfig(primers_file=tmp_path / "p", specimens_file=tmp_path / "s",
                            output_dir=tmp_path)
    app = create_app(event_log, state, config, Commands(event_log, state),
                     allowed_origins=[host_origin])
    port = _free_port()
    serve_in_thread(app, "127.0.0.1", port)
    origin = f"http://127.0.0.1:{port}"
    _wait_up(origin + "/api/viewers")
    return origin, event_log, state


@pytest.mark.parametrize("with_session", [False, True])
def test_dashboard_served_from_a_foreign_origin(tmp_path, browser, with_session):
    # Host and API must know each other's origin: allocate the host port
    # first, allow it on the API, then start the host pointed at the API.
    host_port = _free_port()
    api_origin, event_log, state = _api(tmp_path, f"http://127.0.0.1:{host_port}")
    host = _HostServer(host_port, api_origin, with_session)

    page = browser.new_page()
    api_requests, failures, errors = [], [], []
    page.on("request", lambda r: api_requests.append(r.url) if r.url.startswith(api_origin) else None)
    page.on("requestfailed", lambda r: failures.append((r.url, r.failure)))
    page.on("pageerror", lambda e: errors.append(str(e)))
    page.on("console", lambda m: errors.append(m.text) if m.type == "error" else None)

    page.goto(host.origin + "/")
    try:
        page.wait_for_function("typeof state !== 'undefined' && state.version > 0", timeout=10000)
    except Exception:
        raise AssertionError(f"dashboard never loaded state; errors={errors} failures={failures} "
                             f"api_requests={api_requests} host_calls={[(m, p) for m, p, _ in host.calls]}")
    assert page.evaluate("Object.keys(state.specimens).sort()") == ["S1", "S2"]

    # Live event over SSE from the API origin
    event_log.emit("specimen.watched", {"specimen_id": "S1", "watched": True})
    page.wait_for_function("state.specimens.S1 && state.specimens.S1.watched === true", timeout=10000)

    # A command posted cross-origin (JSON body => CORS preflight) lands on the API
    page.evaluate("postCommand('unwatch', { specimen_id: 'S1' })")
    deadline = time.monotonic() + 10
    while state.specimens["S1"].watched and time.monotonic() < deadline:
        time.sleep(0.05)
    assert not state.specimens["S1"].watched
    page.wait_for_function("state.specimens.S1.watched === false", timeout=10000)

    assert any(u.startswith(api_origin + "/api/state") for u in api_requests)
    assert any(u.startswith(api_origin + "/events") for u in api_requests)
    assert any(u.startswith(api_origin + "/static/derived.js") for u in api_requests)
    assert any(u.startswith(api_origin + "/api/commands") for u in api_requests)
    assert not failures, failures
    assert not errors, errors

    host_paths = [(m, p) for m, p, _ in host.calls]
    if with_session:
        # token from the page's own origin, then the exchange with the bearer,
        # both before the first API call
        assert host_paths[:3] == [("GET", "/"), ("GET", "/token"), ("POST", "/session")]
        session_headers = [h for m, p, h in host.calls if p == "/session"][0]
        assert session_headers.get("Authorization") == "Bearer tok-123"
    else:
        assert host_paths == [("GET", "/")]
    page.close()


# --- the fragment handoff (a run API serving its own pages) ---

class _SessionApi:
    """A stand-in run API on one origin that serves the page, requires a
    session cookie for the API, exchanges a bearer token for that cookie,
    and keeps an SSE connection open; plus, on a second origin, a host's
    authorize route that bounces back with a token in the fragment."""

    def __init__(self, port: int, host_port: int, snapshot: dict):
        self.origin = f"http://127.0.0.1:{port}"
        self.host_origin = f"http://127.0.0.1:{host_port}"
        self.calls: list[tuple[str, str, dict]] = []
        self.snapshot = snapshot
        runtime = {"apiBase": self.origin, "assetBase": self.origin, "pageBase": "",
                   "tokenEndpoint": f"{self.host_origin}/authorize",
                   "sessionEndpoint": f"{self.origin}/session"}
        self.html = render_page("index.html", runtime).encode()
        api = self

        class Api(BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.1"

            def log_message(self, *a):
                pass

            def _send(self, code, body, ctype="application/json", extra=()):
                self.send_response(code)
                self.send_header("Content-Type", ctype)
                self.send_header("Content-Length", str(len(body)))
                for k, v in extra:
                    self.send_header(k, v)
                self.end_headers()
                self.wfile.write(body)

            def _session(self):
                return "specimux_session=ok" in (self.headers.get("Cookie") or "")

            def do_GET(self):
                api.calls.append(("GET", self.path, dict(self.headers)))
                path = self.path.split("?")[0]
                if path == "/":
                    self._send(200, api.html, "text/html; charset=utf-8")
                elif path.startswith("/static/"):
                    f = STATIC_DIR / path[len("/static/"):]
                    self._send(200, f.read_bytes(), "text/javascript" if f.suffix == ".js" else "text/css")
                elif not self._session():
                    self._send(401, b'{"error": "A run session is required"}')
                elif path == "/api/state":
                    self._send(200, json.dumps(api.snapshot).encode())
                elif path == "/api/viewers":
                    self._send(200, b'{"sse_clients": 0}')
                elif path == "/events":
                    self.send_response(200)
                    self.send_header("Content-Type", "text/event-stream")
                    self.send_header("Cache-Control", "no-cache")
                    self.end_headers()
                    self.wfile.write(b": open\n\n")
                    self.wfile.flush()
                    time.sleep(30)   # hold the stream open for the test's life
                else:
                    self._send(404, b"{}")

            def do_POST(self):
                api.calls.append(("POST", self.path, dict(self.headers)))
                if self.path == "/session":
                    auth = self.headers.get("Authorization") or ""
                    if auth == "Bearer tok-good":
                        self._send(200, b'{"expires_in": 43200}',
                                   extra=[("Set-Cookie", "specimux_session=ok; Path=/; HttpOnly; SameSite=Lax")])
                    else:
                        self._send(401, b'{"error": "bad token"}')
                else:
                    self._send(404, b"{}")

        class Host(BaseHTTPRequestHandler):
            def log_message(self, *a):
                pass

            def do_GET(self):
                api.calls.append(("GET", "host:" + self.path, dict(self.headers)))
                from urllib.parse import parse_qs, urlsplit
                q = parse_qs(urlsplit(self.path).query)
                back = q.get("return", [""])[0]
                self.send_response(302)
                self.send_header("Location", back + "#token=tok-good")
                self.send_header("Content-Length", "0")
                self.end_headers()

        self.api_server = ThreadingHTTPServer(("127.0.0.1", port), Api)
        self.api_server.daemon_threads = True
        self.host_server = HTTPServer(("127.0.0.1", host_port), Host)
        threading.Thread(target=self.api_server.serve_forever, daemon=True).start()
        threading.Thread(target=self.host_server.serve_forever, daemon=True).start()


def _snapshot(tmp_path) -> dict:
    log = EventLog(tmp_path / "events.jsonl")
    log.emit("pipeline.started", {"mode": "batch", "config_summary": {"min_reads": 10}})
    log.emit("specimux.completed", {"specimens": {"S1": 40, "S2": 12}})
    _, state = load_run(tmp_path / "events.jsonl")
    return state.to_dict()


@pytest.mark.parametrize("entry", ["bounce", "fragment", "stale-fragment"])
def test_cross_origin_token_endpoint_is_a_bounce_with_a_fragment_token(tmp_path, browser, entry):
    """The page opened without a session navigates to the host's authorize
    URL with a return parameter, comes back with #token=, exchanges it for
    the cookie, strips the fragment, and loads. Opened with a fragment
    token already (a link from the host), it exchanges without leaving. A
    stale fragment token is exchanged, refused, and then the bounce."""
    api = _SessionApi(_free_port(), _free_port(), _snapshot(tmp_path))
    page = browser.new_page()
    errors = []
    page.on("pageerror", lambda e: errors.append(str(e)))
    # the 401s that drive the bounce are logged by the browser as resource errors
    page.on("console", lambda m: errors.append(m.text) if m.type == "error" and "401" not in m.text else None)

    start = {"bounce": api.origin + "/", "fragment": api.origin + "/#token=tok-good",
             "stale-fragment": api.origin + "/#token=tok-old"}[entry]
    page.goto(start)
    try:
        page.wait_for_function("typeof state !== 'undefined' && state.version > 0", timeout=10000)
    except Exception:
        raise AssertionError(f"dashboard never loaded; errors={errors} "
                             f"calls={[(m, p) for m, p, _ in api.calls]}")
    assert page.evaluate("Object.keys(state.specimens).sort()") == ["S1", "S2"]
    assert page.url == api.origin + "/", "the token must not stay in the address bar"
    assert not errors, errors

    paths = [(m, p.split("?")[0]) for m, p, _ in api.calls]
    session_auths = [h.get("Authorization") for m, p, h in api.calls if p == "/session"]
    authorize_calls = [(p, h) for m, p, h in api.calls if p.startswith("host:/authorize")]
    if entry == "bounce":
        # api/state without a session → 401 → probe → authorize → back with a token
        assert paths.index(("GET", "/api/state")) < paths.index(("GET", "host:/authorize"))
        assert len(authorize_calls) == 1
        from urllib.parse import parse_qs, urlsplit
        assert parse_qs(urlsplit(authorize_calls[0][0][len("host:"):]).query)["return"] == [api.origin + "/"]
        assert session_auths == ["Bearer tok-good"]
    elif entry == "fragment":
        assert authorize_calls == []
        assert session_auths == ["Bearer tok-good"]
    else:
        assert session_auths == ["Bearer tok-old", "Bearer tok-good"]
        assert len(authorize_calls) == 1
    # the API was only ever answered with the cookie after the exchange
    assert ("GET", "/events") in paths
    page.close()
