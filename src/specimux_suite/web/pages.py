"""The dashboard pages as served: package HTML with a runtime config injected.

The pages are templates in two small ways, and this module is the whole
contract for whoever serves them (the local server here, or a host that
proxies them onto its own origin):

1. ``{{asset_base}}`` and ``{{page_base}}`` tokens in the markup are
   replaced with the base URLs for ``/static/...`` assets and for the
   other pages (``/present``, ``/admin``, ``/``). Locally both are empty.
2. The ``<script id="specimux-runtime" type="application/json">`` tag's
   content is replaced with the runtime config the pages read at load
   (``static/runtime.js``): ``apiBase`` (where ``/api/...``, ``/events``
   and ``/photos/...`` live), ``assetBase``, ``pageBase``, and, when the
   API needs a session, ``tokenEndpoint`` and ``sessionEndpoint`` (the
   API's token exchange, ``POST`` with a Bearer run token, which sets the
   session cookie). A same-origin token endpoint is fetched as JSON
   (``{"token", "expires_in"}``); a cross-origin one is the host's
   authorize URL, which the page navigates to with a ``return`` query
   parameter and which comes back with ``#token=...`` in the fragment
   (see ``static/runtime.js``). Locally the endpoints are null.

Every API and asset reference in the pages goes through one of these;
``tests/test_pages.py`` fails on a root-relative URL that does not.
"""

import json
import re
from pathlib import Path

STATIC_DIR = Path(__file__).parent / "static"

# Specimen and sequence names as they appear in FASTA headers/filenames.
SAFE_NAME_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]*$")

RUNTIME_TAG_RE = re.compile(
    r'(<script id="specimux-runtime" type="application/json">)(.*?)(</script>)',
    re.DOTALL,
)

DEFAULT_RUNTIME = {
    "apiBase": "",
    "assetBase": "",
    "pageBase": "",
    "tokenEndpoint": None,
    "sessionEndpoint": None,
}


def inject_runtime(html: str, runtime: dict | None = None) -> str:
    """Fill a page template's tokens and runtime tag."""
    rt = {**DEFAULT_RUNTIME, **(runtime or {})}
    html = html.replace("{{asset_base}}", rt["assetBase"] or "")
    html = html.replace("{{page_base}}", rt["pageBase"] or "")
    # "</" inside the JSON would end the script tag early
    payload = json.dumps(rt).replace("</", "<\\/")
    html, n = RUNTIME_TAG_RE.subn(lambda m: m.group(1) + payload + m.group(3), html, count=1)
    if n == 0:
        raise ValueError("page has no specimux-runtime tag")
    return html


def render_page(name: str, runtime: dict | None = None) -> str:
    """The HTML for one of the shipped pages (``index.html``, ...)."""
    page = STATIC_DIR / name
    if not page.exists():
        return f"<h1>{name} not found</h1><p>Static files not found.</p>"
    return inject_runtime(page.read_text(encoding="utf-8"), runtime)
