"""The dashboard pages as served: static HTML from package data."""

import re
from pathlib import Path

STATIC_DIR = Path(__file__).parent / "static"

# Specimen and sequence names as they appear in FASTA headers/filenames.
SAFE_NAME_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]*$")


def render_page(name: str) -> str:
    """The HTML for one of the shipped pages (``index.html``, ...)."""
    page = STATIC_DIR / name
    if not page.exists():
        return f"<h1>{name} not found</h1><p>Static files not found.</p>"
    return page.read_text(encoding="utf-8")
