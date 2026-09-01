"""Structural lints on the web pages' event mirrors.

Two invariants that have each bitten us as silent failures:

1. **Every ``applyEvent`` case must be in that page's SSE listener list.**
   ``EventSource`` only delivers named events to explicitly registered
   listeners, so a case without a listener is dead code that looks alive —
   the page just silently never updates for that event type.

2. **Decision logic lives in derived.js, nowhere else.** Pages may define
   a function with a shared-logic name only as a one-line delegation to
   ``SpecimuxDerived`` (binding page state); re-inlining the logic would
   recreate the drift the parity harness exists to prevent.

These are regex-level checks on the page source — no browser, no node —
so they are cheap enough to run on every pytest invocation. They assert
structure, not content: adding an event type or changing a function's
behavior (in derived.js) never breaks them.
"""

import re
from pathlib import Path

import pytest

STATIC = Path(__file__).resolve().parent.parent / "src" / "specimux_suite" / "web" / "static"
PAGES = ["index.html", "present.html", "admin.html"]

# Function names whose logic must live only in derived.js. A page-level
# `function <name>(...)` is allowed only if its body immediately delegates.
SHARED_NAMES = [
    "reprocessBand", "reprocessAssessment", "isReprocessCandidate",
    "agreementRank", "effectiveTargetStatus", "getTopMatch", "findTopMatch",
    "_findTopMatch", "clusterFilterRouting", "clusterFlagged",
    "getTargetStatus", "getActiveMatches", "hasIdentification",
    "isHitOnTarget", "hitGenusLower",
]


def _applyevent_cases(text: str, page: str) -> set[str]:
    m = re.search(r"function applyEvent\b.*?\n\}", text, re.DOTALL)
    assert m, f"{page}: applyEvent not found"
    cases = set(re.findall(r"case '([\w.]+)':", m.group(0)))
    assert len(cases) >= 3, f"{page}: suspiciously few applyEvent cases {cases}"
    return cases


def _sse_listeners(text: str, page: str) -> set[str]:
    m = re.search(r"for \(const type of \[(.*?)\]\)", text, re.DOTALL)
    assert m, f"{page}: SSE listener registration loop not found"
    return set(re.findall(r"'([\w.]+)'", m.group(1)))


@pytest.mark.parametrize("page", PAGES)
def test_every_applyevent_case_has_sse_listener(page):
    text = (STATIC / page).read_text(encoding="utf-8")
    cases = _applyevent_cases(text, page)
    listeners = _sse_listeners(text, page)
    missing = cases - listeners
    assert not missing, (
        f"{page}: applyEvent handles {sorted(missing)} but the SSE listener "
        "list doesn't register them — EventSource will silently drop these "
        "events (see 'Adding a new event type' in CLAUDE.md)"
    )


@pytest.mark.parametrize("page", PAGES)
def test_shared_logic_only_delegates(page):
    text = (STATIC / page).read_text(encoding="utf-8")
    offenders = []
    for name in SHARED_NAMES:
        for m in re.finditer(rf"function {re.escape(name)}\s*\(", text):
            body = text[m.start():m.start() + 250]
            if "SpecimuxDerived" not in body:
                offenders.append(name)
    assert not offenders, (
        f"{page}: {offenders} define shared decision logic inline — move the "
        "logic to derived.js and delegate (const {{...}} = SpecimuxDerived or "
        "a one-line wrapper), or the parity harness can't guard it"
    )
    if "SpecimuxDerived" in text:
        assert 'src="/static/derived.js"' in text, (
            f"{page}: uses SpecimuxDerived but doesn't load /static/derived.js"
        )


def test_derived_js_defines_each_shared_function_once():
    text = (STATIC / "derived.js").read_text(encoding="utf-8")
    for name in ["reprocessBand", "reprocessAssessment", "agreementRank",
                 "effectiveTargetStatus", "getTopMatch", "findTopMatch",
                 "clusterFilterRouting", "getTargetStatus", "getActiveMatches",
                 "hasIdentification", "isHitOnTarget"]:
        defs = re.findall(rf"function {name}\s*\(", text)
        assert len(defs) == 1, f"derived.js defines {name} {len(defs)} times"
