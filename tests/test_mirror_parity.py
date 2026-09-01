"""Mirror-parity harness: scheduler.py vs the production derived.js.

Replays captured event logs through PipelineState, then runs the SAME
specimen snapshots (via ``to_dict`` — so the test also proves the snapshot
carries every field the client logic needs) through ``derived.js`` under
node, and diffs the decisions: confidence band, reason token, and the
reprocess-eligibility gate at several ratios.

Design notes (deliberate, to keep this harness low-noise as we iterate):

- **Parity, not golden files.** Nothing here asserts a specific band for a
  specific specimen. An intentional logic change made on both sides passes
  untouched; only Python↔JS divergence fails. There is nothing to re-bless.
- **Decisions, not renderings.** Only semantic values are compared (band
  numbers, canonical reason tokens, eligibility booleans, ratios rounded
  to 9 dp). Display labels, ordering, and formatting are out of scope.
- **Regenerable fixtures.** ``tests/fixtures/parity/*.events.jsonl`` are
  produced by ``tests/tools/make_parity_fixture.py`` from full run logs —
  never hand-edited. Coverage of rare branches real runs don't hit (band
  1/2, gate early-outs) comes from synthetic specimens below instead of
  ever-larger fixtures.
- Skips cleanly when node isn't installed (node is a dev-only, optional
  test dependency; the shipped tool remains pure Python).

Slow lane: set SPECIMUX_PARITY_EVENTS to a colon-separated list of full
events.jsonl files (e.g. the multi-MB originals under
~/mm/data/specimux-suite-fixtures/) to run the same parity sweep over
entire runs.
"""

import json
import os
import shutil
import subprocess
from pathlib import Path

import pytest

from specimux_suite.events import _dict_to_event
from specimux_suite.scheduler import confidence_band, reprocess_assessment
from specimux_suite.state import (
    ClusterInfo,
    IdentificationMatch,
    PipelineState,
    SpecimenState,
    SpecimenStatus,
)

REPO = Path(__file__).resolve().parent.parent
DERIVED_JS = REPO / "src" / "specimux_suite" / "web" / "static" / "derived.js"
RUNNER = Path(__file__).parent / "parity_runner.js"
FIXTURE_DIR = Path(__file__).parent / "fixtures" / "parity"

# Gate behavior differs across these: default, tight (everything reprocesses),
# and loose (bands 1-3's half-gate is what lets specimens through).
REPROCESS_RATIOS = [0.5, 0.05, 2.0]

NODE = shutil.which("node")
pytestmark = pytest.mark.skipif(NODE is None, reason="node not installed")


def _fixture_paths():
    paths = sorted(FIXTURE_DIR.glob("*.events.jsonl"))
    extra = os.environ.get("SPECIMUX_PARITY_EVENTS", "")
    for p in extra.split(":"):
        if p.strip():
            paths.append(Path(p.strip()).expanduser())
    return paths


def _load_state(path: Path) -> PipelineState:
    state = PipelineState()
    with open(path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                state.apply(_dict_to_event(json.loads(line)))
    return state


def _python_decisions(state: PipelineState) -> dict:
    out = {}
    for sid, spec in state.specimens.items():
        band, reason = confidence_band(spec)
        assessments = []
        for r in REPROCESS_RATIOS:
            eligible, ratio, a_band, a_reason = reprocess_assessment(spec, r)
            assessments.append({
                "eligible": eligible,
                "ratio": "inf" if ratio == float("inf") else round(ratio, 9),
                "band": a_band,
                "reason": a_reason,
            })
        out[sid] = {"band": band, "reason": reason, "assessments": assessments}
    return out


def _js_decisions(specimen_dicts: dict) -> dict:
    payload = json.dumps({
        "derived_path": str(DERIVED_JS),
        "specimens": specimen_dicts,
        "reprocess_ratios": REPROCESS_RATIOS,
    })
    proc = subprocess.run(
        [NODE, str(RUNNER)], input=payload, capture_output=True,
        text=True, timeout=60,
    )
    assert proc.returncode == 0, f"parity_runner.js failed:\n{proc.stderr}"
    return json.loads(proc.stdout)


def _assert_parity(state: PipelineState, min_bands: int = 0):
    snapshot = state.to_dict()["specimens"]
    py = _python_decisions(state)
    js = _js_decisions(snapshot)
    assert set(py) == set(js)
    mismatches = [
        f"{sid}: python={py[sid]} js={js[sid]}"
        for sid in sorted(py) if py[sid] != js[sid]
    ]
    assert not mismatches, (
        f"{len(mismatches)} specimen(s) where scheduler.py and derived.js "
        "disagree:\n" + "\n".join(mismatches[:10])
    )
    # Fixture-health guard: a trim regression that produced trivial coverage
    # would make this test pass vacuously.
    bands = {v["band"] for v in py.values()}
    assert len(bands) >= min_bands, f"fixture only exercises bands {bands}"


@pytest.mark.parametrize("path", _fixture_paths(), ids=lambda p: p.name)
def test_replayed_run_parity(path):
    if not path.exists():
        pytest.skip(f"{path} not found")
    _assert_parity(_load_state(path), min_bands=2)


def _synthetic_state() -> PipelineState:
    """Branches real runs rarely hit: bands 1-2, gate early-outs, edge fields.

    Synthetic inputs keep this parity (both sides see the same input);
    they exist so rare branches don't depend on ever-larger fixtures.
    """
    state = PipelineState()
    specs = state.specimens

    def spec(sid, **kw):
        s = SpecimenState(specimen_id=sid, **kw)
        specs[sid] = s
        return s

    hit = lambda name, ident: {"ref_id": name, "name": name,
                               "identity": ident, "adjusted_identity": ident}

    # Band 1: consensus exists, nothing hit the reference DB
    spec("no_match", status=SpecimenStatus.NO_MATCH, total_reads=40,
         reads_at_last_consensus=20, consensus_version=1,
         clusters=[ClusterInfo(name="c1", size=30)],
         identification=[IdentificationMatch(cluster="c1", top_hits=[])])
    # Band 2: best hit below 90%
    spec("low_identity", status=SpecimenStatus.IDENTIFIED, total_reads=60,
         reads_at_last_consensus=40, consensus_version=1,
         community_taxon="Russula fake",
         clusters=[ClusterInfo(name="c1", size=50)],
         identification=[IdentificationMatch(
             cluster="c1", top_hits=[hit("Russula sp", 0.85)])])
    # Gate early-outs
    spec("never_processed", total_reads=100)
    spec("running", status=SpecimenStatus.CONSENSUS_RUNNING, total_reads=100,
         reads_at_last_consensus=50, consensus_version=1)
    spec("too_few_new", status=SpecimenStatus.IDENTIFIED, total_reads=54,
         reads_at_last_consensus=50, consensus_version=1,
         clusters=[ClusterInfo(name="c1", size=40)],
         identification=[IdentificationMatch(
             cluster="c1", top_hits=[hit("Amanita x", 0.99)])])
    # Infinite growth ratio: processed at 0 reads, reads appeared later
    spec("zero_base", status=SpecimenStatus.IDENTIFIED, total_reads=25,
         reads_at_last_consensus=0, consensus_version=1,
         clusters=[ClusterInfo(name="c1", size=10)],
         identification=[IdentificationMatch(
             cluster="c1", top_hits=[hit("Amanita x", 0.99)])])
    # No clusters after processing (no-consensus path)
    spec("no_clusters", status=SpecimenStatus.CONSENSUS_DONE, total_reads=80,
         reads_at_last_consensus=20, consensus_version=2)
    # Ambiguous bases + chimera on the dominant cluster (marginal path)
    spec("marginal_flags", status=SpecimenStatus.IDENTIFIED, total_reads=90,
         reads_at_last_consensus=30, consensus_version=1,
         community_taxon="Boletus edulis", community_genus="Boletus",
         clusters=[ClusterInfo(name="c1", size=70, ambig=2, chimera="v1+v2"),
                   ClusterInfo(name="c2", size=5)],
         identification=[
             IdentificationMatch(cluster="c1",
                                 top_hits=[hit("Boletus edulis", 0.995)]),
             IdentificationMatch(cluster="c2",
                                 top_hits=[hit("Trichoderma sp", 0.99)])])
    # Threshold pinning: one specimen just inside each identity band edge,
    # so a one-sided drift of the 0.90/0.98 constants can't hide between
    # fixture datapoints (verified: the 0.98 marginal check drifted to 0.97
    # undetected before these existed)
    for sid, ident in [("ident_0975", 0.975), ("ident_0985", 0.985),
                       ("ident_0895", 0.895), ("ident_0905", 0.905)]:
        spec(sid, status=SpecimenStatus.IDENTIFIED, total_reads=60,
             reads_at_last_consensus=30, consensus_version=1,
             community_taxon="Russula fake",
             clusters=[ClusterInfo(name="c1", size=50)],
             identification=[IdentificationMatch(
                 cluster="c1", top_hits=[hit("Russula sp", ident)])])
    # Minority on-target: community genus only in a small cluster
    spec("minority", status=SpecimenStatus.IDENTIFIED, total_reads=120,
         reads_at_last_consensus=40, consensus_version=1,
         community_taxon="Cortinarius sp",
         clusters=[ClusterInfo(name="c1", size=100),
                   ClusterInfo(name="c2", size=8)],
         identification=[
             IdentificationMatch(cluster="c1",
                                 top_hits=[hit("Hypomyces sp", 0.99)]),
             IdentificationMatch(cluster="c2",
                                 top_hits=[hit("Cortinarius fake", 0.99)])])
    return state


def test_synthetic_edge_parity():
    _assert_parity(_synthetic_state())
    # These branches are the reason the synthetic set exists — if a
    # refactor stops reaching them, the coverage silently evaporates.
    py = _python_decisions(_synthetic_state())
    assert py["no_match"]["band"] == 1
    assert py["low_identity"]["band"] == 2
    assert py["minority"]["reason"] == "minority_on_target"
    assert py["zero_base"]["assessments"][0]["ratio"] == "inf"
