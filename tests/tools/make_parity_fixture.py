#!/usr/bin/env python3
"""Trim a full events.jsonl into a small parity-test fixture.

Selects a deterministic subset of specimens that covers the decision-logic
branches the mirror-parity tests exercise (every confidence band, flagged
clusters, multi-version reprocesses, stale in-flight events, iNat
corrections, variants, taxonomy-agreement lineage walks), then filters the
event stream down to just those specimens. Fixtures are always regenerated
with this tool, never hand-edited:

    python tests/tools/make_parity_fixture.py \
        ~/mm/data/specimux-suite-fixtures/ont98-replay-with-corrections.events.jsonl \
        tests/fixtures/parity/ont98-corrections-mini.events.jsonl

Determinism: selection is sorted-order based, so the same source log always
yields the same fixture.
"""

import argparse
import json
import sys
from collections import defaultdict
from pathlib import Path

from specimux_suite.events import _dict_to_event
from specimux_suite.scheduler import confidence_band
from specimux_suite.state import PipelineState


def _iter_lines(path: Path):
    with open(path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                yield json.loads(line)


def _hit_genera(match: dict):
    for hit in match.get("top_hits", []):
        name = hit.get("name") or hit.get("ref_id") or ""
        if name.split():
            yield name.split()[0].lower()


def categorize(source: Path):
    """Replay the log; return (state, {category: [sid, ...]})."""
    state = PipelineState()
    # Track consensus generation ourselves so we can spot events that arrive
    # for a superseded generation (the staleness-guard branch).
    cv: dict[str, int] = defaultdict(int)
    stale_sids: set[str] = set()
    corrected_sids: set[str] = set()

    for raw in _iter_lines(source):
        data = raw.get("data", {})
        etype = raw.get("type", "")
        sid = data.get("specimen_id")
        if etype == "consensus.completed" and sid:
            cv[sid] += 1
        elif etype in ("identification.completed", "summarize.completed") and sid:
            ecv = data.get("consensus_version")
            if ecv is not None and ecv != cv[sid]:
                stale_sids.add(sid)
        elif etype == "inat.correction" and sid:
            corrected_sids.add(sid)
        state.apply(_dict_to_event(raw))

    cats: dict[str, list[str]] = defaultdict(list)
    for sid in sorted(state.specimens):
        spec = state.specimens[sid]
        band, _ = confidence_band(spec)
        cats[f"band_{band}"].append(sid)
        if spec.consensus_version >= 2:
            cats["multi_version"].append(sid)
        if spec.variants:
            cats["variants"].append(sid)
        if not spec.community_taxon:
            cats["no_field_id"].append(sid)
        for c in spec.clusters:
            if c.chimera or (c.err_factor is not None and c.err_factor > 1.5) \
                    or (c.cer_factor is not None and c.cer_factor < 1.0):
                cats["flagged_cluster"].append(sid)
                break
        # Lineage-walk candidates: has a field ID with ancestors, but the
        # hits are a different genus (exercises agreementRank's deep path)
        if spec.community_taxon and spec.inat_ancestors:
            community = (spec.community_genus
                         or spec.community_taxon.split()[0]).lower()
            hit_genera = {g for m in [
                {"top_hits": m.top_hits} for m in spec.identification
            ] for g in _hit_genera(m)}
            if hit_genera and community not in hit_genera:
                cats["off_genus_with_ancestors"].append(sid)
    for sid in sorted(stale_sids):
        cats["stale_event"].append(sid)
    for sid in sorted(corrected_sids):
        cats["corrected"].append(sid)
    return state, cats


def select_sids(cats: dict, per_category: int) -> set[str]:
    keep: set[str] = set()
    for cat in sorted(cats):
        # Prefer sids not already covered, so categories don't collapse
        # onto the same few specimens
        fresh = [s for s in cats[cat] if s not in keep]
        chosen = (fresh + [s for s in cats[cat] if s in keep])[:per_category]
        keep.update(chosen)
    return keep


def trim_events(source: Path, keep: set[str], genera: set[str]):
    for raw in _iter_lines(source):
        etype = raw.get("type", "")
        data = raw.get("data", {})
        if etype == "specimux.progress":
            continue  # bulky, no specimen state
        sid = data.get("specimen_id")
        if sid is not None and sid not in keep:
            continue
        if etype == "specimens.loaded":
            data["specimens"] = [s for s in data.get("specimens", [])
                                 if s.get("specimen_id") in keep]
        elif etype == "specimens.taxa":
            data["taxa"] = {k: v for k, v in data.get("taxa", {}).items()
                            if k in keep}
        elif etype == "taxa.lineage":
            data["lineages"] = {k: v for k, v in data.get("lineages", {}).items()
                                if k.lower() in genera}
        elif etype == "specimux.completed":
            data["specimens"] = {k: v for k, v in data.get("specimens", {}).items()
                                 if k in keep}
        elif etype == "inat.suggestions":
            data["suggestions"] = [r for r in data.get("suggestions", [])
                                   if r.get("specimen_id") in keep]
        yield raw


def needed_genera(source: Path, keep: set[str]) -> set[str]:
    genera: set[str] = set()
    for raw in _iter_lines(source):
        data = raw.get("data", {})
        etype = raw.get("type", "")
        if etype == "identification.completed" and data.get("specimen_id") in keep:
            for m in data.get("matches", []):
                genera.update(_hit_genera(m))
        elif etype == "specimens.taxa":
            for sid, taxon in data.get("taxa", {}).items():
                if sid not in keep:
                    continue
                if isinstance(taxon, dict):
                    name = taxon.get("genus") or taxon.get("name") or ""
                else:
                    name = taxon or ""
                if name.split():
                    genera.add(name.split()[0].lower())
    return genera


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("source", type=Path)
    ap.add_argument("output", type=Path)
    ap.add_argument("--per-category", type=int, default=3)
    args = ap.parse_args()

    _, cats = categorize(args.source)
    keep = select_sids(cats, args.per_category)
    genera = needed_genera(args.source, keep)

    args.output.parent.mkdir(parents=True, exist_ok=True)
    n = 0
    with open(args.output, "w", encoding="utf-8") as f:
        for raw in trim_events(args.source, keep, genera):
            f.write(json.dumps(raw) + "\n")
            n += 1

    print(f"kept {len(keep)} specimens, {n} events -> {args.output} "
          f"({args.output.stat().st_size / 1024:.0f} KB)")
    for cat in sorted(cats):
        picked = [s for s in cats[cat] if s in keep]
        print(f"  {cat}: {len(cats[cat])} candidates, {len(picked)} kept")


if __name__ == "__main__":
    sys.exit(main())
