# Changelog

Notable changes to specimux-suite. The format loosely follows
[Keep a Changelog](https://keepachangelog.com/); versions follow
[semantic versioning](https://semver.org/) within the 0.x caveat that
minor releases may change APIs and formats.

## Unreleased

### Mushroom Observer field IDs

- Specimen IDs tagged `MO<digits>` now resolve against Mushroom Observer
  the way `iNat<digits>` ones resolve against iNaturalist: consensus name
  and genus for on/off-target detection, observation photos with
  attribution, observer credits, and the first naming as the field ID.
  Runs may mix both providers. Dashboard and highlights links go to the
  right site.
- Higher-rank taxonomy stays iNaturalist's: the MO consensus is mapped
  onto iNat taxonomy at genus level, so taxonomy-level agreement works
  unchanged. Provisional names filed above genus (e.g. "Boletaceae sp.
  'AL01'") resolve at their actual rank and behave like an iNat
  family-level ID. MO specimens are excluded from the observation-ID
  typo audit by design (all of MO is Fungi); ids MO reports as nonexistent
  are listed on `/admin` instead. Startup shows the MO fetch as two
  progress steps (observations, then mapping genera onto iNat taxonomy).

## 0.2.0 — 2026-09-01

The audience release: a projector-ready highlights screen, iNaturalist
integration deep enough to catch field-label mistakes, and a pipeline
that picks up exactly where it left off.

### Highlights screen (`/present`)

- New full-screen carousel designed for a projector at a live event:
  identification cards with full-bleed iNat photos, a drifting consensus
  sequence ribbon, field-ID journeys, and observer credits.
- Programming, not queueing: each slot draws from weighted channels
  (news / encores / big picture / people) so bursts of results never
  crowd out variety. News is perishable; only strong novelty interrupts.
- Card types: identifications with novelty tiers (with a similarity
  gauge and careful wording — an unmatched sequence is not a claimed new
  species), burst roll-ups with photo thumbnails when results land
  faster than the carousel can honor individually, run milestones,
  family spotlights with photo mosaics, a run-by-family chart,
  "Many eyes see clearly" refinement journeys (first ID → community ID,
  DNA-confirmed), and two correction callouts — "Surprise!" (confident
  DNA far from the field ID) and "Hmmm…" (observation filed under a
  non-fungal kingdom, likely a label mix-up).
- Novel, surprise, and mix-up cards carry distinct visual flags (colored
  top rule, ambient wash, filled eyebrow chip) so they read from across
  a room. Strong novelty earns encores at growing intervals, ending in a
  "Mystery solved" card if more reads resolve it.
- Multi-photo cycling on identification cards; operator keys (space to
  pause, → to advance, f for fullscreen).

### iNaturalist integration

- Community taxa now include photos, observers, taxon ancestries, and
  each observation's first identification — the true field ID.
- Taxonomy-level agreement: when the sequence and field ID differ by
  name, their deepest shared rank is resolved via iNat taxonomy —
  genus-or-deeper counts as agreement (✓), a shared family or tribe
  shows as ≈, and the on/off-target filters always match the indicator.
- iNat ID typo audit: observation IDs that resolve to non-fungal taxa
  (or nothing) are checked against single-digit-edit candidates, ranked
  by observer-in-run and sequence-genus evidence, and written to
  `summary/inat_id_suggestions.tsv` for review before MycoMap upload.
  Inspired by Alan Rockefeller's
  [inat.finder.py](https://github.com/AlanRockefeller/inat.finder.py).
- New localhost-only admin page (`/admin`) to accept or dismiss
  suggested corrections. An accepted correction heals the running
  pipeline live — field ID, photos, agreement, and the final outputs —
  and survives restarts.
- Startup now blocks on the iNat fetches (field IDs, audit, lineages)
  with per-stage progress bars, opening the dashboard only when it can
  render fully populated; `--inat-background` restores fetch-while-
  running. Photos warm a local cache in the background either way (the
  UIs fall back to iNat URLs), several times faster than before.

### Dashboard

- Top-match selection prefers clean clusters over NS/LQ/chimera-flagged
  ones, and picks the best identity among on-target hits rather than the
  largest cluster.
- Summarization results stream in during the run (see below), and
  header links lead to the highlights screen and admin page.
- Identity warnings and scheduler confidence align at the new
  thresholds: red ⚠ / "low identity" below 95% (was 90%), amber below
  98%.

### Pipeline

- Incremental summarization: each specimen is summarized as soon as its
  identification lands (dedicated serial lane), so the Summary tab fills
  during the run instead of at the end. `--no-incremental-summarize`
  restores the old behavior; the aggregate `summary.fasta` still runs at
  the end.
- Restart recovery: restarting on an existing output directory resumes
  stranded work — specimens killed mid-consensus are rescheduled instead
  of stuck as "processing" forever, interrupted identifications are
  resubmitted, and unsummarized specimens flow through the incremental
  lane.
- The dashboard's SSE fan-out moved to a single broadcaster thread, so
  additional viewers cost approximately nothing (validated at 30
  clients); cached photos are served with immutable cache headers.

### Internal

- Client-side decision logic (top-match ranking, confidence banding,
  taxonomy agreement, cluster routing) is single-sourced in
  `web/static/derived.js`, with a node-based parity harness that replays
  real event logs through both the Python scheduler and the production
  JS to catch one-sided drift, plus structural lints on the event
  mirrors.

## 0.1.2 — 2026-08-20

- Finalization now identifies and summarizes specimens whose consensus
  was in flight when it started, and heals output directories with
  previously stranded specimens.

## 0.1.1 — 2026-08-14

- Lowered the default `--min-reads` to 10, with a 5-new-read floor on
  reprocessing so small specimens don't re-run on every file.
- Fixed `name="..."` parsing for both legacy and mm-to-ref reference
  database headers.
- Quieter idle-scheduler logging.

## 0.1.0 — 2026-08-13

- Initial release: batch and live pipelines (specimux → speconsense →
  vsearch identification → speconsense-summarize), event-sourced state,
  real-time web dashboard with Processing/Summary/Forecast tabs,
  confidence-ordered reprocess scheduling, LAN sharing with QR code, and
  `specimux-replay` for simulated runs.
