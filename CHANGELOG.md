# Changelog

Notable changes to specimux-suite. The format loosely follows
[Keep a Changelog](https://keepachangelog.com/); versions follow
[semantic versioning](https://semver.org/) within the 0.x caveat that
minor releases may change APIs and formats.

## 0.3.7 — 2026-09-25

- **Host status on the dashboard.** `create_viewer_app` takes an optional
  `status` callable: a host (such as a hosted service) reports a run's
  progress before the pipeline has started, for example an upload or
  basecalling, as `{"text", "progress"}`. It is served in `/api/state`
  and the dashboard's viewer poll, and the dashboard shows it as a banner
  with a progress bar, reloading once when it goes away. Nothing changes
  when no status is given.
- **`summary/` holds only speconsense-summarize's output**, so it can be
  zipped as the MycoMap summary package as is. The per-specimen
  `*-variants-combined.fasta` (the input to identifying a specimen's
  variants) now lives in `.staging/`, and the iNat ID audit files
  (`inat_id_suggestions.tsv`, `inat_id_corrections.tsv`) are written to the
  output directory. Restarting a run from an earlier version moves its
  audit files and removes the old combined FASTAs.
- Requires speconsense 0.8.8, whose `--aggregate-only` writes
  `quality_report.txt`, so an incrementally summarized run's `summary/`
  now has the same quality report as a single speconsense-summarize run.

## 0.3.6 — 2026-09-24

- **A run without a reference database is summarized.** Summarizing was
  chained behind identification, so without a reference (nothing
  identifies) specimens stayed "consensus built", the summary step found
  none eligible and the run ended with an empty summary directory and no
  `summary.fasta`. Summarizing needs no reference: without one a specimen
  is now summarized straight after consensus (live mode as it completes,
  batch mode in the final round). Runs with a reference are unchanged.
- GitHub Actions moved to their Node 24 releases.

## 0.3.5 — 2026-09-24

- **Specimens with no reads say so.** A specimen that demultiplexing gave
  no reads used to show "queued" for ever in batch mode (and after a live
  run's finalization), and counted in the Queued total. Once no more reads
  can arrive — batch's demux succeeded, or live finalization completed —
  it shows **no reads** and leaves the count. The run state carries
  `demux_finished` for the pages.
- **`--no-photo-cache`.** The run doesn't download observation photos into
  the output dir, and the highlights screen loads them from iNaturalist /
  Mushroom Observer directly (the setting rides `config_summary`). For a
  dashboard served on the internet: the cache is there for a flaky venue
  network, and hosted runs were each storing about 0.5 GB of photos.

## 0.3.4 — 2026-09-23

A live-mode fix.

- **Live mode no longer demultiplexes a file twice when the watch
  directory is reached through a symlink.** On macOS `/tmp` and `/var` are
  links into `/private`: the filesystem events named a new file by its
  real path and the watcher's directory scan by the given one, the two
  counted as different files, and the file's reads could be appended to
  the specimen FASTQs twice (it depended on which saw the file first). The
  watcher now knows a file by its real path and claims it in one step.

## 0.3.3 — 2026-09-23

A faster end of run, for everyone. Publishing each specimen's summary
pruned its stale files by examining every file in the summary directory,
which grows to tens of thousands of files over a large run; by the end
each publish cost most of a second of Python, serialized across the
summarize threads. On a million reads the summaries after consensus took
about 13 minutes with the CPU mostly idle. The prune now looks only at
files named for the specimen: the same run's summaries after consensus
took 77 seconds, and the whole run went from 32 to 20 minutes.

## 0.3.2 — 2026-09-22

For hosted runs on local disk, found in the first full-scale cloud run:
on a network filesystem the demux ran about 200 reads per second with
the CPU idle, against 39,000 on local disk.

- `--mirror-dir DIR` copies what a dashboard reads into DIR as it is
  published: the event log, each specimen's consensus FASTA, the summary
  FASTAs and the photo cache. The run itself works in the output dir, so
  a hosted engine can keep that on local disk and serve the dashboard
  from shared storage. Each file is replaced atomically and lands before
  the event that announces it.
- `--event-log PATH` sets where the event log is written (by default the
  output dir, or the mirror dir when there is one).

Local use is unchanged.

## 0.3.1 — 2026-09-21

Two fixes found while putting the hosted dashboard in front of a real
browser.

- Bundled profiles load under any suite version. The bundled `default`
  and `herbarium` profiles still pinned 0.1.x, so `--profile default`
  refused to load on 0.2.1 and 0.3.0 with "Profile 'default' requires
  specimux-suite version 0.1.*". Bundled profiles ship with the
  installed version, so their pin is no longer checked; user profiles
  keep the check.
- The pages' runtime handles a cross-origin token endpoint: the page
  navigates to the host's authorize URL with a return parameter and
  comes back with the run token in the URL fragment, which it exchanges
  for the session cookie and strips from history. A same-origin token
  endpoint that answers 401 takes the same route, through the host's
  login. Only a hosted run API uses this; local runs are unaffected.

## 0.3.0 — 2026-09-21

The extension release: the pieces a hosting service needs to run the
pipeline elsewhere and show the same dashboard, plus two restart-safety
fixes found while designing it. Local use is unchanged apart from the
new options.

### Extension interface

- The dashboard's read side is now a library: `create_viewer_app` in
  `web/viewer.py` builds the state, SSE, specimen, sequence, photo and
  page routes from any event source, with no pipeline attached, so one
  process can host many runs. `load_run` opens a past log with or
  without interrupted-run healing; `tests/tools/serve_fixture.py` uses
  it. A sequence the log has announced but the disk does not yet hold
  answers 503 with Retry-After instead of 404.
- Every user action on a run (`watch`, `unwatch`, `correct`, `dismiss`,
  `rescan`, `finalize`, `abort`) is a method on a commands facade
  (`commands.py`) and one route, `POST /api/commands`, replaces the
  per-action routes (`/api/watch/...`, `/api/admin/inat/...`). Mutation
  events carry `actor` and `command_id`; every command closes with a
  `command.outcome` event (applied, rejected with reason, or noop), and a
  redelivered command id is a silent noop, so the event log is the audit
  trail. Viewer commands stay open, admin commands stay localhost-only.
- Plugins: `--plugin NAME` (entry-point group `specimux_suite.plugins`
  or a `module:factory` path) loads objects that run alongside the
  pipeline with the event log, state, commands facade and config;
  `--plugin-opt KEY=VALUE` passes options.
- Event forwarding: `--forward-events URL` mirrors the run's events to
  an HTTP endpoint in version order, at least once, resuming from the
  last acknowledged version after a restart (`forward-ack.json`);
  `--forward-header` adds an authorization header.
- Pages take an injected runtime config (API, asset and page bases,
  token and session endpoints) through `static/runtime.js`, so the
  dashboard can be served under a prefix or from another origin; the
  viewer accepts an allowed-origins list for CORS. A Playwright test
  runs the dashboard from a foreign origin.

### Restart safety

- Demux commit boundary: a run killed after specimux appended reads but
  before `specimux.completed` used to re-append every read on restart.
  The runner now records output-file lengths in `specimux-inflight.json`
  before each demux and startup rolls an interrupted demux back to them.
- Atomic publication: speconsense and speconsense-summarize rewrite the
  files the dashboard serves in place; they now write to `.staging/`
  and are published per file atomically, so a reader sees a whole old
  or whole new generation, never a partial one. A failed tool leaves the
  published generation untouched.

### Other

- The User-Agent for outbound API calls carries the project URL.
- Dev extras declare the HTTP clients the web tests need (`httpx`, and
  `httpx2` for starlette 1.x).

## 0.2.1 — 2026-09-19

Mushroom Observer joins iNaturalist as a field-ID source.

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
