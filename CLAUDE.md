# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Quick reference

- **Install**: `pip install -e '.[dev]'`
- **Test**: `pytest tests/`
- **Single test**: `pytest tests/test_state.py::test_read_totals -v`
- **Run batch**: `specimux-suite batch <primers> <specimens> <reads.fastq> [--reference-db <refs.fasta>]`
- **Run live**: `specimux-suite live <primers> <specimens> <watch_dir> [--reference-db <refs.fasta>]`
- **Replay**: `specimux-replay <source.fastq> <output_dir> [--reads-per-file 4000] [--delay 30]`
- **Serve a past run's dashboard** (no processing): `python tests/tools/serve_fixture.py <events.jsonl> [--port 8765]`

## Architecture

**Event-sourced pipeline.** All state changes are recorded as append-only JSONL events (`output_dir/events.jsonl` with automatic rotation at 100MB). `PipelineState` is a pure in-memory materialized view — it is never persisted to disk. The pipeline holds a single live state instance: history is replayed once at startup, then the instance stays current via an `EventLog` listener that applies each event as it is emitted. The scheduler, console, and web API all share that instance (never reassign it); web requests serve O(state) snapshots with no replay.

**Pipeline flow:**
```
watcher (live) or CLI (batch)
  → specimux runner (demux reads into per-specimen FASTQs)
    → scheduler (prioritize specimens for consensus)
      → speconsense runner (generate consensus sequences per specimen)
        → identify runner (vsearch + adjusted-identity scoring)
          → summarize runner (speconsense-summarize for variant extraction)
            → variant identification (re-identify using variant sequences)
              → aggregate (generate summary.fasta)
                → web dashboard (SSE-streamed events)
```

**Batch vs live:** Batch runs specimux once on a single FASTQ, then consensus with interleaved identification, then summarization with interleaved variant identification. Live mode watches a directory for new FASTQs; when one stabilizes, specimux runs immediately while in-flight consensus jobs continue — consensus jobs read copy-on-write snapshots (`output_dir/snapshots/`, via `clone_or_copy`) taken at submission time on the orchestrator thread, so specimux appending to live per-specimen FASTQs can never race them. New consensus submissions (and thus snapshots) pause during demux (`_draining`). Ctrl+C triggers finalization: drain remaining files, process all eligible specimens (ignoring reprocess_ratio), run summarization, and exit; a second Ctrl+C aborts.

**Restart recovery.** Stage transitions are driven by in-process callbacks, so a killed run strands specimens mid-pipeline; restarting on an existing event log heals all three classes (`tests/test_restart_recovery.py`): `PipelineState.rebuild` demotes phantom `CONSENSUS_RUNNING` specimens to the status their data implies (both scheduler paths skip "running", so a phantom would be stuck forever — normalization happens only at rebuild time, never during live apply); live startup runs `_submit_stranded_identifications()` (shared with batch rounds) for consensus-done specimens whose identification never ran; and the incremental summarize lane is seeded at startup with identified/no_match specimens whose summaries are missing or stale. Anything that replays a log for serving should use `state.rebuild()`, not a manual apply loop, to get the same healing. A fourth class is the demux itself: specimux appends to per-specimen FASTQs and `specimux.completed` is emitted afterwards, so a process killed in between would have its restart re-append every read. The runner writes a manifest of output-file lengths (`specimux-inflight.json`) before each demux and clears it after the completion event; `Pipeline.__init__` calls `SpecimuxRunner.recover_interrupted()` first thing, which truncates outputs back to the manifest and removes files the interrupted demux created (`tests/test_demux_recovery.py` kills a real process mid-demux and restarts).

**Runners** are subprocess wrappers that follow a consistent pattern: emit `*.started` event → run external tool → parse output → emit `*.completed` event. All bioinformatics tools (specimux, speconsense, speconsense-summarize, vsearch) are invoked as subprocesses.

**Scheduler** has two-tier prioritization: never-processed specimens (by read count descending) take priority over reprocessing candidates. Specimens below `min_reads` are skipped. Watched specimens (starred in the dashboard) receive a priority boost and are processed first. Reprocessing candidates are ordered by confidence band (`confidence_band()` in `scheduler.py`, mirrored client-side as `reprocessBand()` in `web/static/derived.js` — parity-tested, see "Shared decision logic" below): no_match > low_identity > off_target/minority_on_target > marginal/pending > confident. Uncertain bands (1–3) pass the eligibility gate at half of `reprocess_ratio`; confident results still require the full ratio and sort last. Reprocessing always requires ≥5 new reads (`MIN_NEW_READS_FOR_REPROCESS`) — the ratio gate alone thrashes on small denominators now that `min_reads` defaults to 10. Confidence never removes work — finalization (`get_all_eligible_jobs`, `min_reads=0`) processes everything with new reads.

**Summarization** is incremental by default: a dedicated serial lane (`_summarize_worker`) summarizes each specimen as soon as its identification lands, then submits variant identification (tracked in `_variant_futures`, separate from `_futures` so scheduler slot math is untouched), so the Summary tab fills during the run. `summarize.started/completed` events carry `consensus_version` and stale generations are dropped by state and both client mirrors — the same race guard as identification. The final round (after `_drain_identifications` + `_drain_incremental_summaries`) only processes stragglers: never-summarized specimens plus any whose `summarize_consensus_version` trails their `consensus_version`. `--no-incremental-summarize` reverts to summarize-at-end. The aggregate pass (`summary.fasta`) still runs only at the end.

**Web server** is two layers: the viewer app factory (`web/viewer.py`, `create_viewer_app(event_log, state, output_dir)`) builds the read side — `/api/state`, `/events` (SSE), `/api/specimens`, `/api/sequence/...`, `/photos/...`, the pages — from any event source with `tail()`/`version` and no pipeline (per-instance SSE broadcaster, so one process can host many runs; `load_run(path, heal=)` opens a log with or without interrupted-run healing; a sequence the log has announced but the disk lacks is a 503 + Retry-After, an unknown one a 404); `web/server.py` adds `POST /api/commands` and `/admin` on top and runs uvicorn in a daemon thread. The pages are served through `web/pages.py`, which injects a runtime config (`apiBase`, `assetBase`, `pageBase`, `tokenEndpoint`, `sessionEndpoint`) into the `specimux-runtime` JSON tag and fills the `{{asset_base}}`/`{{page_base}}` tokens; every API/asset/page reference in the pages goes through `static/runtime.js` (`SpecimuxRuntime.fetch/eventSource/apiUrl`), never a root-relative path (`tests/test_pages.py` lints this and runs the dashboard from a foreign origin under Playwright, with CORS from `allowed_origins`). The runtime also owns the session protocol for a hosted run API: token from the page's own origin, exchanged with a Bearer POST for a cookie, refreshed before expiry and on a 401. The `/events` SSE endpoint uses `EventLog.tail()` which blocks waiting for new events, enabling real-time dashboard updates. The single-page dashboard (`web/static/index.html`) has three tabs: Processing (raw cluster-level results), Summary (variant-level results), and Forecast (live stop estimator). All compute display status client-side from event data. The Forecast tab keeps per-file snapshots from `specimux.completed` events (`state.demuxHistory`) and forecasts each below-threshold specimen's crossing on the cumulative-matched-reads clock — per-specimen read share is stationary on that clock (validated on ont98 + run116; NOT stationary on input reads or wall time), with 90% Poisson intervals and a first-half/second-half share drift self-check.

**Plugins** (`plugins.py`) run alongside the pipeline: `start(context)` when a run begins (before `pipeline.started`), `shutdown()` in the run's `finally`; the `PluginContext` carries the event log, state, commands facade, config and output dir. Loaded by `--plugin NAME` (entry-point group `specimux_suite.plugins`, or a `module:factory` path) with a flat options dict from `--plugin-opt`. A plugin may register `EventLog` listeners and start threads, must never emit from a listener, and never touches run files. The suite's own plugin is the HTTP event forwarder (`forward.py`, `--forward-events URL`): a thread tails the log from the last acknowledged version and POSTs batches in order, at least once — the log is the buffer, the ack file (`forward-ack.json`) is the resume point, the receiver dedupes by version.

**Profiles** bundle pipeline settings and tool configurations into reusable YAML presets. Suite profiles can reference tool-level profiles and set tool parameters. See `INTEGRATION.md` for the full profile contract.

## Event types

All events use dot notation. Key types: `pipeline.started`, `specimens.loaded`, `specimens.taxa`, `taxa.lineage`, `inat.suggestions`, `inat.correction`, `inat.suggestion_dismissed`, `mo.unresolved`, `file.detected`, `file.stable`, `specimux.started`, `specimux.progress`, `specimux.completed`, `specimen.updated`, `specimen.watched`, `command.outcome`, `consensus.started`, `consensus.completed`, `identification.completed`, `summarize.started`, `summarize.completed`, `summarize.aggregate_completed`, `finalization.started`, `finalization.completed`, `pipeline.error`.

Specimen status transitions:
```
WAITING → CONSENSUS_RUNNING → CONSENSUS_DONE → IDENTIFIED → SUMMARIZED
                                              → NO_MATCH   → SUMMARIZED
                                              → ERROR
```

## Test data

- Unit tests: `test_data/` (synthetic)
- Integration: subset of `~/mm/data/ont98/data/filteredcalls.fastq` (e.g. `head -100000` for 25k reads)
- Config: `~/mm/data/ont98/data/primers.fasta`, `~/mm/data/ont98/data/Index.txt`
- Reference DB: `~/mm/data/general/mycomap_reference.fasta` (or the larger `iNaturalist20250902.fasta`)

## Adding a new event type

When adding a new event type, there are **four places** that must be updated (six if the highlights screen consumes it):

1. **Emit the event** — call `event_log.emit("new.event", {...})` from the appropriate place (pipeline, runner, etc.)
2. **State handler** — add `_on_new_event` method to `PipelineState` in `state.py` and register it in the `_handlers` dict
3. **Dashboard `applyEvent()`** — add a `case` in the `switch` in `index.html` to apply the event to client-side state
4. **Dashboard SSE listener list** — add the event type string to the `for (const type of [...])` array in `connect()` (`index.html`). The `EventSource` only delivers named SSE events to explicitly registered listeners — missing this step silently drops the event.
5. **(if relevant) `/present` `applyEvent()`** — `present.html` keeps its own state mirror with the same switch pattern
6. **(if relevant) `/present` SSE listener list** — same silent-drop gotcha as the dashboard

`admin.html` (`/admin`) keeps a third mirror with the same two spots for events it consumes.

Clients bootstrap from `/api/state` and subscribe SSE at the snapshot version, so `applyEvent` never sees historical events — any state a new event builds must also ride the snapshot (`PipelineState.to_dict`).

`tests/test_event_mirror_lint.py` enforces the applyEvent-case ⊆ SSE-listener-list invariant on all three pages, so forgetting step 4/6 now fails the suite instead of silently dropping events.

## Shared decision logic (derived.js) and the mirror-parity harness

All client-side *decision* logic — top-match selection, NS/LQ/chimera routing, taxonomy agreement, effective target status, and the scheduler mirrors — lives in one place: `web/static/derived.js` (plain script, UMD-style: pages get `window.SpecimuxDerived`, node can `require()` it). Pages bind their state via one-line adapters only; `tests/test_event_mirror_lint.py` fails if a page re-inlines a shared function body.

The remaining cross-language pair — `scheduler.py` (`confidence_band`, `reprocess_assessment`) ↔ `derived.js` (`reprocessBand`, `reprocessAssessment`) — is guarded by `tests/test_mirror_parity.py`: it replays captured event logs through `PipelineState`, feeds the `to_dict()` snapshots to the production derived.js under node, and diffs the decisions. **Parity, not golden files**: an intentional change made on both sides passes with no test churn; only one-sided drift fails. Reason tokens are canonical scheduler strings on both sides (pages map them to display labels, e.g. `REPROC_REASON_LABELS` in index.html).

- Fixtures: `tests/fixtures/parity/*.events.jsonl`, generated (never hand-edited) by `python tests/tools/make_parity_fixture.py <full-events.jsonl> <out.jsonl>` from full run logs (originals at `~/mm/data/specimux-suite-fixtures/`).
- Rare branches (bands 1–2, gate early-outs, threshold edges) are pinned by synthetic specimens in the test itself — extend those rather than growing fixtures.
- Slow lane: `SPECIMUX_PARITY_EVENTS=<full1>:<full2> pytest tests/test_mirror_parity.py` sweeps entire runs.
- node is an optional dev dependency; parity tests skip when absent (the shipped tool stays pure Python).
- When changing decision logic: edit `derived.js` **and** the scheduler functions, keeping reason tokens and thresholds identical — parity tells you if you missed one. Add a threshold-edge synthetic when you add a threshold.

## Key design decisions

- `scan_specimen_reads()` returns **cumulative** totals from the output directory, not deltas. State recomputes `total_matched_reads` from specimen totals to avoid double-counting.
- `_futures` dict (specimen_id → Future) is ground truth for in-flight work, since state may lag behind actual submissions.
- The dashboard computes "queued" status client-side using the same logic as the scheduler (min_reads threshold, reprocess_ratio).
- `adjusted-identity` library is used for homopolymer-aware scoring of vsearch hits.
- The Summary tab strictly shows variant-level identification results — no fallback to raw cluster identifications.
- iNaturalist data is fetched at startup **blocking by default** (`Pipeline.prefetch_inat`): field IDs, ID audit, and restart lineages complete with progress bars (`progress.py`, pure stdlib) before the web server starts and the browser opens, so the UIs come up fully populated. Photos are the exception — every page falls back to the iNat photo URL on a cache miss, so the photo cache warms on a background thread (small worker pool against the S3/CDN hosts; the cache exists for flaky venue Wi-Fi, not startup correctness). `--inat-background` restores fetch-while-running for everything; Ctrl+C during the prefetch skips the rest and falls back to the background path. Everything is cached per output dir (`inat_taxon_cache.json`, …), so a restart's prefetch is near-instant. On-target/off-target detection compares the top hit genus against the community taxon genus.
- User actions are commands, commands become events, and admin is localhost-only. Every way a user acts on a run (`watch`, `unwatch`, `correct`, `dismiss`, `rescan`, `finalize`, `abort`) is a method on the commands facade (`commands.py`, `pipeline.commands`); the web route `POST /api/commands`, tests, and plugins call it — nothing else emits action events, and nothing touches state or files directly; the pipeline reacts via `EventLog` listeners. Every mutation event carries `actor` and `command_id`, and every non-duplicate command closes with a `command.outcome` event (applied / rejected + reason / noop), so the log is the audit trail and a redelivered command id is a silent noop (seeded from the log at open). Viewer commands (watch/unwatch) are open to anyone who can see the dashboard; admin commands and `/admin` are gated to localhost clients with a Host-header check (DNS rebinding) and a required `X-Specimux-Admin` header on POSTs (CSRF via forced preflight) — see `_admin_denial` in `server.py`. There is no TLS, so never add a password over the wire; if second-device admin is ever needed, mint a one-time token at the laptop. A reverse proxy/tunnel would make every request look local — don't expose the port that way.
- Mushroom Observer is a second field-ID provider (`mo.py`), deliberately narrower than iNat: `MO<digits>` specimen tags resolve through MO's observations endpoint only, into the same `specimens.taxa` record shape (plus `provider: "mo"`), so state and the page mirrors are provider-agnostic. No MO typo audit or corrections (all of MO is Fungi; MO volume per run is low) — ids MO reports nonexistent ride a `mo.unresolved` event to a plain list on `/admin`. Entries whose genus→lineage lookup failed transiently are served but not cached, so the next run completes them. Higher-rank taxonomy stays iNat's: the MO consensus genus is mapped through `fetch_genus_lineages`, so `ancestors` are iNat taxon ids and `agreementRank` is unchanged; a first token iNat says is not a genus (MO files provisional names like "Boletaceae sp. 'AL01'" at rank species) is resolved at any rank (`rank=None`, cached under `any:` keys) and treated like an iNat family-level community taxon — no genus, lineage kept. MO tags need ≥4 digits so provisional-name state codes ("fasciculare-MO01") aren't read as ids. API gotchas: a batch with any nonexistent id fails whole (the fetcher parses the id out of the error and retries), and anonymous traffic is limited to 20 requests/minute (between batches wait the longer of 5 s or the last `run_time`; MO photo downloads are serialized at ~2/s in `photos.py` since the docs don't say whether the image host counts). Pages derive links/labels from the tag via `observationRef` in `derived.js` — never regex the specimen id inline. MO photo ids are prefixed `mo` so they can't collide in the shared photo cache; MO photo records carry `large_url` explicitly (iNat's is derived by size substitution).
- The reference-DB contract is deliberately minimal — a FASTA with `name="..."` headers — so users can mint their own references. Never consume other header fields (e.g. `sintax_*`). Taxonomy for a hit comes from its name's first token (the genus) resolved against iNat taxonomy (`fetch_genus_lineages`, cached in `inat_lineage_cache.json`, emitted as `taxa.lineage` events); taxonomy-level field-ID agreement (`agreementRank` in `derived.js`) compares the observation's `ancestor_ids` against the genus lineage. Display-only — scheduler confidence banding stays genus-based.
