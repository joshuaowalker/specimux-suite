# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Quick reference

- **Install**: `pip install -e '.[dev]'`
- **Test**: `pytest tests/`
- **Single test**: `pytest tests/test_state.py::test_read_totals -v`
- **Run batch**: `specimux-suite batch <primers> <specimens> <reads.fastq> [--reference-db <refs.fasta>]`
- **Run live**: `specimux-suite live <primers> <specimens> <watch_dir> [--reference-db <refs.fasta>]`
- **Replay**: `specimux-replay <source.fastq> <output_dir> [--reads-per-file 4000] [--delay 30]`

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

**Runners** are subprocess wrappers that follow a consistent pattern: emit `*.started` event → run external tool → parse output → emit `*.completed` event. All bioinformatics tools (specimux, speconsense, speconsense-summarize, vsearch) are invoked as subprocesses.

**Scheduler** has two-tier prioritization: never-processed specimens (by read count descending) take priority over reprocessing candidates. Specimens below `min_reads` are skipped. Watched specimens (starred in the dashboard) receive a priority boost and are processed first. Reprocessing candidates are ordered by confidence band (`confidence_band()` in `scheduler.py`, mirrored client-side as `reprocessBand()` in `index.html` — keep in sync): no_match > low_identity > off_target/minority_on_target > marginal/pending > confident. Uncertain bands (1–3) pass the eligibility gate at half of `reprocess_ratio`; confident results still require the full ratio and sort last. Reprocessing always requires ≥5 new reads (`MIN_NEW_READS_FOR_REPROCESS`) — the ratio gate alone thrashes on small denominators now that `min_reads` defaults to 10. Confidence never removes work — finalization (`get_all_eligible_jobs`, `min_reads=0`) processes everything with new reads.

**Summarization** runs after all consensus and identification is complete. Each specimen's consensus output is passed to speconsense-summarize, which extracts variant sequences. Variant identification is interleaved with summarization — as each specimen's summarize completes, its variants are immediately submitted for identification rather than waiting for all specimens to finish. After all summarize+identification work drains, an aggregate pass generates `summary.fasta`.

**Web server** runs FastAPI+uvicorn in a daemon thread. The `/events` SSE endpoint uses `EventLog.tail()` which blocks waiting for new events, enabling real-time dashboard updates. The single-page dashboard (`web/static/index.html`) has three tabs: Processing (raw cluster-level results), Summary (variant-level results), and Forecast (live stop estimator). All compute display status client-side from event data. The Forecast tab keeps per-file snapshots from `specimux.completed` events (`state.demuxHistory`) and forecasts each below-threshold specimen's crossing on the cumulative-matched-reads clock — per-specimen read share is stationary on that clock (validated on ont98 + run116; NOT stationary on input reads or wall time), with 90% Poisson intervals and a first-half/second-half share drift self-check.

**Profiles** bundle pipeline settings and tool configurations into reusable YAML presets. Suite profiles can reference tool-level profiles and set tool parameters. See `INTEGRATION.md` for the full profile contract.

## Event types

All events use dot notation. Key types: `pipeline.started`, `specimens.loaded`, `specimens.taxa`, `file.detected`, `file.stable`, `specimux.started`, `specimux.progress`, `specimux.completed`, `specimen.updated`, `specimen.watched`, `consensus.started`, `consensus.completed`, `identification.completed`, `summarize.started`, `summarize.completed`, `summarize.aggregate_completed`, `finalization.started`, `finalization.completed`, `pipeline.error`.

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

When adding a new event type, there are **four places** that must be updated:

1. **Emit the event** — call `event_log.emit("new.event", {...})` from the appropriate place (pipeline, runner, etc.)
2. **State handler** — add `_on_new_event` method to `PipelineState` in `state.py` and register it in the `_handlers` dict
3. **Dashboard `applyEvent()`** — add a `case` in the `switch` in `index.html` to apply the event to client-side state
4. **Dashboard SSE listener list** — add the event type string to the `for (const type of [...])` array in `connect()` (`index.html`). The `EventSource` only delivers named SSE events to explicitly registered listeners — missing this step silently drops the event.

## Key design decisions

- `scan_specimen_reads()` returns **cumulative** totals from the output directory, not deltas. State recomputes `total_matched_reads` from specimen totals to avoid double-counting.
- `_futures` dict (specimen_id → Future) is ground truth for in-flight work, since state may lag behind actual submissions.
- The dashboard computes "queued" status client-side using the same logic as the scheduler (min_reads threshold, reprocess_ratio).
- `adjusted-identity` library is used for homopolymer-aware scoring of vsearch hits.
- The Summary tab strictly shows variant-level identification results — no fallback to raw cluster identifications.
- iNaturalist community taxa are fetched asynchronously at startup and cached to `inat_taxon_cache.json`. On-target/off-target detection compares the top hit genus against the community taxon genus.
