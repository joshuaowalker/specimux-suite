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

**Event-sourced pipeline.** All state changes are recorded as append-only JSONL events (`output_dir/events.jsonl` with automatic rotation at 100MB). `PipelineState` is a pure in-memory materialized view rebuilt by replaying all events — it is never persisted to disk. State rebuild is the single source of truth; the web API rebuilds on every request for freshness.

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

**Batch vs live:** Batch runs specimux once on a single FASTQ, then consensus with interleaved identification, then summarization with interleaved variant identification. Live mode watches a directory for new FASTQs; when one stabilizes, it *drains* all in-flight consensus jobs, runs specimux with all cores, then resumes scheduling. This drain-run-resume pattern is central to live mode correctness. Ctrl+C triggers finalization: drain remaining files, process all eligible specimens (ignoring reprocess_ratio), run summarization, and exit.

**Runners** are subprocess wrappers that follow a consistent pattern: emit `*.started` event → run external tool → parse output → emit `*.completed` event. All bioinformatics tools (specimux, speconsense, speconsense-summarize, vsearch) are invoked as subprocesses.

**Scheduler** has two-tier prioritization: never-processed specimens (by read count descending) take priority over reprocessing candidates (which require `new_reads / previous_reads > reprocess_ratio`). Specimens below `min_reads` are skipped. Watched specimens (starred in the dashboard) receive a priority boost and are processed first.

**Summarization** runs after all consensus and identification is complete. Each specimen's consensus output is passed to speconsense-summarize, which extracts variant sequences. Variant identification is interleaved with summarization — as each specimen's summarize completes, its variants are immediately submitted for identification rather than waiting for all specimens to finish. After all summarize+identification work drains, an aggregate pass generates `summary.fasta`.

**Web server** runs FastAPI+uvicorn in a daemon thread. The `/events` SSE endpoint uses `EventLog.tail()` which blocks waiting for new events, enabling real-time dashboard updates. The single-page dashboard (`web/static/index.html`) has two tabs: Processing (raw cluster-level results) and Summary (variant-level results). Both compute display status client-side from event data.

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
- Integration: `~/mm/data/ont98/scale-test/all25k.fastq`
- Config: `~/mm/data/ont98/data/primers.fasta`, `~/mm/data/ont98/data/Index.txt`
- Reference DB: `~/mm/data/general/iNaturalist20250902.fasta`

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
