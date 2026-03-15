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
          → web dashboard (SSE-streamed events)
```

**Batch vs live:** Batch runs specimux once on a single FASTQ, then consensus, then identification. Live mode watches a directory for new FASTQs; when one stabilizes, it *drains* all in-flight consensus jobs, runs specimux with all cores, then resumes scheduling. This drain-run-resume pattern is central to live mode correctness.

**Runners** are subprocess wrappers that follow a consistent pattern: emit `*.started` event → run external tool → parse output → emit `*.completed` event. All bioinformatics tools (specimux, speconsense, vsearch) are invoked as subprocesses.

**Scheduler** has two-tier prioritization: never-processed specimens (by read count descending) take priority over reprocessing candidates (which require `new_reads / previous_reads > reprocess_ratio`). Specimens below `min_reads` are skipped.

**Web server** runs FastAPI+uvicorn in a daemon thread. The `/events` SSE endpoint uses `EventLog.tail()` which blocks waiting for new events, enabling real-time dashboard updates. The single-page dashboard (`web/static/index.html`) computes display status client-side from event data.

## Event types

All events use dot notation. Key types: `pipeline.started`, `specimens.loaded`, `file.detected`, `file.stable`, `specimux.started`, `specimux.completed`, `specimen.updated`, `consensus.started`, `consensus.completed`, `identification.completed`, `pipeline.error`.

Specimen status transitions: `WAITING → CONSENSUS_RUNNING → CONSENSUS_DONE → IDENTIFIED | NO_MATCH | ERROR`

## Test data

- Unit tests: `test_data/` (synthetic)
- Integration: `~/mm/data/ont98/scale-test/all25k.fastq`
- Config: `~/mm/data/ont98/data/primers.fasta`, `~/mm/data/ont98/data/Index.txt`
- Reference DB: `~/mm/data/general/iNaturalist20250902.fasta`

## Key design decisions

- `scan_specimen_reads()` returns **cumulative** totals from the output directory, not deltas. State recomputes `total_matched_reads` from specimen totals to avoid double-counting.
- `_futures` dict (specimen_id → Future) is ground truth for in-flight work, since state may lag behind actual submissions.
- The dashboard computes "queued" status client-side using the same logic as the scheduler (min_reads threshold, reprocess_ratio).
- `adjusted-identity` library is used for homopolymer-aware scoring of vsearch hits.
