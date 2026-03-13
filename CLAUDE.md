# specimux-suite

Orchestration and UX layer for the Mycomap fungal DNA barcoding pipeline.

## Quick reference

- **Install**: `pip install -e '.[dev]'`
- **Test**: `pytest tests/`
- **Run batch**: `specimux-suite batch <primers> <specimens> <reads.fastq> [--reference-db <refs.fasta>]`
- **Run live**: `specimux-suite live <primers> <specimens> <watch_dir> [--reference-db <refs.fasta>]`
- **Simulate**: `specimux-simulate <source.fastq> <output_dir> [--reads-per-file 4000] [--delay 30]`

## Architecture

Event-sourced pipeline: all state changes are recorded as JSONL events.
PipelineState is an in-memory materialized view rebuilt by replaying events.

Components: watcher → specimux runner → scheduler → speconsense runner → identify runner → web UX

All bioinformatics tools (specimux, speconsense, vsearch) are invoked as subprocesses.
adjusted-identity is the sole Python library dependency for scoring.

## Test data

- Unit tests: `test_data/` (synthetic)
- Integration: `~/mm/data/ont98/scale-test/all25k.fastq`
- Config: `~/mm/data/ont98/data/primers.fasta`, `~/mm/data/ont98/data/Index.txt`
- Reference DB: `~/mm/data/general/iNaturalist20250902.fasta`
