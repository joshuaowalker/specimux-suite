# specimux-suite

Orchestration and real-time dashboard for the Mycomap fungal DNA barcoding pipeline. Manages the full workflow from raw nanopore reads through demultiplexing, consensus generation, and species identification.

## Installation

Requires Python 3.11+.

```bash
pip install -e '.[dev]'
```

The pipeline invokes external bioinformatics tools as subprocesses — **specimux**, **speconsense**, and **vsearch** must be installed and available on your `PATH`.

## Quick start

### Batch mode

Process a single FASTQ file end-to-end:

```bash
specimux-suite batch primers.fasta specimens.tsv reads.fastq \
    --reference-db references.fasta
```

### Live mode

Watch a directory for new FASTQ files (e.g., from a running MinION sequencer) and process them as they appear:

```bash
specimux-suite live primers.fasta specimens.tsv /path/to/minknow/output/ \
    --reference-db references.fasta
```

A web dashboard opens automatically at `http://127.0.0.1:8077` showing real-time progress.

## Input files

**Primers** — FASTA file containing primer sequences used for demultiplexing.

**Specimens** — Tab-separated file with at least `SampleID` and `PrimerPool` columns:

```
SampleID	PrimerPool
spec001	pool1
spec002	pool1
iNat12345	pool2
```

**Reads** — Standard FASTQ format (batch mode expects a single file; live mode watches a directory for `*.fastq` files).

**Reference database** — Optional FASTA file of reference sequences for species identification via vsearch.

## Options

### Common options

| Option | Default | Description |
|---|---|---|
| `-o, --output-dir` | `specimux-suite-output` | Output directory |
| `--reference-db` | — | Reference FASTA for identification |
| `--min-reads` | `30` | Minimum reads before running consensus |
| `--reprocess-ratio` | `0.5` | Ratio of new/previous reads to trigger reprocessing |
| `--workers` | half of CPU cores | Number of worker threads |
| `--specimux-args` | — | Extra arguments passed through to specimux |
| `--speconsense-args` | — | Extra arguments passed through to speconsense |
| `--log-level` | `INFO` | Logging verbosity (`DEBUG`, `INFO`, `WARNING`, `ERROR`) |

### Web dashboard options

| Option | Default | Description |
|---|---|---|
| `--web-host` | `127.0.0.1` | Dashboard listen address |
| `--web-port` | `8077` | Dashboard listen port |
| `--share [N]` | — | Share dashboard on LAN (optional max client limit, default 20) |
| `--no-web` | — | Disable the web dashboard |
| `--no-open` | — | Don't auto-open dashboard in browser |

### Live mode options

| Option | Default | Description |
|---|---|---|
| `--settle-time` | `30` | Seconds to wait for a file to stabilize before processing |
| `--watch-pattern` | `*.fastq` | Glob pattern for files to watch |
| `--presample` | `100` | Reads to subsample for incremental consensus (0 = unlimited) |

## Web dashboard

The built-in dashboard provides a real-time view of pipeline progress, updated via server-sent events (SSE).

Features:
- Specimen table with status, identity score, top identification match, and read count
- Color-coded identity scores and status indicators
- Search and sort across all columns
- Expandable cluster-level identification details
- Pipeline statistics (total reads, matched reads, specimen counts by status)
- QR code for sharing the dashboard URL on your local network (with `--share`)

## Replay

For testing or demos, `specimux-replay` splits a source FASTQ into timed chunks that mimic MinKNOW output:

```bash
specimux-replay source.fastq simulated_output/ --reads-per-file 4000 --delay 30
```

Files are written atomically with MinKNOW-style filenames. Pair with live mode to replay a sequencing run:

```bash
# Terminal 1: start the pipeline
specimux-suite live primers.fasta specimens.tsv simulated_output/ --reference-db refs.fasta

# Terminal 2: replay sequencer output
specimux-replay source.fastq simulated_output/
```

## Output

The output directory contains:

```
output_dir/
├── events.jsonl                    # Append-only event log
├── specimux/full/{pool}/
│   └── {specimen_id}.fastq         # Demultiplexed reads per specimen
├── consensus/{specimen_id}/
│   └── clusters.fasta              # Consensus sequences
└── identification/
    └── {specimen_id}.tsv           # Identification results
```

The event log (`events.jsonl`) records every state change and is the single source of truth for pipeline state. It rotates automatically at 100 MB.

## Development

```bash
# Install with dev dependencies
pip install -e '.[dev]'

# Run tests
pytest tests/

# Run a single test
pytest tests/test_state.py::test_read_totals -v
```
