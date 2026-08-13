# specimux-suite

> **Early preview release.** This project is under active development — APIs, event formats, and CLI options may change between versions. Feedback and bug reports are welcome at [github.com/joshuaowalker/specimux-suite/issues](https://github.com/joshuaowalker/specimux-suite/issues).

Orchestration and real-time dashboard for the [Mycomap](https://mycomap.org) fungal DNA barcoding pipeline. Manages the full workflow from raw nanopore reads through demultiplexing, consensus generation, variant summarization, and species identification.

## Installation

Requires Python 3.11+.

```bash
pip install -e '.[dev]'
```

The pipeline invokes external bioinformatics tools as subprocesses — **specimux**, **speconsense**, and **vsearch** must be installed and available on your `PATH`.

| Tool | Purpose | Required |
|---|---|---|
| [specimux](https://github.com/joshuaowalker/specimux) | Demultiplexing reads by primer pool and specimen | Yes |
| [speconsense](https://github.com/joshuaowalker/speconsense) | Consensus sequence generation and variant summarization | Yes |
| [vsearch](https://github.com/torognes/vsearch) | Reference database matching for species identification | Only if `--reference-db` is provided |

See each tool's repository for installation instructions. These tools have system-level dependencies that pip cannot install:

| Dependency | Required by | Install |
|---|---|---|
| [SPOA](https://github.com/rvaser/spoa) | speconsense | `conda install bioconda::spoa` |
| [MCL](https://micans.org/mcl/) | speconsense (optional, recommended) | `conda install bioconda::mcl` |
| [vsearch](https://github.com/torognes/vsearch) | speconsense (scalability mode), specimux-suite (identification) | `conda install bioconda::vsearch` |

## Quick start

### Batch mode

Process a single FASTQ file end-to-end:

```bash
specimux-suite batch primers.fasta specimens.tsv reads.fastq \
    --reference-db references.fasta
```

### Live mode

Watch a directory for new FASTQ files (e.g., from a running MinION sequencer) and process them incrementally as they appear:

```bash
specimux-suite live primers.fasta specimens.tsv /path/to/minknow/output/ \
    --reference-db references.fasta
```

A web dashboard opens automatically at `http://127.0.0.1:8077` showing real-time progress. Press Ctrl+C to finalize — the pipeline will drain remaining files, process all eligible specimens regardless of threshold, and run summarization before exiting.

### Profiles

Profiles bundle pipeline settings and tool configurations into reusable presets:

```bash
# List available profiles
specimux-suite batch --list-profiles

# Use a profile
specimux-suite batch -p herbarium primers.fasta specimens.tsv reads.fastq
```

Bundled profiles include `default` (standard settings) and `herbarium` (relaxed thresholds for degraded DNA). Custom profiles can be placed in `~/.config/specimux-suite/profiles/`.

## Input files

**Primers** — FASTA file containing primer sequences used for demultiplexing.

**Specimens** — Tab-separated file with at least `SampleID` and `PrimerPool` columns. Specimen IDs containing an iNaturalist observation ID (e.g., `iNat12345`) enable automatic community taxon lookup for on-target/off-target detection.

```
SampleID	PrimerPool
spec001	pool1
spec002	pool1
specimen-B--iNat12345	pool2
```

**Reads** — Standard FASTQ format (batch mode expects a single file; live mode watches a directory for `*.fastq` files).

**Reference database** — Optional FASTA file of reference sequences for species identification via vsearch. The sequence ID (first whitespace-delimited token) is used as the match key. An optional `name="..."` field in the header provides a display name; without it, the name is derived from the ID by replacing underscores with spaces (e.g., `Genus_species_authority` becomes "Genus species").

```
>MycoMap_12345_Trametes_versicolor_US_Indiana name="Trametes versicolor"
ACGTACGT...
>MycoMap_67890_Stereum_ostrea_US_Ohio
TGCATGCA...
```

## Options

### Common options

| Option | Default | Description |
|---|---|---|
| `-p, --profile` | — | Load a suite profile preset |
| `--list-profiles` | — | List available profiles and exit |
| `-o, --output-dir` | `specimux-suite-output` | Output directory |
| `--reference-db` | — | Reference FASTA for identification |
| `--min-reads` | `30` | Minimum reads before running consensus |
| `--reprocess-ratio` | `0.5` | Ratio of new/previous reads to trigger reprocessing |
| `--workers` | half of CPU cores | Number of worker threads |
| `--identify-min-coverage` | `0.5` | Minimum query/target coverage for identification hits |
| `--specimux-args` | — | Extra arguments passed through to specimux |
| `--speconsense-args` | — | Extra arguments passed through to speconsense |
| `--summarize-args` | — | Extra arguments passed through to speconsense-summarize |
| `--log-level` | `INFO` | Logging verbosity (`DEBUG`, `INFO`, `WARNING`, `ERROR`) |

### Web dashboard options

| Option | Default | Description |
|---|---|---|
| `--web-host` | `127.0.0.1` | Dashboard listen address |
| `--web-port` | `8077` | Dashboard listen port |
| `--share [N]` | — | Share dashboard on LAN with QR code (optional max client limit, default 20) |
| `--no-web` | — | Disable the web dashboard |
| `--no-open` | — | Don't auto-open dashboard in browser |

### Live mode options

| Option | Default | Description |
|---|---|---|
| `--settle-time` | `30` | Seconds to wait for a file to stabilize before processing |
| `--presample` | `100` | Reads to subsample for incremental consensus (0 = unlimited) |

## Pipeline

### Processing stages

```
FASTQ reads
  → specimux (demultiplex into per-specimen FASTQs)
    → speconsense (generate consensus sequences per specimen)
      → vsearch + adjusted-identity (identify species from reference DB)
        → speconsense-summarize (extract and identify variant sequences)
```

### Scheduling

The scheduler uses two-tier prioritization:

1. **Never-processed specimens** — prioritized by read count (highest first), processed once they reach `--min-reads`
2. **Reprocessing candidates** — specimens with enough new reads since last consensus (controlled by `--reprocess-ratio`)

In live mode, watched specimens (starred in the dashboard) receive a priority boost and are processed first.

### Live mode concurrency

Consensus jobs read copy-on-write snapshots of their input FASTQs (instant on APFS/btrfs/XFS, a plain copy elsewhere), so when a new FASTQ file stabilizes, specimux demultiplexes it immediately — appending to the live per-specimen files while in-flight consensus jobs keep running on their snapshots. Demultiplexing uses whatever worker threads aren't occupied by consensus jobs, and newly-ready specimens are scheduled as soon as it finishes.

## Web dashboard

The built-in dashboard provides a real-time view of pipeline progress, streamed via server-sent events (SSE).

### Processing tab

- Specimen table with status, read count, top identification match, and identity score
- Color-coded status badges (queued, processing, identified, no match, error)
- On-target/off-target indicators when community taxa are available
- Identity warnings for low-confidence matches (<98% or <90%)
- Expandable cluster-level detail with per-cluster identification and sequence viewer
- Search, sort, and filter (novel, on-target, off-target, no-match, watched)

### Summary tab

- Variant-level results after summarization
- Variant count per specimen with expandable detail rows
- Per-variant identification, read count, and sequence length
- Identification results shown only after variant-level identification completes

### Watch feature

Click the star on any specimen row to boost its scheduling priority. Watched specimens are processed ahead of all others in live mode.

### Sharing

Use `--share` to bind the dashboard to your LAN address and display a QR code for easy access from other devices.

## Replay

For testing or demos, `specimux-replay` splits a source FASTQ into timed chunks that mimic MinKNOW output:

```bash
specimux-replay source.fastq simulated_output/ --reads-per-file 4000 --delay 30
```

| Option | Default | Description |
|---|---|---|
| `--reads-per-file` | `4000` | Reads per output file |
| `--delay` | `30` | Seconds between files |
| `--gzip` | — | Compress output files (.fastq.gz) |

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
├── events.jsonl                    # Append-only event log (rotates at 100 MB)
├── inat_taxon_cache.json           # Cached iNaturalist community taxa
├── specimux/full/{pool}/
│   └── {specimen_id}.fastq         # Demultiplexed reads per specimen
├── consensus/{specimen_id}/
│   └── {specimen_id}-all.fasta     # Consensus sequences (one or more clusters)
├── summary/
│   ├── {variant_id}-RiC*.fasta     # Individual variant sequences
│   └── summary.fasta               # Aggregated summary sequences
└── identification/
    └── {specimen_id}.tsv           # vsearch hits with adjusted-identity scores
```

The event log (`events.jsonl`) records every state change and is the single source of truth for pipeline state. Pipeline state is an in-memory materialized view rebuilt by replaying all events — it is never persisted to disk.

## Development

```bash
# Install with dev dependencies
pip install -e '.[dev]'

# Run tests
pytest tests/

# Run a single test
pytest tests/test_state.py::test_read_totals -v
```

## License

BSD 3-Clause. See [LICENSE](LICENSE).
