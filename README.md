# specimux-suite

> **Early preview release.** This project is under active development — APIs, event formats, and CLI options may change between versions. Feedback and bug reports are welcome at [github.com/joshuaowalker/specimux-suite/issues](https://github.com/joshuaowalker/specimux-suite/issues).

Orchestration and real-time dashboard for the [Mycomap](https://mycomap.org) fungal DNA barcoding pipeline. Manages the full workflow from raw nanopore reads through demultiplexing, consensus generation, variant summarization, and species identification.

## Installation

Requires Python 3.11+.

```bash
pip install specimux-suite
```

Or, for development, from a source checkout:

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

On startup the suite fetches iNaturalist data for the run (field IDs, an observation-ID audit, taxonomy) with progress bars, then opens a web dashboard at `http://127.0.0.1:8077` showing real-time progress — use `--inat-background` to skip the wait and let the dashboard fill in as data arrives. Press Ctrl+C to finalize — the pipeline will drain remaining files, process all eligible specimens regardless of threshold, and run summarization before exiting.

Restarting either mode on an existing output directory picks up where the previous run left off: the event log is replayed, interrupted consensus/identification/summarization work is resumed, and only new reads are processed.

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

**Specimens** — Tab-separated file with at least `SampleID` and `PrimerPool` columns. Specimen IDs containing an iNaturalist observation ID (e.g., `iNat12345`) enable the iNaturalist integration: community taxon lookup for on-target/off-target detection, observation photos and observer credits, field-ID comparison, and the observation-ID typo audit. Specimen IDs containing a Mushroom Observer observation ID (e.g., `MO346513`) get the same treatment from MO's consensus name, images and namings — except the typo audit, and higher-rank taxonomy comes from iNaturalist (the MO consensus is mapped onto iNat taxonomy at genus level). A run may mix both.

```
SampleID	PrimerPool
spec001	pool1
spec002	pool1
specimen-B--iNat12345	pool2
specimen-C-MO346513	pool2
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
| `--min-reads` | `10` | Minimum reads before running consensus |
| `--reprocess-ratio` | `0.5` | Ratio of new/previous reads to trigger reprocessing |
| `--workers` | half of CPU cores | Number of worker threads |
| `--identify-min-coverage` | `0.5` | Minimum query/target coverage for identification hits |
| `--specimux-args` | — | Extra arguments passed through to specimux |
| `--speconsense-args` | — | Extra arguments passed through to speconsense |
| `--summarize-args` | — | Extra arguments passed through to speconsense-summarize |
| `--no-incremental-summarize` | — | Only summarize in the final round instead of per specimen as identifications land |
| `--inat-background` | — | Fetch iNaturalist data in the background instead of blocking with progress bars at startup |
| `--no-photo-cache` | — | Don't cache observation photos in the output dir; the pages load them from iNaturalist / Mushroom Observer directly. For a dashboard served on the internet, where a copy of every photo per run is wasted |
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

### Plugins and event forwarding

| Option | Default | Description |
|---|---|---|
| `--forward-events URL` | — | Mirror the run's events to an HTTP endpoint in batches (see below) |
| `--forward-header 'Name: value'` | — | Header sent with every forwarded batch, e.g. an authorization token |
| `--plugin NAME` | — | Load a plugin by entry-point name or `module:factory` path (repeatable) |
| `--plugin-opt KEY=VALUE` | — | Option passed to every plugin's factory (repeatable) |
| `--mirror-dir DIR` | — | Copy what a dashboard reads (event log, consensus and summary FASTAs, photos) into DIR as it is published, while the run works in the output dir. For a run on local disk served from shared storage |
| `--event-log PATH` | output dir (or `--mirror-dir`) | Where the event log is written |

A plugin is an object with `start(context)` and `shutdown()` that runs alongside the pipeline; the context gives it the event log, the state, the commands facade (every user action on the run: `watch`, `correct`, `finalize`, ...), the config and the output dir. Packages register plugins in the `specimux_suite.plugins` entry-point group. The suite ships one: the HTTP event forwarder, which POSTs the event log to a URL in version order, at least once, resuming from the last acknowledged version after a restart (`forward-ack.json` in the output dir). Each batch is JSON `{"events": [...], "from_version", "to_version"}` and a 2xx acknowledges it; the receiver dedupes by version. Use it to mirror a run to a read-only dashboard elsewhere.

This interface (plugins, the forwarder, the viewer app factory and the pages' injected runtime config) exists so the suite can be hosted: a separate project, specimux-cloud, runs the pipeline as a cloud service and serves the same dashboard from a run API. The suite itself stays a local tool and has no cloud dependency.

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
2. **Reprocessing candidates** — specimens with enough new reads since last consensus (controlled by `--reprocess-ratio`, and always at least 5 new reads so small specimens don't re-run on every file)

In live mode, watched specimens (starred in the dashboard) receive a priority boost and are processed first.

Within the reprocessing tier, candidates are ordered by **result confidence** —
uncertain results are revisited first, since additional depth might change the
answer:

1. **No match** — consensus produced but nothing hit the reference database
2. **Low identity** — best hit below 95% adjusted identity
3. **Off-target** — no hit matches the iNaturalist community genus (a
   mycoparasite or yeast contaminant may be dominating the true target), or the
   community genus appears only in a minority cluster
4. **Marginal** — identity 95–98%, or ambiguous bases in the consensus
5. **Confident** — ≥98% identity and on-target

Uncertain results (the first three) also re-enter the queue at half the
configured `--reprocess-ratio`, so depth reaches them sooner. Confidence only
reorders work — confident specimens still reprocess whenever workers are free,
and finalization always processes every specimen with unprocessed reads,
regardless of confidence. The dashboard shows a small ↻ chip on each queued
reprocess candidate with the reason it was prioritized.

### Live mode concurrency

Consensus jobs read copy-on-write snapshots of their input FASTQs (instant on APFS/btrfs/XFS, a plain copy elsewhere), so when a new FASTQ file stabilizes, specimux demultiplexes it immediately — appending to the live per-specimen files while in-flight consensus jobs keep running on their snapshots. Demultiplexing uses whatever worker threads aren't occupied by consensus jobs, and newly-ready specimens are scheduled as soon as it finishes.

## Web dashboard

The built-in dashboard provides a real-time view of pipeline progress, streamed via server-sent events (SSE).

### Processing tab

- Specimen table with status, read count, top identification match, and identity score (clean clusters preferred over NS/LQ/chimera-flagged ones; best identity among on-target hits)
- Color-coded status badges (queued, processing, identified, no match, error)
- On-target/off-target indicators when community taxa are available — taxonomy-aware: a sequence agreeing with the field ID at genus or deeper shows ✓ even when the names differ, a shared family or tribe shows ≈, and the filters always match the indicator
- Identity warnings for low-confidence matches (<98% or <95%)
- Expandable cluster-level detail with per-cluster identification and sequence viewer
- Cluster quality badges: NS/LQ routing preview and CHIMERA (speconsense 0.8.6+ two-parent recombinant flag; routed to the `.chimera` track when summarize runs with `--filter-chimeras`, otherwise kept in Summary and badged for review)
- Search, sort, and filter (novel, on-target, off-target, no-match, watched)

### Summary tab

- Variant-level results, filled in during the run: each specimen is summarized as soon as its identification lands (see `--no-incremental-summarize`)
- Variant count per specimen with expandable detail rows
- Per-variant identification, read count, and sequence length
- Identification results shown only after variant-level identification completes

### Forecast tab

A live stop estimator for the sequencing operator (live mode, after at least two
files have been demultiplexed). From the per-file demultiplex history it estimates
each specimen's read-accumulation rate on the cumulative-matched-reads clock and
projects when below-threshold specimens will cross — with 90% intervals — plus:

- A headline: how many specimens are over threshold, how many more are projected
  to cross within the next hour, and how many will likely never make it
- A viewer-selectable forecast threshold (chips for common values plus a custom
  input) — view-only, the scheduler keeps using `min_reads`
- Threshold sensitivity (≥10 / min_reads / ≥100 / selected), since downstream
  verification often succeeds well below `min_reads`
- The specimens-over-threshold accumulation curve for the run so far
- A rate-drift self-check that flags when the forecast's stationarity assumption
  looks shaky for the current run

### Watch feature

Click the star on any specimen row to boost its scheduling priority. Watched specimens are processed ahead of all others in live mode.

### Sharing

Use `--share` to bind the dashboard to your LAN address and display a QR code for easy access from other devices.

## Highlights screen

The dashboard's **Highlights ↗** link opens `/present` — a full-screen carousel designed for a projector at a live event. It cycles through cards: fresh identifications with full-bleed iNaturalist photos and a drifting consensus-sequence ribbon, burst roll-ups when results land quickly, run milestones, family spotlights with photo mosaics, field-ID-refinement journeys confirmed by DNA, and visually flagged callouts — novel candidates (no close reference match), surprises (confident DNA far from the field ID), and likely label mix-ups (an observation filed under a non-fungal kingdom that sequences cleanly). Card selection draws from weighted channels so a busy stretch never crowds out variety; only strong novelty interrupts.

Operator keys: **space** pauses, **→** advances, **f** toggles fullscreen. Any number of viewers can open it (each is an independent client of the same event stream).

## Admin page

`/admin` (linked from the dashboard header, available only from localhost) reviews the iNaturalist observation-ID audit: specimen IDs whose embedded observation resolves to a non-fungal taxon or to nothing are checked against single-digit-edit candidates, ranked by evidence (candidate's observer has other specimens in the run, candidate's taxon matches the sequence). Accepting a correction heals the running pipeline live — field ID, photos, and agreement recover, and the corrected mapping is written to `summary/inat_id_corrections.tsv` for patching before MycoMap upload. All suspects, suggestions, and statuses are also written to `summary/inat_id_suggestions.tsv`. Mushroom Observer ids that don't resolve are listed on the same page (no automatic correction: fix the sheet and restart).

The correction approach — recovering the intended observation from a mistyped ID by checking digit-edit permutations against plausible observations — was inspired by Alan Rockefeller's [inat.finder.py](https://github.com/AlanRockefeller/inat.finder.py).

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
├── inat_taxon_cache.json           # Cached iNaturalist observations (taxa, photos, observers)
├── mo_taxon_cache.json             # Cached Mushroom Observer observations (same shape)
├── inat_lineage_cache.json         # Cached genus lineages for taxonomy-level agreement
├── inat_photos/                    # Local photo cache (iNat + MO) for the dashboard and highlights screen
├── specimux-inflight.json          # Present only while a demux runs; a restart rolls the demux back from it
├── forward-ack.json                # With --forward-events: last event version the receiver acknowledged
├── .staging/                       # Tool output before atomic publication into consensus/ and summary/
├── specimux/full/{pool}/
│   └── {specimen_id}.fastq         # Demultiplexed reads per specimen
├── consensus/{specimen_id}/
│   └── {specimen_id}-all.fasta     # Consensus sequences (one or more clusters)
├── summary/
│   ├── {variant_id}-RiC*.fasta     # Individual variant sequences
│   ├── summary.fasta               # Aggregated summary sequences
│   ├── inat_id_suggestions.tsv     # iNat observation-ID audit (suspects + suggested fixes)
│   └── inat_id_corrections.tsv     # Admin-accepted ID corrections, for pre-upload patching
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
