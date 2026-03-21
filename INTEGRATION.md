# Integration Contracts

Reference for developers working on specimux, speconsense, or specimux-suite.
These contracts define how the three repositories interact at runtime.

---

## Ecosystem Overview

| Repository | Role | Installable independently? |
|---|---|---|
| **specimux** | Demultiplexes ONT reads by primer and specimen barcode | Yes |
| **speconsense** | Generates consensus sequences from per-specimen FASTQs | Yes |
| **speconsense-summarize** | Extracts and summarizes variant sequences from consensus output | Yes (ships with speconsense) |
| **specimux-suite** | Orchestration pipeline — runs specimux, speconsense, speconsense-summarize, and vsearch as subprocesses; provides web dashboard and event log | Yes (requires specimux and speconsense on `$PATH`) |

Each tool is a standalone CLI. The suite never imports from specimux or speconsense — it invokes them as subprocesses and communicates through files, exit codes, and the progress protocol described below.

---

## Profile Contract

### Tool-level profiles (specimux, speconsense)

Each tool supports YAML configuration profiles that bundle parameter presets.

**Locations (resolution order):**

1. User directory: `~/.config/<tool>/profiles/<name>.yaml`
2. Bundled in the package

If a requested profile is not found in either location, the tool exits with an error listing available profiles.

**CLI flags:**

- `-p <name>` — select a profile by name (without `.yaml` extension)
- `--list-profiles` — print available profiles and exit

**YAML format:**

```yaml
<tool>-version: "X.Y.*"    # glob pattern for compatible tool versions
# tool-specific parameters follow
key: value
```

The version field is validated at startup. Glob matching allows patch-level flexibility (e.g., `"1.2.*"` matches `1.2.0` through `1.2.99`).

**Override order (lowest to highest precedence):**

1. Tool defaults
2. Profile values (`-p <name>`)
3. Explicit CLI arguments (`--key value`)

### Suite-level profiles (specimux-suite)

The suite has its own profile system that can reference tool-level profiles and set tool parameters.

**Locations (resolution order):**

1. User directory: `~/.config/specimux-suite/profiles/<name>.yaml`
2. Bundled in the package

**YAML format:**

```yaml
specimux-suite-version: "X.Y.*"

suite:
  # suite-level settings (scheduler, web server, etc.)
  min-reads: 30
  reprocess-ratio: 0.5
  workers: 4
  live-presample: 100

specimux:
  profile: "some-profile"   # passed as -p to specimux subprocess
  key: value                 # passed as --key value to specimux subprocess

speconsense:
  profile: "some-profile"   # passed as -p to speconsense subprocess
  key: value                 # passed as --key value to speconsense subprocess

summarize:
  profile: "some-profile"   # passed as -p to speconsense-summarize subprocess
  key: value                 # passed as --key value to speconsense-summarize subprocess

identify:
  vsearch-min-identity: 0.85
  vsearch-max-accepts: 10
  identify-min-coverage: 0.5
```

**How suite profiles translate to subprocess arguments:**

- `specimux.profile` / `speconsense.profile` / `summarize.profile` values become `-p <name>` on the subprocess command line.
- Other keys in those sections become `--key value` arguments.
- This means a suite profile can fully configure a pipeline run without the user touching tool-level profiles directly.

### Two-pass CLI parsing

Both specimux and speconsense use two-pass CLI parsing:

1. **First pass:** Extract `-p <name>` to load the profile YAML.
2. **Second pass:** Parse all arguments, with profile values as defaults. Explicit CLI flags override profile values.

This ensures the override order (defaults < profile < CLI) is respected without requiring the profile to be loaded before argparse runs.

---

## Progress Protocol

Tools that support progress reporting accept a `--progress-file PATH` flag. They write JSONL (one JSON object per line) to this file during execution.

### Line types

**Progress update** (emitted approximately every 1 second, throttled):

```jsonl
{"type": "progress", "processed": 12000, "matched": 9600, "total_est": 50000, "rate": 0.80}
```

| Field | Type | Description |
|---|---|---|
| `type` | string | Always `"progress"` |
| `processed` | int | Reads processed so far |
| `matched` | int | Reads that matched a specimen |
| `total_est` | int | Estimated total reads in the input |
| `rate` | float | Match rate (`matched / processed`) |

**Completion** (emitted once when processing finishes):

```jsonl
{"type": "complete", "processed": 50000, "matched": 39800, "rate": 0.796}
```

| Field | Type | Description |
|---|---|---|
| `type` | string | Always `"complete"` |
| `processed` | int | Final count of reads processed |
| `matched` | int | Final count of matched reads |
| `rate` | float | Final match rate |

### How the suite consumes progress

The suite spawns a tail thread that watches the progress file. Each new line is parsed and translated into a `specimux.progress` event on the suite's event log, enabling real-time dashboard updates via SSE.

---

## Subprocess Invocation Contract

The suite constructs command lines for each tool following a fixed structure. Argument precedence goes left to right (later arguments win).

### specimux

```
specimux <primers> <specimens> <fastq> \
  -F -O <output_dir> -t <workers> \
  [-p <profile>] \
  [--key value ...] \
  [--progress-file <path>] \
  [<--specimux-args ...>]
```

| Segment | Source | Precedence |
|---|---|---|
| Positional args, `-F`, `-O`, `-t` | Suite hardcoded / computed | Base (lowest) |
| `-p <profile>` | Suite profile `specimux.profile` or `--specimux-profile` CLI | Middle |
| `--key value` pairs | Suite profile `specimux.*` keys or suite CLI flags | Higher |
| `--progress-file <path>` | Suite-generated temp path | N/A (additive) |
| `--specimux-args` contents | User escape hatch, passed through verbatim | Highest |

### speconsense

```
speconsense <specimen.fastq> \
  -O <output_dir> \
  [--presample N] \
  [-p <profile>] \
  [--key value ...] \
  [<--speconsense-args ...>]
```

| Segment | Source | Precedence |
|---|---|---|
| Positional args, `-O`, `--presample` | Suite hardcoded / computed | Base (lowest) |
| `-p <profile>` | Suite profile `speconsense.profile` or `--speconsense-profile` CLI | Middle |
| `--key value` pairs | Suite profile `speconsense.*` keys or suite CLI flags | Higher |
| `--speconsense-args` contents | User escape hatch, passed through verbatim | Highest |

### speconsense-summarize

```
speconsense-summarize \
  --source <consensus_dir> \
  --summary-dir <summary_dir> \
  [--specimen <specimen_id>] \
  [--aggregate-only] \
  [-p <profile>] \
  [--key value ...] \
  [<--summarize-args ...>]
```

The suite invokes speconsense-summarize in two modes:

1. **Single-specimen mode** (`--specimen <id>`) — runs per specimen after consensus, extracts variants. Returns JSON on stdout with a `variants` array.
2. **Aggregate-only mode** (`--aggregate-only`) — runs once after all specimens are summarized to generate `summary.fasta` and other aggregate files.

| Segment | Source | Precedence |
|---|---|---|
| `--source`, `--summary-dir`, `--specimen` | Suite hardcoded / computed | Base (lowest) |
| `-p <profile>` | Suite profile `summarize.profile` | Middle |
| `--key value` pairs | Suite profile `summarize.*` keys | Higher |
| `--summarize-args` contents | User escape hatch, passed through verbatim | Highest |

### vsearch (identification)

```
vsearch --usearch_global <query> \
  --db <ref_db> \
  --blast6out <output> \
  --id <min_identity> \
  --maxaccepts <max_accepts>
```

vsearch does not support profiles or the progress protocol. Its arguments are controlled entirely by the suite's `identify` configuration section.

---

## Event Types

The suite records all state changes as JSONL events in `output_dir/events.jsonl` (rotated at 100 MB). These events are the single source of truth — `PipelineState` is rebuilt by replaying them.

### Pipeline lifecycle

| Event | Key fields | When |
|---|---|---|
| `pipeline.started` | `mode`, `config_summary` | Pipeline begins (batch or live) |
| `pipeline.error` | `component`, `message`, `details` | Error in any component |
| `finalization.started` | `specimen_count`, `specimen_ids` | Finalization begins (may re-emit if new files arrive during finalization) |
| `finalization.completed` | | Pipeline finished all work |

### Specimen tracking

| Event | Key fields | When |
|---|---|---|
| `specimens.loaded` | specimen list | Specimen index parsed at startup |
| `specimens.taxa` | `taxa` (dict of specimen_id → taxon info) | iNaturalist community taxa fetched |
| `specimen.updated` | `specimen_id`, `pool`, `total_reads` | Cumulative read count changes after demux |
| `specimen.watched` | `specimen_id`, `watched` (bool) | User toggles watch/priority boost from dashboard |

### specimux (demultiplexing)

| Event | Key fields | When |
|---|---|---|
| `specimux.started` | `job_id`, `file_path`, `input_reads` | Subprocess launched |
| `specimux.progress` | `job_id`, `processed`, `matched`, `total_est`, `rate` | Progress file update received |
| `specimux.completed` | `job_id`, `exit_code`, `specimens`, `input_reads`, `matched_reads` | Subprocess exited |

### speconsense (consensus)

| Event | Key fields | When |
|---|---|---|
| `consensus.started` | `specimen_id`, `job_id`, `read_count` | Subprocess launched |
| `consensus.completed` | `specimen_id`, `job_id`, `clusters` | Subprocess exited |

### Identification

| Event | Key fields | When |
|---|---|---|
| `identification.completed` | `specimen_id`, `matches` | vsearch + adjusted-identity scoring finished |

### Summarization

| Event | Key fields | When |
|---|---|---|
| `summarize.started` | `specimen_id`, `job_id` | Per-specimen summarize subprocess launched |
| `summarize.completed` | `specimen_id`, `job_id`, `variant_count`, `variants` | Per-specimen summarize subprocess exited |
| `summarize.aggregate_completed` | | Aggregate summary generation finished |

### Live mode (additional)

| Event | Key fields | When |
|---|---|---|
| `file.detected` | file path | New FASTQ appears in watch directory |
| `file.stable` | file path | File size stable, ready to process |

### Specimen status transitions

```
WAITING → CONSENSUS_RUNNING → CONSENSUS_DONE → IDENTIFIED → SUMMARIZED
                                              → NO_MATCH   → SUMMARIZED
                                              → ERROR
```

- `WAITING`: specimen exists but has not been scheduled for consensus
- `CONSENSUS_RUNNING`: consensus subprocess in flight
- `CONSENSUS_DONE`: consensus finished, clusters available, awaiting identification
- `IDENTIFIED`: at least one identification hit found
- `NO_MATCH`: identification ran but no hits passed filters
- `SUMMARIZED`: variant summarization complete (identification.completed does not change this status)
- `ERROR`: unrecoverable failure

Status is derived from events, not stored directly. The web dashboard computes display status client-side from the event stream, adding computed states like "queued" (meets min_reads threshold but not yet scheduled).
