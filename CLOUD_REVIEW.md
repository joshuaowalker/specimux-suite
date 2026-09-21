# Cloud architecture review

Review of the revised [CLOUD.md](CLOUD.md), September 20, 2026.
This review supersedes the initial findings recorded in this file.

The revision is substantially stronger: the completion manifest, faithful
viewer replay, cookie transport, durable control-plane storage, and revised
milestones address much of the first review. Six significant issues remain.

## 1. API fencing does not fence filesystem writes

The design rejects stale ingest and completion reports, but an old worker
still has direct access to the shared EFS directory. It could corrupt outputs
or append conflicting event versions even when its HTTP requests are rejected.

Require confirmed termination before allowing another writer, or isolate
attempts and explicitly promote their outputs. Use a service-assigned
generation per run and stage rather than relying solely on Batch's job-local
attempt counter.

Relevant design: **What the wrapper does — Fencing**. See also
[AWS retry semantics](https://docs.aws.amazon.com/batch/latest/userguide/job_retries.html).

## 2. EFS durability does not make restart recovery atomic

The claim that shared storage makes checkpoints unnecessary is too strong.
If demux appends reads and dies before recording completion, restarting can
repeat work against partially modified files. The current
[runner](src/specimux_suite/runners/specimux_runner.py) writes outputs before
emitting its completion event.

Define a recoverable commit boundary—such as staged per-input outputs or
rollback to recorded file lengths—and test interruption at that boundary.
The proposed local-directory mirroring fallback reintroduces the original
consistency problem and should not be presented as safe without its own
recovery protocol.

Relevant design: **What the wrapper does — Output dir on EFS**.

## 3. Conditional database updates do not cover external side effects

If Batch accepts a submission and the API dies before storing its job ID,
startup reconciliation cannot discover it merely by describing known jobs.
Similarly, recording a command and sending it to SQS are separate operations.

Specify durable pending actions with reconciliation, plus worker admission
that rejects duplicate launches. The global stage cap also needs a shared
stage reservation; conditions on separate run records cannot enforce a
cross-run limit.

Relevant design: **Control-plane state**.

## 4. Command IDs provide auditing, but not yet safe execution

A command can execute and then be redelivered before acknowledgment. Define
engine-side deduplication, ordering where required, and explicit outcomes for
rejected or no-op commands.

Recover acknowledgments from the persisted event log as well as HTTP ingest;
otherwise a worker dying after writing its event can leave an applied command
marked pending.

Relevant design: **Control-plane state — Commands**. This matters because
[standard SQS delivery can duplicate messages](https://docs.aws.amazon.com/en_gb/AWSSimpleQueueService/latest/SQSDeveloperGuide/standard-queues-at-least-once-delivery.html).

## 5. The completion barrier needs immutable inputs and a precise definition of “ingested”

Currently the wrapper records files after downloading and renaming them,
which does not prove successful demultiplexing. Finalization must explicitly
drain and account for every manifest entry, including failures.

Also bind processing to immutable object versions or enforce write-once
keys: previously issued upload URLs can remain usable after `complete` and
replace objects. Define how the job-page completion button obtains an
authoritative manifest when the uploader has not finished.

Relevant design: **Run lifecycle — Completion is a verifiable barrier** and
**What the wrapper does — Input**. See also
[AWS presigned URL behavior](https://docs.aws.amazon.com/AmazonS3/latest/userguide/using-presigned-url.html).

## 6. Redirecting sealed artifacts can break the dashboard API

The existing [sequence endpoint](src/specimux_suite/web/server.py) returns
JSON containing a sequence extracted from FASTA; redirecting it to the
archived FASTA does not preserve that contract.

Keep API-side extraction with an S3-backed cache, or publish equivalent JSON
artifacts. Shared EFS also exposes files while they are being rewritten, so
“not yet available” handling needs atomic artifact publication to avoid
serving partial content.

Relevant design: **The run API — Viewer**.

## Smaller clarifications

- Specify cookie scoping for multiple run tabs, explicit credentials on
  cross-origin fetches, and mutation-origin checks. A single run-token
  cookie at a common path would let tabs overwrite each other's
  authorization.
- Update the diagrams: some still show periodic output sync and the run API
  appending the event log, contradicting the revised single-writer EFS
  design.

## Acceptance tests

The next acceptance tests should target these exact failure windows:

- Interruption during demux, including after output writes but before the
  completion event.
- A lost submission response or API crash after Batch accepts a job but
  before the job handle is persisted.
- Duplicate commands and worker failure between applying a command and
  acknowledging it.
- An old worker continuing after its replacement starts.
