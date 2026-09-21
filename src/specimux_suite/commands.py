"""The commands facade: every way a user may act on a run, in one place.

A command is a typed call — ``watch``, ``unwatch``, ``correct``, ``dismiss``,
``rescan``, ``finalize``, ``abort`` — that the engine turns into events.
The local web routes call the facade, tests call it directly, and a plugin
(a remote command poller, say) calls it from its own thread. Nothing else
emits user-action events.

Every command carries an actor and a command id. The mutation event it
produces records both (additive fields on the event data), and a
``command.outcome`` event closes every non-duplicate command — applied,
rejected with a reason, or noop — so the log is a complete audit trail and
a remote caller can confirm a command from the log alone. Ids the log
already holds are deduplicated: a redelivered command is a noop and emits
nothing, which makes retries safe.
"""

import logging
import re
import threading
import uuid
from dataclasses import dataclass, field
from typing import Optional, Protocol

from .events import EventLog
from .state import PipelineState

logger = logging.getLogger(__name__)

# Commands any viewer may issue vs. those reserved for the run's operator.
# The facade does not enforce this — the caller (web route, plugin) decides
# who may issue what; the split is here so every caller shares it.
VIEWER_COMMANDS = frozenset({"watch", "unwatch"})
ADMIN_COMMANDS = frozenset({"correct", "dismiss", "rescan", "finalize", "abort"})
ALL_COMMANDS = VIEWER_COMMANDS | ADMIN_COMMANDS

APPLIED = "applied"
REJECTED = "rejected"
NOOP = "noop"

DEFAULT_ACTOR = "operator"


@dataclass
class CommandResult:
    command_id: str
    command: str
    outcome: str  # APPLIED | REJECTED | NOOP
    reason: Optional[str] = None
    data: dict = field(default_factory=dict)

    @property
    def ok(self) -> bool:
        return self.outcome != REJECTED

    def to_dict(self) -> dict:
        d = {"command_id": self.command_id, "command": self.command,
             "outcome": self.outcome, **self.data}
        if self.reason:
            d["reason"] = self.reason
        return d


class RunControl(Protocol):
    """What the facade needs from a running pipeline for the control commands.

    Each method returns None when the request was accepted or a reason
    string when it cannot be honoured. A viewer with no pipeline passes no
    control at all and those commands are rejected.
    """

    def request_finalize(self) -> Optional[str]: ...

    def request_abort(self) -> Optional[str]: ...

    def rescan_inat(self) -> Optional[str]: ...


class Commands:
    def __init__(self, event_log: EventLog, state: PipelineState,
                 control: Optional[RunControl] = None):
        self.event_log = event_log
        self.state = state
        self.control = control
        self._lock = threading.Lock()
        # Ids the log already carries, so a redelivered command is a noop.
        self._seen: set[str] = applied_command_ids(event_log)

    # --- the commands ---

    def watch(self, specimen_id: str, *, actor: str = DEFAULT_ACTOR,
              command_id: Optional[str] = None) -> CommandResult:
        return self._set_watch(specimen_id, True, actor, command_id)

    def unwatch(self, specimen_id: str, *, actor: str = DEFAULT_ACTOR,
                command_id: Optional[str] = None) -> CommandResult:
        return self._set_watch(specimen_id, False, actor, command_id)

    def _set_watch(self, specimen_id, watched, actor, command_id) -> CommandResult:
        name = "watch" if watched else "unwatch"
        args = {"specimen_id": specimen_id}

        def apply(cid):
            spec = self.state.specimens.get(specimen_id)
            if spec is None:
                return REJECTED, "Specimen not found"
            if spec.watched == watched:
                return NOOP, "Already " + ("watched" if watched else "unwatched")
            self.event_log.emit("specimen.watched", {
                "specimen_id": specimen_id, "watched": watched,
                "actor": actor, "command_id": cid,
            })
            return APPLIED, None

        return self._run(name, args, actor, command_id, apply)

    def correct(self, specimen_id: str, new_obs_id: str, *, actor: str = DEFAULT_ACTOR,
                command_id: Optional[str] = None) -> CommandResult:
        """Accept an iNat ID correction: emits inat.correction (pipeline heals)."""
        new_obs_id = str(new_obs_id or "")
        args = {"specimen_id": specimen_id, "new_obs_id": new_obs_id}

        def apply(cid):
            if specimen_id not in self.state.specimens:
                return REJECTED, "Specimen not found"
            if not new_obs_id.isdigit() or len(new_obs_id) > 12:
                return REJECTED, "Invalid observation id"
            m = re.search(r"iNat(\d+)", specimen_id)
            self.event_log.emit("inat.correction", {
                "specimen_id": specimen_id,
                "old_obs_id": m.group(1) if m else "",
                "new_obs_id": new_obs_id,
                "actor": actor, "command_id": cid,
            })
            return APPLIED, None

        return self._run("correct", args, actor, command_id, apply)

    def dismiss(self, specimen_id: str, *, actor: str = DEFAULT_ACTOR,
                command_id: Optional[str] = None) -> CommandResult:
        """Mark an iNat suggestion reviewed-no-change."""
        args = {"specimen_id": specimen_id}

        def apply(cid):
            if specimen_id not in self.state.specimens:
                return REJECTED, "Specimen not found"
            if specimen_id in self.state.inat_dismissed:
                return NOOP, "Already dismissed"
            self.event_log.emit("inat.suggestion_dismissed", {
                "specimen_id": specimen_id, "actor": actor, "command_id": cid,
            })
            return APPLIED, None

        return self._run("dismiss", args, actor, command_id, apply)

    def rescan(self, *, actor: str = DEFAULT_ACTOR,
               command_id: Optional[str] = None) -> CommandResult:
        """Re-run the iNat ID audit (network, in the background)."""
        return self._control("rescan", actor, command_id,
                             lambda c: c.rescan_inat())

    def finalize(self, *, actor: str = DEFAULT_ACTOR,
                 command_id: Optional[str] = None) -> CommandResult:
        """Finish a live run: drain files, process everything, summarize."""
        return self._control("finalize", actor, command_id,
                             lambda c: c.request_finalize())

    def abort(self, *, actor: str = DEFAULT_ACTOR,
              command_id: Optional[str] = None) -> CommandResult:
        """Stop the run now; running jobs finish, queued ones are dropped."""
        return self._control("abort", actor, command_id,
                             lambda c: c.request_abort())

    def _control(self, name, actor, command_id, call) -> CommandResult:
        def apply(cid):
            if self.control is None:
                return REJECTED, "No pipeline is attached to this viewer"
            reason = call(self.control)
            return (REJECTED, reason) if reason else (APPLIED, None)

        return self._run(name, {}, actor, command_id, apply)

    # --- dispatch by name (web route, command poller) ---

    def dispatch(self, command: str, args: Optional[dict] = None, *,
                 actor: str = DEFAULT_ACTOR,
                 command_id: Optional[str] = None) -> CommandResult:
        args = dict(args or {})
        if command not in ALL_COMMANDS:
            return CommandResult(command_id or _new_id(), command, REJECTED,
                                 f"Unknown command: {command}")
        try:
            if command in ("watch", "unwatch", "dismiss"):
                return getattr(self, command)(str(args.get("specimen_id") or ""),
                                              actor=actor, command_id=command_id)
            if command == "correct":
                return self.correct(str(args.get("specimen_id") or ""),
                                    args.get("new_obs_id"),
                                    actor=actor, command_id=command_id)
            return getattr(self, command)(actor=actor, command_id=command_id)
        except Exception as e:  # a command must never take the caller down
            logger.exception(f"Command {command} failed")
            return CommandResult(command_id or _new_id(), command, REJECTED,
                                 f"{type(e).__name__}: {e}")

    def applied_ids(self) -> set[str]:
        """Command ids the log carries (seeded at open, kept current)."""
        with self._lock:
            return set(self._seen)

    # --- plumbing ---

    def _run(self, name, args, actor, command_id, apply) -> CommandResult:
        cid = command_id or _new_id()
        with self._lock:
            if cid in self._seen:
                return CommandResult(cid, name, NOOP, "Duplicate command id", dict(args))
            self._seen.add(cid)
            outcome, reason = apply(cid)
            self.event_log.emit("command.outcome", {
                "command_id": cid, "command": name, "actor": actor,
                "outcome": outcome, "reason": reason, "args": dict(args),
            })
        if outcome == REJECTED:
            logger.info(f"Command {name} rejected ({actor}): {reason}")
        return CommandResult(cid, name, outcome, reason, dict(args))


def applied_command_ids(event_log: EventLog) -> set[str]:
    """Every command id recorded in a log (mutation and outcome events)."""
    ids = set()
    for event in event_log.replay():
        cid = event.data.get("command_id") if isinstance(event.data, dict) else None
        if cid:
            ids.add(cid)
    return ids


def _new_id() -> str:
    return uuid.uuid4().hex[:12]
