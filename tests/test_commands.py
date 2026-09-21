"""The commands facade: every user action on a run, as a typed call.

Invariants a remote caller (a command poller) and the audit trail rely on:

- each mutation event records its actor and command id;
- every non-duplicate command closes with a ``command.outcome`` event
  (applied / rejected + reason / noop), so the log alone confirms it;
- a redelivered command id is a noop that emits nothing, seeded from the
  log at open, so retries and restarts are safe;
- control commands (finalize, abort, rescan) go through the pipeline's
  hooks and are rejected when no pipeline is attached.

And the local web route over it: viewer commands are open, admin commands
are localhost-only with the marker header, rejections are 400s.
"""

import pytest
from fastapi.testclient import TestClient

from specimux_suite.commands import APPLIED, NOOP, REJECTED, Commands, applied_command_ids
from specimux_suite.config import PipelineConfig
from specimux_suite.events import EventLog
from specimux_suite.state import PipelineState
from specimux_suite.web.server import create_app


def _open(tmp_path, control=None):
    log = EventLog(tmp_path / "events.jsonl")
    log.emit("specimux.completed", {"specimens": {"S1--iNat123456": 40, "S2": 12}})
    state = PipelineState()
    state.rebuild(log)
    log.add_listener(state.apply)
    return log, state, Commands(log, state, control=control)


def _events(log, type_=None):
    return [e for e in log.replay() if type_ is None or e.type == type_]


class FakeControl:
    def __init__(self, refuse=None):
        self.calls = []
        self.refuse = refuse

    def request_finalize(self):
        self.calls.append("finalize")
        return self.refuse

    def request_abort(self):
        self.calls.append("abort")
        return self.refuse

    def rescan_inat(self):
        self.calls.append("rescan")
        return self.refuse


def test_watch_records_actor_and_id_and_outcome(tmp_path):
    log, state, cmds = _open(tmp_path)
    r = cmds.watch("S2", actor="alice", command_id="c1")
    assert r.outcome == APPLIED and r.command_id == "c1"
    assert state.specimens["S2"].watched
    watched = _events(log, "specimen.watched")[-1].data
    assert watched["actor"] == "alice" and watched["command_id"] == "c1"
    outcome = _events(log, "command.outcome")[-1].data
    assert outcome == {"command_id": "c1", "command": "watch", "actor": "alice",
                       "outcome": APPLIED, "reason": None, "args": {"specimen_id": "S2"}}
    assert state.command_outcomes[-1]["command_id"] == "c1"
    assert state.to_dict()["command_outcomes"][-1]["outcome"] == APPLIED


def test_noop_and_rejected_still_close_with_an_outcome(tmp_path):
    log, state, cmds = _open(tmp_path)
    cmds.watch("S2", command_id="c1")
    again = cmds.watch("S2", command_id="c2")
    assert again.outcome == NOOP and "Already" in again.reason
    missing = cmds.watch("nope", command_id="c3")
    assert missing.outcome == REJECTED and missing.reason == "Specimen not found"
    assert not missing.ok
    outcomes = [e.data["outcome"] for e in _events(log, "command.outcome")]
    assert outcomes == [APPLIED, NOOP, REJECTED]
    assert len(_events(log, "specimen.watched")) == 1


def test_duplicate_command_id_is_a_silent_noop(tmp_path):
    log, state, cmds = _open(tmp_path)
    cmds.watch("S2", command_id="dup")
    cmds.unwatch("S2", command_id="dup")  # redelivered id: ignored entirely
    assert state.specimens["S2"].watched
    assert len(_events(log, "command.outcome")) == 1
    r = cmds.unwatch("S2", command_id="dup")
    assert r.outcome == NOOP and r.reason == "Duplicate command id"


def test_dedupe_is_seeded_from_the_log_on_reopen(tmp_path):
    log, state, cmds = _open(tmp_path)
    cmds.watch("S2", command_id="seen-before")
    assert applied_command_ids(log) == {"seen-before"}
    # A new facade over the same log (a restart) still knows the id
    log2 = EventLog(tmp_path / "events.jsonl")
    state2 = PipelineState()
    state2.rebuild(log2)
    cmds2 = Commands(log2, state2)
    assert "seen-before" in cmds2.applied_ids()
    assert cmds2.unwatch("S2", command_id="seen-before").outcome == NOOP
    assert state2.specimens["S2"].watched


def test_generated_ids_are_unique(tmp_path):
    _, _, cmds = _open(tmp_path)
    a = cmds.watch("S2")
    b = cmds.unwatch("S2")
    assert a.command_id != b.command_id and len(a.command_id) >= 8


def test_correct_and_dismiss(tmp_path):
    log, state, cmds = _open(tmp_path)
    r = cmds.correct("S1--iNat123456", "654321", actor="op", command_id="k1")
    assert r.outcome == APPLIED
    ev = _events(log, "inat.correction")[-1].data
    assert ev["old_obs_id"] == "123456" and ev["new_obs_id"] == "654321"
    assert ev["actor"] == "op" and ev["command_id"] == "k1"
    assert cmds.correct("S1--iNat123456", "x1").outcome == REJECTED
    assert cmds.correct("S1--iNat123456", "1" * 13).outcome == REJECTED
    assert cmds.correct("missing", "1").reason == "Specimen not found"
    assert cmds.dismiss("S2").outcome == APPLIED
    assert "S2" in state.inat_dismissed
    assert cmds.dismiss("S2").outcome == NOOP
    assert _events(log, "inat.suggestion_dismissed")[-1].data["actor"] == "operator"


def test_control_commands_need_a_pipeline(tmp_path):
    _, _, cmds = _open(tmp_path)
    for name in ("finalize", "abort", "rescan"):
        r = getattr(cmds, name)()
        assert r.outcome == REJECTED and "No pipeline" in r.reason


def test_control_commands_go_through_the_hooks(tmp_path):
    ctl = FakeControl()
    log, _, cmds = _open(tmp_path, control=ctl)
    assert cmds.finalize(actor="remote", command_id="f1").outcome == APPLIED
    assert cmds.abort().outcome == APPLIED
    assert cmds.rescan().outcome == APPLIED
    assert ctl.calls == ["finalize", "abort", "rescan"]
    assert _events(log, "command.outcome")[0].data["actor"] == "remote"
    refusing = FakeControl(refuse="Finalize applies to live runs only")
    _, _, cmds2 = _open(tmp_path / "b", control=refusing)
    r = cmds2.finalize()
    assert r.outcome == REJECTED and r.reason == "Finalize applies to live runs only"


def test_dispatch_by_name(tmp_path):
    ctl = FakeControl()
    _, state, cmds = _open(tmp_path, control=ctl)
    assert cmds.dispatch("watch", {"specimen_id": "S2"}).outcome == APPLIED
    assert state.specimens["S2"].watched
    assert cmds.dispatch("correct", {"specimen_id": "S2", "new_obs_id": "42"}).outcome == APPLIED
    assert cmds.dispatch("finalize", command_id="x").command_id == "x"
    assert cmds.dispatch("reboot").outcome == REJECTED
    assert cmds.dispatch("watch", {}).outcome == REJECTED  # empty specimen id


def test_pipeline_implements_the_hooks(tmp_path):
    from specimux_suite.pipeline import Pipeline
    config = PipelineConfig(primers_file=tmp_path / "p.fasta",
                            specimens_file=tmp_path / "s.tsv",
                            reads_file=tmp_path / "r.fastq",
                            output_dir=tmp_path / "out", workers=1)
    pipeline = Pipeline(config)
    try:
        cmds = pipeline.commands
        assert cmds.finalize().reason == "Finalize applies to live runs only"
        pipeline._mode = "live"
        assert cmds.finalize().outcome == APPLIED
        assert pipeline.cmd_queue.get_nowait() == "finalize"
        assert cmds.abort().outcome == APPLIED
        assert pipeline._shutdown.is_set()
        assert cmds.abort().outcome == REJECTED
        assert cmds.finalize().outcome == REJECTED
    finally:
        pipeline._executor.shutdown(wait=True)


# --- the web route ---

def _client(tmp_path, local=True, control=None):
    log, state, cmds = _open(tmp_path, control=control)
    config = PipelineConfig(primers_file=tmp_path / "p.fasta",
                            specimens_file=tmp_path / "s.tsv",
                            output_dir=tmp_path)
    app = create_app(log, state, config, cmds)
    host = "127.0.0.1" if local else "192.168.1.9"
    c = TestClient(app, base_url="http://localhost", client=(host, 5555))
    return c, log, state


def test_route_viewer_commands_are_open(tmp_path):
    c, log, state = _client(tmp_path, local=False)
    r = c.post("/api/commands", json={"command": "watch", "specimen_id": "S2"})
    assert r.status_code == 200
    body = r.json()
    assert body["outcome"] == APPLIED and body["specimen_id"] == "S2"
    assert state.specimens["S2"].watched
    assert _events(log, "specimen.watched")[-1].data["actor"] == "viewer:192.168.1.9"
    # the client cannot choose its actor or spoof one
    r = c.post("/api/commands", json={"command": "unwatch", "specimen_id": "S2", "actor": "root"})
    assert _events(log, "specimen.watched")[-1].data["actor"] == "viewer:192.168.1.9"


def test_route_admin_commands_are_gated(tmp_path):
    c, log, _ = _client(tmp_path, local=False)
    r = c.post("/api/commands", json={"command": "dismiss", "specimen_id": "S2"},
               headers={"X-Specimux-Admin": "1"})
    assert r.status_code == 403
    c, log, state = _client(tmp_path / "local", local=True)
    # localhost without the marker header (CSRF) is still refused
    r = c.post("/api/commands", json={"command": "dismiss", "specimen_id": "S2"})
    assert r.status_code == 403
    r = c.post("/api/commands", json={"command": "dismiss", "specimen_id": "S2"},
               headers={"X-Specimux-Admin": "1"})
    assert r.status_code == 200 and r.json()["outcome"] == APPLIED
    assert _events(log, "inat.suggestion_dismissed")[-1].data["actor"] == "operator"


def test_route_rejections_and_bad_input(tmp_path):
    c, _, _ = _client(tmp_path)
    r = c.post("/api/commands", json={"command": "watch", "specimen_id": "nope"})
    assert r.status_code == 400
    assert r.json()["error"] == "Specimen not found" and r.json()["outcome"] == REJECTED
    assert c.post("/api/commands", json={"command": "reboot"}).status_code == 400
    assert c.post("/api/commands", json=[1]).status_code == 400
    assert c.post("/api/commands", content=b"{", headers={"Content-Type": "application/json"}).status_code == 400
    r = c.post("/api/commands", json={"command": "finalize"}, headers={"X-Specimux-Admin": "1"})
    assert r.status_code == 400 and "No pipeline" in r.json()["error"]


def test_route_honours_a_client_command_id(tmp_path):
    c, log, _ = _client(tmp_path)
    r = c.post("/api/commands", json={"command": "watch", "specimen_id": "S2", "command_id": "page-7"})
    assert r.json()["command_id"] == "page-7"
    r = c.post("/api/commands", json={"command": "unwatch", "specimen_id": "S2", "command_id": "page-7"})
    assert r.json()["outcome"] == NOOP
