"""Tests for iNat observation-ID typo detection and correction suggestions."""

import json

from specimux_suite.inat_check import (
    SUGGESTIONS_FILENAME,
    check_inat_ids,
    digit_edit_candidates,
    find_suspects,
    rank_suggestions,
)


def test_digit_edit_candidates_properties():
    cands = digit_edit_candidates("12345")
    assert "12345" not in cands
    assert all(c[0] != "0" for c in cands)
    assert cands["21345"] == "transposition"
    assert cands["12395"] == "substitution"
    assert cands["1234"] == "deletion"
    assert cands["123456"] == "insertion"
    # A candidate reachable by transposition and substitution keeps the
    # more typo-plausible edit: 12354 is a transposition of the last two.
    assert cands["12354"] == "transposition"


def test_digit_edit_candidates_leading_zero_excluded():
    cands = digit_edit_candidates("123")
    assert "023" not in cands  # substitution of first digit to 0
    assert "0123" not in cands  # insertion of 0 at front
    assert "23" in cands  # deletion of first digit is fine


def test_find_suspects_flags_wrong_kingdom_and_missing():
    inat_ids = {f"s{i}": str(100 + i) for i in range(10)}
    info = {sid: {"found": True, "iconic": "Fungi"} for sid in inat_ids}
    info["s1"]["iconic"] = "Plantae"
    info["s2"]["iconic"] = "Protozoa"  # myxomycetes: legitimate foray target
    info["s3"]["iconic"] = ""          # no community ID yet: not suspect
    info["s4"] = {"found": False}      # observation doesn't exist

    suspects = find_suspects(inat_ids, info)
    problems = {s["specimen_id"]: s["problem"] for s in suspects}
    assert problems == {"s1": "non_fungi:Plantae", "s4": "not_found"}


def test_find_suspects_skips_not_found_when_fetch_failed():
    # Under 80% resolved: not-found detection off, wrong-kingdom stays on.
    inat_ids = {f"s{i}": str(100 + i) for i in range(10)}
    info = {sid: {"found": False} for sid in inat_ids}
    info["s0"] = {"found": True, "iconic": "Plantae"}
    suspects = find_suspects(inat_ids, info)
    assert [s["problem"] for s in suspects] == ["non_fungi:Plantae"]


def test_rank_suggestions_ordering():
    candidates = {"111": "insertion", "222": "transposition", "333": "substitution",
                  "444": "transposition", "555": "transposition"}
    fetched = {
        "111": {"taxon": "Russula peckii", "iconic": "Fungi", "login": "alice",
                "name": "Alice", "observed_on": ""},
        "222": {"taxon": "Quercus alba", "iconic": "Plantae", "login": "bob",
                "name": "", "observed_on": ""},          # wrong kingdom: dropped
        "333": {"taxon": "Lactarius", "iconic": "Fungi", "login": "carol",
                "name": "", "observed_on": ""},
        "444": {"taxon": "Amanita muscaria", "iconic": "Fungi", "login": "dave",
                "name": "", "observed_on": ""},
        # "555" doesn't exist on iNat: not in fetched
    }
    ranked = rank_suggestions(candidates, fetched, run_observers={"alice"},
                              hit_genera={"lactarius"})
    # alice is in the run (strongest), then carol's sequence match, then dave
    assert [r["obs_id"] for r in ranked] == ["111", "333", "444"]
    assert ranked[0]["observer_in_run"] and not ranked[0]["seq_match"]
    assert ranked[1]["seq_match"]


def test_check_inat_ids_writes_tsv(tmp_path, monkeypatch):
    inat_ids = {"specA": "12345", "specB": "99999"}
    info = {"specA": {"found": True, "iconic": "Insecta"},
            "specB": {"found": True, "iconic": "Fungi"}}

    # Only one candidate of specA's edits "exists": 21345, a fungal obs by
    # an observer who has other specimens in this run.
    payload = {"results": [{
        "id": 21345,
        "taxon": {"name": "Cortinarius caperatus", "iconic_taxon_name": "Fungi"},
        "user": {"login": "forayer", "name": "Fora Yer"},
        "observed_on": "2026-08-30",
    }]}

    class FakeResponse:
        def __enter__(self): return self
        def __exit__(self, *a): return False
        def read(self): return json.dumps(payload).encode()

    monkeypatch.setattr("specimux_suite.inat_check.urllib.request.urlopen",
                        lambda req, timeout=None: FakeResponse())
    monkeypatch.setattr("specimux_suite.inat_check.time.sleep", lambda s: None)

    records = check_inat_ids(
        inat_ids, info, run_observers={"forayer"},
        hit_genera_by_specimen={"specA": {"cortinarius"}},
        out_dir=tmp_path,
    )
    assert len(records) == 1
    r = records[0]
    assert r["specimen_id"] == "specA"
    assert r["suggested_obs_id"] == "21345"
    assert r["suggested_taxon"] == "Cortinarius caperatus"
    assert "observer_in_run" in r["evidence"] and "sequence_genus_match" in r["evidence"]
    assert "transposition" in r["evidence"]

    lines = (tmp_path / SUGGESTIONS_FILENAME).read_text().splitlines()
    assert lines[0].startswith("specimen_id\tobs_id\tproblem")
    assert len(lines) == 2
    assert "non_fungi:Insecta" in lines[1]


def test_check_inat_ids_no_suspects_writes_nothing(tmp_path):
    records = check_inat_ids(
        {"specA": "123"}, {"specA": {"found": True, "iconic": "Fungi"}},
        set(), {}, out_dir=tmp_path,
    )
    assert records == []
    assert not (tmp_path / SUGGESTIONS_FILENAME).exists()


# --- run_inat_check (state-driven entry point) ---

from specimux_suite.events import EventLog
from specimux_suite.state import PipelineState
from specimux_suite.inat_check import (
    CORRECTIONS_FILENAME,
    run_inat_check,
    write_corrections_tsv,
)


def _state_with_suspect(tmp_path):
    log = EventLog(tmp_path / "events.jsonl")
    log.emit("specimux.completed", {"exit_code": 0, "specimens": {
        "specA--iNat12345": 40, "specB--iNat67890": 30}})
    log.emit("specimens.taxa", {"taxa": {
        "specA--iNat12345": {"name": "Penstemon", "genus": "Penstemon",
                             "iconic_taxon": "Plantae", "ancestors": [],
                             "photos": [], "observer": {"login": "forayer", "name": ""}},
        "specB--iNat67890": {"name": "Russula peckii", "genus": "Russula",
                             "iconic_taxon": "Fungi", "ancestors": [],
                             "photos": [], "observer": {"login": "forayer", "name": ""}},
    }})
    state = PipelineState()
    state.rebuild(log)
    return log, state


def test_run_inat_check_emits_event_and_statuses(tmp_path, monkeypatch):
    log, state = _state_with_suspect(tmp_path)
    # Pretend the admin already accepted a correction for specA
    log.emit("inat.correction", {"specimen_id": "specA--iNat12345",
                                 "old_obs_id": "12345", "new_obs_id": "21345"})
    state.rebuild(EventLog(tmp_path / "events.jsonl"))

    monkeypatch.setattr("specimux_suite.inat_check.fetch_observations_summary",
                        lambda ids, abort=None, progress=None: {})

    out_dir = tmp_path / "summary"
    assert run_inat_check(state, log, out_dir)

    events = list(EventLog(tmp_path / "events.jsonl").replay())
    sugg = [e for e in events if e.type == "inat.suggestions"]
    assert len(sugg) == 1
    records = sugg[0].data["suggestions"]
    assert len(records) == 1
    assert records[0]["specimen_id"] == "specA--iNat12345"
    assert records[0]["status"] == "accepted"
    # Corrections TSV regenerated from state
    lines = (out_dir / CORRECTIONS_FILENAME).read_text().splitlines()
    assert lines[1] == "specA--iNat12345\t12345\t21345"


def test_state_inat_admin_events(tmp_path):
    log = EventLog(tmp_path / "events.jsonl")
    log.emit("inat.suggestions", {"suggestions": [{"specimen_id": "s1", "obs_id": "1",
                                                   "problem": "non_fungi:Plantae"}]})
    log.emit("inat.correction", {"specimen_id": "s1", "old_obs_id": "1", "new_obs_id": "2"})
    log.emit("inat.suggestion_dismissed", {"specimen_id": "s2"})
    state = PipelineState()
    state.rebuild(log)
    assert state.inat_suggestions[0]["specimen_id"] == "s1"
    assert state.inat_corrections["s1"] == {"old": "1", "new": "2"}
    assert state.inat_dismissed == {"s2"}
    d = state.to_dict()
    assert d["inat_corrections"]["s1"]["new"] == "2"
    assert d["inat_dismissed"] == ["s2"]


def test_write_corrections_tsv_empty_writes_nothing(tmp_path):
    assert write_corrections_tsv({}, tmp_path) is None
    assert not (tmp_path / CORRECTIONS_FILENAME).exists()


# --- admin gate ---

from specimux_suite.web.server import _admin_denial, _host_base


def test_host_base():
    assert _host_base("localhost:8077") == "localhost"
    assert _host_base("127.0.0.1") == "127.0.0.1"
    assert _host_base("[::1]:8077") == "::1"
    assert _host_base(None) == ""


def test_admin_denial():
    ok = _admin_denial("127.0.0.1", "localhost:8077", "1")
    assert ok is None
    assert _admin_denial("192.168.1.5", "localhost:8077", "1")   # LAN client
    assert _admin_denial("127.0.0.1", "evil.example:8077", "1")  # DNS rebinding
    assert _admin_denial("127.0.0.1", "localhost:8077", None)    # missing CSRF header
    assert _admin_denial(None, "localhost:8077", "1")            # no client info
    assert _admin_denial("::1", "[::1]:8077", "1") is None       # IPv6 localhost
