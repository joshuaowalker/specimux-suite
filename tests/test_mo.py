"""Mushroom Observer field IDs: tag parsing, record shape, missing-id retry,
caching, photo handoff, and the pipeline prefetch stage."""

import json
import threading
from unittest.mock import patch

import specimux_suite.mo as mo
from specimux_suite.mo import (
    _parse_consensus,
    _parse_first_naming,
    _parse_observer,
    _parse_photos,
    extract_mo_ids,
    fetch_mo_taxa,
    observation_url,
)
from specimux_suite.photos import first_photos, prefetch_photos


# ---------------------------------------------------------------------------
# Specimen-name convention (checked against real Index.txt sheets)
# ---------------------------------------------------------------------------

def test_extract_mo_ids_matches_real_sheet_shapes():
    specimens = [
        {"specimen_id": "ONT03.96-H12-GS23-204.Run033.ONT03.07.G01-MO346513"},
        {"specimen_id": "ONT01.Foltz82-Wasilewski_2013_3-MICH-F-251621-Pennsylvania-MO137852"},
        # a few sheets drop the separator before the tag
        {"specimen_id": "ONT04.14-F02-CM24-08210MO523685"},
        # re-run suffix after the tag
        {"specimen_id": "ONT03-Foltz118-Ben_20130617-MICH-F-251657-Florida-MO136780_specimen2"},
        # bare -MO with no digits carries no id
        {"specimen_id": "ONT01.Foltz45-Powers_2013070201-MICH-F-251584-Michigan-MO"},
        # iNat-tagged and untagged specimens are not MO
        {"specimen_id": "ONT01.01-A01--iNat233404001"},
        {"specimen_id": "DEMO123-plain"},
        # provisional-name state codes are not observation ids
        {"specimen_id": "ONT05.14-F02-Hypholoma-fasciculare-MO01"},
    ]
    assert extract_mo_ids(specimens) == {
        "ONT03.96-H12-GS23-204.Run033.ONT03.07.G01-MO346513": "346513",
        "ONT01.Foltz82-Wasilewski_2013_3-MICH-F-251621-Pennsylvania-MO137852": "137852",
        "ONT04.14-F02-CM24-08210MO523685": "523685",
        "ONT03-Foltz118-Ben_20130617-MICH-F-251657-Florida-MO136780_specimen2": "136780",
    }


def test_observation_url():
    assert observation_url("346513") == "https://mushroomobserver.org/obs/346513"


# ---------------------------------------------------------------------------
# Record parsing (shapes from live API2 responses)
# ---------------------------------------------------------------------------

def _obs(**overrides):
    base = {
        "id": 500000,
        "owner": {"id": 77818, "login_name": "myconaut710", "legal_name": "Kyle Canan "},
        "consensus": {"id": 114559, "name": "Clitocybe lamoureae",
                      "author": "Armada & al.", "rank": "species"},
        "namings": [
            {"id": 693217, "name": {"name": "Clitocybe", "rank": "suborder"},
             "owner": {"login_name": "Alan Rockefeller"}, "confidence": 1.8},
            {"id": 691019, "name": {"name": "Fungi", "rank": "kingdom"},
             "owner": {"login_name": "myconaut710"}, "confidence": 0.8},
            {"id": 781120, "name": {"name": "Clitocybe lamoureae", "rank": "species"},
             "owner": {"login_name": "pinonbistro"}, "confidence": 2.5},
        ],
        "primary_image": {"id": 1491270, "license": "Creative Commons Wikipedia Compatible v3.0",
                          "copyright_holder": "Kyle Canan "},
        "images": [
            {"id": 1491270, "license": "CC", "copyright_holder": "Kyle Canan "},  # dup of primary
            {"id": 1491271, "license": "Creative Commons Wikipedia Compatible v3.0",
             "copyright_holder": "Kyle Canan "},
            {"id": 1491272, "license": "", "copyright_holder": ""},
            {"id": 1491273, "license": "CC", "copyright_holder": "x"},
        ],
    }
    base.update(overrides)
    return base


def test_parse_photos_primary_first_deduped_capped_and_prefixed():
    photos = _parse_photos(_obs())
    assert [p["id"] for p in photos] == ["mo1491270", "mo1491271", "mo1491272"]
    assert photos[0]["url"] == "https://images.mushroomobserver.org/thumb/1491270.jpg"
    assert photos[0]["large_url"] == "https://images.mushroomobserver.org/960/1491270.jpg"
    assert photos[0]["attribution"] == "(c) Kyle Canan, Creative Commons Wikipedia Compatible v3.0"
    assert photos[2]["attribution"] == "" and photos[2]["license_code"] is None


def test_parse_observer_and_first_naming():
    assert _parse_observer(_obs()) == {"login": "myconaut710", "name": "Kyle Canan"}
    assert _parse_observer({"owner": {}}) == {}
    # lowest naming id is the field ID, regardless of list order
    assert _parse_first_naming(_obs()) == {"name": "Fungi", "login": "myconaut710"}
    assert _parse_first_naming({"namings": []}) == {}


def test_parse_consensus_genus_only_at_genus_or_below():
    assert _parse_consensus(_obs()) == ("Clitocybe lamoureae", "Clitocybe")
    genus_level = _obs(consensus={"name": "Amanita", "rank": "genus"})
    assert _parse_consensus(genus_level) == ("Amanita", "Amanita")
    section = _obs(consensus={"name": "Amanita sect. Vaginatae", "rank": "section"})
    assert _parse_consensus(section) == ("Amanita sect. Vaginatae", "Amanita")
    # "Clitocybe sensu lato" is filed at rank suborder: a name, but no genus
    sensu_lato = _obs(consensus={"name": "Clitocybe", "author": "sensu lato", "rank": "suborder"})
    assert _parse_consensus(sensu_lato) == ("Clitocybe", "")
    assert _parse_consensus(_obs(consensus=None)) == ("", "")


# ---------------------------------------------------------------------------
# Fetch: batching, the fatal-missing-id retry, iNat genus mapping, cache
# ---------------------------------------------------------------------------

def _fake_api(responses):
    """_request stub returning canned responses in order; records the ids asked."""
    calls = []

    def _request(ids):
        calls.append(list(ids))
        return responses.pop(0)
    return _request, calls


def test_fetch_retries_without_missing_ids_and_maps_genus_onto_inat(tmp_path, monkeypatch):
    monkeypatch.setattr(mo.time, "sleep", lambda s: None)
    missing_error = {"version": 2.0, "run_time": 0.1, "errors": [{
        "code": "API2::ObjectNotFoundByID",
        "details": "Observation #500024 does not exist, or someone has deleted it.",
        "fatal": "true"}]}
    ok = {"version": 2.0, "run_time": 0.2, "results": [
        _obs(id=500000),
        _obs(id=346513, consensus={"name": "Laccaria laccata", "rank": "species"},
             owner={"login_name": "LoganW", "legal_name": "Logan Wiedenfeld"}),
    ]}
    request, calls = _fake_api([missing_error, ok])
    monkeypatch.setattr(mo, "_request", request)
    lineages = {
        "Clitocybe": [{"id": 48460, "rank": "kingdom", "name": "Fungi"},
                      {"id": 47170, "rank": "phylum", "name": "Basidiomycota"},
                      {"id": 121, "rank": "genus", "name": "Clitocybe"}],
        "Laccaria": [],  # explicit no-match on iNat: not a genus there
    }
    any_rank = {"Laccaria": [{"id": 48460, "rank": "kingdom", "name": "Fungi"},
                             {"id": 9001, "rank": "family", "name": "Laccaria"}]}

    def fake_lineages(names, cache_dir=None, abort=None, progress=None, rank="genus"):
        return lineages if rank == "genus" else {n: any_rank.get(n, []) for n in names}
    with patch("specimux_suite.mo.fetch_genus_lineages", side_effect=fake_lineages) as fgl:
        progress = []
        unresolved = []
        taxa = fetch_mo_taxa(
            {"a-MO500000": "500000", "a2-MO500000": "500000",
             "b-MO346513": "346513", "c-MO500024": "500024"},
            cache_dir=tmp_path, progress=lambda d, t: progress.append((d, t)),
            lineage_progress=lambda d, t: None, unresolved=unresolved,
        )
    assert unresolved == [{"specimen_id": "c-MO500024", "obs_id": "500024"}]
    assert fgl.call_args_list[0].kwargs["progress"] is not None
    assert calls == [["500000", "346513", "500024"], ["500000", "346513"]]
    assert fgl.call_args_list[0].args[0] == ["Clitocybe", "Laccaria"]
    assert fgl.call_args_list[1].args[0] == ["Laccaria"]
    assert fgl.call_args_list[1].kwargs["rank"] is None
    assert set(taxa) == {"a-MO500000", "a2-MO500000", "b-MO346513"}
    rec = taxa["a-MO500000"]
    assert rec["provider"] == "mo"
    assert rec["name"] == "Clitocybe lamoureae" and rec["genus"] == "Clitocybe"
    assert rec["iconic_taxon"] == "Fungi"
    assert rec["ancestors"] == [48460, 47170, 121]
    assert rec["observer"]["login"] == "myconaut710"
    assert rec["first_id"] == {"name": "Fungi", "login": "myconaut710"}
    assert rec["photos"][0]["id"] == "mo1491270"
    assert taxa["a2-MO500000"] is rec  # shared entry per observation
    # not an iNat genus → genus dropped (no spurious off-target), name kept,
    # and the token's any-rank lineage backs taxonomy-level agreement
    assert taxa["b-MO346513"]["ancestors"] == [48460, 9001]
    assert taxa["b-MO346513"]["genus"] == ""
    assert taxa["b-MO346513"]["name"] == "Laccaria laccata"
    assert progress == [(0, 3), (3, 3)]
    cache = json.loads((tmp_path / "mo_taxon_cache.json").read_text())
    assert set(cache) == {"500000", "346513"}


def test_fetch_paces_batches_by_run_time(tmp_path, monkeypatch):
    """Between batches the fetcher waits max(5 s, last run_time), the
    documented courtesy; the first request is not delayed."""
    sleeps = []
    monkeypatch.setattr(mo.time, "sleep", lambda s: sleeps.append(s))
    monkeypatch.setattr(mo, "MAX_BATCH_SIZE", 2)
    request, calls = _fake_api([
        {"run_time": 7.5, "results": [_obs(id=1), _obs(id=2)]},
        {"run_time": 0.2, "results": [_obs(id=3), _obs(id=4)]},
        {"run_time": 0.1, "results": [_obs(id=5)]},
    ])
    monkeypatch.setattr(mo, "_request", request)
    with patch("specimux_suite.mo.fetch_genus_lineages", return_value={}):
        taxa = fetch_mo_taxa({f"s{i}-MO{i:04d}": str(i) for i in range(1, 6)}, cache_dir=tmp_path)
    assert len(calls) == 3 and len(taxa) == 5
    assert sleeps == [7.5, 5.0]


def test_fetch_keeps_genus_when_lineage_fetch_fails_transiently(tmp_path, monkeypatch):
    monkeypatch.setattr(mo.time, "sleep", lambda s: None)
    request, _ = _fake_api([{"run_time": 0.1, "results": [_obs(id=1)]}])
    monkeypatch.setattr(mo, "_request", request)
    with patch("specimux_suite.mo.fetch_genus_lineages", return_value={}):  # absent = transient
        taxa = fetch_mo_taxa({"x-MO0001": "1"}, cache_dir=tmp_path)
    assert taxa["x-MO0001"]["genus"] == "Clitocybe"
    assert taxa["x-MO0001"]["ancestors"] == []
    # served this run, but not cached: the next run completes the lineage
    assert not (tmp_path / "mo_taxon_cache.json").exists()


def test_fetch_serves_cache_without_network(tmp_path, monkeypatch):
    entry = {"provider": "mo", "name": "Amanita aurorae", "genus": "Amanita",
             "iconic_taxon": "Fungi", "ancestors": [1], "photos": [],
             "observer": {}, "first_id": {}}
    (tmp_path / "mo_taxon_cache.json").write_text(json.dumps({"521559": entry}))
    monkeypatch.setattr(mo, "_request", lambda ids: (_ for _ in ()).throw(AssertionError("network")))
    assert fetch_mo_taxa({"x-MO521559": "521559"}, cache_dir=tmp_path) == {"x-MO521559": entry}


def test_fetch_survives_non_missing_api_error(tmp_path, monkeypatch):
    monkeypatch.setattr(mo.time, "sleep", lambda s: None)
    request, _ = _fake_api([{"errors": [{"code": "API2::BadParameterValue", "details": "nope"}]}])
    monkeypatch.setattr(mo, "_request", request)
    unresolved = []
    assert fetch_mo_taxa({"x-MO0001": "1"}, cache_dir=tmp_path, unresolved=unresolved) == {}
    assert unresolved == []  # a failed batch is transient, not "not found"
    assert not (tmp_path / "mo_taxon_cache.json").exists()


# ---------------------------------------------------------------------------
# Photo cache: MO records carry large_url explicitly
# ---------------------------------------------------------------------------

def test_first_photos_carries_large_url_and_prefetch_uses_it(tmp_path, monkeypatch):
    taxa = {
        "x-MO1": {"photos": [{"id": "mo7", "url": "https://images.mushroomobserver.org/thumb/7.jpg",
                              "large_url": "https://images.mushroomobserver.org/960/7.jpg"}]},
        "y--iNat2": {"photos": [{"id": 9, "url": "https://a/photos/9/square.jpg"}]},
    }
    assert first_photos(taxa) == [
        {"id": "mo7", "url": "https://images.mushroomobserver.org/thumb/7.jpg",
         "large_url": "https://images.mushroomobserver.org/960/7.jpg"},
        {"id": 9, "url": "https://a/photos/9/square.jpg"},
    ]
    fetched = []

    class _Resp:
        def __enter__(self): return self
        def __exit__(self, *a): return False
        def read(self): return b"jpg"

    def fake_urlopen(req, timeout=0):
        fetched.append(req.full_url)
        return _Resp()
    monkeypatch.setattr("specimux_suite.photos.urllib.request.urlopen", fake_urlopen)
    monkeypatch.setattr("specimux_suite.photos.time.sleep", lambda s: None)
    assert prefetch_photos(taxa, tmp_path) == 2
    assert sorted(fetched) == ["https://a/photos/9/large.jpg",
                               "https://images.mushroomobserver.org/960/7.jpg"]
    assert (tmp_path / "mo7_large.jpg").exists() and (tmp_path / "9_large.jpg").exists()


def test_prefetch_serializes_mo_photo_downloads(tmp_path, monkeypatch):
    """MO images go one at a time with a pause; iNat hosts keep the pool."""
    import specimux_suite.photos as photos
    taxa = {f"s{i}-MO{i:04d}": {"photos": [{"id": f"mo{i}",
                                              "url": f"https://images.mushroomobserver.org/thumb/{i}.jpg",
                                              "large_url": f"https://images.mushroomobserver.org/960/{i}.jpg"}]}
            for i in range(6)}
    in_flight = 0
    max_in_flight = 0
    state_lock = threading.Lock()
    mo_sleeps = []

    class _Resp:
        def __enter__(self): return self
        def __exit__(self, *a): return False
        def read(self): return b"jpg"

    def fake_urlopen(req, timeout=0):
        nonlocal in_flight, max_in_flight
        with state_lock:
            in_flight += 1
            max_in_flight = max(max_in_flight, in_flight)
        time_orig_sleep(0.01)
        with state_lock:
            in_flight -= 1
        return _Resp()

    time_orig_sleep = __import__("time").sleep
    monkeypatch.setattr("specimux_suite.photos.urllib.request.urlopen", fake_urlopen)
    monkeypatch.setattr("specimux_suite.photos.time.sleep", lambda s: mo_sleeps.append(s))
    assert prefetch_photos(taxa, tmp_path) == 6
    assert max_in_flight == 1
    assert mo_sleeps == [photos._MO_FETCH_DELAY_S] * 6


# ---------------------------------------------------------------------------
# Pipeline: the MO stage runs after iNat in both prefetch paths
# ---------------------------------------------------------------------------

def _make_pipeline(tmp_path, blocking=True):
    from specimux_suite.config import PipelineConfig
    (tmp_path / "specimens.tsv").write_text(
        "SampleID\tPrimerPool\n"
        "specA--iNat111\tp1\n"
        "specB-MO222222\tp1\n"
        "specC-MO333333\tp1\n"
    )
    config = PipelineConfig(
        primers_file=tmp_path / "primers.fasta",
        specimens_file=tmp_path / "specimens.tsv",
        reads_file=tmp_path / "reads.fastq",
        output_dir=tmp_path / "output",
        inat_blocking=blocking,
    )
    with patch("specimux_suite.pipeline.SpecimuxRunner"), \
         patch("specimux_suite.pipeline.SpeconsenseRunner"), \
         patch("specimux_suite.pipeline.SummarizeRunner"), \
         patch("specimux_suite.pipeline._check_tool_on_path", return_value=True):
        from specimux_suite.pipeline import Pipeline
        return Pipeline(config)


def test_blocking_prefetch_runs_mo_stage_and_hands_both_to_photos(tmp_path):
    pipeline = _make_pipeline(tmp_path)
    try:
        inat_taxa = {"specA--iNat111": {"name": "Amanita x", "genus": "Amanita", "photos": []}}
        mo_taxa = {"specB-MO222222": {"provider": "mo", "name": "Laccaria laccata",
                                   "genus": "Laccaria", "photos": []}}
        photos_arg = {}
        photos_called = threading.Event()

        def fake_photos(taxa, *a, **k):
            photos_arg.update(taxa)
            photos_called.set()
        def fake_mo(mo_ids, **kw):
            kw["unresolved"].append({"specimen_id": "specC-MO333333", "obs_id": "333333"})
            kw["lineage_progress"](1, 1)
            return mo_taxa
        with patch("specimux_suite.pipeline.fetch_community_taxa", return_value=inat_taxa), \
             patch("specimux_suite.pipeline.fetch_mo_taxa", side_effect=fake_mo) as fmo, \
             patch("specimux_suite.pipeline.run_inat_check"), \
             patch("specimux_suite.pipeline.prefetch_photos", side_effect=fake_photos):
            pipeline.prefetch_inat(show_progress=False)
            assert photos_called.wait(timeout=5.0)
        assert fmo.call_args.args[0] == {"specB-MO222222": "222222", "specC-MO333333": "333333"}
        assert set(photos_arg) == {"specA--iNat111", "specB-MO222222"}
        taxa_events = [e for e in pipeline.event_log.replay() if e.type == "specimens.taxa"]
        assert len(taxa_events) == 2
        assert "specB-MO222222" in taxa_events[1].data["taxa"]
        unresolved_events = [e for e in pipeline.event_log.replay() if e.type == "mo.unresolved"]
        assert [e.data for e in unresolved_events] == [
            {"unresolved": [{"specimen_id": "specC-MO333333", "obs_id": "333333"}]}]
        assert pipeline.state.mo_unresolved == [{"specimen_id": "specC-MO333333", "obs_id": "333333"}]
        assert pipeline.state.to_dict()["mo_unresolved"] == pipeline.state.mo_unresolved
        spec = pipeline.state.specimens["specB-MO222222"]
        assert spec.community_taxon == "Laccaria laccata"
        assert spec.community_genus == "Laccaria"
    finally:
        pipeline._shutdown.set()
        pipeline._executor.shutdown(wait=False)


def test_background_fetch_runs_mo_stage_even_without_inat_ids(tmp_path):
    pipeline = _make_pipeline(tmp_path, blocking=False)
    try:
        mo_taxa = {"specB-MO222222": {"provider": "mo", "name": "Laccaria laccata",
                                   "genus": "Laccaria", "photos": []}}
        with patch("specimux_suite.pipeline.fetch_community_taxa", return_value={}), \
             patch("specimux_suite.pipeline.fetch_mo_taxa", return_value=mo_taxa), \
             patch("specimux_suite.pipeline.run_inat_check") as check, \
             patch("specimux_suite.pipeline.prefetch_photos") as photos:
            # --inat-background: loading specimens spawns the fetch thread
            pipeline._load_specimens()
            for t in threading.enumerate():
                if t.name == "inat-fetch":
                    t.join(timeout=5.0)
        assert not check.called  # no iNat taxa → no audit
        assert photos.call_args.args[0] == mo_taxa
        assert pipeline.state.specimens["specB-MO222222"].community_taxon == "Laccaria laccata"
    finally:
        pipeline._shutdown.set()
        pipeline._executor.shutdown(wait=False)
