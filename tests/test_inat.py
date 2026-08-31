"""Tests for iNaturalist observation parsing and the photo cache."""

import json

from specimux_suite.inat import (
    MAX_PHOTOS_PER_OBSERVATION,
    _parse_observation_observer,
    _parse_observation_photos,
    fetch_community_taxa,
)
from specimux_suite.photos import (
    cached_photo_name,
    first_photos,
    photo_size_url,
    prefetch_photos,
)


def _obs(photos=None, user=None):
    return {"id": 123, "photos": photos or [], "user": user or {}}


def test_parse_photos_extracts_display_fields():
    photos = _parse_observation_photos(_obs(photos=[
        {"id": 7241, "url": "https://host/photos/7241/square.jpg",
         "license_code": "cc-by-nc", "attribution": "(c) A. Observer (CC BY-NC)"},
        {"id": 7242, "url": "https://host/photos/7242/square.jpg",
         "license_code": None, "attribution": None},
    ]))
    assert photos == [
        {"id": 7241, "url": "https://host/photos/7241/square.jpg",
         "license_code": "cc-by-nc", "attribution": "(c) A. Observer (CC BY-NC)"},
        {"id": 7242, "url": "https://host/photos/7242/square.jpg",
         "license_code": None, "attribution": ""},
    ]


def test_parse_photos_caps_count_and_skips_malformed():
    many = [{"id": i, "url": f"https://host/photos/{i}/square.jpg"}
            for i in range(1, MAX_PHOTOS_PER_OBSERVATION + 3)]
    many.insert(0, {"id": None, "url": "x"})  # malformed: no id
    many.insert(0, {"id": 99})                # malformed: no url
    photos = _parse_observation_photos(_obs(photos=many))
    assert len(photos) == MAX_PHOTOS_PER_OBSERVATION
    assert photos[0]["id"] == 1


def test_parse_observer():
    assert _parse_observation_observer(_obs(user={"login": "hsinger", "name": "Harte Singer"})) == \
        {"login": "hsinger", "name": "Harte Singer"}
    assert _parse_observation_observer(_obs(user={"login": "anon", "name": None})) == \
        {"login": "anon", "name": ""}
    assert _parse_observation_observer(_obs()) == {}


def test_cache_accepts_new_entries_and_discards_legacy(tmp_path):
    # Legacy entries (no photos key) must be discarded; new-format entries and
    # taxonless-but-photographed entries are valid. With every requested id
    # cached, fetch_community_taxa returns without touching the network.
    cache = {
        "111": {"name": "Lactarius peckii", "genus": "Lactarius",
                "iconic_taxon": "Fungi", "photos": [], "observer": {},
                "ancestors": [47170, 48627],
                "first_id": {"name": "Lactarius", "login": "x"}},
        "222": {"name": "Russula", "genus": "Russula", "iconic_taxon": "Fungi"},  # legacy
        "333": {"name": "", "genus": "", "iconic_taxon": "",
                "photos": [{"id": 9, "url": "u", "license_code": None, "attribution": ""}],
                "observer": {"login": "x", "name": ""}, "ancestors": [],
                "first_id": {}},
    }
    (tmp_path / "inat_taxon_cache.json").write_text(json.dumps(cache), encoding="utf-8")

    result = fetch_community_taxa({"spec1": "111"}, cache_dir=tmp_path)
    assert result["spec1"]["name"] == "Lactarius peckii"
    assert result["spec1"]["photos"] == []

    result = fetch_community_taxa({"spec3": "333"}, cache_dir=tmp_path)
    assert result["spec3"]["photos"][0]["id"] == 9


def test_photo_size_url():
    assert photo_size_url("https://a/photos/7/square.jpg", "large") == "https://a/photos/7/large.jpg"
    assert photo_size_url("https://a/photos/7/square.jpeg?1", "medium") == "https://a/photos/7/medium.jpeg?1"


def test_first_photos_dedupes_shared_entries():
    entry = {"photos": [{"id": 5, "url": "u5"}, {"id": 6, "url": "u6"}]}
    taxa = {"specA": entry, "specB": entry, "specC": {"photos": []}}
    assert first_photos(taxa) == [{"id": 5, "url": "u5"}]


def test_prefetch_skips_existing(tmp_path, monkeypatch):
    fetched_urls = []

    class FakeResponse:
        def __enter__(self): return self
        def __exit__(self, *a): return False
        def read(self): return b"jpegbytes"

    def fake_urlopen(req, timeout=None):
        fetched_urls.append(req.full_url)
        return FakeResponse()

    monkeypatch.setattr("specimux_suite.photos.urllib.request.urlopen", fake_urlopen)
    monkeypatch.setattr("specimux_suite.photos.time.sleep", lambda s: None)

    cache_dir = tmp_path / "inat_photos"
    cache_dir.mkdir()
    (cache_dir / cached_photo_name(5)).write_bytes(b"already here")

    taxa = {
        "specA": {"photos": [{"id": 5, "url": "https://a/photos/5/square.jpg"}]},
        "specB": {"photos": [{"id": 6, "url": "https://a/photos/6/square.jpg"}]},
    }
    n = prefetch_photos(taxa, cache_dir)
    assert n == 1
    assert fetched_urls == ["https://a/photos/6/large.jpg"]
    assert (cache_dir / cached_photo_name(6)).read_bytes() == b"jpegbytes"
    assert (cache_dir / cached_photo_name(5)).read_bytes() == b"already here"


def test_prefetch_aborts_between_downloads(tmp_path, monkeypatch):
    class AbortAfterFirst:
        def __init__(self): self.calls = 0
        def is_set(self):
            self.calls += 1
            return self.calls > 1

    class FakeResponse:
        def __enter__(self): return self
        def __exit__(self, *a): return False
        def read(self): return b"x"

    monkeypatch.setattr("specimux_suite.photos.urllib.request.urlopen",
                        lambda req, timeout=None: FakeResponse())
    monkeypatch.setattr("specimux_suite.photos.time.sleep", lambda s: None)

    taxa = {f"s{i}": {"photos": [{"id": i, "url": f"https://a/photos/{i}/square.jpg"}]}
            for i in range(4)}
    n = prefetch_photos(taxa, tmp_path / "photos", abort=AbortAfterFirst())
    assert n == 1


# --- genus lineages ---

from specimux_suite.inat import _pick_genus_match, fetch_genus_lineages


def _taxon(id, name, rank="genus", iconic="Fungi", obs=100, active=True, ancestors=None):
    return {"id": id, "name": name, "rank": rank, "iconic_taxon_name": iconic,
            "observations_count": obs, "is_active": active,
            "ancestor_ids": ancestors or []}


def test_pick_genus_match_exact_fungi_most_observed():
    results = [
        _taxon(1, "Morus", iconic="Plantae", obs=200000),
        _taxon(2, "Morus", iconic="Aves", obs=32000),
        _taxon(3, "Fatoua", iconic="Plantae", obs=12000),  # fuzzy hit
        _taxon(4, "Morus", iconic="Fungi", obs=5),
    ]
    assert _pick_genus_match(results, "Morus")["id"] == 4  # Fungi preferred
    assert _pick_genus_match(results[:3], "Morus")["id"] == 1  # else most observed
    assert _pick_genus_match(results, "Nonexistent") is None
    inactive = [_taxon(9, "Russula", active=False)]
    assert _pick_genus_match(inactive, "Russula") is None


def test_fetch_genus_lineages(tmp_path, monkeypatch):
    search_payload = {"results": [
        _taxon(48339, "Russula", ancestors=[48460, 47170, 50814, 48339]),
    ]}
    details_payload = {"results": [
        {"id": 48460, "rank": "stateofmatter", "name": "Life"},
        {"id": 47170, "rank": "kingdom", "name": "Fungi"},
        {"id": 50814, "rank": "family", "name": "Russulaceae"},
        {"id": 48339, "rank": "genus", "name": "Russula"},
    ]}

    class FakeResponse:
        def __init__(self, payload): self.payload = payload
        def __enter__(self): return self
        def __exit__(self, *a): return False
        def read(self): return json.dumps(self.payload).encode()

    def fake_urlopen(req, timeout=None):
        return FakeResponse(search_payload if "q=" in req.full_url else details_payload)

    monkeypatch.setattr("specimux_suite.inat.urllib.request.urlopen", fake_urlopen)
    monkeypatch.setattr("specimux_suite.inat.time.sleep", lambda s: None)

    result = fetch_genus_lineages(["Russula"], cache_dir=tmp_path)
    assert [e["name"] for e in result["Russula"]] == ["Life", "Fungi", "Russulaceae", "Russula"]
    assert result["Russula"][-1]["rank"] == "genus"

    # Cached now: a second call must not hit the network
    monkeypatch.setattr("specimux_suite.inat.urllib.request.urlopen",
                        lambda *a, **k: (_ for _ in ()).throw(AssertionError("network hit")))
    again = fetch_genus_lineages(["Russula"], cache_dir=tmp_path)
    assert again["Russula"] == result["Russula"]


def test_fetch_genus_lineages_unresolved_cached_as_empty(tmp_path, monkeypatch):
    class FakeResponse:
        def __enter__(self): return self
        def __exit__(self, *a): return False
        def read(self): return json.dumps({"results": []}).encode()

    monkeypatch.setattr("specimux_suite.inat.urllib.request.urlopen",
                        lambda req, timeout=None: FakeResponse())
    monkeypatch.setattr("specimux_suite.inat.time.sleep", lambda s: None)

    result = fetch_genus_lineages(["Notagenus"], cache_dir=tmp_path)
    assert result["Notagenus"] == []
    cached = json.loads((tmp_path / "inat_lineage_cache.json").read_text())
    assert cached["notagenus"] == []


def test_parse_first_identification():
    from specimux_suite.inat import _parse_first_identification
    obs = {"identifications": [
        {"created_at": "2026-08-30T10:00:00", "taxon": {"name": "Amanita elliptosperma"},
         "user": {"login": "expert"}},
        {"created_at": "2026-08-29T14:00:00", "taxon": {"name": "Amanita"},
         "user": {"login": "forayer"}},
    ]}
    assert _parse_first_identification(obs) == {"name": "Amanita", "login": "forayer"}
    assert _parse_first_identification({"identifications": []}) == {}
    assert _parse_first_identification({}) == {}


def test_apply_corrections_overrides_name_derived_ids():
    from specimux_suite.inat import apply_corrections
    ids = {"specA--iNat111": "111", "specB--iNat222": "222"}
    corrections = {"specA--iNat111": {"old": "111", "new": "999"}}
    assert apply_corrections(ids, corrections) == {"specA--iNat111": "999",
                                                   "specB--iNat222": "222"}
    assert apply_corrections(ids, {}) is ids  # no copy when nothing to do
