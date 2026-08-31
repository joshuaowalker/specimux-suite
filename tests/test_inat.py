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
                "iconic_taxon": "Fungi", "photos": [], "observer": {}},
        "222": {"name": "Russula", "genus": "Russula", "iconic_taxon": "Fungi"},  # legacy
        "333": {"name": "", "genus": "", "iconic_taxon": "",
                "photos": [{"id": 9, "url": "u", "license_code": None, "attribution": ""}],
                "observer": {"login": "x", "name": ""}},
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
