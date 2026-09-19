"""Fetch field-ID information from Mushroom Observer observations.

Mushroom Observer (MO) sits alongside iNaturalist as a source of field IDs,
observer credits and photos. A specimen ID carrying an `MO<digits>` tag
(e.g. `ONT03.96-H12-GS23-204-MO346513`) is resolved through MO's API2 into
the same record shape `inat.fetch_community_taxa` produces, so state, the
event mirrors and the pages need no provider-specific fields.

Deliberate scope (all of MO is Fungi, and MO volume per run is low):

- No observation-ID typo audit or corrections for MO specimens.
- Higher-rank taxonomy comes from iNaturalist: the MO consensus is mapped
  onto iNat taxonomy at genus level (`inat.fetch_genus_lineages`), so the
  record's `ancestors` are iNat taxon ids and taxonomy-level agreement
  works unchanged. Only MO's observations endpoint is used.

API notes (https://mushroomobserver.org/api-docs/): anonymous JSON reads;
`detail=high` pages at 100; a batch containing ANY nonexistent id fails as
a whole (`API2::ObjectNotFoundByID`), so missing ids are parsed out of the
error and the batch retried; anonymous traffic is limited to 20 requests a
minute — wait the longer of 5 s or the last response's `run_time`.
"""

import json
import logging
import re
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

from .inat import MAX_PHOTOS_PER_OBSERVATION, fetch_genus_lineages

logger = logging.getLogger(__name__)

API_URL = "https://mushroomobserver.org/api2/observations"
IMAGE_HOST = "https://images.mushroomobserver.org"
OBSERVATION_URL = "https://mushroomobserver.org/obs/{id}"
MAX_BATCH_SIZE = 100
MIN_REQUEST_INTERVAL_S = 5.0
_USER_AGENT = "specimux-suite/0.2"

# Photo size variants served by the image host: thumb (160), 320, 640, 960,
# 1280, orig. `thumb` plays the role of iNat's `square`; 960 ≈ iNat `large`.
THUMB_SIZE = "thumb"
LARGE_SIZE = "960"

# MO ranks at or below genus, where the name's first token is the genus.
# Names like "Clitocybe sensu lato" sit at rank suborder and get no genus.
_GENUS_OR_BELOW_RANKS = frozenset({
    "genus", "subgenus", "section", "subsection", "stirps", "series",
    "species", "subspecies", "variety", "form", "group",
})

# `MO` followed by at least four digits, not glued to a preceding letter
# (so "DEMO123" can't match). A preceding digit is allowed: a few real
# sheets drop the separator ("CM24-08210MO523685"). The digit minimum keeps
# provisional-name suffixes such as "fasciculare-MO01" (state codes in
# MyCoMap-style names) from being read as observation ids; MO ids below
# 1000 date from 2006 and don't appear on modern sheets. Mirrored by
# observationRef in web/static/derived.js.
_MO_ID_RE = re.compile(r"(?<![A-Za-z])MO(\d{4,})")
_MISSING_ID_RE = re.compile(r"#(\d+)")


def extract_mo_ids(specimens: list[dict]) -> dict[str, str]:
    """Extract Mushroom Observer observation IDs from specimen IDs.

    Returns {specimen_id: mo_observation_id} for specimens that have one.
    """
    result = {}
    for s in specimens:
        m = _MO_ID_RE.search(s["specimen_id"])
        if m:
            result[s["specimen_id"]] = m.group(1)
    return result


def observation_url(obs_id) -> str:
    return OBSERVATION_URL.format(id=obs_id)


def image_url(image_id, size: str) -> str:
    return f"{IMAGE_HOST}/{size}/{image_id}.jpg"


def _parse_photos(obs: dict) -> list[dict]:
    """Display-ready photo records: {id, url, large_url, license_code, attribution}.

    `id` is prefixed ("mo<image id>") so cached files can't collide with iNat
    photo ids in the shared photo cache. `url` is the thumbnail; `large_url`
    the projector-quality variant (iNat records derive it by substitution,
    MO records carry it explicitly).
    """
    photos = []
    seen = set()
    candidates = []
    if obs.get("primary_image"):
        candidates.append(obs["primary_image"])
    candidates.extend(obs.get("images") or [])
    for img in candidates:
        img_id = img.get("id")
        if not img_id or img_id in seen:
            continue
        seen.add(img_id)
        holder = (img.get("copyright_holder") or "").strip()
        license_name = (img.get("license") or "").strip()
        attribution = f"(c) {holder}" if holder else ""
        if license_name:
            attribution = f"{attribution}, {license_name}" if attribution else license_name
        photos.append({
            "id": f"mo{img_id}",
            "url": image_url(img_id, THUMB_SIZE),
            "large_url": image_url(img_id, LARGE_SIZE),
            "license_code": license_name or None,
            "attribution": attribution,
        })
        if len(photos) >= MAX_PHOTOS_PER_OBSERVATION:
            break
    return photos


def _parse_observer(obs: dict) -> dict:
    owner = obs.get("owner") or {}
    login = owner.get("login_name") or ""
    if not login:
        return {}
    return {"login": login, "name": (owner.get("legal_name") or "").strip()}


def _parse_first_naming(obs: dict) -> dict:
    """The earliest naming — the field ID. Naming ids are monotonic, so the
    lowest id is the first proposal (the high-detail record carries no
    timestamps). Returns {name, login} or {}."""
    namings = [n for n in (obs.get("namings") or []) if isinstance(n, dict)]
    if not namings:
        return {}
    first = min(namings, key=lambda n: n.get("id") or float("inf"))
    name = (first.get("name") or {}).get("name") or ""
    if not name:
        return {}
    return {"name": name, "login": (first.get("owner") or {}).get("login_name") or ""}


def _parse_consensus(obs: dict) -> tuple[str, str]:
    """(consensus name, genus) — genus only when the rank is genus or below."""
    consensus = obs.get("consensus") or {}
    name = (consensus.get("name") or "").strip()
    if not name:
        return "", ""
    rank = (consensus.get("rank") or "").lower()
    genus = name.split()[0] if rank in _GENUS_OR_BELOW_RANKS else ""
    return name, genus


def _request(ids: list[str]) -> dict:
    query = urllib.parse.urlencode({
        "id": ",".join(ids), "detail": "high", "format": "json",
    })
    req = urllib.request.Request(
        f"{API_URL}?{query}", headers={"User-Agent": _USER_AGENT})
    try:
        with urllib.request.urlopen(req, timeout=60) as resp:
            return json.loads(resp.read())
    except urllib.error.HTTPError as e:
        # API2 reports errors as JSON with a 4xx status; keep the body so
        # the missing-id case can be handled by the caller.
        try:
            return json.loads(e.read())
        except (json.JSONDecodeError, OSError):
            raise e


def _fetch_batch(ids: list[str], abort=None) -> tuple[list[dict], set[str], float]:
    """Fetch one batch, retrying without ids MO reports as nonexistent.

    Returns (observation records, missing ids, last run_time).
    """
    remaining = list(ids)
    missing: set[str] = set()
    run_time = 0.0
    while remaining:
        if abort is not None and abort.is_set():
            break
        data = _request(remaining)
        run_time = float(data.get("run_time") or 0.0)
        errors = data.get("errors") or []
        if not errors:
            return data.get("results") or [], missing, run_time
        not_found = set()
        for err in errors:
            if "ObjectNotFoundByID" in str(err.get("code", "")):
                m = _MISSING_ID_RE.search(str(err.get("details", "")))
                if m and m.group(1) in remaining:
                    not_found.add(m.group(1))
        if not not_found:
            details = "; ".join(str(e.get("details") or e.get("code")) for e in errors)
            raise RuntimeError(f"Mushroom Observer API error: {details}")
        missing |= not_found
        remaining = [i for i in remaining if i not in not_found]
        logger.warning(
            f"Mushroom Observer: {', '.join(sorted(not_found))} not found — retrying batch without")
        time.sleep(max(MIN_REQUEST_INTERVAL_S, run_time))
    return [], missing, run_time


def fetch_mo_taxa(
    mo_ids: dict[str, str],
    cache_dir: Path | None = None,
    abort=None,
    progress=None,
    lineage_progress=None,
    unresolved: list | None = None,
) -> dict[str, dict]:
    """Batch-fetch field IDs for Mushroom Observer observations.

    Args:
        mo_ids: {specimen_id: mo_observation_id}
        cache_dir: directory for the persistent cache (mo_taxon_cache.json;
            genus lineages share inat_lineage_cache.json)
        abort: optional threading.Event checked between requests
        progress: optional callback(done, total) over observations
        lineage_progress: optional callback(done, total) over the genera
            being mapped onto iNat taxonomy (the second, slower phase)
        unresolved: optional list; appended with {specimen_id, obs_id} for
            every observation MO reports as nonexistent (a mistyped or
            deleted id — there is no audit for MO, so the admin page lists
            these). Batches lost to a network failure are not reported:
            they are retried next run.

    Returns {specimen_id: record} in the `fetch_community_taxa` shape
    ({name, genus, iconic_taxon, ancestors, photos, observer, first_id})
    plus `provider: "mo"`, for every observation found.
    """
    if not mo_ids:
        return {}

    cache: dict[str, dict] = {}
    cache_file = cache_dir / "mo_taxon_cache.json" if cache_dir else None
    if cache_file and cache_file.exists():
        try:
            raw = json.loads(cache_file.read_text(encoding="utf-8"))
            for obs_id, val in raw.items():
                if (isinstance(val, dict) and "iconic_taxon" in val
                        and "photos" in val and "ancestors" in val
                        and "first_id" in val):
                    cache[obs_id] = val
        except (json.JSONDecodeError, OSError):
            pass

    result: dict[str, dict] = {}
    obs_id_to_specimens: dict[str, list[str]] = {}
    for specimen_id, obs_id in mo_ids.items():
        if obs_id in cache:
            result[specimen_id] = cache[obs_id]
        else:
            obs_id_to_specimens.setdefault(obs_id, []).append(specimen_id)

    unique_obs_ids = list(obs_id_to_specimens)
    if not unique_obs_ids:
        return result

    logger.info(f"Fetching field IDs for {len(unique_obs_ids)} Mushroom Observer observations")
    if progress:
        progress(0, len(unique_obs_ids))

    fetched: dict[str, dict] = {}
    not_found: list[str] = []
    last_run_time = 0.0
    for i in range(0, len(unique_obs_ids), MAX_BATCH_SIZE):
        if abort is not None and abort.is_set():
            logger.info("Mushroom Observer fetch aborted (shutdown)")
            break
        if i > 0:
            # The documented courtesy: wait the longer of 5 s or the last
            # response's run_time before the next request.
            time.sleep(max(MIN_REQUEST_INTERVAL_S, last_run_time))
        batch = unique_obs_ids[i: i + MAX_BATCH_SIZE]
        try:
            records, missing, last_run_time = _fetch_batch(batch, abort=abort)
        except (urllib.error.URLError, OSError, json.JSONDecodeError, RuntimeError) as e:
            logger.warning(f"Failed to fetch Mushroom Observer batch: {e}")
            records, missing = [], set()
            batch = []  # transient: nothing in this batch is "not found"
        got = set()
        for obs in records:
            if isinstance(obs, dict) and obs.get("id") is not None:
                fetched[str(obs["id"])] = obs
                got.add(str(obs["id"]))
        # Anything a successful batch didn't return is gone on MO's side:
        # reported nonexistent, or silently absent (e.g. deleted).
        if batch and not (abort is not None and abort.is_set()):
            not_found.extend(o for o in batch if o not in got)
        if progress:
            progress(min(i + MAX_BATCH_SIZE, len(unique_obs_ids)), len(unique_obs_ids))
    if not_found:
        logger.warning(f"Mushroom Observer: {len(not_found)} observation id(s) not found: "
                       + ", ".join(not_found))
        if unresolved is not None:
            for obs_id in not_found:
                for specimen_id in obs_id_to_specimens.get(obs_id, []):
                    unresolved.append({"specimen_id": specimen_id, "obs_id": obs_id})

    # Map the MO consensus onto iNat taxonomy at genus level: the record's
    # ancestors are the iNat lineage ids of the consensus genus (cached in
    # inat_lineage_cache.json, shared with the reference-hit lineages).
    entries: dict[str, dict] = {}
    for obs_id, obs in fetched.items():
        name, genus = _parse_consensus(obs)
        entries[obs_id] = {
            "provider": "mo",
            "name": name,
            "genus": genus,
            "iconic_taxon": "Fungi" if name else "",
            "ancestors": [],
            "photos": _parse_photos(obs),
            "observer": _parse_observer(obs),
            "first_id": _parse_first_naming(obs),
        }
    # MO files provisional names like "Boletaceae sp. 'AL01'" at rank
    # species, so the first token isn't always a genus. An explicit
    # no-match from iNat (lineage []) means the token is not a genus there:
    # resolve it at whatever rank iNat has it and treat the observation the
    # way an iNat family-level community taxon is treated — no genus, but a
    # lineage for taxonomy-level agreement — rather than have every real
    # hit read as off-target. A token absent from the result is a transient
    # fetch failure: keep the genus and retry next run.
    genera = sorted({e["genus"] for e in entries.values() if e["genus"]})
    transient: set[str] = set()  # obs ids whose lineage lookup failed: don't cache
    if genera and not (abort is not None and abort.is_set()):
        lineages = fetch_genus_lineages(genera, cache_dir=cache_dir, abort=abort,
                                        progress=lineage_progress)
        not_genera = sorted({g for g in genera if lineages.get(g) == []})
        any_rank = (fetch_genus_lineages(not_genera, cache_dir=cache_dir, abort=abort, rank=None)
                    if not_genera and not (abort is not None and abort.is_set()) else {})
        for obs_id, entry in entries.items():
            lineage = lineages.get(entry["genus"])
            if lineage is None:
                transient.add(obs_id)
                continue
            if not lineage:
                token = entry["genus"]
                entry["genus"] = ""
                lineage = any_rank.get(token) or []
                found = f"{lineage[-1].get('rank')} on iNaturalist" if lineage else "not on iNaturalist"
                logger.info(f"Mushroom Observer name {entry['name']!r}: {token!r} is not a genus ({found})")
            entry["ancestors"] = [item["id"] for item in lineage if item.get("id") is not None]

    cached_any = False
    for obs_id, entry in entries.items():
        # An entry whose genus never reached iNat (transient failure, or
        # shutdown before the lineage phase) is served this run but not
        # cached, so the next run completes it instead of freezing empty
        # ancestors forever.
        if obs_id not in transient and (not entry["genus"] or entry["ancestors"]):
            cache[obs_id] = entry
            cached_any = True
        for specimen_id in obs_id_to_specimens.get(obs_id, []):
            result[specimen_id] = entry

    if cache_file and cached_any:
        try:
            cache_file.parent.mkdir(parents=True, exist_ok=True)
            cache_file.write_text(json.dumps(cache), encoding="utf-8")
        except OSError as e:
            logger.warning(f"Failed to save Mushroom Observer cache: {e}")

    return result
