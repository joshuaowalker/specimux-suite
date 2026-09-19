"""Fetch community taxon information from iNaturalist observations API."""

import json
import logging
import re
import time
import urllib.request
import urllib.error
import urllib.parse
from pathlib import Path

logger = logging.getLogger(__name__)

MAX_BATCH_SIZE = 200
API_URL = "https://api.inaturalist.org/v1/observations"
TAXA_API_URL = "https://api.inaturalist.org/v1/taxa"

# Ranks where the first word of the name is the genus
_GENUS_IN_NAME_RANKS = frozenset({
    "species", "genus", "subspecies", "variety", "form", "hybrid",
})

# Photos kept per observation in the cache/event payload. The first photo is
# usually the observer's best shot; more than a few just bloats the taxa event.
MAX_PHOTOS_PER_OBSERVATION = 3


def _parse_observation_photos(obs: dict) -> list[dict]:
    """Extract display-ready photo records from an observations API result.

    Each record: {id, url, license_code, attribution}. `url` is the square
    thumbnail; other sizes (small/medium/large/original) are reachable by
    substituting the size name in the URL — this works on both the open-data
    S3 host (CC-licensed photos) and static.inaturalist.org (ARR photos).
    """
    photos = []
    for p in obs.get("photos") or []:
        if not p.get("id") or not p.get("url"):
            continue
        photos.append({
            "id": p["id"],
            "url": p["url"],
            "license_code": p.get("license_code"),
            "attribution": p.get("attribution") or "",
        })
        if len(photos) >= MAX_PHOTOS_PER_OBSERVATION:
            break
    return photos


def _parse_observation_observer(obs: dict) -> dict:
    """Extract the observer as {login, name} (name may be empty)."""
    user = obs.get("user") or {}
    login = user.get("login") or ""
    if not login:
        return {}
    return {"login": login, "name": user.get("name") or ""}


def _parse_first_identification(obs: dict) -> dict:
    """The observation's earliest identification — the true field ID.

    At a foray the first iNat ID is made in the field; later ones are
    refinements at the display tables. Returns {name, login} or {}.
    """
    idents = obs.get("identifications") or []
    if not idents:
        return {}
    first = min(idents, key=lambda i: i.get("created_at") or "~")
    taxon = first.get("taxon") or {}
    if not taxon.get("name"):
        return {}
    return {
        "name": taxon["name"],
        "login": (first.get("user") or {}).get("login") or "",
    }


def extract_inat_ids(specimens: list[dict]) -> dict[str, str]:
    """Extract iNaturalist observation IDs from specimen IDs.

    Returns {specimen_id: inat_observation_id} for specimens that have one.
    """
    result = {}
    for s in specimens:
        m = re.search(r"iNat(\d+)", s["specimen_id"])
        if m:
            result[s["specimen_id"]] = m.group(1)
    return result


def apply_corrections(inat_ids: dict[str, str], corrections: dict[str, dict]) -> dict[str, str]:
    """Override name-derived observation IDs with admin-accepted corrections.

    A specimen's name permanently embeds its (possibly mistyped) obs id, so
    every consumer that extracts IDs from names must remap through the
    accepted corrections — otherwise a restart's taxa fetch resurrects the
    wrong observation and clobbers the healed field ID.
    """
    if not corrections:
        return inat_ids
    return {
        sid: (corrections.get(sid) or {}).get("new") or obs_id
        for sid, obs_id in inat_ids.items()
    }


def _resolve_genera(ancestor_ids_by_taxon: dict[int, list[int]]) -> dict[int, str]:
    """Resolve genus names for infrageneric taxa using their ancestor IDs.

    The observations API provides ancestor_ids (numeric) but not full ancestor
    objects. We batch-fetch those ancestor IDs via the taxa endpoint to find
    which one has rank=genus.

    Args:
        ancestor_ids_by_taxon: {taxon_id: [ancestor_ids from observations API]}

    Returns:
        {taxon_id: genus_name}
    """
    result: dict[int, str] = {}
    if not ancestor_ids_by_taxon:
        return result

    # Collect all unique ancestor IDs we need to look up
    all_ancestor_ids: set[int] = set()
    for aids in ancestor_ids_by_taxon.values():
        all_ancestor_ids.update(aids)

    # Batch-fetch ancestor taxa to find which are genera
    genus_by_id: dict[int, str] = {}  # {ancestor_taxon_id: genus_name}
    ancestor_list = list(all_ancestor_ids)

    for i in range(0, len(ancestor_list), MAX_BATCH_SIZE):
        batch = ancestor_list[i : i + MAX_BATCH_SIZE]
        ids_str = ",".join(str(tid) for tid in batch)
        url = f"{TAXA_API_URL}?per_page={MAX_BATCH_SIZE}&id={ids_str}"

        try:
            req = urllib.request.Request(url, headers={"User-Agent": "specimux-suite/0.1"})
            with urllib.request.urlopen(req, timeout=30) as resp:
                data = json.loads(resp.read())

            for taxon in data.get("results", []):
                if taxon.get("rank") == "genus":
                    genus_by_id[taxon["id"]] = taxon.get("name", "")

        except (urllib.error.URLError, OSError, json.JSONDecodeError, KeyError) as e:
            logger.warning(f"Failed to fetch iNaturalist taxa batch: {e}")

        if i + MAX_BATCH_SIZE < len(ancestor_list):
            time.sleep(1)

    # Map back: for each infrageneric taxon, find its genus ancestor
    for taxon_id, aids in ancestor_ids_by_taxon.items():
        for aid in aids:
            if aid in genus_by_id:
                result[taxon_id] = genus_by_id[aid]
                break

    return result


def fetch_community_taxa(
    inat_ids: dict[str, str],
    cache_dir: Path | None = None,
    abort=None,
    progress=None,
) -> dict[str, dict]:
    """Batch-fetch community taxon names for iNaturalist observations.

    Args:
        inat_ids: {specimen_id: observation_id}
        cache_dir: directory for persistent cache file
        abort: optional threading.Event; checked between batches so a
            shutting-down pipeline stops making network calls promptly

    Returns:
        {specimen_id: {"name": taxon_name, "genus": genus_name,
                       "iconic_taxon": ..., "photos": [...], "observer": {...}}}
        for every observation found. `name`/`genus` are empty when the
        observation has no community taxon yet — the entry still carries
        photos and observer for display.
    """
    if not inat_ids:
        return {}

    # Load cache — handles both old (string) and new (dict) formats.
    # Legacy entries missing genus, iconic_taxon, photos, or ancestors are
    # discarded for re-fetch. A genus-less entry is only valid when the observation had no
    # community taxon at all (name empty) — otherwise it's a failed genus
    # resolution worth retrying.
    cache: dict[str, dict] = {}
    cache_file = cache_dir / "inat_taxon_cache.json" if cache_dir else None
    if cache_file and cache_file.exists():
        try:
            raw = json.loads(cache_file.read_text(encoding="utf-8"))
            for obs_id, val in raw.items():
                if (isinstance(val, dict) and "iconic_taxon" in val
                        and "photos" in val and "ancestors" in val
                        and "first_id" in val
                        and (val.get("genus") or not val.get("name"))):
                    cache[obs_id] = val
                # else: discard — will be re-fetched
        except (json.JSONDecodeError, OSError):
            pass

    result: dict[str, dict] = {}
    to_fetch: list[tuple[str, str]] = []  # (specimen_id, obs_id)

    for specimen_id, obs_id in inat_ids.items():
        if obs_id in cache:
            result[specimen_id] = cache[obs_id]
        else:
            to_fetch.append((specimen_id, obs_id))

    if not to_fetch:
        return result

    # Batch fetch
    obs_id_to_specimens: dict[str, list[str]] = {}
    for specimen_id, obs_id in to_fetch:
        obs_id_to_specimens.setdefault(obs_id, []).append(specimen_id)

    unique_obs_ids = list(obs_id_to_specimens.keys())
    logger.info(f"Fetching community taxon for {len(unique_obs_ids)} iNaturalist observations")
    if progress:
        progress(0, len(unique_obs_ids))

    # First pass: fetch observations and extract taxa
    # Track infrageneric taxa that need genus resolution
    needs_genus_obs: dict[int, list[str]] = {}  # {taxon_id: [obs_ids]}
    ancestor_ids_by_taxon: dict[int, list[int]] = {}  # {taxon_id: [ancestor_ids]}

    for i in range(0, len(unique_obs_ids), MAX_BATCH_SIZE):
        if abort is not None and abort.is_set():
            logger.info("iNaturalist fetch aborted (shutdown)")
            break
        batch = unique_obs_ids[i : i + MAX_BATCH_SIZE]
        ids_str = ",".join(batch)
        url = f"{API_URL}?per_page={MAX_BATCH_SIZE}&id={ids_str}"

        try:
            req = urllib.request.Request(url, headers={"User-Agent": "specimux-suite/0.1"})
            with urllib.request.urlopen(req, timeout=30) as resp:
                data = json.loads(resp.read())

            for obs in data.get("results", []):
                obs_id = str(obs["id"])
                taxon = obs.get("taxon") or {}
                taxon_name = taxon.get("name") or ""
                genus = ""
                if taxon_name:
                    rank = taxon.get("rank", "")
                    if rank in _GENUS_IN_NAME_RANKS:
                        genus = taxon_name.split()[0]
                    else:
                        # Infrageneric rank — need to resolve genus via taxa API
                        taxon_id = taxon.get("id")
                        if taxon_id:
                            needs_genus_obs.setdefault(taxon_id, []).append(obs_id)
                            if taxon_id not in ancestor_ids_by_taxon:
                                ancestor_ids_by_taxon[taxon_id] = [
                                    aid for aid in (taxon.get("ancestor_ids") or [])
                                    if aid != taxon_id
                                ]

                # Ancestor taxon ids (root→self, self included): the raw
                # material for taxonomy-level agreement with sequence IDs.
                ancestors = [aid for aid in (taxon.get("ancestor_ids") or [])]
                if taxon.get("id") and taxon["id"] not in ancestors:
                    ancestors.append(taxon["id"])

                iconic_taxon = taxon.get("iconic_taxon_name") or ""
                entry = {
                    "name": taxon_name,
                    "genus": genus,
                    "iconic_taxon": iconic_taxon,
                    "ancestors": ancestors,
                    "photos": _parse_observation_photos(obs),
                    "observer": _parse_observation_observer(obs),
                    "first_id": _parse_first_identification(obs),
                }
                cache[obs_id] = entry
                for specimen_id in obs_id_to_specimens.get(obs_id, []):
                    result[specimen_id] = entry

        except (urllib.error.URLError, OSError, json.JSONDecodeError, KeyError) as e:
            logger.warning(f"Failed to fetch iNaturalist batch: {e}")

        if progress:
            progress(min(i + MAX_BATCH_SIZE, len(unique_obs_ids)), len(unique_obs_ids))
        # Rate limiting between batches
        if i + MAX_BATCH_SIZE < len(unique_obs_ids):
            time.sleep(1)

    # Second pass: resolve genera for infrageneric taxa by fetching their ancestors
    if abort is not None and abort.is_set():
        needs_genus_obs = {}
    if needs_genus_obs:
        logger.info(f"Resolving genus for {len(needs_genus_obs)} infrageneric taxa")
        genera = _resolve_genera(ancestor_ids_by_taxon)
        for taxon_id, obs_ids in needs_genus_obs.items():
            genus = genera.get(taxon_id, "")
            for obs_id in obs_ids:
                if obs_id in cache:
                    cache[obs_id]["genus"] = genus
                    for specimen_id in obs_id_to_specimens.get(obs_id, []):
                        if specimen_id in result:
                            result[specimen_id]["genus"] = genus

    # Save cache
    if cache_file:
        try:
            cache_file.parent.mkdir(parents=True, exist_ok=True)
            cache_file.write_text(json.dumps(cache), encoding="utf-8")
        except OSError as e:
            logger.warning(f"Failed to save iNat cache: {e}")

    return result


# ---------------------------------------------------------------------------
# Genus lineages for taxonomy-level agreement between field IDs and sequence
# IDs. The reference-DB contract is deliberately just `name="..."` (users
# mint their own references), so the hit name's first token — the genus — is
# resolved against iNaturalist taxonomy rather than trusting any lineage a
# reference file might embed.
# ---------------------------------------------------------------------------

_LINEAGE_SEARCH_DELAY_S = 0.6


def _pick_genus_match(results: list[dict], genus: str, rank: str | None = "genus") -> dict | None:
    """Choose the right taxon for a name from taxa-search results.

    Search is fuzzy and genus names are homonymous across nomenclature codes
    (e.g. Morus the mulberry vs Morus the gannet), so: exact name match and
    active only, at `rank` (any rank when None), prefer Fungi, then the
    most-observed.
    """
    exact = [t for t in results
             if (t.get("name") or "").lower() == genus.lower()
             and (rank is None or t.get("rank") == rank) and t.get("is_active", True)]
    if not exact:
        return None
    exact.sort(key=lambda t: (
        (t.get("iconic_taxon_name") or "") != "Fungi",
        -(t.get("observations_count") or 0),
    ))
    return exact[0]


def fetch_genus_lineages(
    genera: list[str],
    cache_dir: Path | None = None,
    abort=None,
    progress=None,
    rank: str | None = "genus",
) -> dict[str, list[dict]]:
    """Resolve genus names to their iNaturalist lineages.

    Returns {genus_as_given: [{id, rank, name}, ...]} ordered root→genus
    (the genus itself is the last entry). A genus that can't be resolved
    maps to [] — cached too, so it isn't retried every run.

    `rank=None` resolves a name at whatever rank iNat has it (e.g. a family
    token like "Boletaceae" from a Mushroom Observer provisional name); those
    lookups are cached under an "any:" key so they never shadow the
    genus-rank entries the reference-hit lineages depend on.
    """
    cache: dict[str, list] = {}
    cache_file = cache_dir / "inat_lineage_cache.json" if cache_dir else None
    if cache_file and cache_file.exists():
        try:
            raw = json.loads(cache_file.read_text(encoding="utf-8"))
            cache = {k: v for k, v in raw.items() if isinstance(v, list)}
        except (json.JSONDecodeError, OSError):
            pass

    key_prefix = "" if rank == "genus" else f"{rank or 'any'}:"
    result: dict[str, list[dict]] = {}
    to_fetch: list[str] = []
    for genus in genera:
        key = key_prefix + genus.lower()
        if key in cache:
            result[genus] = cache[key]
        elif genus:
            to_fetch.append(genus)

    if not to_fetch:
        return result

    # Pass 1: search each genus for its taxon and ancestor ids
    if progress:
        progress(0, len(to_fetch))
    ancestor_ids_by_genus: dict[str, list[int]] = {}
    all_ancestor_ids: set[int] = set()
    for n, genus in enumerate(to_fetch):
        if abort is not None and abort.is_set():
            logger.info("iNaturalist lineage fetch aborted (shutdown)")
            return result
        rank_q = f"&rank={urllib.parse.quote(rank)}" if rank else ""
        url = f"{TAXA_API_URL}?q={urllib.parse.quote(genus)}{rank_q}&per_page=10"
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "specimux-suite/0.1"})
            with urllib.request.urlopen(req, timeout=30) as resp:
                data = json.loads(resp.read())
            taxon = _pick_genus_match(data.get("results", []), genus, rank)
            if taxon:
                aids = [aid for aid in (taxon.get("ancestor_ids") or [])]
                if taxon["id"] not in aids:
                    aids.append(taxon["id"])
                ancestor_ids_by_genus[genus] = aids
                all_ancestor_ids.update(aids)
            else:
                ancestor_ids_by_genus[genus] = []
        except (urllib.error.URLError, OSError, json.JSONDecodeError, KeyError) as e:
            logger.warning(f"Failed to search iNaturalist genus {genus}: {e}")
            continue  # transient: leave uncached so it retries next wave
        finally:
            if progress:
                progress(n + 1, len(to_fetch))
        time.sleep(_LINEAGE_SEARCH_DELAY_S)

    # Pass 2: batch-fetch rank+name for every ancestor id
    details: dict[int, dict] = {}
    ancestor_list = list(all_ancestor_ids)
    for i in range(0, len(ancestor_list), MAX_BATCH_SIZE):
        if abort is not None and abort.is_set():
            return result
        batch = ancestor_list[i : i + MAX_BATCH_SIZE]
        ids_str = ",".join(str(tid) for tid in batch)
        url = f"{TAXA_API_URL}?per_page={MAX_BATCH_SIZE}&id={ids_str}"
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "specimux-suite/0.1"})
            with urllib.request.urlopen(req, timeout=30) as resp:
                data = json.loads(resp.read())
            for t in data.get("results", []):
                details[t["id"]] = {"rank": t.get("rank", ""), "name": t.get("name", "")}
        except (urllib.error.URLError, OSError, json.JSONDecodeError, KeyError) as e:
            logger.warning(f"Failed to fetch iNaturalist taxa batch: {e}")
        if i + MAX_BATCH_SIZE < len(ancestor_list):
            time.sleep(1)

    for genus, aids in ancestor_ids_by_genus.items():
        lineage = [
            {"id": aid, **details[aid]} for aid in aids if aid in details
        ]
        # An unresolved search is final ([]); a resolved search whose detail
        # fetch failed entirely stays uncached to retry later.
        if lineage or not aids:
            result[genus] = lineage
            cache[key_prefix + genus.lower()] = lineage

    if cache_file:
        try:
            cache_file.parent.mkdir(parents=True, exist_ok=True)
            cache_file.write_text(json.dumps(cache), encoding="utf-8")
        except OSError as e:
            logger.warning(f"Failed to save iNat lineage cache: {e}")

    return result
