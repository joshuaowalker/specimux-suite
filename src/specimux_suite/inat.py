"""Fetch community taxon information from iNaturalist observations API."""

import json
import logging
import re
import time
import urllib.request
import urllib.error
from pathlib import Path

logger = logging.getLogger(__name__)

MAX_BATCH_SIZE = 200
API_URL = "https://api.inaturalist.org/v1/observations"
TAXA_API_URL = "https://api.inaturalist.org/v1/taxa"

# Ranks where the first word of the name is the genus
_GENUS_IN_NAME_RANKS = frozenset({
    "species", "genus", "subspecies", "variety", "form", "hybrid",
})


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
) -> dict[str, dict]:
    """Batch-fetch community taxon names for iNaturalist observations.

    Args:
        inat_ids: {specimen_id: observation_id}
        cache_dir: directory for persistent cache file

    Returns:
        {specimen_id: {"name": taxon_name, "genus": genus_name}}
        for observations that have a community taxon.
    """
    if not inat_ids:
        return {}

    # Load cache — handles both old (string) and new (dict) formats.
    # Legacy entries missing genus or iconic_taxon are discarded for re-fetch.
    cache: dict[str, dict] = {}
    cache_file = cache_dir / "inat_taxon_cache.json" if cache_dir else None
    if cache_file and cache_file.exists():
        try:
            raw = json.loads(cache_file.read_text())
            for obs_id, val in raw.items():
                if isinstance(val, dict) and val.get("genus") and "iconic_taxon" in val:
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

    # First pass: fetch observations and extract taxa
    # Track infrageneric taxa that need genus resolution
    needs_genus_obs: dict[int, list[str]] = {}  # {taxon_id: [obs_ids]}
    ancestor_ids_by_taxon: dict[int, list[int]] = {}  # {taxon_id: [ancestor_ids]}

    for i in range(0, len(unique_obs_ids), MAX_BATCH_SIZE):
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
                taxon_name = taxon.get("name")
                if not taxon_name:
                    continue
                rank = taxon.get("rank", "")
                if rank in _GENUS_IN_NAME_RANKS:
                    genus = taxon_name.split()[0]
                else:
                    # Infrageneric rank — need to resolve genus via taxa API
                    genus = ""
                    taxon_id = taxon.get("id")
                    if taxon_id:
                        needs_genus_obs.setdefault(taxon_id, []).append(obs_id)
                        if taxon_id not in ancestor_ids_by_taxon:
                            ancestor_ids_by_taxon[taxon_id] = [
                                aid for aid in (taxon.get("ancestor_ids") or [])
                                if aid != taxon_id
                            ]

                iconic_taxon = taxon.get("iconic_taxon_name") or ""
                entry = {"name": taxon_name, "genus": genus, "iconic_taxon": iconic_taxon}
                cache[obs_id] = entry
                for specimen_id in obs_id_to_specimens.get(obs_id, []):
                    result[specimen_id] = entry

        except (urllib.error.URLError, OSError, json.JSONDecodeError, KeyError) as e:
            logger.warning(f"Failed to fetch iNaturalist batch: {e}")

        # Rate limiting between batches
        if i + MAX_BATCH_SIZE < len(unique_obs_ids):
            time.sleep(1)

    # Second pass: resolve genera for infrageneric taxa by fetching their ancestors
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
            cache_file.write_text(json.dumps(cache))
        except OSError as e:
            logger.warning(f"Failed to save iNat cache: {e}")

    return result
