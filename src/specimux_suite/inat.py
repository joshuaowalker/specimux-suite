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


def fetch_community_taxa(
    inat_ids: dict[str, str],
    cache_dir: Path | None = None,
) -> dict[str, str]:
    """Batch-fetch community taxon names for iNaturalist observations.

    Args:
        inat_ids: {specimen_id: observation_id}
        cache_dir: directory for persistent cache file

    Returns:
        {specimen_id: taxon_name} for observations that have a community taxon.
    """
    if not inat_ids:
        return {}

    # Load cache
    cache: dict[str, str] = {}
    cache_file = cache_dir / "inat_taxon_cache.json" if cache_dir else None
    if cache_file and cache_file.exists():
        try:
            cache = json.loads(cache_file.read_text())
        except (json.JSONDecodeError, OSError):
            pass

    result: dict[str, str] = {}
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
                if taxon_name:
                    cache[obs_id] = taxon_name
                    for specimen_id in obs_id_to_specimens.get(obs_id, []):
                        result[specimen_id] = taxon_name

        except (urllib.error.URLError, OSError, json.JSONDecodeError, KeyError) as e:
            logger.warning(f"Failed to fetch iNaturalist batch: {e}")

        # Rate limiting between batches
        if i + MAX_BATCH_SIZE < len(unique_obs_ids):
            time.sleep(1)

    # Save cache
    if cache_file:
        try:
            cache_file.parent.mkdir(parents=True, exist_ok=True)
            cache_file.write_text(json.dumps(cache))
        except OSError as e:
            logger.warning(f"Failed to save iNat cache: {e}")

    return result
