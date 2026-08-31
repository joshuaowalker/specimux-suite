"""Prefetch iNaturalist observation photos into a local disk cache.

The /present highlights screen shows full-bleed specimen photos. Venue Wi-Fi
is the weak link at a live event, so each observation's first photo is pulled
once in the background and served locally by the web server (mounted at
/photos); the client falls back to the iNat URL for anything not yet cached.
"""

import logging
import re
import time
import urllib.error
import urllib.request
from pathlib import Path

logger = logging.getLogger(__name__)

# 1024px max dimension — full-bleed quality on a projector, ~550KB each.
PHOTO_SIZE = "large"
_FETCH_DELAY_S = 0.25


def photo_cache_dir(output_dir: Path) -> Path:
    return Path(output_dir) / "inat_photos"


def cached_photo_name(photo_id, size: str = PHOTO_SIZE) -> str:
    return f"{photo_id}_{size}.jpg"


def photo_size_url(url: str, size: str) -> str:
    """Rewrite a photo URL to another size variant.

    The API returns the square-thumbnail URL; substituting the size name in
    the path works on both photo hosts (open-data S3 and static.inaturalist.org).
    """
    return re.sub(r"/square\.", f"/{size}.", url, count=1)


def first_photos(taxa: dict) -> list[dict]:
    """Each observation's first photo from a fetch_community_taxa result.

    Entries are shared between specimens of the same observation, so dedupe
    by photo id. Returns [{id, url}].
    """
    seen = set()
    result = []
    for entry in taxa.values():
        photos = entry.get("photos") or []
        if not photos:
            continue
        p = photos[0]
        if p["id"] in seen:
            continue
        seen.add(p["id"])
        result.append({"id": p["id"], "url": p["url"]})
    return result


def prefetch_photos(
    taxa: dict,
    cache_dir: Path,
    abort=None,
    size: str = PHOTO_SIZE,
) -> int:
    """Download each observation's first photo into cache_dir.

    Skips photos already on disk, writes atomically (tmp + rename), and
    checks `abort` between downloads so shutdown isn't held up. Individual
    failures are logged and skipped. Returns the number downloaded.
    """
    photos = first_photos(taxa)
    if not photos:
        return 0

    cache_dir.mkdir(parents=True, exist_ok=True)
    to_fetch = [p for p in photos if not (cache_dir / cached_photo_name(p["id"], size)).exists()]
    if not to_fetch:
        return 0

    logger.info(f"Prefetching {len(to_fetch)} iNaturalist photos to {cache_dir}")
    fetched = 0
    for p in to_fetch:
        if abort is not None and abort.is_set():
            logger.info(f"Photo prefetch aborted (shutdown) after {fetched} photos")
            return fetched
        url = photo_size_url(p["url"], size)
        dest = cache_dir / cached_photo_name(p["id"], size)
        tmp = dest.with_suffix(".tmp")
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "specimux-suite/0.1"})
            with urllib.request.urlopen(req, timeout=30) as resp:
                tmp.write_bytes(resp.read())
            tmp.rename(dest)
            fetched += 1
        except (urllib.error.URLError, OSError) as e:
            logger.warning(f"Failed to fetch photo {p['id']}: {e}")
            tmp.unlink(missing_ok=True)
        time.sleep(_FETCH_DELAY_S)

    logger.info(f"Photo prefetch complete: {fetched} downloaded, "
                f"{len(photos) - len(to_fetch)} already cached")
    return fetched
