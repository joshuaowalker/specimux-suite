"""Prefetch observation photos (iNaturalist, Mushroom Observer) into a local disk cache.

The /present highlights screen shows full-bleed specimen photos. Venue Wi-Fi
is the weak link at a live event, so each observation's first photo is pulled
once in the background and served locally by the web server (mounted at
/photos); the client falls back to the provider's URL for anything not yet
cached. Photo records carry either a `large_url` (Mushroom Observer) or an
iNat-style `url` whose size name can be substituted.
"""

import logging
import re
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from .util import USER_AGENT
logger = logging.getLogger(__name__)

# 1024px max dimension — full-bleed quality on a projector, ~550KB each.
PHOTO_SIZE = "large"
# Photos come from iNat's S3/CDN hosts, not the rate-limited API, so a
# small worker pool with a per-worker pause is polite and warms a
# ~700-photo cache in about a minute instead of several.
_FETCH_DELAY_S = 0.1
_FETCH_WORKERS = 4
# Mushroom Observer serves sized images from its own nginx host. Its API
# docs limit anonymous traffic to MO without saying whether that covers
# the image host, so MO downloads are serialized at ~2/s to stay well
# inside any reading of the rule; iNat hosts keep the fast path.
_MO_PHOTO_HOST = "mushroomobserver.org"
_MO_FETCH_DELAY_S = 0.5
_mo_photo_lock = threading.Lock()


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
    """Each observation's first photo from a fetch_community_taxa / fetch_mo_taxa result.

    Entries are shared between specimens of the same observation, so dedupe
    by photo id. Returns [{id, url, large_url?}].
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
        rec = {"id": p["id"], "url": p["url"]}
        if p.get("large_url"):
            rec["large_url"] = p["large_url"]
        result.append(rec)
    return result


def prefetch_photos(
    taxa: dict,
    cache_dir: Path,
    abort=None,
    size: str = PHOTO_SIZE,
    progress=None,
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

    logger.info(f"Prefetching {len(to_fetch)} observation photos to {cache_dir}")
    if progress:
        progress(0, len(to_fetch))
    fetched = 0
    done = 0
    lock = threading.Lock()

    def fetch_one(p) -> None:
        nonlocal fetched, done
        if abort is not None and abort.is_set():
            return
        url = p.get("large_url") if size == PHOTO_SIZE and p.get("large_url") else photo_size_url(p["url"], size)
        dest = cache_dir / cached_photo_name(p["id"], size)
        # Distinct photo ids mean distinct dest/tmp paths per task (first_photos
        # dedupes), so the plain .tmp name can't collide across workers
        tmp = dest.with_suffix(".tmp")
        ok = False
        is_mo = _MO_PHOTO_HOST in urllib.parse.urlsplit(url).netloc
        if is_mo:
            _mo_photo_lock.acquire()
        try:
            req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
            with urllib.request.urlopen(req, timeout=30) as resp:
                tmp.write_bytes(resp.read())
            tmp.rename(dest)
            ok = True
        except (urllib.error.URLError, OSError) as e:
            logger.warning(f"Failed to fetch photo {p['id']}: {e}")
            tmp.unlink(missing_ok=True)
        finally:
            if is_mo:
                # Hold the lock through the pause so MO sees one request
                # per _MO_FETCH_DELAY_S regardless of the worker count.
                time.sleep(_MO_FETCH_DELAY_S)
                _mo_photo_lock.release()
        with lock:
            done += 1
            if ok:
                fetched += 1
            if progress:
                progress(done, len(to_fetch))
        if not is_mo:
            time.sleep(_FETCH_DELAY_S)

    with ThreadPoolExecutor(max_workers=_FETCH_WORKERS) as pool:
        list(pool.map(fetch_one, to_fetch))

    if abort is not None and abort.is_set():
        logger.info(f"Photo prefetch aborted (shutdown) after {fetched} photos")
        return fetched
    logger.info(f"Photo prefetch complete: {fetched} downloaded, "
                f"{len(photos) - len(to_fetch)} already cached")
    return fetched
