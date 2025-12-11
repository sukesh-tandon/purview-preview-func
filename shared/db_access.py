"""
shared/db_access.py — Purview V1 (DAB-first, no SQL, no requests)
Production-stable version with:
 - Correct OData URL encoding (spaces → %20, quotes preserved)
 - Strict URL validation
 - Lender JSON TTL+LRU caching
 - Minimal retry backoff for DAB
 - Clean merging of static JSON + dynamic DB row
 - No external dependencies (works with minimal requirements.txt)
"""

import os
import json
import logging
import time
import threading
from collections import OrderedDict
from pathlib import Path
from typing import Optional, Dict, Any, Tuple
from urllib import request, parse, error
from urllib.parse import urlparse
import ssl

from shared.models import RedirectPreview
from shared.config import (
    PUBLIC_BASE_URL,
    DEFAULT_OG_IMAGE_URL,
    DEFAULT_THEME_COLOR,
)

# -----------------------------------------------------
# CONFIG
# -----------------------------------------------------

DAB_BASE_URL = os.getenv(
    "DAB_BASE_URL",
    "https://duit-dab-api.mangobay-e9dc6af5.centralindia.azurecontainerapps.io/api",
)
DAB_REDIRECTS_PATH = os.getenv("DAB_REDIRECTS_PATH", "redirects")

DAB_TIMEOUT = float(os.getenv("DAB_TIMEOUT", "4"))
DAB_RETRIES = int(os.getenv("DAB_RETRIES", "2"))
DAB_BACKOFF = float(os.getenv("DAB_BACKOFF", "0.35"))

LENDER_CACHE_TTL = int(os.getenv("LENDER_CACHE_TTL", "3600"))
LENDER_CACHE_MAX = int(os.getenv("LENDER_CACHE_MAX", "128"))

if not DAB_BASE_URL or not DAB_BASE_URL.startswith("http"):
    raise ValueError("INVALID DAB_BASE_URL")

FUNCTION_ROOT = Path(__file__).resolve().parents[2]
LENDER_JSON_DIR = FUNCTION_ROOT / "redirect_previews" / "lenders"


# -----------------------------------------------------
# TTL + LRU CACHE
# -----------------------------------------------------

class LruTtlCache:
    def __init__(self, max_size: int, ttl_sec: int):
        self.max_size = max_size
        self.ttl = ttl_sec
        self.lock = threading.RLock()
        self.data: "OrderedDict[str, Tuple[Any, float]]" = OrderedDict()

    def get(self, key: str):
        with self.lock:
            if key not in self.data:
                return None
            val, ts = self.data[key]
            if time.time() - ts > self.ttl:
                del self.data[key]
                return None
            self.data.move_to_end(key)
            return val

    def set(self, key: str, value: Any):
        with self.lock:
            if key in self.data:
                del self.data[key]
            self.data[key] = (value, time.time())
            while len(self.data) > self.max_size:
                self.data.popitem(last=False)


_lender_cache = LruTtlCache(LENDER_CACHE_MAX, LENDER_CACHE_TTL)


# -----------------------------------------------------
# HELPERS
# -----------------------------------------------------

def _is_valid_http_url(url: str) -> bool:
    if not url:
        return False
    try:
        p = urlparse(url)
    except Exception:
        return False
    return p.scheme in ("http", "https") and bool(p.netloc)


def _normalize_lender(name: str) -> str:
    return name.strip().lower().replace(" ", "_")


def _load_lender_json(lender: str) -> Optional[Dict[str, Any]]:
    norm = _normalize_lender(lender)

    cached = _lender_cache.get(norm)
    if cached is not None:
        return cached

    path = LENDER_JSON_DIR / f"{norm}_default.json"
    if not path.exists():
        logging.warning("[Purview] Missing lender JSON: %s", path)
        return None

    try:
        with open(path, "r", encoding="utf-8") as fh:
            data = json.load(fh)
        _lender_cache.set(norm, data)
        return data
    except Exception:
        logging.exception("[Purview] Failed reading lender JSON: %s", path)
        return None


# -----------------------------------------------------
# CORRECT ODATA URL BUILDER  (THIS FIXES YOUR PRODUCTION BUG)
# -----------------------------------------------------

def _build_dab_url_for_token(token: str) -> str:
    """
    urllib cannot accept spaces → must encode to %20.
    DAB requires quotes to remain unencoded.
    """
    base = DAB_BASE_URL.rstrip("/")
    path = DAB_REDIRECTS_PATH.strip("/")

    raw = f"token eq '{token}'"
    # Encode ONLY spaces → %20; preserve single quotes
    encoded = raw.replace(" ", "%20")

    return f"{base}/{path}?$filter={encoded}&$top=1"


# -----------------------------------------------------
# DAB HTTP FETCH WITH RETRIES
# -----------------------------------------------------

def _http_get(url: str) -> Optional[str]:
    ctx = ssl.create_default_context()
    req = request.Request(url, method="GET")
    req.add_header("Accept", "application/json")
    req.add_header("User-Agent", "duit-purview-v1")

    try:
        with request.urlopen(req, timeout=DAB_TIMEOUT, context=ctx) as resp:
            if 200 <= resp.status < 300:
                return resp.read().decode("utf-8")
            if 400 <= resp.status < 500:
                logging.warning("[Purview][DAB] Client error %d for %s", resp.status, url)
                return None
            logging.warning("[Purview][DAB] Server error %d for %s", resp.status, url)
    except Exception as ex:
        logging.warning("[Purview][DAB] HTTP exception: %s", ex)

    return None


def _http_get_with_retries(url: str) -> Optional[str]:
    for attempt in range(DAB_RETRIES + 1):
        body = _http_get(url)
        if body is not None:
            return body
        if attempt < DAB_RETRIES:
            time.sleep(DAB_BACKOFF * (2 ** attempt))
    return None


# -----------------------------------------------------
# DAB LOOKUP
# -----------------------------------------------------

def _dab_lookup(token: str) -> Optional[Dict[str, Any]]:
    url = _build_dab_url_for_token(token)
    body = _http_get_with_retries(url)

    if not body:
        logging.info("[Purview][DAB] Empty body for token=%s", token)
        return None

    try:
        payload = json.loads(body)
    except:
        logging.exception("[Purview][DAB] Invalid JSON for token=%s", token)
        return None

    rows = payload.get("value") if isinstance(payload, dict) else payload
    if not rows:
        logging.info("[Purview][DAB] No row for token=%s", token)
        return None

    row = rows[0] if isinstance(rows, list) else rows

    destination = (
        row.get("destination_url")
        or row.get("dest_url")
        or row.get("destination")
        or row.get("url")
    )

    lender = row.get("lender")
    mobile = row.get("mobile") or row.get("msisdn")
    campaign_id = row.get("campaign_id") or row.get("campaign")

    if not _is_valid_http_url(destination):
        logging.warning("[Purview][DAB] Invalid destination_url for %s: %s", token, destination)
        return None

    if not lender:
        logging.warning("[Purview][DAB] Missing lender for %s", token)
        return None

    return {
        "destination_url": destination,
        "lender": lender,
        "mobile": mobile,
        "campaign_id": campaign_id,
    }


# -----------------------------------------------------
# PUBLIC API FOR __init__.py
# -----------------------------------------------------

def get_redirect_preview(token: Optional[str]) -> Optional[RedirectPreview]:
    if not token:
        return None

    token = token.strip()
    if not token:
        return None

    row = _dab_lookup(token)
    if not row:
        logging.info("[Purview] No DAB row for token=%s", token)
        return None

    destination = row["destination_url"]
    lender = row["lender"]
    mobile = row.get("mobile")
    campaign_id = row.get("campaign_id")

    cfg = _load_lender_json(lender)
    cached = cfg is not None

    logging.info("PurviewHit token=%s lender=%s cached=%s", token, lender, cached)

    if not cfg:
        return RedirectPreview(
            token=token,
            title="Your loan preview is ready",
            description="Tap to view your personalised loan offer.",
            image_url=DEFAULT_OG_IMAGE_URL,
            theme_color=DEFAULT_THEME_COLOR,
            target_url=destination,
            canonical_url=destination,
            meta={"lender": lender, "mobile": mobile, "campaign_id": campaign_id, "fallback": True},
        )

    return RedirectPreview(
        token=token,
        title=cfg.get("title") or f"Your {lender} loan preview is ready",
        description=cfg.get("\
description") or "",
        image_url=cfg.get("image_url") or DEFAULT_OG_IMAGE_URL,
        theme_color=cfg.get("theme_color") or DEFAULT_THEME_COLOR,
        target_url=destination,
        canonical_url=destination,
        meta={"lender": lender, "mobile": mobile, "campaign_id": campaign_id},
    )
