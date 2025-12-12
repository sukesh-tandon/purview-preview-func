"""
shared/db_access.py — Purview V1 (FINAL PRODUCTION-STABLE VERSION)

This version includes:
 - Correct absolute URL building (preserves /api)
 - Correct OData filter encoding (spaces → %20, quotes preserved)
 - Removes $top (your DAB rejects it)
 - Full URL logging before request
 - Full body logging for HTTP 400 from DAB
 - Lender JSON caching
 - Zero SQL, zero pyodbc
"""

import os
import json
import logging
import time
import ssl
import threading
from urllib import request, error
from urllib.parse import urlparse, urljoin
from pathlib import Path
from collections import OrderedDict
from typing import Optional, Dict, Any

from shared.models import RedirectPreview
from shared.config import DEFAULT_OG_IMAGE_URL, DEFAULT_THEME_COLOR

# -----------------------------------------------------
# ENV CONFIG
# -----------------------------------------------------

DAB_BASE_URL = os.getenv(
    "DAB_BASE_URL",
    "https://duit-dab-api.mangobay-e9dc6af5.centralindia.azurecontainerapps.io/api"
)

DAB_REDIRECTS_PATH = os.getenv("DAB_REDIRECTS_PATH", "redirects")

DAB_TIMEOUT = float(os.getenv("DAB_TIMEOUT", "4"))
DAB_RETRIES = int(os.getenv("DAB_RETRIES", "2"))
DAB_BACKOFF = float(os.getenv("DAB_BACKOFF", "0.25"))

LENDER_CACHE_TTL = int(os.getenv("LENDER_CACHE_TTL", "3600"))
LENDER_CACHE_MAX = int(os.getenv("LENDER_CACHE_MAX", "128"))

FUNCTION_ROOT = Path(__file__).resolve().parents[2]
LENDER_JSON_DIR = FUNCTION_ROOT / "redirect_previews" / "lenders"

# -----------------------------------------------------
# TTL + LRU CACHE
# -----------------------------------------------------

class LruTtlCache:
    def __init__(self, max_size, ttl):
        self.max_size = max_size
        self.ttl = ttl
        self.lock = threading.RLock()
        self.store = OrderedDict()

    def get(self, key):
        with self.lock:
            if key not in self.store:
                return None
            val, ts = self.store[key]
            if time.time() - ts > self.ttl:
                del self.store[key]
                return None
            self.store.move_to_end(key)
            return val

    def set(self, key, value):
        with self.lock:
            if key in self.store:
                del self.store[key]
            self.store[key] = (value, time.time())
            while len(self.store) > self.max_size:
                self.store.popitem(last=False)

_lender_cache = LruTtlCache(LENDER_CACHE_MAX, LENDER_CACHE_TTL)

# -----------------------------------------------------
# HELPERS
# -----------------------------------------------------

def _is_valid_http_url(url: str) -> bool:
    try:
        p = urlparse(url)
        return p.scheme in ("http", "https") and bool(p.netloc)
    except:
        return False

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
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
            _lender_cache.set(norm, data)
            return data
    except:
        logging.exception("[Purview] Failed loading lender json: %s", path)
        return None

# -----------------------------------------------------
# CORRECT DAB URL BUILDER (NO $top)
# -----------------------------------------------------

def _build_dab_url_for_token(token: str) -> str:
    """
    FINAL WORKING VERSION:
      - DAB_BASE_URL must include /api
      - DO NOT start relative path with "/" → urljoin would drop /api
      - DAB does NOT support $top → remove it
      - Encode spaces as %20, preserve quotes
    """

    raw_filter = f"token eq '{token}'"
    encoded_filter = raw_filter.replace(" ", "%20")

    # NO leading slash, NO $top
    relative = f"{DAB_REDIRECTS_PATH.strip('/')}?$filter={encoded_filter}"

    # urljoin preserves /api when relative has no leading slash
    return urljoin(DAB_BASE_URL.rstrip('/') + "/", relative)

# -----------------------------------------------------
# HTTP GET + RETRIES + BODY LOGGING
# -----------------------------------------------------

def _http_get(url: str) -> Optional[str]:
    ctx = ssl.create_default_context()

    req = request.Request(url, method="GET")
    req.add_header("Accept", "application/json")
    req.add_header("User-Agent", "duit-purview-v1")

    try:
        with request.urlopen(req, timeout=DAB_TIMEOUT, context=ctx) as resp:
            if resp.status == 200:
                return resp.read().decode("utf-8")

            # Log body for >= 400
            body = resp.read().decode("utf-8", errors="ignore")
            logging.warning("[Purview][DAB] HTTP %d body=%s", resp.status, body)
            return None

    except error.HTTPError as he:
        body = ""
        try:
            if he.fp:
                body = he.fp.read().decode("utf-8", errors="ignore")
        except:
            pass
        logging.warning("[Purview][DAB] HTTPError %s body=%s", he.code, body)
        return None

    except Exception as ex:
        logging.warning("[Purview][DAB] Exception: %s", ex)
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

    # CRITICAL: Log the exact URL
    logging.error("[Purview][DAB] GET %s", url)

    body = _http_get_with_retries(url)
    if not body:
        logging.info("[Purview][DAB] No response for token=%s", token)
        return None

    try:
        payload = json.loads(body)
    except:
        logging.exception("[Purview][DAB] Invalid JSON for token=%s", token)
        return None

    rows = payload.get("value")
    if not rows:
        logging.info("[Purview][DAB] No rows for %s", token)
        return None

    row = rows[0]

    dest = row.get("destination_url")
    lender = row.get("lender")
    mobile = row.get("mobile")
    campaign_id = row.get("campaign_id")

    if not _is_valid_http_url(dest):
        logging.warning("[Purview][DAB] Invalid destination_url for %s: %s", token, dest)
        return None

    if not lender:
        logging.warning("[Purview][DAB] Missing lender for %s", token)
        return None

    return {
        "destination_url": dest,
        "lender": lender,
        "mobile": mobile,
        "campaign_id": campaign_id,
    }

# -----------------------------------------------------
# PUBLIC ENTRYPOINT
# -----------------------------------------------------

def get_redirect_preview(token: Optional[str]) -> Optional[RedirectPreview]:
    if not token:
        return None

    token = token.strip()
    if not token:
        return None

    row = _dab_lookup(token)
    if not row:
        return None

    dest = row["destination_url"]
    lender = row["lender"]
    mobile = row.get("mobile")
    campaign_id = row.get("campaign_id")

    cfg = _load_lender_json(lender)
    cached = bool(cfg)

    logging.info("PurviewHit token=%s lender=%s cached=%s", token, lender, cached)

    if not cfg:
        return RedirectPreview(
            token=token,
            title="Your loan preview is ready",
            description="Tap to view your personalised loan offer.",
            image_url=DEFAULT_OG_IMAGE_URL,
            theme_color=DEFAULT_THEME_COLOR,
            target_url=dest,
            canonical_url=dest,
            meta={"lender": lender, "mobile": mobile, "campaign_id": campaign_id, "fallback": True},
        )

    return RedirectPreview(
        token=token,
        title=cfg.get("title", f"Your {lender} loan preview is ready"),
        description=cfg.get("description", ""),
        image_url=cfg.get("image_url", DEFAULT_OG_IMAGE_URL),
        theme_color=cfg.get("theme_color", DEFAULT_THEME_COLOR),
        target_url=dest,
        canonical_url=dest,
        meta={"lender": lender, "mobile": mobile, "campaign_id": campaign_id},
    )
