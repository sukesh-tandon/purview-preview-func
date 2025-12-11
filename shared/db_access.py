"""
shared/db_access.py — production-grade DAB-first lookup for Purview V1.

Goals:
 - Query Data API Builder (DAB) for the token row using correctly encoded OData $filter.
 - Validate destination URL strictly.
 - Load static lender JSON (redirect_previews/lenders/<normalized>_default.json).
 - Merge static JSON + dynamic row -> RedirectPreview (target/canonical = destination_url).
 - Use in-process thread-safe TTL+LRU cache to minimize disk IO.
 - Use simple retry/backoff for DAB calls (no external libs).
 - Structured logging for easy App Insights ingestion.
 - No pyodbc, no requests; works with minimal requirements.

Assumptions:
 - `shared.models.RedirectPreview` and `shared.config` exist and are compatible.
"""

from __future__ import annotations

import os
import json
import logging
import time
import threading
from collections import OrderedDict
from dataclasses import dataclass
from pathlib import Path
from typing import Optional, Dict, Any, Tuple
from urllib import request, parse, error
import ssl
from urllib.parse import urlparse

# Import your project's model and config (unchanged)
from shared.models import RedirectPreview
from shared.config import (
    PUBLIC_BASE_URL,
    DEFAULT_OG_IMAGE_URL,
    DEFAULT_THEME_COLOR,
)

# -----------------------------
# Configuration (env-vars)
# -----------------------------
DAB_BASE_URL = os.getenv(
    "DAB_BASE_URL",
    "https://duit-dab-api.mangobay-e9dc6af5.centralindia.azurecontainerapps.io/api",
)
DAB_REDIRECTS_PATH = os.getenv("DAB_REDIRECTS_PATH", "redirects")

# DAB network controls
DAB_TIMEOUT = float(os.getenv("DAB_TIMEOUT", "4.0"))         # seconds per request
DAB_RETRIES = int(os.getenv("DAB_RETRIES", "2"))            # retry attempts after first
DAB_BACKOFF_FACTOR = float(os.getenv("DAB_BACKOFF", "0.35"))  # seconds * (2**attempt)

# Cache controls
LENDER_CACHE_TTL = int(os.getenv("LENDER_CACHE_TTL", "3600"))  # seconds
LENDER_CACHE_MAX = int(os.getenv("LENDER_CACHE_MAX", "128"))   # max entries

# Validate DAB_BASE_URL
if not DAB_BASE_URL or not DAB_BASE_URL.startswith("http"):
    raise ValueError("INVALID DAB_BASE_URL environment variable")

# Location of lender JSON files (unchanged folder layout)
FUNCTION_ROOT = Path(__file__).resolve().parents[2]
LENDER_JSON_DIR = FUNCTION_ROOT / "redirect_previews" / "lenders"

# -----------------------------
# Thread-safe LRU + TTL cache
# -----------------------------
class LruTtlCache:
    def __init__(self, max_size: int, ttl_seconds: int):
        self.max_size = max_size
        self.ttl = ttl_seconds
        self.lock = threading.RLock()
        self.store: "OrderedDict[str, Tuple[Any, float]]" = OrderedDict()

    def get(self, key: str):
        with self.lock:
            v = self.store.get(key)
            if not v:
                return None
            data, ts = v
            if time.time() - ts > self.ttl:
                # expired
                del self.store[key]
                return None
            # move to end (most recent)
            self.store.move_to_end(key)
            return data

    def set(self, key: str, value: Any):
        with self.lock:
            if key in self.store:
                del self.store[key]
            self.store[key] = (value, time.time())
            # evict oldest if beyond capacity
            while len(self.store) > self.max_size:
                self.store.popitem(last=False)

    def contains(self, key: str) -> bool:
        with self.lock:
            return key in self.store and (time.time() - self.store[key][1]) <= self.ttl

_lender_cache = LruTtlCache(LENDER_CACHE_MAX, LENDER_CACHE_TTL)


# -----------------------------
# Utilities: URL validation
# -----------------------------
def _is_valid_http_url(url: str) -> bool:
    """
    Strict validation using urllib.parse:
      - scheme must be http or https
      - netloc must be present
      - path allowed
    Returns True if likely a real destination URL.
    """
    if not url:
        return False
    try:
        p = urlparse(url)
    except Exception:
        return False
    return p.scheme in ("http", "https") and bool(p.netloc)


# -----------------------------
# Lender JSON loader
# -----------------------------
def _normalize_lender(name: str) -> str:
    return name.strip().lower().replace(" ", "_")


def _load_lender_json(lender_name: str) -> Optional[Dict[str, Any]]:
    """
    Load JSON with LRU+TTL caching. Returns parsed dict or None.
    """
    normalized = _normalize_lender(lender_name)
    # cache lookup
    cached = _lender_cache.get(normalized)
    if cached is not None:
        return cached

    # disk path
    filename = f"{normalized}_default.json"
    path = LENDER_JSON_DIR / filename
    if not path.exists():
        logging.warning("[Purview] Lender JSON missing: %s", str(path))
        return None

    try:
        with open(path, "r", encoding="utf-8") as fh:
            data = json.load(fh)
    except Exception:
        logging.exception("[Purview] Failed to parse lender JSON: %s", str(path))
        return None

    _lender_cache.set(normalized, data)
    return data


# -----------------------------
# DAB (Data API Builder) query
# -----------------------------
def _build_dab_url_for_token(token: str) -> str:
    """
    Build DAB URL for the token without over-encoding the OData filter.
    We allow space, = and single-quote to remain intact.
    """
    base = DAB_BASE_URL.rstrip("/")
    path = DAB_REDIRECTS_PATH.strip("/")
    filter_value = f"token eq '{token}'"
    # preserve characters used by OData syntax: space, =, and single quote
    encoded_filter = parse.quote(filter_value, safe=" ='")
    return f"{base}/{path}?$filter={encoded_filter}&$top=1"


def _http_get_with_retries(url: str, timeout: float, retries: int, backoff: float) -> Optional[str]:
    """
    Minimal retry + exponential backoff. Returns response body (str) or None.
    Retries on URLError or HTTP 5xx. Not on HTTP 4xx (client error).
    """
    attempt = 0
    ctx = ssl.create_default_context()
    while True:
        try:
            req = request.Request(url, method="GET")
            req.add_header("Accept", "application/json")
            req.add_header("User-Agent", "duit-purview-v1/1.0")
            with request.urlopen(req, timeout=timeout, context=ctx) as resp:
                # 2xx success
                status = getattr(resp, "status", None) or resp.getcode()
                body = resp.read().decode("utf-8")
                if 200 <= status < 300:
                    return body
                # If 4xx -> don't retry (client error like bad filter)
                if 400 <= status < 500:
                    logging.warning("[Purview][DAB] HTTP %d (client error) for url=%s", status, url)
                    return None
                # 5xx -> retry
                logging.warning("[Purview][DAB] HTTP %d (server error), attempt %d/%d", status, attempt, retries)
                # fall through to retry
        except error.HTTPError as he:
            code = getattr(he, "code", None)
            # Do not retry on 4xx; treat as permanent failure
            if code and 400 <= code < 500:
                logging.warning("[Purview][DAB] HTTPError %s (client) for url=%s", code, url)
                return None
            logging.warning("[Purview][DAB] HTTPError %s; attempt %d/%d", code, attempt, retries)
        except error.URLError as ue:
            logging.warning("[Purview][DAB] URLError: %s; attempt %d/%d", ue.reason, attempt, retries)
        except Exception:
            logging.exception("[Purview][DAB] Unexpected error; attempt %d/%d", attempt, retries)

        # retry decision
        if attempt >= retries:
            logging.error("[Purview][DAB] Exhausted retries for url=%s", url)
            return None
        sleep_time = backoff * (2 ** attempt)
        time.sleep(sleep_time)
        attempt += 1


def _dab_lookup(token: str) -> Optional[Dict[str, Any]]:
    """
    Return dict: { destination_url, lender, mobile, campaign_id } or None.
    Handles these input shapes from DAB:
      - {"value":[ {...} ]}  (OData envelope)
      - [ {...}, ... ]       (array)
      - {...}                (single object)
    """
    url = _build_dab_url_for_token(token)
    body = _http_get_with_retries(url, timeout=DAB_TIMEOUT, retries=DAB_RETRIES, backoff=DAB_BACKOFF_FACTOR)
    if not body:
        logging.info("[Purview][DAB] No response body for token=%s", token)
        return None

    try:
        payload = json.loads(body)
    except Exception:
        logging.exception("[Purview][DAB] JSON decode failed for token=%s", token)
        return None

    # handle envelope/list/object
    rows = payload.get("value") if isinstance(payload, dict) and "value" in payload else payload
    if not rows:
        logging.info("[Purview][DAB] DAB returned empty for token=%s", token)
        return None

    row = rows[0] if isinstance(rows, list) and len(rows) > 0 else (rows if isinstance(rows, dict) else None)
    if not row:
        logging.info("[Purview][DAB] No usable row for token=%s", token)
        return None

    destination = (row.get("destination_url") or row.get("dest_url") or row.get("destination") or row.get("url"))
    lender = row.get("lender")
    mobile = row.get("mobile") or row.get("msisdn")
    campaign_id = row.get("campaign_id") or row.get("campaign")

    # strict validation
    if not destination or not _is_valid_http_url(destination):
        logging.warning("[Purview][DAB] Invalid destination_url for token=%s: %s", token, destination)
        return None
    if not lender:
        logging.warning("[Purview][DAB] Missing lender for token=%s", token)
        return None

    return {
        "destination_url": destination,
        "lender": lender,
        "mobile": mobile,
        "campaign_id": campaign_id,
    }


# -----------------------------
# Public API (used by __init__.py)
# -----------------------------
def get_redirect_preview(token: Optional[str]) -> Optional[RedirectPreview]:
    """
    Steps:
     1) validate token
     2) query DAB for the token
     3) load lender JSON (cached)
     4) merge into RedirectPreview (target/canonical = destination_url)
     5) return RedirectPreview or None
    """
    if not token:
        return None
    token = token.strip()
    if not token:
        return None

    row = _dab_lookup(token)
    if not row:
        logging.info("[Purview] No redirect row for token=%s", token)
        return None

    destination_url = row["destination_url"]
    lender = row["lender"]
    mobile = row.get("mobile")
    campaign_id = row.get("campaign_id")

    cfg = _load_lender_json(lender)
    cached = cfg is not None
    # structured log for AppInsights
    logging.info("Purview hit", extra={"token": token, "lender": lender, "cached": bool(cached)})

    # If static JSON missing, return a fallback preview that still points to the destination_url
    if not cfg:
        return RedirectPreview(
            token=token,
            title="Your loan preview is ready",
            description="Tap to view your personalised loan offer.",
            image_url=DEFAULT_OG_IMAGE_URL,
            theme_color=DEFAULT_THEME_COLOR,
            target_url=destination_url,
            canonical_url=destination_url,
            meta={"lender": lender, "mobile": mobile, "campaign_id": campaign_id, "fallback": True},
        )

    # Build preview merging static + dynamic
    title = cfg.get("title") or f"Your {lender} loan preview is ready"
    description = cfg.get("description") or ""
    image_url = cfg.get("image_url") or DEFAULT_OG_IMAGE_URL
    theme_color = cfg.get("theme_color") or DEFAULT_THEME_COLOR

    preview = RedirectPreview(
        token=token,
        title=title,
        description=description,
        image_url=image_url,
        theme_color=theme_color,
        target_url=destination_url,
        canonical_url=destination_url,
        meta={"lender": lender, "mobile": mobile, "campaign_id": campaign_id},
    )
    return preview
