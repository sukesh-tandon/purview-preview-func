"""
Purview V1 - DB access layer (DAB-first, no SQL).
Fully dynamic token support, validated URL output,
cached lender JSON loading, correct fallback behaviour.
"""

import os
import json
import logging
import time
import threading
from pathlib import Path
from typing import Optional, Dict, Any
from urllib import request, parse, error
import ssl
import re

from shared.models import RedirectPreview
from shared.config import (
    PUBLIC_BASE_URL,
    DEFAULT_OG_IMAGE_URL,
    DEFAULT_THEME_COLOR,
)

# -------------------------------------------------------------------
# CONFIG
# -------------------------------------------------------------------

DAB_BASE_URL = os.getenv(
    "DAB_BASE_URL",
    "https://duit-dab-api.mangobay-e9dc6af5.centralindia.azurecontainerapps.io/api"
)
DAB_REDIRECTS_PATH = os.getenv("DAB_REDIRECTS_PATH", "redirects")
DAB_TIMEOUT = int(os.getenv("DAB_TIMEOUT", "4"))

# Validate config
if not DAB_BASE_URL or not DAB_BASE_URL.startswith("http"):
    raise ValueError("INVALID DAB_BASE_URL environment variable")

# Folder location of lender configs
FUNCTION_ROOT = Path(__file__).resolve().parents[2]
LENDER_JSON_DIR = FUNCTION_ROOT / "redirect_previews" / "lenders"

# Cached JSONs with TTL
_lender_cache: Dict[str, tuple[Dict[str, Any], float]] = {}
CACHE_TTL = 3600  # 1 hour
_cache_lock = threading.Lock()


# -------------------------------------------------------------------
# HELPERS
# -------------------------------------------------------------------

def _is_valid_url(url: str) -> bool:
    return bool(re.match(r'^https?://[^\s<>"]+$', url))


def _normalize_lender(name: str) -> str:
    return name.strip().lower().replace(" ", "_")


def _load_lender_json_cached(lender_name: str) -> Optional[Dict[str, Any]]:
    """
    Load lender JSON from disk with TTL-based caching.
    """
    normalized = _normalize_lender(lender_name)
    filename = f"{normalized}_default.json"
    path = LENDER_JSON_DIR / filename

    now = time.time()

    with _cache_lock:
        if normalized in _lender_cache:
            data, ts = _lender_cache[normalized]
            if now - ts < CACHE_TTL:
                return data
            # TTL expired → evict
            del _lender_cache[normalized]

    # Load from disk
    if not path.exists():
        logging.warning(f"[Purview] Lender JSON missing: {path}")
        return None

    try:
        with open(path, "r", encoding="utf-8") as fh:
            data = json.load(fh)
    except Exception:
        logging.exception(f"[Purview] Failed to load lender JSON: {path}")
        return None

    with _cache_lock:
        _lender_cache[normalized] = (data, now)

    return data


def _build_fallback_preview(token: str) -> RedirectPreview:
    """
    Generic fallback preview if lender JSON missing.
    """
    url = f"{PUBLIC_BASE_URL.rstrip('/')}/p/{token}"
    return RedirectPreview(
        token=token,
        title="Your loan preview is ready",
        description="Tap to view your personalised loan offer.",
        target_url=url,
        image_url=DEFAULT_OG_IMAGE_URL,
        theme_color=DEFAULT_THEME_COLOR,
        canonical_url=url,
        meta={"fallback": True},
    )


# -------------------------------------------------------------------
# DAB LOOKUP
# -------------------------------------------------------------------

def _dab_lookup(token: str) -> Optional[Dict[str, Any]]:
    """
    Query Data API Builder for redirect row:

        GET {DAB_BASE_URL}/{DAB_REDIRECTS_PATH}?$filter=token eq '{token}'&$top=1
    """
    try:
        base = DAB_BASE_URL.rstrip("/")
        path = DAB_REDIRECTS_PATH.strip("/")
        filter_value = f"token eq '{token}'"
        qs = parse.urlencode({"$filter": filter_value, "$top": "1"})
        url = f"{base}/{path}?{qs}"

        ctx = ssl.create_default_context()
        req = request.Request(url, method="GET")
        req.add_header("Accept", "application/json")
        req.add_header("User-Agent", "duit-purview-v1")

        with request.urlopen(req, timeout=DAB_TIMEOUT, context=ctx) as resp:
            if resp.status != 200:
                logging.warning(f"[Purview][DAB] Non-200: {resp.status}")
                return None

            payload = json.loads(resp.read().decode("utf-8"))

        rows = payload.get("value", payload) if isinstance(payload, dict) else payload
        if not rows:
            logging.info(f"[Purview][DAB] No row for token={token}")
            return None

        row = rows[0] if isinstance(rows, list) else rows

        dest = (
            row.get("destination_url")
            or row.get("dest_url")
            or row.get("destination")
            or row.get("url")
        )
        lender = row.get("lender")
        mobile = row.get("mobile") or row.get("msisdn")
        campaign_id = row.get("campaign_id") or row.get("campaign")

        # URL validation
        if not dest or not _is_valid_url(dest):
            logging.warning(f"[Purview][DAB] Invalid dest_url returned: {dest}")
            return None

        if not lender:
            logging.warning(f"[Purview][DAB] Missing lender for token={token}")
            return None

        return {
            "destination_url": dest,
            "lender": lender,
            "mobile": mobile,
            "campaign_id": campaign_id,
        }

    except error.HTTPError as he:
        logging.warning(f"[Purview][DAB] HTTPError {he.code}: {he.reason}")
        return None
    except error.URLError as ue:
        logging.warning(f"[Purview][DAB] URLError: {ue.reason}")
        return None
    except Exception:
        logging.exception("[Purview][DAB] Unexpected exception")
        return None


# -------------------------------------------------------------------
# PUBLIC API — main function used by __init__.py
# -------------------------------------------------------------------

def get_redirect_preview(token: Optional[str]) -> Optional[RedirectPreview]:
    """
    Steps:
      1. Validate token.
      2. Query DAB → row.
      3. Load lender JSON → merge.
      4. Create RedirectPreview.
      5. Return preview or None.
    """

    if not token:
        return None

    token = token.strip()
    if not token:
        return None

    # --- STEP 1: DAB lookup ---
    row = _dab_lookup(token)
    if not row:
        logging.info(f"[Purview] No redirect row for token={token}")
        return None

    dest_url = row["destination_url"]
    lender = row["lender"]
    mobile = row.get("mobile")
    campaign_id = row.get("campaign_id")

    # --- STEP 2: Load lender JSON ---
    cfg = _load_lender_json_cached(lender)
    cached = cfg is not None

    logging.info(
        f"Purview hit: token={token}, lender={lender}, cached={cached}"
    )

    if not cfg:
        return _build_fallback_preview(token)

    # --- STEP 3: Construct preview ---
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
        target_url=dest_url,
        canonical_url=dest_url,
        meta={"lender": lender, "mobile": mobile, "campaign_id": campaign_id},
    )

    return preview
