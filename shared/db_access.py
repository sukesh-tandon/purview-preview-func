import os
import json
import logging
from pathlib import Path
from typing import Dict, Optional, Any

import pyodbc

from shared.models import RedirectPreview
from shared.config import (
    PUBLIC_BASE_URL,
    DEFAULT_OG_IMAGE_URL,
    DEFAULT_THEME_COLOR,
)

# ----------------------------------------------------------------------
# DATABASE CONFIG – HARD-CODED FOR PURVIEW V1
# ----------------------------------------------------------------------

SQL_CONN_STR = (
    "Driver={ODBC Driver 18 for SQL Server};"
    "Server=tcp:duit-sqlserver.database.windows.net,1433;"
    "Database=campaign-db;"
    "Uid=purview_readonly_user;"
    "Pwd=Blue@2703;"
    "Encrypt=yes;"
    "TrustServerCertificate=no;"
    "Connection Timeout=30;"
)

TABLE_REDIRECTS = "redirects"

COL_TOKEN = "token"
COL_DEST_URL = "destination_url"
COL_MOBILE = "mobile"
COL_LENDER = "lender"
COL_CAMPAIGN_ID = "campaign_id"

# ----------------------------------------------------------------------
# PATHS FOR LENDER JSON ASSETS
# ----------------------------------------------------------------------

FUNCTION_ROOT = Path(__file__).resolve().parents[2]
LENDER_JSON_DIR = FUNCTION_ROOT / "redirect_previews" / "lenders"


# ----------------------------------------------------------------------
# HELPERS
# ----------------------------------------------------------------------

def _normalize_lender(name: str) -> str:
    """
    Normalizes lender into correct JSON filename base.
    PayMe → payme
    Ram Fincorp → ram_fincorp
    """
    return name.strip().lower().replace(" ", "_")


def _load_lender_json(lender: str) -> Optional[Dict[str, Any]]:
    """
    Loads redirect_previews/lenders/<normalized>_default.json
    """
    normalized = _normalize_lender(lender)
    path = LENDER_JSON_DIR / f"{normalized}_default.json"

    if not path.exists():
        logging.warning(f"[Purview] Lender JSON not found: {path}")
        return None

    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        logging.exception(f"[Purview] Failed to read lender JSON: {path}")
        return None


def _get_redirect_row(token: str) -> Optional[Dict[str, Any]]:
    """
    Fetch row for the given token from campaign-db.redirects.

    Expected columns:
        token, destination_url, mobile, lender, campaign_id
    """

    try:
        conn = pyodbc.connect(SQL_CONN_STR, timeout=3)
        cursor = conn.cursor()

        query = f"""
            SELECT TOP 1
                [{COL_DEST_URL}] AS dest_url,
                [{COL_LENDER}] AS lender,
                [{COL_MOBILE}] AS mobile,
                [{COL_CAMPAIGN_ID}] AS campaign_id
            FROM [{TABLE_REDIRECTS}]
            WHERE [{COL_TOKEN}] = ?
        """

        cursor.execute(query, token)
        row = cursor.fetchone()

        cursor.close()
        conn.close()

        if not row:
            logging.warning(f"[Purview] Token not found in redirects: {token}")
            return None

        return {
            "dest_url": row.dest_url,
            "lender": row.lender,
            "mobile": row.mobile,
            "campaign_id": row.campaign_id,
        }

    except Exception:
        logging.exception(f"[Purview] SQL lookup failed for token={token}")
        return None


def _fallback_preview(token: str) -> RedirectPreview:
    """
    Used only if lender JSON missing.
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
    )


# ----------------------------------------------------------------------
# MAIN FUNCTION FOR PURVIEW V1
# ----------------------------------------------------------------------

def get_redirect_preview(token: Optional[str]) -> Optional[RedirectPreview]:
    """
    Purview V1 flow:
        1. token → redirect DB lookup
        2. extract lender + destination_url
        3. load lender OG JSON
        4. return ready RedirectPreview
    """

    if not token:
        return None

    token = token.strip()
    if not token:
        return None

    # STEP 1 — DB lookup
    row = _get_redirect_row(token)
    if not row:
        return None

    lender = row["lender"]
    dest_url = row["dest_url"]

    if not lender or not dest_url:
        logging.warning(f"[Purview] Missing lender or dest_url for token={token}")
        return None

    # STEP 2 — Load lender JSON
    cfg = _load_lender_json(lender)

    if not cfg:
        logging.warning(f"[Purview] Missing lender JSON: {lender}")
        return _fallback_preview(token)

    # STEP 3 — Build final RedirectPreview
    return RedirectPreview(
        token=token,
        title=cfg.get("title") or f"Your {lender} loan preview is ready",
        description=cfg.get("description") or "Tap to view your loan details.",
        image_url=cfg.get("image_url") or DEFAULT_OG_IMAGE_URL,
        theme_color=cfg.get("theme_color") or DEFAULT_THEME_COLOR,
        target_url=dest_url,
        canonical_url=dest_url,
    )
