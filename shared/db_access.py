import os
import json
from pathlib import Path
from typing import Any, Dict
from .models import RedirectPreview
from .config import (
    PUBLIC_BASE_URL,
    DEFAULT_OG_IMAGE_URL,
    DEFAULT_THEME_COLOR,
)

#
# DIRECTORY SETUP
#

BASE_DIR = Path(__file__).resolve().parent.parent
PACKAGE_PREVIEWS_DIR = BASE_DIR / "redirect_previews"               # existing token-based JSON folder
LENDER_DIR = BASE_DIR / "redirect_previews" / "lenders"             # NEW lender-based config folder
TMP_DIR = Path("/tmp/redirect-previews")
TMP_DIR.mkdir(parents=True, exist_ok=True)


#
# UTILITIES
#

def _load_json(path: Path) -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def _hydrate_tmp_from_package(token: str) -> Path:
    """
    Copies a packaged token.json → /tmp so Function cold starts
    still work exactly like before.
    """
    src_json = PACKAGE_PREVIEWS_DIR / f"{token}.json"
    dest_json = TMP_DIR / f"{token}.json"

    if src_json.exists():
        if not dest_json.exists():
            try:
                dest_json.write_text(src_json.read_text(), encoding="utf-8")
            except Exception:
                pass
        return dest_json

    return dest_json  # may not exist → caller handles fallback


def _default_preview(token: str) -> RedirectPreview:
    """
    Your existing fallback preview.
    """
    target_url = f"{PUBLIC_BASE_URL.rstrip('/')}/p/{token}"
    return RedirectPreview(
        token=token,
        title="Your loan preview is ready",
        description="Tap to view your personalised loan offer.",
        target_url=target_url,
        image_url=DEFAULT_OG_IMAGE_URL,
        theme_color=DEFAULT_THEME_COLOR,
        canonical_url=target_url,
    )


def _normalize_lender(lender: str) -> str:
    """
    Normalization rule:
    'Ram Fincorp' → 'ram_fincorp'
    'Poonawalla STPL' → 'poonawalla_stpl'
    'PayMe' → 'payme'
    """
    return lender.strip().lower().replace(" ", "_")


#
# 🚀 NEW: LENDER-BASED LOGIC (Option A)
#

def _load_lender_default(lender: str) -> Dict[str, Any] | None:
    """
    Loads <normalized>_default.json from redirect_previews/lenders/
    e.g. PayMe → payme_default.json
    """
    normalized = _normalize_lender(lender)
    filename = f"{normalized}_default.json"
    full_path = LENDER_DIR / filename

    if full_path.exists():
        try:
            return _load_json(full_path)
        except Exception:
            return None

    return None


#
# MAIN FUNCTION USED BY PURVIEW
#

def get_redirect_preview(token: str | None) -> RedirectPreview:
    """
    FINAL Version — SAFE — ZERO infra risk.

    Priority:
    1) If <token>.json exists → use legacy behavior
    2) If <token>.json contains 'lender' → use lender-based config file
    3) Else fallback generic preview
    """

    if not token:
        return _default_preview("")

    token = token.strip()
    if not token:
        return _default_preview("")

    #
    # 1️⃣ Try legacy token-specific JSON
    #
    token_json_path = _hydrate_tmp_from_package(token)
    data = None

    if token_json_path.exists():
        try:
            data = _load_json(token_json_path)
        except Exception:
            data = None

    # If old-style JSON with no lender → return it exactly as before
    if data and "lender" not in data:
        title = str(data.get("title") or "Your loan preview is ready")
        description = str(data.get("description") or "Tap to view your personalised loan offer.")
        target_url = str(data.get("target_url") or f"{PUBLIC_BASE_URL.rstrip('/')}/p/{token}")
        image_url = str(data.get("image_url") or DEFAULT_OG_IMAGE_URL)
        theme_color = str(data.get("theme_color") or DEFAULT_THEME_COLOR)
        canonical_url = str(data.get("canonical_url") or target_url)

        return RedirectPreview(
            token=token,
            title=title,
            description=description,
            target_url=target_url,
            image_url=image_url,
            theme_color=theme_color,
            canonical_url=canonical_url,
        )

    #
    # 2️⃣ If token.json contains lender → NEW lender-based config
    #
    if data and "lender" in data:
        lender = str(data["lender"]).strip()
        dest_url = str(
            data.get("target_url") or f"{PUBLIC_BASE_URL.rstrip('/')}/p/{token}"
        )

        lender_cfg = _load_lender_default(lender)

        if lender_cfg:
            return RedirectPreview(
                token=token,
                title=lender_cfg.get("title") or "Your loan preview is ready",
                description=lender_cfg.get("description") or "Tap to view your loan details.",
                target_url=dest_url,
                image_url=lender_cfg.get("image_url") or DEFAULT_OG_IMAGE_URL,
                theme_color=lender_cfg.get("theme_color") or DEFAULT_THEME_COLOR,
                canonical_url=dest_url,
            )

    #
    # 3️⃣ Absolute fallback
    #
    return _default_preview(token)
