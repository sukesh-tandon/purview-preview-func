import os
import json
from pathlib import Path
from typing import Any, Dict

# Correct imports based on your structure
from shared.models import RedirectPreview
from shared.config import (
    PUBLIC_BASE_URL,
    DEFAULT_OG_IMAGE_URL,
    DEFAULT_THEME_COLOR,
)

#
# DIRECTORY SETUP
#

# /home/site/wwwroot/shared/db_access.py → parents[2] = function root
FUNCTION_ROOT = Path(__file__).resolve().parents[2]

PACKAGE_PREVIEWS_DIR = FUNCTION_ROOT / "redirect_previews"
LENDER_DIR = PACKAGE_PREVIEWS_DIR / "lenders"

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
    Copy packaged token.json into /tmp so cold starts behave identically.
    """
    src = PACKAGE_PREVIEWS_DIR / f"{token}.json"
    dst = TMP_DIR / f"{token}.json"

    if src.exists():
        if not dst.exists():
            try:
                dst.write_text(src.read_text(), encoding="utf-8")
            except Exception:
                pass
        return dst

    return dst  # may not exist → caller checks .exists()


def _default_preview(token: str) -> RedirectPreview:
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


def _normalize_lender(name: str) -> str:
    return name.strip().lower().replace(" ", "_")


def _load_lender_default(lender: str) -> Dict[str, Any] | None:
    normalized = _normalize_lender(lender)
    path = LENDER_DIR / f"{normalized}_default.json"

    if path.exists():
        try:
            return _load_json(path)
        except Exception:
            pass

    return None


#
# MAIN FUNCTION
#

def get_redirect_preview(token: str | None) -> RedirectPreview | None:
    """
    Priority:
    1. token.json exists → hydrate it
    2. token.json has lender → use lender defaults
    3. no token.json → return None (404)
    4. safe fallback only for valid token cases
    """
    if not token:
        return None

    token = token.strip()
    if not token:
        return None

    # 1️⃣ Load legacy token.json
    token_path = _hydrate_tmp_from_package(token)
    data = None

    if token_path.exists():
        try:
            data = _load_json(token_path)
        except Exception:
            data = None

    # 1A → legacy format (no lender field)
    if data and "lender" not in data:
        return RedirectPreview(
            token=token,
            title=str(data.get("title") or "Your loan preview is ready"),
            description=str(data.get("description") or "Tap to view your personalised loan offer."),
            target_url=str(data.get("target_url") or f"{PUBLIC_BASE_URL.rstrip('/')}/p/{token}"),
            image_url=str(data.get("image_url") or DEFAULT_OG_IMAGE_URL),
            theme_color=str(data.get("theme_color") or DEFAULT_THEME_COLOR),
            canonical_url=str(data.get("canonical_url") or f"{PUBLIC_BASE_URL.rstrip('/')}/p/{token}"),
        )

    # 2️⃣ lender → lender based defaults
    if data and "lender" in data:
        lender = str(data["lender"]).strip()
        dest_url = str(data.get("target_url") or f"{PUBLIC_BASE_URL.rstrip('/')}/p/{token}")

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

        # lender exists but no config → fallback
        return _default_preview(token)

    # 3️⃣ Completely missing token.json → return None (404)
    return None
