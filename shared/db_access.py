import json
from pathlib import Path
from typing import Any, Dict

from .models import RedirectPreview
from .config import PUBLIC_BASE_URL, DEFAULT_THEME_COLOR, DEFAULT_OG_IMAGE_URL

# Runtime directory where Azure Functions Linux allows writes
TMP_PREVIEWS_DIR = Path("/tmp/redirect-previews")

# Packaged JSON files included in the function package
PACKAGE_PREVIEWS_DIR = Path(__file__).resolve().parent.parent / "redirect_previews"


def _ensure_tmp_dir() -> None:
    try:
        TMP_PREVIEWS_DIR.mkdir(parents=True, exist_ok=True)
    except OSError:
        pass


def _load_json(path: Path) -> Dict[str, Any]:
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def _hydrate_tmp_from_package(token: str) -> Path:
    """
    Ensures /tmp contains the JSON metadata for the given token.
    If not present, copies from packaged JSONs.
    """
    _ensure_tmp_dir()

    tmp_path = TMP_PREVIEWS_DIR / f"{token}.json"
    if tmp_path.exists():
        return tmp_path

    package_path = PACKAGE_PREVIEWS_DIR / f"{token}.json"
    if package_path.exists():
        try:
            tmp_path.write_text(package_path.read_text(encoding="utf-8"), encoding="utf-8")
        except OSError:
            pass

    return tmp_path


def _default_preview(token: str) -> RedirectPreview:
    safe = (token or "loan").strip() or "loan"
    target_url = f"{PUBLIC_BASE_URL.rstrip('/')}/p/{safe}"

    return RedirectPreview(
        token=safe,
        title="Check your loan offer",
        description="Tap to see if you’re eligible for a fast personal loan through Duit. Instant decision, no collateral.",
        target_url=target_url,
        image_url=DEFAULT_OG_IMAGE_URL,
        theme_color=DEFAULT_THEME_COLOR,
        canonical_url=target_url,
    )


def get_redirect_preview(token: str | None) -> RedirectPreview:
    """
    Loads redirect preview metadata from JSON files.
    Falls back to safe defaults for unknown tokens.
    """
    if not token:
        return _default_preview("")

    token = token.strip()
    if not token:
        return _default_preview("")

    # Copy metadata JSON into /tmp if available
    json_path = _hydrate_tmp_from_package(token)

    data: Dict[str, Any] | None = None
    try:
        if json_path.exists():
            data = _load_json(json_path)
    except (OSError, ValueError, json.JSONDecodeError):
        data = None

    if not data:
        return _default_preview(token)

    title = str(data.get("title") or "Your loan preview is ready")
    description = str(data.get("description") or "Tap to view your personalised loan options.")
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


__all__ = ["get_redirect_preview"]
