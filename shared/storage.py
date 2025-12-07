from typing import Optional

from .config import get_settings
from .models import RedirectPreview


def resolve_image_url(preview: Optional[RedirectPreview]) -> str:
    """
    Decide which image URL to use for OG / hero:
    1) token-level og_image_url from redirect_previews
    2) DEFAULT_OG_IMAGE_URL environment variable
    """
    settings = get_settings()

    if preview and preview.og_image_url:
        return preview.og_image_url

    return settings.default_og_image_url
