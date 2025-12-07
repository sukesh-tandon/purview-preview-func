from typing import Optional, List

from .config import get_settings


def build_lender_image_url(lender_id: str, image_name: str) -> str:
    """
    Construct URL for a lender image inside:
    purview-assets/lenders/{lender_id}/{image_name}
    """
    settings = get_settings()

    return f"{settings.asset_base_url}/lenders/{lender_id}/{image_name}"


def build_default_image_url() -> str:
    """
    Return the default OG image defined in environment variables.
    """
    settings = get_settings()
    return settings.default_og_image_url


def select_hero_image(
    lender_id: str,
    sql_og_image: Optional[str],
) -> str:
    """
    Determine the best hero image using fallback precedence:
    1. SQL og_image_url (absolute)
    2. Blob: lenders/{lender_id}/hero.jpg
    3. Default hero image
    """

    settings = get_settings()

    # 1) Use SQL-provided URL if present (full https URL)
    if sql_og_image and sql_og_image.startswith("http"):
        return sql_og_image

    # 2) Try lender-specific hero image
    lender_hero = f"{settings.asset_base_url}/lenders/{lender_id}/hero.jpg"
    # Do not check existence (bots never require HEAD checks)
    return lender_hero


def build_carousel_urls(
    lender_id: str,
    sql_images: List[str],
) -> List[str]:
    """
    Construct absolute URLs for carousel images.

    Rules:
    - If SQL entries are absolute URLs → use directly.
    - If SQL entries are filenames → map to lenders/{lender_id}/.
    """

    settings = get_settings()
    urls: List[str] = []

    for img in sql_images:
        if not img.strip():
            continue

        if img.startswith("http"):
            urls.append(img)
        else:
            urls.append(f"{settings.asset_base_url}/lenders/{lender_id}/{img}")

    return urls
