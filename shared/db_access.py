from typing import Optional, List
import json

from azure.data.tables import TableServiceClient
from .config import get_settings
from .models import RedirectPreview


def get_table_client():
    """
    Create Table client using connection string.
    Works in Python 3.13 with no native deps.
    """
    settings = get_settings()
    service = TableServiceClient.from_connection_string(
        conn_str=settings.table_connection_string
    )
    return service.get_table_client(table_name="redirectpreviews")


def get_redirect_preview(token: str) -> Optional[RedirectPreview]:
    """
    Fetch redirect preview from Azure Table Storage.
    RowKey == token.
    """
    table = get_table_client()

    try:
        entity = table.get_entity(partition_key="token", row_key=token)
    except Exception:
        return None

    # Extract fields safely
    lender = entity.get("lender", "")
    lender_display_name = entity.get("lender_display_name", "") or lender
    og_image_url = entity.get("og_image_url", "")
    raw_carousel = entity.get("carousel_images", "")
    cta_url = entity.get("cta_url", "")

    # Parse carousel images
    images: List[str] = []

    if raw_carousel:
        try:
            parsed = json.loads(raw_carousel)
            if isinstance(parsed, list):
                images = [str(x).strip() for x in parsed if str(x).strip()]
        except Exception:
            images = [
                part.strip() for part in raw_carousel.split(",") if part.strip()
            ]

    return RedirectPreview(
        token=token,
        lender=lender,
        lender_display_name=lender_display_name,
        og_image_url=og_image_url,
        carousel_images=images,
        cta_url=cta_url,
    )
