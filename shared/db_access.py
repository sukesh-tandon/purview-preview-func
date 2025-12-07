import json
from typing import Optional, List

import pyodbc

from .config import get_settings
from .models import RedirectPreview


def get_connection() -> pyodbc.Connection:
    """
    Create a new SQL connection using the configured connection string.
    """
    settings = get_settings()
    return pyodbc.connect(settings.sql_connection_string)


def get_redirect_preview(token: str) -> Optional[RedirectPreview]:
    """
    Fetch a single row from dbo.redirect_previews by token.
    Returns None if not found.
    """
    conn = get_connection()
    try:
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT token, lender, lender_display_name, og_image_url, carousel_images, cta_url
            FROM dbo.redirect_previews
            WHERE token = ?
            """,
            token,
        )
        row = cursor.fetchone()
        if not row:
            return None

        token_db = row[0]
        lender = row[1]
        lender_display_name = row[2]
        og_image_url = row[3]
        carousel_raw = row[4]
        cta_url = row[5]

        images: List[str] = []
        if carousel_raw:
            try:
                # Attempt JSON array first
                parsed = json.loads(carousel_raw)
                if isinstance(parsed, list):
                    images = [str(x) for x in parsed if str(x).strip()]
                else:
                    images = []
            except Exception:
                # Fallback: comma-separated list
                images = [
                    part.strip()
                    for part in str(carousel_raw).split(",")
                    if part.strip()
                ]

        return RedirectPreview(
            token=token_db,
            lender=lender,
            lender_display_name=lender_display_name or lender,
            og_image_url=og_image_url or "",
            carousel_images=images,
            cta_url=cta_url or "",
        )

    finally:
        conn.close()
