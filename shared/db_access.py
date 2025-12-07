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
    Fetch preview metadata from dbo.redirect_previews for this token.
    Returns None if the token is missing.
    """

    settings = get_settings()
    conn = get_connection()

    try:
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT token, lender, lender_display_name,
                   og_image_url, carousel_images, cta_url
            FROM dbo.redirect_previews
            WHERE token = ?
            """,
            token,
        )

        row = cursor.fetchone()
        if not row:
            return None

        token_db = row[0]
        lender = row[1] or ""
        lender_display_name = row[2] or lender
        og_image_url = row[3] or ""

        carousel_raw = row[4]
        cta_url_db = row[5] or ""

        # --- Parse carousel image list ---
        images: List[str] = []
        if carousel_raw:
            try:
                parsed = json.loads(carousel_raw)
                if isinstance(parsed, list):
                    images = [str(x).strip() for x in parsed if str(x).strip()]
                else:
                    images = []
            except Exception:
                # Comma separated fallback
                images = [
                    item.strip()
                    for item in str(carousel_raw).split(",")
                    if item.strip()
                ]

        # --- CTA fallback to DUITAI/{token} ---
        if not cta_url_db.strip():
            # Always guarantee correct CTA for V1
            cta_url = f"{settings.public_base_url}/DUITAI/{token_db}"
        else:
            cta_url = cta_url_db

        return RedirectPreview(
