import json
from typing import Optional, List

import pytds              # pure python SQL driver, works on Azure Linux

from .config import get_settings
from .models import RedirectPreview


def get_connection() -> pytds.Connection:
    """
    Create a new SQL connection using pytds.
    Azure SQL connection string format:
    Server=tcp:<server>.database.windows.net,1433;Database=...;User ID=...;Password=...
    """
    settings = get_settings()

    # Parse connection string manually → pytds does not accept raw ODBC format
    conn_str = settings.sql_connection_string
    pairs = dict(
        item.strip().split("=", 1)
        for item in conn_str.split(";")
        if "=" in item
    )

    server = pairs.get("Server") or pairs.get("server")
    if server and server.lower().startswith("tcp:"):
        server = server[4:]  # remove leading tcp:

    database = pairs.get("Database") or pairs.get("database")
    user = pairs.get("User ID") or pairs.get("uid") or pairs.get("user")
    password = pairs.get("Password") or pairs.get("pwd")

    host, port = (server.split(",") + ["1433"])[:2]

    return pytds.connect(
        server=host,
        database=database,
        user=user,
        password=password,
        port=int(port),
        timeout=5,
        login_timeout=5,
        as_dict=False,
    )


def get_redirect_preview(token: str) -> Optional[RedirectPreview]:
    """
    Fetch a row from dbo.redirect_previews using pytds.
    """
    conn = get_connection()
    try:
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT token, lender, lender_display_name,
                   og_image_url, carousel_images, cta_url
            FROM dbo.redirect_previews
            WHERE token = %s
            """,
            (token,),
        )
        row = cursor.fetchone()
        if not row:
            return None

        (
            token_db,
            lender,
            lender_display_name,
            og_image_url,
            carousel_raw,
            cta_url,
        ) = row

        images: List[str] = []
        if carousel_raw:
            try:
                parsed = json.loads(carousel_raw)
                if isinstance(parsed, list):
                    images = [str(x) for x in parsed if str(x).strip()]
            except Exception:
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
