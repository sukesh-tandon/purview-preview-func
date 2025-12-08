import json
import aiohttp
from typing import Optional, List

from azure.identity import ClientSecretCredential
from .config import get_settings
from .models import RedirectPreview


# Azure SQL REST endpoint format
# https://<server>.database.windows.net/rest/v1/query?database=<dbname>
REST_URL_TEMPLATE = "https://{server}/rest/v1/query?database={db}"


async def run_sql_query(sql: str, params: List = None):
    """
    Execute SQL using Azure SQL REST API
    Auth: AAD service principal (ClientSecretCredential)
    """
    settings = get_settings()

    credential = ClientSecretCredential(
        tenant_id=settings.sql_tenant_id,
        client_id=settings.sql_client_id,
        client_secret=settings.sql_client_secret
    )

    token = credential.get_token("https://database.windows.net/.default").token

    url = REST_URL_TEMPLATE.format(
        server=settings.sql_server_host,
        db=settings.sql_database,
    )

    body = {"query": sql}
    if params:
        body["parameters"] = params

    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }

    async with aiohttp.ClientSession() as session:
        async with session.post(url, headers=headers, json=body) as resp:
            data = await resp.json()
            return data


async def get_redirect_preview(token: str) -> Optional[RedirectPreview]:
    sql = """
        SELECT token, lender, lender_display_name,
               og_image_url, carousel_images, cta_url
        FROM dbo.redirect_previews
        WHERE token = @token
    """

    sql_params = [
        {"name": "token", "value": token}
    ]

    result = await run_sql_query(sql, sql_params)

    rows = result.get("rows", [])
    if not rows:
        return None

    row = rows[0]
    token_db = row["token"]
    lender = row["lender"]
    lender_display_name = row["lender_display_name"] or ""
    og_image_url = row["og_image_url"] or ""
    raw_carousel = row["carousel_images"] or ""
    cta_url = row["cta_url"] or ""

    # Parse carousel
    images: List[str] = []
    if raw_carousel:
        try:
            parsed = json.loads(raw_carousel)
            if isinstance(parsed, list):
                images = [str(x).strip() for x in parsed]
        except Exception:
            images = [p.strip() for p in raw_carousel.split(",") if p.strip()]

    return RedirectPreview(
        token=token_db,
        lender=lender,
        lender_display_name=lender_display_name,
        og_image_url=og_image_url,
        carousel_images=images,
        cta_url=cta_url,
    )
