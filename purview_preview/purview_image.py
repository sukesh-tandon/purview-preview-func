# purview_preview/purview_image.py
import logging
import os
import mimetypes
from urllib.parse import urlparse, unquote

import azure.functions as func

from shared.db_access import get_redirect_preview
from typing import Optional

# Try to import DefaultAzureCredential if available (managed identity)
try:
    from azure.identity import DefaultAzureCredential  # optional
    _HAS_AZURE_IDENTITY = True
except Exception:
    _HAS_AZURE_IDENTITY = False

from azure.storage.blob import BlobClient, ContainerClient, BlobServiceClient  # azure-storage-blob must be installed


def _parse_blob_url(url: str):
    """
    Parse a blob url like:
    https://<account>.blob.core.windows.net/<container>/<path/to/blob.jpg>
    Returns (account_url, container_name, blob_path)
    """
    p = urlparse(url)
    # p.netloc => <account>.blob.core.windows.net
    account_url = f"{p.scheme}://{p.netloc}"
    # path starts with '/'
    path = p.path.lstrip("/")
    parts = path.split("/", 1)
    if len(parts) == 1:
        container = parts[0]
        blob_path = ""
    else:
        container, blob_path = parts[0], parts[1]
    # decode URL-encoded parts
    return account_url, container, unquote(blob_path)


def _get_blob_client_from_url(blob_url: str) -> Optional[BlobClient]:
    """
    Create a BlobClient for a given blob_url using:
      1) DefaultAzureCredential (managed identity) if available, else
      2) STORAGE_CONNECTION_STRING env fallback
    """
    account_url, container, blob_path = _parse_blob_url(blob_url)
    # If DefaultAzureCredential is available in runtime and managed identity is enabled, prefer it.
    if _HAS_AZURE_IDENTITY:
        try:
            cred = DefaultAzureCredential()
            # BlobClient.from_blob_url accepts credential
            return BlobClient.from_blob_url(blob_url, credential=cred)
        except Exception as ex:
            logging.warning("[Image] DefaultAzureCredential failing: %s", ex)

    # Fallback: connection string
    conn = os.getenv("STORAGE_CONNECTION_STRING")
    if conn:
        try:
            # Build client from connection string
            service = BlobServiceClient.from_connection_string(conn)
            container_client = service.get_container_client(container)
            return container_client.get_blob_client(blob_path)
        except Exception as ex:
            logging.exception("[Image] Failed to create BlobClient from connection string: %s", ex)

    logging.error("[Image] No available credential to access blob. Set managed identity or STORAGE_CONNECTION_STRING.")
    return None


def _guess_content_type(name: str) -> str:
    ctype, _ = mimetypes.guess_type(name)
    return ctype or "application/octet-stream"


def main(req: func.HttpRequest) -> func.HttpResponse:
    """
    Route: /api/purview-image/{token}
    Returns binary image bytes with correct Content-Type.
    """
    try:
        route_params = getattr(req, "route_params", {}) or {}
        token = route_params.get("token") or req.params.get("token") or req.params.get("t")

        if not token:
            return func.HttpResponse("token missing", status_code=400)

        # Use existing logic to resolve token -> RedirectPreview (and therefore image_url)
        preview = get_redirect_preview(token)
        if not preview:
            return func.HttpResponse("Not found", status_code=404)

        # Expect preview.image_url to be the canonical blob URL (as your JSON contains).
        blob_url = getattr(preview, "image_url", None)
        if not blob_url:
            # fallback: try reading from lender JSON again (db_access should do this but safety)
            logging.warning("[Image] preview has no image_url for token=%s", token)
            return func.HttpResponse("No image", status_code=404)

        # Create blob client
        blob_client = _get_blob_client_from_url(blob_url)
        if not blob_client:
            return func.HttpResponse("Image unavailable (auth)", status_code=503)

        # Download blob
        try:
            downloader = blob_client.download_blob()
            data = downloader.readall()
        except Exception as ex:
            logging.exception("[Image] Failed download for %s", blob_url)
            return func.HttpResponse("Image not found", status_code=404)

        # Determine content-type from blob name / url
        _, _, blob_path = _parse_blob_url(blob_url)
        content_type = _guess_content_type(blob_path)

        # Cache for a reasonable time - images are static; messengers also cache aggressively.
        headers = {
            "Content-Type": content_type,
            "Cache-Control": "public, max-age=604800, immutable"  # 7 days
        }

        return func.HttpResponse(body=data, status_code=200, headers=headers, mimetype=content_type)

    except Exception:
        logging.exception("[Image] Unhandled error")
        return func.HttpResponse("Image error", status_code=500)
