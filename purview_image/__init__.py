import azure.functions as func
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient
import logging
import os

ACCOUNT_NAME = "stduitcampaigns"
CONTAINER = "purview-assets"

def main(req: func.HttpRequest) -> func.HttpResponse:
    lender = req.route_params.get("lender")
    if not lender:
        return func.HttpResponse("Missing lender", status_code=400)

    blob_path = f"{lender}/purview_v1.jpg"

    try:
        # Managed Identity
        credential = DefaultAzureCredential()

        svc = BlobServiceClient(
            account_url=f"https://{ACCOUNT_NAME}.blob.core.windows.net",
            credential=credential,
        )

        blob = svc.get_blob_client(CONTAINER, blob_path)
        data = blob.download_blob().readall()

        return func.HttpResponse(
            body=data,
            status_code=200,
            mimetype="image/jpeg",
            headers={"Cache-Control": "public, max-age=86400"},
        )

    except Exception as e:
        logging.error(f"[purview_image] Failed to get {blob_path}: {e}")
        return func.HttpResponse("Not found", status_code=404)
