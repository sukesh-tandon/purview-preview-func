# purview_preview/__init__.py
import logging
import azure.functions as func

from shared.db_access import get_redirect_preview
from urllib.parse import quote_plus

def main(req: func.HttpRequest) -> func.HttpResponse:
    """
    Azure Function entry point.
    Route: purview-preview/{token}
    """
    try:
        logging.info("PURVIEW-V1 request received")

        route_params = getattr(req, "route_params", {}) or {}
        token = (
            route_params.get("token")
            or req.params.get("token")
            or req.params.get("t")
        )

        # Health probes / always-on
        if token in (None, "", "health", "favicon.ico", "warmup", "ready"):
            return func.HttpResponse(status_code=204)

        if not token:
            logging.warning("No token provided in route or query.")
            return func.HttpResponse(
                "Invalid link: token missing",
                status_code=400,
                mimetype="text/plain",
            )

        preview = get_redirect_preview(token)

        if not preview:
            logging.warning(f"No preview found for token: {token}")
            return func.HttpResponse(
                "Preview not found. Token may be expired.",
                status_code=404,
                mimetype="text/plain",
            )

        html = _build_html(preview, token)
        return func.HttpResponse(
            html,
            status_code=200,
            mimetype="text/html",
            headers={"Cache-Control": "public, max-age=60"},
        )

    except Exception as exc:
        logging.exception("PURVIEW-V1: unhandled exception")
        return func.HttpResponse(
            "Preview service temporarily unavailable.",
            status_code=500,
            mimetype="text/plain",
        )


def _build_html(preview, token) -> str:
    """
    Builds the on-screen Purview page + OG tags.
    Uses internal token-based image endpoint for og:image.
    """

    title = preview.title or ""
    description = preview.description or ""
    # OG image now points to our internal endpoint which serves the blob
    # we url-quote token so it is safe in path
    image_endpoint = f"/api/purview-image/{quote_plus(token)}"
    canonical_url = preview.canonical_url or ""
    target_url = preview.target_url or canonical_url
    theme_color = preview.theme_color or "#ffffff"

    # Make image_url absolute for OG fetchers (they need an absolute URL).
    # We build absolute using the Host header (Azure will include).
    # Using relative paths may break OG fetchers, so we use the full origin if possible.
    # We'll insert absolute URL in meta by relying on the Host header at request-time.
    # To simplify, set full URL via protocol + host if available in JS-less environment:
    # We'll let Azure / clients resolve relative URL by creating absolute URL in meta tag using window.location if needed,
    # but safe route: include absolute path with 'https://' + host if host is available.
    # However since we are server-side, the Host isn't passed here — the purview fetchers will accept absolute public function URL.
    # Best practice: build based on known function host if set in env:
    FUNCTION_HOST = (os.getenv("FUNCTION_HOST") or "").rstrip("/")  # optional env override

    if FUNCTION_HOST:
        og_image_url = FUNCTION_HOST + image_endpoint
    else:
        # Fallback to relative URL — most scrapers will resolve it from the request URL.
        og_image_url = image_endpoint

    # Inline HTML (same look and feel you had), plus hero image referencing internal endpoint
    return f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="utf-8" />
    <title>{title}</title>
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <meta name="description" content="{description}" />
    <meta name="theme-color" content="{theme_color}" />
    <link rel="canonical" href="{canonical_url}" />

    <!-- Open Graph -->
    <meta property="og:type" content="website" />
    <meta property="og:title" content="{title}" />
    <meta property="og:description" content="{description}" />
    <meta property="og:url" content="{canonical_url}" />
    <meta property="og:image" content="{og_image_url}" />

    <!-- Twitter -->
    <meta name="twitter:card" content="summary_large_image" />
    <meta name="twitter:title" content="{title}" />
    <meta name="twitter:description" content="{description}" />
    <meta name="twitter:image" content="{og_image_url}" />

    <style>
        body {{
            margin: 0;
            font-family: system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto;
            background: #050816;
            color: #f4f4f5;
            display: flex;
            align-items: center;
            justify-content: center;
            min-height: 100vh;
            flex-direction: column;
        }}

        .hero {{
            width: 100%;
            max-width: 480px;
            margin-bottom: 1.5rem;
            border-radius: 1rem;
            overflow: hidden;
            box-shadow: 0 20px 40px rgba(0,0,0,0.45);
        }}

        .hero img {{
            width: 100%;
            display: block;
        }}

        .card {{
            max-width: 480px;
            padding: 1.5rem;
            border-radius: 1.25rem;
            background: radial-gradient(circle at top, #111827 0, #020617 55%);
            box-shadow:
                0 18px 45px rgba(0, 0, 0, 0.6),
                0 0 0 1px rgba(148, 163, 184, 0.2);
        }}

        .card-title {{
            font-size: 1.3rem;
            font-weight: 600;
            margin-bottom: 0.5rem;
        }}

        .card-desc {{
            font-size: 1rem;
            color: #d1d5db;
            margin-bottom: 1.25rem;
        }}

        .card-button {{
            padding: 0.65rem 1.2rem;
            border-radius: 999px;
            background: {theme_color};
            color: #0b1020;
            font-weight: 600;
            font-size: 0.95rem;
            text-decoration: none;
        }}
    </style>
</head>

<body>

    <!-- Hero image is served by internal image endpoint -->
    <div class="hero">
        <img src="{image_endpoint}" alt="Offer Preview" />
    </div>

    <main class="card">
        <div class="card-title">{title}</div>
        <div class="card-desc">{description}</div>
        <a class="card-button" href="{target_url}">
            Check my offer ↗
        </a>
    </main>

</body>
</html>
"""
