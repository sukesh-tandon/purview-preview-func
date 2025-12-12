import logging
import azure.functions as func

from shared.db_access import get_redirect_preview


def main(req: func.HttpRequest) -> func.HttpResponse:
    """
    Azure Function entry point.
    """

    try:
        logging.info("PURVIEW-V1 request received")

        route_params = getattr(req, "route_params", {}) or {}
        token = (
            route_params.get("token")
            or req.params.get("token")
            or req.params.get("t")
        )

        # --------------------------------------------------------------
        # HEALTH CHECK / ALWAYS-ON BLOCKERS (Azure calls every 10 sec)
        # --------------------------------------------------------------
        if token in (None, "", "health", "favicon.ico", "warmup", "ready"):
            return func.HttpResponse(status_code=204)

        if not token:
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

        html = _build_html(preview)

        return func.HttpResponse(
            html,
            status_code=200,
            mimetype="text/html",
            headers={"Cache-Control": "public, max-age=60"},
        )

    except Exception:
        logging.exception("PURVIEW-V1: unhandled exception")
        return func.HttpResponse(
            "Preview service temporarily unavailable.",
            status_code=500,
            mimetype="text/plain",
        )


# ====================================================================
# INTERNAL HELPERS
# ====================================================================

def _build_html(preview) -> str:
    """
    Builds the on-screen Purview page + OG tags.
    """

    title = preview.title or ""
    description = preview.description or ""
    image_url = preview.image_url or ""
    canonical_url = preview.canonical_url or ""
    target_url = preview.target_url or canonical_url
    theme_color = preview.theme_color or "#ffffff"

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="utf-8" />
    <title>{title}</title>

    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <meta name="description" content="{description}" />
    <meta name="theme-color" content="{theme_color}" />
    <link rel="canonical" href="{canonical_url}" />

    <!-- OG -->
    <meta property="og:type" content="website" />
    <meta property="og:title" content="{title}" />
    <meta property="og:description" content="{description}" />
    <meta property="og:url" content="{canonical_url}" />
    <meta property="og:image" content="{image_url}" />

    <!-- Twitter -->
    <meta name="twitter:card" content="summary_large_image" />
    <meta name="twitter:title" content="{title}" />
    <meta name="twitter:description" content="{description}" />
    <meta name="twitter:image" content="{image_url}" />

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

    <!-- NEW: Lender-specific hero/banner image -->
    <div class="hero">
        <img src="{image_url}" alt="Offer Preview" />
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
