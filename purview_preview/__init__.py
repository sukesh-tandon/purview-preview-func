import logging
import azure.functions as func
from shared.db_access import get_redirect_preview


def main(req: func.HttpRequest, token: str) -> func.HttpResponse:
    """
    Azure Function entry point.
    Token is injected directly from route: /purview-preview/{token}
    """
    logging.info(f"PURVIEW-V1 request received for token: {token}")

    # --------------------------------------------------------------------------------------
    # Validate token
    # --------------------------------------------------------------------------------------
    if not token:
        logging.warning("No token provided in route or query.")
        return func.HttpResponse("Invalid link: token missing", status_code=400)

    # --------------------------------------------------------------------------------------
    # Load preview JSON → /tmp/redirect-previews/{token}.json
    # --------------------------------------------------------------------------------------
    preview = get_redirect_preview(token)

    if not preview:
        logging.warning(f"No preview found for token: {token}")
        return func.HttpResponse(
            "Preview not found. Token may be expired.",
            status_code=404
        )

    # --------------------------------------------------------------------------------------
    # Build ultra-fast OG/HTML response
    # --------------------------------------------------------------------------------------
    html = _build_html(preview)

    return func.HttpResponse(
        html,
        status_code=200,
        mimetype="text/html",
        headers={
            # Cache for 60 sec (safe for messaging preview)
            "Cache-Control": "public, max-age=60"
        }
    )


# ====================================================================
# INTERNAL HELPERS
# ====================================================================

def _build_html(preview) -> str:
    """
    Generates optimized OG preview compatible with:
    - WhatsApp
    - RCS
    - iMessage
    - Facebook
    - LinkedIn
    """

    title = preview.title or ""
    description = preview.description or ""
    image_url = preview.image_url or ""
    canonical_url = preview.canonical_url or ""
    theme_color = preview.theme_color or "#ffffff"
    target_url = preview.target_url or canonical_url

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
    <meta property="og:image" content="{image_url}" />

    <!-- Twitter -->
    <meta name="twitter:card" content="summary_large_image" />
    <meta name="twitter:title" content="{title}" />
    <meta name="twitter:description" content="{description}" />
    <meta name="twitter:image" content="{image_url}" />

    <style>
        :root {{
            color-scheme: light dark;
        }}
        body {{
            margin: 0;
            font-family: system-ui, -apple-system, BlinkMacSystemFont,
                         "Segoe UI", Roboto, Ubuntu, sans-serif;
            background: #050816;
            color: #f4f4f5;
            display: flex;
            align-items: center;
            justify-content: center;
            min-height: 100vh;
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
            display: inline-flex;
            align-items: center;
            justify-content: center;
            padding: 0.65rem 1.2rem;
            border-radius: 999px;
            background: {theme_color};
            color: #0b1020;
            font-weight: 600;
            font-size: 0.95rem;
            text-decoration: none;
        }}
        .card-button span {{
            margin-left: 0.35rem;
            font-size: 1.05rem;
        }}
    </style>
</head>

<body>
    <main class="card">
        <div class="card-title">{title}</div>
        <div class="card-desc">{description}</div>
        <a class="card-button" href="{target_url}">
            Check my offer <span>↗</span>
        </a>
    </main>
</body>
</html>
"""
