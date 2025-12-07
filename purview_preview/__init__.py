import json
import html
from typing import List

import azure.functions as func

from ..shared.config import get_settings
from ..shared.db_access import get_redirect_preview
from ..shared.models import PreviewPage
from ..shared.storage import resolve_image_url


def build_preview_page(token: str) -> PreviewPage:
    """
    Build the preview model (title, description, image, etc.)
    based on redirect_previews row if present, or defaults otherwise.
    """
    settings = get_settings()
    rp = get_redirect_preview(token)

    if rp:
        title = f"Check your {rp.lender_display_name} offer"
        description = (
            "Tap to see your personalised loan offer and continue securely via Duit Digital."
        )
        image_url = resolve_image_url(rp)
        carousel_images: List[str] = rp.carousel_images[:5]
        lender_display_name = rp.lender_display_name or rp.lender or "Duit Digital partner offer"
        cta_url = rp.cta_url or f"{settings.public_base_url}/DUITAI/{token}"
    else:
        title = "Check your loan offer with Duit Digital"
        description = (
            "Tap to see if you are eligible for loan offers from our lending partners."
        )
        image_url = resolve_image_url(None)
        carousel_images = []
        lender_display_name = "Duit Digital partner offer"
        cta_url = f"{settings.public_base_url}/DUITAI/{token}"

    # Even though the function is at /purview-preview/{token},
    # canonical should point to the real redirect URL used in messages.
    canonical_url = f"{settings.public_base_url}/DUITAI/{token}"

    return PreviewPage(
        token=token,
        title=title,
        description=description,
        image_url=image_url,
        carousel_images=carousel_images,
        lender_display_name=lender_display_name,
        theme_color=settings.default_theme_color,
        canonical_url=canonical_url,
        cta_url=cta_url,
    )


def render_carousel_images(images: List[str]) -> str:
    """
    Render static horizontal image carousel (0–5 images).
    """
    parts: List[str] = []
    for url in images:
        safe_url = html.escape(url, quote=True)
        parts.append(
            f'<img src="{safe_url}" '
            'style="width:78%;max-width:260px;display:inline-block;'
            'border-radius:10px;margin-right:8px;" />'
        )
    return "".join(parts)


def render_html(page: PreviewPage) -> str:
    """
    Render full HTML document: head (OG/meta) + body (hero + optional carousel + CTA).
    """
    title = html.escape(page.title)
    description = html.escape(page.description)
    image_url = html.escape(page.image_url, quote=True)
    canonical_url = html.escape(page.canonical_url, quote=True)
    cta_url = html.escape(page.cta_url, quote=True)
    lender_display_name = html.escape(page.lender_display_name)
    theme_color = html.escape(page.theme_color, quote=True)

    carousel_html = render_carousel_images(page.carousel_images)

    # HEAD + BODY combined template
    html_doc = f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="utf-8" />
    <title>{title}</title>

    <!-- Basic SEO + Mobile -->
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <meta name="description" content="{description}" />
    <link rel="canonical" href="{canonical_url}" />
    <meta name="theme-color" content="{theme_color}" />

    <!-- Open Graph (WhatsApp / RCS / Social Apps) -->
    <meta property="og:title" content="{title}" />
    <meta property="og:description" content="{description}" />
    <meta property="og:image" content="{image_url}" />
    <meta property="og:url" content="{canonical_url}" />
    <meta property="og:type" content="website" />
    <meta property="og:site_name" content="Duit Digital" />
    <meta property="og:image:width" content="1200" />
    <meta property="og:image:height" content="628" />

    <!-- WhatsApp-specific optimisations -->
    <meta property="al:android:url" content="{cta_url}" />
    <meta property="al:web:url" content="{cta_url}" />
    <meta name="format-detection" content="telephone=no" />

    <!-- Twitter / X -->
    <meta name="twitter:card" content="summary_large_image" />
    <meta name="twitter:title" content="{title}" />
    <meta name="twitter:description" content="{description}" />
    <meta name="twitter:image" content="{image_url}" />
    <meta name="twitter:url" content="{canonical_url}" />

    <!-- Android SMS Link Preview (WebView Preview Engines) -->
    <meta itemprop="name" content="{title}" />
    <meta itemprop="description" content="{description}" />
    <meta itemprop="image" content="{image_url}" />

    <!-- Optional: keep crawlers from indexing preview pages -->
    <meta name="robots" content="noindex, nofollow, noarchive" />
</head>

<body style="margin:0;padding:0;background:#f5f5f5;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,Helvetica,Arial,sans-serif;">

    <main style="max-width:520px;margin:24px auto;padding:12px;">

        <!-- CARD WRAPPER -->
        <section style="background:#fff;border-radius:14px;padding:16px;box-shadow:0 2px 10px rgba(0,0,0,0.07);">

            <!-- LENDER LABEL -->
            <div style="font-size:12px;font-weight:600;text-transform:uppercase;letter-spacing:0.07em;color:#555;margin-bottom:8px;text-align:center;">
                {lender_display_name} · via Duit Digital
            </div>

            <!-- HERO IMAGE -->
            <img src="{image_url}" alt="Offer Preview"
                style="width:100%;border-radius:12px;display:block;margin:0 auto 16px auto;" />

            <!-- OPTIONAL CAROUSEL (STATIC CARDS, VISIBLE ONLY IF PRESENT) -->
            <div style="overflow-x:auto;white-space:nowrap;padding-bottom:8px;margin-bottom:16px;">
                {carousel_html}
            </div>

            <!-- OFFER TITLE -->
            <h1 style="font-size:20px;font-weight:700;text-align:center;margin:0 0 12px 0;color:#111;">
                {title}
            </h1>

            <!-- DESCRIPTION -->
            <p style="font-size:14px;line-height:1.45;text-align:center;margin:0 0 20px 0;color:#333;">
                {description}
            </p>

            <!-- CTA BUTTON -->
            <a href="{cta_url}"
               style="
                    display:block;
                    width:100%;
                    background:#0E5DF2;
                    color:#fff;
                    text-align:center;
                    padding:14px 0;
                    border-radius:999px;
                    font-size:16px;
                    font-weight:600;
                    text-decoration:none;
                    margin:0 auto 8px auto;
                ">
                Continue to Your Offer
            </a>

            <!-- TRUST TEXT -->
            <div style="font-size:11px;text-align:center;color:#888;margin-top:6px;">
                Secured redirect via Duit Digital
            </div>

        </section>

    </main>

</body>
</html>
"""
    return html_doc


def main(req: func.HttpRequest) -> func.HttpResponse:
    """
    HTTP entrypoint for Purview preview.

    - Reads {token} from route.
    - Fetches redirect_previews row (if any).
    - Returns HTML with OG/meta + preview page.
    """
    token = req.route_params.get("token", "").strip()

    if not token:
        return func.HttpResponse(
            "Missing token.",
            status_code=400,
            mimetype="text/plain"
        )

    try:
        page = build_preview_page(token)
        html_doc = render_html(page)
    except Exception as exc:
        # Very conservative error behaviour: generic fallback preview.
        # Do NOT leak internal errors.
        fallback_html = """<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="utf-8" />
    <title>Duit Digital preview unavailable</title>
    <meta name="robots" content="noindex, nofollow, noarchive" />
</head>
<body>
    <p>Preview is temporarily unavailable.</p>
</body>
</html>
"""
        return func.HttpResponse(
            body=fallback_html,
            status_code=200,
            mimetype="text/html"
        )

    return func.HttpResponse(
        body=html_doc,
        status_code=200,
        mimetype="text/html"
    )
