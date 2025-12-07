import html
from typing import List, Iterable

import azure.functions as func

from ..shared.config import get_settings
from ..shared.db_access import get_redirect_preview
from ..shared.models import PreviewPage
from ..shared.storage import (
    select_hero_image,
    build_carousel_urls,
    build_default_image_url,
)

# -------------------------------------------------------------------
# VALIDATION + SANITIZATION HELPERS
# -------------------------------------------------------------------

def is_valid_url(url: str) -> bool:
    """Simple, safe URL validator."""
    if not url or not isinstance(url, str):
        return False
    return url.startswith("http://") or url.startswith("https://")


def safe_url(url: str, fallback: str) -> str:
    """Return URL if valid, otherwise fallback."""
    return url if is_valid_url(url) else fallback


def clean_display_name(name: str, fallback: str = "Duit Digital partner offer") -> str:
    """Sanitize lender_display_name."""
    if not name or not isinstance(name, str):
        return fallback
    cleaned = name.strip()
    return cleaned if cleaned else fallback


def is_valid_hex_color(value: str) -> bool:
    """Validate #RRGGBB color."""
    if not isinstance(value, str):
        return False
    value = value.strip()
    if len(value) != 7 or not value.startswith("#"):
        return False
    try:
        int(value[1:], 16)
        return True
    except ValueError:
        return False


def _safe_iter_urls(urls: Iterable[str] | None, max_items: int = 5) -> List[str]:
    """Normalize list-like of URLs; limit max length."""
    if not urls:
        return []
    return [str(u) for u in list(urls)[:max_items]]

# -------------------------------------------------------------------
# MAIN BUILDER
# -------------------------------------------------------------------

def build_preview_page(token: str) -> PreviewPage:
    """
    Build PreviewPage metadata using redirect_previews if present,
    else fallback defaults.
    """
    settings = get_settings()
    rp = get_redirect_preview(token)

    canonical_url = f"{settings.public_base_url}/DUITAI/{token}"

    if rp:
        # Hero (SQL → lender hero → default)
        hero_url = select_hero_image(
            lender_id=rp.lender,
            sql_og_image=rp.og_image_url,
        )

        # Carousel (max 5)
        carousel_urls = build_carousel_urls(
            lender_id=rp.lender,
            sql_images=_safe_iter_urls(rp.carousel_images),
        )

        # Lender name cleanup
        lender_display_name = clean_display_name(
            rp.lender_display_name or rp.lender,
            fallback="Duit Digital partner offer",
        )

        title = f"Check your {lender_display_name} offer"
        description = (
            "Tap to see your personalised loan offer and continue securely via Duit Digital."
        )

        # CTA fallback to canonical
        cta_url = safe_url(rp.cta_url, canonical_url)

    else:
        # DEFAULT PREVIEW
        hero_url = build_default_image_url()
        carousel_urls = []
        lender_display_name = "Duit Digital partner offer"
        title = "Check your loan offer with Duit Digital"
        description = (
            "Tap to see if you are eligible for loan offers from our lending partners."
        )
        cta_url = canonical_url

    # Theme color validation
    theme_color = (
        settings.default_theme_color
        if is_valid_hex_color(settings.default_theme_color)
        else "#0E5DF2"
    )

    return PreviewPage(
        token=token,
        title=title,
        description=description,
        image_url=hero_url,
        carousel_images=carousel_urls,
        lender_display_name=lender_display_name,
        theme_color=theme_color,
        canonical_url=canonical_url,
        cta_url=cta_url,
    )

# -------------------------------------------------------------------
# RENDERING
# -------------------------------------------------------------------

def render_carousel_images(images: List[str]) -> str:
    """Render static horizontal carousel."""
    parts: List[str] = []
    for url in images:
        if not is_valid_url(url):
            continue
        safe = html.escape(url, quote=True)
        parts.append(
            f'<img src="{safe}" '
            'loading="lazy" '
            'style="width:78%;max-width:260px;display:inline-block;'
            'border-radius:10px;margin-right:8px;" />'
        )
    return "".join(parts)


def render_html(page: PreviewPage) -> str:
    """Full HTML (OG/meta + preview UI)."""

    # Always escaped + defaulted
    title = html.escape(page.title or "Loan Offer")
    description = html.escape(page.description or "")
    image_url = html.escape(page.image_url or "", quote=True)
    canonical_url = html.escape(page.canonical_url or "", quote=True)
    cta_url = html.escape(page.cta_url or "", quote=True)
    lender_display_name = html.escape(page.lender_display_name or "")
    theme_color = html.escape(page.theme_color or "#0E5DF2", quote=True)

    carousel_html = render_carousel_images(page.carousel_images)

    html_doc = f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="utf-8" />
    <title>{title}</title>

    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <meta name="description" content="{description}" />
    <link rel="canonical" href="{canonical_url}" />
    <meta name="theme-color" content="{theme_color}" />

    <meta property="og:title" content="{title}" />
    <meta property="og:description" content="{description}" />
    <meta property="og:image" content="{image_url}" />
    <meta property="og:url" content="{canonical_url}" />
    <meta property="og:type" content="website" />
    <meta property="og:site_name" content="Duit Digital" />
    <meta property="og:image:width" content="1200" />
    <meta property="og:image:height" content="628" />

    <meta property="al:android:url" content="{cta_url}" />
    <meta property="al:web:url" content="{cta_url}" />
    <meta name="format-detection" content="telephone=no" />

    <meta name="twitter:card" content="summary_large_image" />
    <meta name="twitter:title" content="{title}" />
    <meta name="twitter:description" content="{description}" />
    <meta name="twitter:image" content="{image_url}" />
    <meta name="twitter:url" content="{canonical_url}" />

    <meta itemprop="name" content="{title}" />
    <meta itemprop="description" content="{description}" />
    <meta itemprop="image" content="{image_url}" />

    <meta name="robots" content="noindex, nofollow, noarchive" />
</head>

<body style="margin:0;padding:0;background:#f5f5f5;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,Helvetica,Arial,sans-serif;">

    <main style="max-width:520px;margin:24px auto;padding:12px;">

        <section style="background:#fff;border-radius:14px;padding:16px;box-shadow:0 2px 10px rgba(0,0,0,0.07);">

            <div style="font-size:12px;font-weight:600;text-transform:uppercase;letter-spacing:0.07em;color:#555;margin-bottom:8px;text-align:center;">
                {lender_display_name} · via Duit Digital
            </div>

            <img src="{image_url}" alt="Offer Preview"
                style="width:100%;border-radius:12px;display:block;margin:0 auto 16px auto;" />

            <div style="overflow-x:auto;white-space:nowrap;padding-bottom:8px;margin-bottom:16px;">
                {carousel_html}
            </div>

            <h1 style="font-size:20px;font-weight:700;text-align:center;margin:0 0 12px 0;color:#111;">
                {title}
            </h1>

            <p style="font-size:14px;line-height:1.45;text-align:center;margin:0 0 20px 0;color:#333;">
                {description}
            </p>

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

            <div style="font-size:11px;text-align:center;color:#888;margin-top:6px;">
                Secured redirect via Duit Digital
            </div>

        </section>

    </main>

</body>
</html>"""

    return html_doc

# -------------------------------------------------------------------
# HTTP ENTRYPOINT
# -------------------------------------------------------------------

def main(req: func.HttpRequest) -> func.HttpResponse:
    token = (req.route_params.get("token") or "").strip()

    if not token:
        return func.HttpResponse("Missing token.", status_code=400)

    try:
        page = build_preview_page(token)
        html_doc = render_html(page)
    except Exception:
        fallback_html = "<!DOCTYPE html><html><body>Preview temporarily unavailable.</body></html>"
        return func.HttpResponse(fallback_html, status_code=200, mimetype="text/html")

    return func.HttpResponse(html_doc, status_code=200, mimetype="text/html")
