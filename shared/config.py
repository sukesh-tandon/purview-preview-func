import os

# Base URL where the redirect/purview pages live
PUBLIC_BASE_URL: str = os.getenv("PUBLIC_BASE_URL", "https://r.duitai.in")

# Default theme color for fallback previews
DEFAULT_THEME_COLOR: str = os.getenv("DEFAULT_THEME_COLOR", "#0047AB")

# Default OpenGraph image for missing tokens
DEFAULT_OG_IMAGE_URL: str = os.getenv(
    "DEFAULT_OG_IMAGE_URL",
    f"{PUBLIC_BASE_URL.rstrip('/')}/static/default-og.png"
)
