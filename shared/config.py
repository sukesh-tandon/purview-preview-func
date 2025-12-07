import os
from dataclasses import dataclass


@dataclass
class Settings:
    sql_connection_string: str
    public_base_url: str
    default_theme_color: str
    default_og_image_url: str
    asset_base_url: str


def get_settings() -> Settings:
    """
    Load environment variables safely for Purview V1.
    """

    return Settings(
        sql_connection_string=os.environ["SQL_CONNECTION_STRING"],

        # Base redirect host
        public_base_url=os.getenv("PUBLIC_BASE_URL", "https://r.duitai.in"),

        # Theme color for OG embeds
        default_theme_color=os.getenv("DEFAULT_THEME_COLOR", "#0E5DF2"),

        # Default fallback OG image
        default_og_image_url=os.getenv(
            "DEFAULT_OG_IMAGE_URL",
            "https://stduitcampaigns.blob.core.windows.net/purview-assets/defaults/hero.jpg"
        ),

        # Root for all Purview images (lender folders)
        asset_base_url=os.getenv(
            "ASSET_BASE_URL",
            "https://stduitcampaigns.blob.core.windows.net/purview-assets"
        ),
    )
