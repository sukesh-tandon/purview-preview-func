import os
from dataclasses import dataclass


@dataclass
class Settings:
    sql_connection_string: str
    public_base_url: str
    default_theme_color: str
    default_og_image_url: str


def get_settings() -> Settings:
    """
    Read configuration from environment variables.
    """
    return Settings(
        sql_connection_string=os.environ["SQL_CONNECTION_STRING"],
        public_base_url=os.getenv("PUBLIC_BASE_URL", "https://r.duitai.in"),
        default_theme_color=os.getenv("DEFAULT_THEME_COLOR", "#0E5DF2"),
        default_og_image_url=os.getenv("DEFAULT_OG_IMAGE_URL", ""),
    )
