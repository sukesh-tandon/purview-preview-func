from dataclasses import dataclass, field
from typing import List


@dataclass
class RedirectPreview:
    """
    Raw data fetched from SQL for a token.
    """
    token: str
    lender: str
    lender_display_name: str
    og_image_url: str
    carousel_images: List[str] = field(default_factory=list)
    cta_url: str = ""


@dataclass
class PreviewPage:
    """
    Final structured payload used to render OG/meta tags and HTML preview.
    """
    token: str
    title: str
    description: str
    image_url: str
    carousel_images: List[str] = field(default_factory=list)
    lender_display_name: str = ""
    theme_color: str = "#0E5DF2"
    canonical_url: str = ""
    cta_url: str = ""
