from dataclasses import dataclass
from typing import List


@dataclass
class RedirectPreview:
    token: str
    lender: str
    lender_display_name: str
    og_image_url: str
    carousel_images: List[str]
    cta_url: str


@dataclass
class PreviewPage:
    token: str
    title: str
    description: str
    image_url: str
    carousel_images: List[str]
    lender_display_name: str
    theme_color: str
    canonical_url: str
    cta_url: str
