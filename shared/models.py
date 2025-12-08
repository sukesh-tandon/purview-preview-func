from dataclasses import dataclass

@dataclass
class RedirectPreview:
    token: str
    title: str
    description: str
    target_url: str
    image_url: str
    theme_color: str
    canonical_url: str
