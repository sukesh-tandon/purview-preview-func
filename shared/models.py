# shared/models.py

class RedirectPreview:
    """
    RedirectPreview represents the fully resolved purview result:
    - Static lender metadata (title, description, image_url, theme_color)
    - Dynamic redirect fields (destination_url, canonical_url)
    - Extra metadata (lender, mobile, campaign_id, etc.)
    """

    def __init__(
        self,
        token: str,
        title: str,
        description: str,
        image_url: str,
        theme_color: str,
        target_url: str,
        canonical_url: str,
        meta: dict | None = None,
    ):
        self.token = token
        self.title = title
        self.description = description
        self.image_url = image_url
        self.theme_color = theme_color
        self.target_url = target_url
        self.canonical_url = canonical_url
        self.meta = meta or {}

    def to_dict(self):
        """Optional helper for JSON debugging."""
        return {
            "token": self.token,
            "title": self.title,
            "description": self.description,
            "image_url": self.image_url,
            "theme_color": self.theme_color,
            "target_url": self.target_url,
            "canonical_url": self.canonical_url,
            "meta": self.meta,
        }
