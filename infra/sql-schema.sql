CREATE TABLE dbo.redirect_previews (
    token               VARCHAR(128)    NOT NULL PRIMARY KEY,
        -- Must match redirects.token

    lender              VARCHAR(100)    NOT NULL,
        -- Same naming as redirects.lender

    lender_display_name VARCHAR(150)    NOT NULL,
        -- Clean user-facing name

    og_image_url        VARCHAR(500)    NOT NULL,
        -- Full URL to OG hero image

    carousel_images     NVARCHAR(MAX)   NULL,
        -- JSON array string OR comma-separated list

    cta_url             VARCHAR(500)    NOT NULL,
        -- Always https://r.duitai.in/DUITAI/{token}

    created_at          DATETIME2       NOT NULL
        DEFAULT (SYSUTCDATETIME()),

    updated_at          DATETIME2       NOT NULL
        DEFAULT (SYSUTCDATETIME())
);
