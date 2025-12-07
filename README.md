\# Purview Preview Function App (V1)



This Function App serves OpenGraph / meta previews and a lightweight HTML preview page

for redirect tokens. It is read-only and does NOT modify or replace the existing

/DUITAI/{token} redirect engine.



\## Key Points



\- Runtime: Azure Functions, Python

\- Trigger: HTTP (anonymous)

\- Route: /api/purview-preview/{token}

\- Reads from: dbo.redirect\_previews (SQL Server)

\- Returns:

&nbsp; - OG/meta tags for bots (WhatsApp / RCS / SMS / Social)

&nbsp; - Minimal HTML preview page with hero image, optional carousel, and CTA



\## Environment Variables



\- `SQL\_CONNECTION\_STRING` (required)

\- `PUBLIC\_BASE\_URL` (defaults to https://r.duitai.in)

\- `DEFAULT\_THEME\_COLOR` (defaults to #0E5DF2)

\- `DEFAULT\_OG\_IMAGE\_URL` (fallback hero image)



\## Safety



\- Uses a dedicated, read-only SQL user.

\- No changes to existing redirect Function Apps or /DUITAI/{token}.

\- Purview is additive and used only for previews.



