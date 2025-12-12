import os
import time
import logging
import json
import hashlib
import re
from typing import Optional, Dict, Any

import azure.functions as func

# try to import your shared helper (existing)
try:
    from shared.db_access import get_redirect_preview  # expected to return an object/dict
except Exception:
    get_redirect_preview = None

# ------------ Config ------------
DAB_CACHE_TTL = int(os.getenv("PURVIEW_LENDER_CACHE_TTL", "3600"))  # 1 hour default
PREVIEW_CACHE_TTL = int(os.getenv("PURVIEW_PREVIEW_CACHE_TTL", "300"))  # per-token preview cache
FUNCTION_HOST = (os.getenv("FUNCTION_HOST") or "").rstrip("/")  # optional override
# If FUNCTION_HOST is not provided, we'll build it from request headers at runtime.

# Local repo fallback path for lender JSONs (may not exist in Azure, but we try)
REPO_LENDERS_PATH = os.path.join(os.getcwd(), "redirect_previews", "lenders")
FALLBACK_JSON = os.path.join(os.getcwd(), "redirect_previews", "fallback_default.json")

# Simple in-memory caches (per warm instance)
_lender_json_cache: Dict[str, tuple[Dict[str, Any], float]] = {}  # lender -> (json, ts)
_preview_cache: Dict[str, tuple[Dict[str, Any], float]] = {}  # token -> (preview_dict, ts)

# Probe detection
_PROBE_TOKENS = {"health", "status", "ping"}
_PROBE_USERAGENTS = [
    "ELB-HealthChecker", "HealthCheck", "curl", "kube-probe", "SiteExtension",
    "Microsoft.Azure.WebSites.Diagnostics/1.0", "Pingdom", "Uptime", "AzureMonitor"
]

# Helper: normalize lender -> file-safe name (payme, poonawalla_stpl, ram_fincorp)
def _normalize_lender(name: str) -> str:
    s = (name or "").strip().lower()
    s = re.sub(r"[^\w\s-]", "", s)  # remove punctuation
    s = re.sub(r"\s+", "_", s)  # spaces -> underscore
    s = s.replace("-", "_")
    return s or "unknown"

def _now() -> float:
    return time.time()

def _load_lender_json_from_repo(normalized: str) -> Optional[Dict[str, Any]]:
    """Try to load lender default JSON from repo at runtime (best-effort)."""
    try:
        path = os.path.join(REPO_LENDERS_PATH, f"{normalized}_default.json")
        if os.path.isfile(path):
            with open(path, "r", encoding="utf-8") as fh:
                return json.load(fh)
    except Exception:
        logging.debug(f"Could not read lender JSON for {normalized} from repo", exc_info=True)
    # fallback: try the generic fallback file if present
    try:
        if os.path.isfile(FALLBACK_JSON):
            with open(FALLBACK_JSON, "r", encoding="utf-8") as fh:
                return json.load(fh)
    except Exception:
        logging.debug("Could not read fallback default JSON", exc_info=True)
    return None

def _get_lender_json(normalized: str) -> Optional[Dict[str, Any]]:
    """Return lender json cached or load from repo; TTL enforced."""
    now = _now()
    entry = _lender_json_cache.get(normalized)
    if entry:
        data, ts = entry
        if now - ts < DAB_CACHE_TTL:
            return data
        else:
            del _lender_json_cache[normalized]

    # load
    data = _load_lender_json_from_repo(normalized)
    if data:
        _lender_json_cache[normalized] = (data, now)
    return data

def _is_probe_request(token: Optional[str], req: func.HttpRequest) -> bool:
    """Detect obvious health probes or irrelevant calls that should be ignored."""
    if not token:
        return False
    t = token.lower()
    if t in _PROBE_TOKENS:
        return True
    ua = (req.headers.get("User-Agent") or "").lower()
    for probe in _PROBE_USERAGENTS:
        if probe.lower() in ua:
            return True
    # some probes call /purview-preview/health or /purview-preview/status
    if t in ("favicon.ico",):
        return True
    return False

def _build_image_endpoint(lender_normalized: str, req: func.HttpRequest) -> str:
    """Construct image endpoint url for lender. Prefer FUNCTION_HOST env, else derive from request."""
    if FUNCTION_HOST:
        base = FUNCTION_HOST
    else:
        scheme = req.headers.get("x-forwarded-proto") or req.headers.get("X-Forwarded-Proto") or "https"
        host = req.headers.get("Host") or req.url.split("/")[2]
        base = f"{scheme}://{host}"
    return f"{base.rstrip('/')}/api/purview-image/{lender_normalized}"

def _hash_preview_token(token: str) -> str:
    return hashlib.sha1(token.encode("utf-8")).hexdigest()

def _cache_preview(token: str, preview: Dict[str, Any]) -> None:
    _preview_cache[token] = (preview, _now())

def _get_cached_preview(token: str) -> Optional[Dict[str, Any]]:
    entry = _preview_cache.get(token)
    if not entry:
        return None
    preview, ts = entry
    if _now() - ts < PREVIEW_CACHE_TTL:
        return preview
    else:
        del _preview_cache[token]
        return None

# HTML builder (keeps minimal and fast) - uses image_url already computed in preview dict
def _build_html(preview: Dict[str, Any]) -> str:
    title = (preview.get("title") or "").replace('"', "&quot;")
    description = (preview.get("description") or "").replace('"', "&quot;")
    image_url = preview.get("image_url") or ""
    canonical_url = preview.get("canonical_url") or preview.get("target_url") or ""
    theme_color = preview.get("theme_color") or "#111827"
    target_url = preview.get("target_url") or canonical_url

    # Simple OG + Twitter meta + minimal UX
    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <title>{title}</title>
  <meta name="viewport" content="width=device-width,initial-scale=1"/>
  <meta name="description" content="{description}"/>
  <meta name="theme-color" content="{theme_color}"/>
  <link rel="canonical" href="{canonical_url}"/>

  <!-- Open Graph -->
  <meta property="og:type" content="website"/>
  <meta property="og:title" content="{title}"/>
  <meta property="og:description" content="{description}"/>
  <meta property="og:image" content="{image_url}"/>
  <meta property="og:url" content="{canonical_url}"/>

  <!-- Twitter -->
  <meta name="twitter:card" content="summary_large_image"/>
  <meta name="twitter:title" content="{title}"/>
  <meta name="twitter:description" content="{description}"/>
  <meta name="twitter:image" content="{image_url}"/>

  <style>
    :root {{ color-scheme: light dark; }}
    body {{ margin:0; font-family: system-ui, -apple-system, "Segoe UI", Roboto, Ubuntu, sans-serif; background:#050816; color:#fff; display:flex; align-items:center; justify-content:center; min-height:100vh; }}
    .card {{ max-width:520px; padding:1.5rem; border-radius:1rem; background:linear-gradient(180deg,#071225,#020617); box-shadow:0 20px 40px rgba(0,0,0,.6); }}
    .title {{ font-size:1.4rem; font-weight:700; margin-bottom:.5rem; }}
    .desc {{ color:#cbd5e1; margin-bottom:1.25rem; }}
    .btn {{ display:inline-block; padding:.6rem 1.2rem; border-radius:999px; background:#1e40af; color:#fff; text-decoration:none; font-weight:600; }}
  </style>
</head>
<body>
  <main class="card" role="main" aria-label="offer preview">
    <div class="title">{title}</div>
    <div class="desc">{description}</div>
    <a href="{target_url}" class="btn">Check my offer ↗</a>
  </main>
</body>
</html>"""

# Main
def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info("PURVIEW-V1 request received")

    # get token from route or query
    route_params = getattr(req, "route_params", {}) or {}
    token = (route_params.get("token") or req.params.get("token") or req.params.get("t"))
    # Quick probe guard - short-circuit health checks and probes
    if _is_probe_request(token, req):
        # avoid logging heavy traces for probes; return 204 No Content (fast)
        logging.debug(f"Probe/health hit ignored token={token}")
        return func.HttpResponse(status_code=204)

    if not token:
        logging.warning("No token provided in route or query.")
        return func.HttpResponse("Invalid link: token missing", status_code=400, mimetype="text/plain")

    # Try preview cache
    cached = _get_cached_preview(token)
    if cached:
        logging.info(f"PurviewHit token={token} cached=True")
        html = _build_html(cached)
        headers = {"Cache-Control": "public, max-age=60"}  # keep same short caching for messaging platforms
        # support ETag for clients
        etag = f"\"{_hash_preview_token(token)}\""
        headers["ETag"] = etag
        return func.HttpResponse(html, status_code=200, mimetype="text/html", headers=headers)

    # Primary: attempt to use shared.db_access.get_redirect_preview if available
    preview_obj = None
    try:
        if get_redirect_preview:
            preview_obj = get_redirect_preview(token)
    except Exception:
        logging.exception("Error calling shared.get_redirect_preview")

    # If shared returned a 'RedirectPreview' style object, try to normalize it into dict
    preview: Dict[str, Any] = {}
    if preview_obj:
        # If it's a dataclass-like or object, attempt attribute access; otherwise assume dict
        if isinstance(preview_obj, dict):
            preview = preview_obj.copy()
        else:
            # object with attributes
            for attr in ("title", "description", "image_url", "canonical_url", "target_url", "theme_color"):
                try:
                    preview[attr] = getattr(preview_obj, attr)
                except Exception:
                    preview[attr] = None
            # capture metadata if present
            try:
                preview_meta = getattr(preview_obj, "meta", None) or getattr(preview_obj, "metadata", None)
                if preview_meta and isinstance(preview_meta, dict):
                    preview.update(preview_meta)
            except Exception:
                pass

    # If no preview_obj from shared, or missing lender -> fallback to DAB lookup inline
    lender_name = preview.get("lender") or preview.get("bank") or None
    if not lender_name:
        # try to fallback to a minimal DAB HTTP call if environment variables available.
        # Avoid implementing raw DAB HTTP here since shared.db_access should already handle it in production.
        logging.info(f"[Purview] preview has no lender from shared; continuing with fallback data for token={token}")

    # Normalize lender and compute image endpoint
    lender_normalized = _normalize_lender(lender_name) if lender_name else None
    if lender_normalized:
        # Build image URL using function host + lender
        try:
            image_ep = _build_image_endpoint(lender_normalized, req)
            preview["image_url"] = image_ep
        except Exception:
            logging.exception("Failed to build image endpoint")
    else:
        # No lender — fallback to fallback_default json if available
        fallback = _get_lender_json("fallback")
        if fallback:
            preview.setdefault("title", fallback.get("title"))
            preview.setdefault("description", fallback.get("description"))
            preview["image_url"] = fallback.get("image_url")  # may point to blob or function
            preview.setdefault("theme_color", fallback.get("theme_color", "#111827"))

    # If we still do not have title/description, provide safe default
    preview.setdefault("title", "Your loan preview is ready")
    preview.setdefault("description", "Tap to view your personalised loan offer.")
    preview.setdefault("theme_color", "#111827")

    # canonical/target url fallback (from preview or build from token)
    preview.setdefault("canonical_url", preview.get("target_url") or f"https://r.duitai.in/p/{token}")
    preview.setdefault("target_url", preview.get("target_url") or preview.get("canonical_url"))

    # Cache the preview result for quick subsequent loads
    _cache_preview(token, preview)
    logging.info(f"PurviewHit token={token} lender={lender_normalized} cached=False")

    # Build HTML + ETag + headers
    html = _build_html(preview)
    headers = {
        "Cache-Control": "public, max-age=60",
        "ETag": f"\"{_hash_preview_token(token)}\""
    }
    return func.HttpResponse(html, status_code=200, mimetype="text/html", headers=headers)
