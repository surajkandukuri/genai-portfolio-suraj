# provisioning/a2_kpidrift_widgetextractor_tableau.py
from __future__ import annotations

import os
import json
import datetime as dt
from typing import Dict, Optional, Tuple
from urllib.parse import urlparse
from supabase import create_client
import os



# Try Cloud (trial) and Public extractors
try:
    from provisioning.a2_kpidrift_capture.a2_kpidrift_widgetextractor_tableau_intrial import (
        capture_tableau_api as _capture_tableau_cloud_api,
    )
except Exception:
    _capture_tableau_cloud_api = None

try:
    from provisioning.a2_kpidrift_capture.a2_kpidrift_widgetextractor_tableau_public import (
        extract_tableau_public as _extract_tableau_public,
    )
except Exception:
    _extract_tableau_public = None


def _utcnow() -> dt.datetime:
    return dt.datetime.utcnow().replace(tzinfo=dt.timezone.utc)

def _cutoff_from_env() -> dt.datetime:
    iso = os.getenv("TABLEAU_TRIAL_CUTOFF_UTC", "2025-09-22T00:00:00Z")
    try:
        if iso.endswith("Z"):
            iso = iso[:-1] + "+00:00"
        return dt.datetime.fromisoformat(iso).astimezone(dt.timezone.utc)
    except Exception:
        return dt.datetime(2025, 9, 22, 0, 0, 0, tzinfo=dt.timezone.utc)

def _log(msg: str):
    print(msg, flush=True)

def _scan_for_views(path_like: str):
    # split on '/' and strip query params like '?iid=1'
    parts = [p.split('?', 1)[0] for p in (path_like or '').split('/') if p]
    if not parts:
        return None, None
    try:
        i = parts.index('views')
    except ValueError:
        return None, None
    wb = parts[i + 1] if len(parts) > i + 1 else None
    view = parts[i + 2] if len(parts) > i + 2 else None
    return wb, view

def _parse_tableau_slugs(url: str):
    from urllib.parse import urlparse
    p = urlparse(url)
    # try the normal path first
    wb, view = _scan_for_views(p.path)
    if wb:
        return wb, view
    # then the fragment (Cloud puts /views/... after '#')
    frag_path = p.fragment
    if frag_path and not frag_path.startswith('/'):
        frag_path = '/' + frag_path
    return _scan_for_views(frag_path)


def extract(
    url: str,
    session_folder: str,
    viewport: Tuple[int, int] = (1920, 1080),
    scale: float = 2.0,
    max_widgets: int = 80,
    try_cloud_first: bool = True,
    workbook_name: Optional[str] = None,
    project_name: Optional[str] = None,
    limit_views: Optional[int] = None,
) -> Dict:
    """
    Orchestrator: Cloud (trial) → Public fallback.
    Fixes:
      - Parse workbook slug from URL fragment (#) when needed.
      - Pass slug to Cloud extractor.
    """
    now = _utcnow()
    cutoff = _cutoff_from_env()
    use_cloud = try_cloud_first and (now < cutoff)

    wb_slug, view_slug = _parse_tableau_slugs(url)
    _log(f"[TABLEAU ORCH] now={now.isoformat()} cutoff={cutoff.isoformat()} → {'Cloud' if use_cloud else 'Public'}")
    _log(f"[TABLEAU ORCH] workbook_slug={wb_slug!r} view_slug={view_slug!r}")

    if use_cloud and _capture_tableau_cloud_api is not None:
        try:
            effective_project = project_name or os.getenv("TABLEAU_DEFAULT_PROJECT", "default")
            _log(f"[TABLEAU ORCH] Trying Cloud (trial). name={workbook_name!r} slug={wb_slug!r} project={effective_project!r}")
            cloud_res = _capture_tableau_cloud_api(
                workbook_name=workbook_name,
                workbook_slug=wb_slug,
                project_name=effective_project,
                limit_views=limit_views,
                session_folder=session_folder,
            )
            if cloud_res and isinstance(cloud_res, dict) and cloud_res.get("exported"):
                _log(f"[TABLEAU ORCH] Cloud export OK. Exported {len(cloud_res['exported'])} view(s).")
                return {
                    "mode": "cloud",
                    "platform": "tableau_cloud",
                    "workbook": cloud_res.get("workbook", ""),
                    "captured_at": cloud_res.get("captured_at"),
                    "session_folder": session_folder,
                    "widgets_count": len(cloud_res["exported"]),
                    "widgets": cloud_res["exported"],
                    "storage_prefix": cloud_res.get("session_prefix", ""),
                    "cloud_raw": cloud_res,
                }
            else:
                _log("[TABLEAU ORCH] Cloud returned no exports; falling back to Public.")
        except Exception as e:
            _log(f"[TABLEAU ORCH] Cloud failed: {e!r}; falling back to Public.")

    if _extract_tableau_public is None:
        raise RuntimeError("Public extractor not available, and Cloud did not succeed.")

    _log("[TABLEAU ORCH] Running Tableau Public extractor…")
    return _extract_tableau_public(
        url=url,
        session_folder=session_folder,
        viewport=viewport,
        scale=scale,
        max_widgets=max_widgets,
    )


if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser(description="Tableau orchestrator (Cloud → Public).")
    ap.add_argument("--url", required=True)
    ap.add_argument("--session", required=True)
    ap.add_argument("--workbook", default=None)
    ap.add_argument("--project", default=None)
    ap.add_argument("--limit", type=int, default=None)
    ap.add_argument("--no-cloud", action="store_true")
    args = ap.parse_args()

    res = extract(
        url=args.url,
        session_folder=args.session,
        try_cloud_first=not args.no_cloud,
        workbook_name=args.workbook,
        project_name=args.project,
        limit_views=args.limit,
    )
    print(json.dumps(res, indent=2))
