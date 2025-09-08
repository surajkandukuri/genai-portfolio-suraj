# -*- coding: utf-8 -*-
"""
Tableau Cloud (trial) widget extractor.
Returns per-view PNGs using Tableau Server Client (TSC).

Output shape (example):
{
  "workbook": "Dup-Olist E-Commerce Dashboard",
  "exported": [
    {"view_id": "...", "view_name": "Number of Orders per Month",
     "path": "tableaucloud_20250908_121500/olist-orders-overview/widgets/Number_of_Orders_per_Month_01.png",
     "w": 792, "h": 829},
    ...
  ],
  "session_prefix": "tableaucloud_20250908_121500/olist-orders-overview"
}
"""
from __future__ import annotations

import os
import io
import re
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import tableauserverclient as TSC
from PIL import Image


# ──────────────────────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────────────────────

def _sget(*keys: str, default=None):
    for k in keys:
        v = os.getenv(k)
        if v:
            return v
    try:
        import streamlit as st  # optional
        for k in keys:
            if k in st.secrets:
                return st.secrets[k]
    except Exception:
        pass
    return default


def _nowstamp() -> str:
    from datetime import datetime
    return datetime.utcnow().strftime("%Y%m%d_%H%M%S")


def _ensure_dir(p: Path) -> Path:
    p.mkdir(parents=True, exist_ok=True)
    return p


def _sanitize(s: str, max_len: int = 120) -> str:
    s = (s or "").strip()
    s = re.sub(r"[^\w\-. ]+", "_", s)
    s = re.sub(r"\s+", "_", s)
    return (s[:max_len] or "untitled").rstrip("._-")


def _slugify(s: str) -> str:
    s = (s or "").lower().strip()
    s = re.sub(r"[^\w\-]+", "-", s)
    s = re.sub(r"-{2,}", "-", s).strip("-")
    return s or "tableau_report"


def _norm(s: str) -> str:
    """Normalization used for tolerant matching (name/content_url)."""
    return re.sub(r"[^a-z0-9]+", "", (s or "").lower())


def _log(msg: str):
    print(f"[CLOUD] {msg}")


# ──────────────────────────────────────────────────────────────────────────────
# Config (env / secrets)
# ──────────────────────────────────────────────────────────────────────────────

TABLEAU_SERVER_URL = _sget("TABLEAU_SERVER_URL")
TABLEAU_SITE_ID    = _sget("TABLEAU_SITE_ID")
TABLEAU_USERNAME   = _sget("TABLEAU_USERNAME")
TABLEAU_PASSWORD   = _sget("TABLEAU_PASSWORD")


# ──────────────────────────────────────────────────────────────────────────────
# Internals
# ──────────────────────────────────────────────────────────────────────────────

def _sign_in() -> Tuple[TSC.Server, TSC.TableauAuth]:
    server = TSC.Server(TABLEAU_SERVER_URL, use_server_version=True)
    auth = TSC.TableauAuth(TABLEAU_USERNAME, TABLEAU_PASSWORD, site_id=TABLEAU_SITE_ID)
    server.auth.sign_in(auth)
    return server, auth


def _find_project_id(server: TSC.Server, project_name: Optional[str]) -> Optional[str]:
    if not project_name:
        return None
    try:
        projs, _ = server.projects.get()
        for p in projs:
            if p.name.lower() == project_name.lower():
                return p.id
    except Exception:
        pass
    return None


def _find_workbook(server: TSC.Server,
                   name: Optional[str],
                   slug: Optional[str],
                   project_name: Optional[str]) -> Optional[TSC.WorkbookItem]:
    """Find workbook by normalized content_url or name; optional project filter."""
    desired_project_id = _find_project_id(server, project_name)

    try:
        wbs = list(TSC.Pager(server.workbooks))
    except Exception:
        wbs, _ = server.workbooks.get()

    if desired_project_id:
        wbs = [wb for wb in wbs if getattr(wb, "project_id", None) == desired_project_id]

    n_slug = _norm(slug or "")
    n_name = _norm(name or "")

    # 1) Prefer content_url match
    if n_slug:
        for wb in wbs:
            if _norm(getattr(wb, "content_url", "")) == n_slug:
                return wb

    # 2) Fallback: exact name (normalized)
    if n_name:
        for wb in wbs:
            if _norm(wb.name) == n_name:
                return wb

    # 3) Fallback: compare normalized name to normalized slug
    if n_slug:
        for wb in wbs:
            if _norm(wb.name) == n_slug:
                return wb

    return None


def _populate_workbook_views(server: TSC.Server, wb: TSC.WorkbookItem):
    """Work with multiple TSC versions."""
    try:
        server.workbooks.populate_views(wb)  # preferred on newer TSC
        return
    except Exception:
        try:
            server.workbooks.populate(wb)    # older fallback
            return
        except Exception:
            pass
    _log("Warning: could not populate workbook views; proceeding anyway.")


def _export_view_png(server: TSC.Server, v: TSC.ViewItem, out_path: Path) -> Tuple[int, int]:
    """
    Populate a view image in a TSC-version-agnostic way and write PNG to out_path.
    Tries, in order:
      1) populate_image (high-res)  -> v.image
      2) populate_preview_image     -> v.preview_image
      3) legacy populate            -> v.image or v.preview_image
    Returns (w, h).
    """
    img_bytes = None

    # 1) Newer TSC: full-resolution image
    try:
        req = TSC.ImageRequestOptions(imageresolution=TSC.ImageRequestOptions.Resolution.High)
        server.views.populate_image(v, req)
        img_bytes = getattr(v, "image", None)
        if img_bytes:
            _log(f"Export via populate_image: {v.name}")
    except Exception:
        pass

    # 2) Preview image (widely supported)
    if not img_bytes:
        try:
            server.views.populate_preview_image(v)
            img_bytes = getattr(v, "preview_image", None)
            if img_bytes:
                _log(f"Export via populate_preview_image: {v.name}")
        except Exception:
            pass

    # 3) Legacy fallback
    if not img_bytes:
        try:
            server.views.populate(v)  # not present on some versions
            img_bytes = getattr(v, "image", None) or getattr(v, "preview_image", None)
            if img_bytes:
                _log(f"Export via legacy populate: {v.name}")
        except Exception:
            pass

    if not img_bytes:
        raise RuntimeError("Could not obtain image for view (no supported populate method)")

    out_path.write_bytes(img_bytes)
    with Image.open(io.BytesIO(img_bytes)) as im:
        w, h = im.size
    return int(w), int(h)


# ──────────────────────────────────────────────────────────────────────────────
# Public entry
# ──────────────────────────────────────────────────────────────────────────────

def capture_tableau_api(
    workbook_name: Optional[str] = None,
    project_name: Optional[str] = None,
    limit_views: Optional[int] = None,
    session_folder: Optional[str] = None,
    workbook_slug: Optional[str] = None,
    view_slug: Optional[str] = None,
) -> Dict:
    """
    Export images for all (or first N) views from a Tableau Cloud workbook.

    Args:
      workbook_name: visible workbook name (optional, used for tolerant matching)
      project_name:  optional project filter
      limit_views:   if set, limit number of exported views
      session_folder: pre-chosen session folder (optional)
      workbook_slug: content_url-ish slug (tolerant matching)
      view_slug:     if provided, export only this view (slug tolerant)

    Returns: dict with keys: 'workbook', 'exported'[], 'session_prefix'
    """
    if not all([TABLEAU_SERVER_URL, TABLEAU_SITE_ID, TABLEAU_USERNAME, TABLEAU_PASSWORD]):
        raise RuntimeError("Tableau Cloud credentials are not configured.")

    server, auth = _sign_in()
    try:
        _log("Signed in. Looking for workbook...")
        wb = _find_workbook(server, workbook_name, workbook_slug, project_name)
        if not wb:
            raise RuntimeError(f"Workbook not found (name={workbook_name!r} slug={workbook_slug!r} project={project_name!r})")

        _populate_workbook_views(server, wb)

        views: List[TSC.ViewItem] = list(getattr(wb, "views", []) or [])
        if not views:
            _log("No views found on workbook after population.")

        if view_slug:
            n_vs = _norm(view_slug)
            views = [v for v in views if _norm(getattr(v, "content_url", "")) == n_vs or _norm(v.name) == n_vs]

        if limit_views is not None:
            views = views[:int(limit_views)]

        ts = _nowstamp()
        wb_title = wb.name or (workbook_name or "Tableau Workbook")
        inferred_slug = _slugify(view_slug or (views[0].name if views else wb_title))
        session_prefix = f"tableaucloud_{ts}/{inferred_slug}"
        out_root = Path("screenshots") / session_prefix
        out_widgets = _ensure_dir(out_root / "widgets")

        exported: List[Dict] = []

        for idx, v in enumerate(views, start=1):
            title = v.name or f"View_{idx}"
            base = f"{_sanitize(title)}_{idx:02d}.png"
            out_path = out_widgets / base
            try:
                w, h = _export_view_png(server, v, out_path)
                exported.append({
                    "view_id": v.id,
                    "view_name": title,
                    "path": f"{session_prefix}/widgets/{base}",
                    "w": w, "h": h
                })
            except Exception as e:
                _log(f"Failed exporting view '{title}': {e}")

        return {
            "workbook": wb_title,
            "exported": exported,
            "session_prefix": f"{session_prefix}",
        }

    finally:
        try:
            server.auth.sign_out()
        except Exception:
            pass
