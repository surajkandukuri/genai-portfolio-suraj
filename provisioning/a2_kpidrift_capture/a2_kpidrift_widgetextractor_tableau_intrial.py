# -*- coding: utf-8 -*-
"""
Tableau Cloud (trial) widget extractor (TSC) — Power BI parity

Flow:
  1) Capture "full" (use first view image from TSC)
  2) Save full locally:        ./screenshots/<session_folder>/<workbook_slug>_full.png
  3) Upload full to Supabase:  widgetextractor/<session_folder>/<workbook_slug>_full.png
  4) Insert screengrab (kdh_screengrab_dim) -> screengrab_id
  5) Export widgets (views) via TSC, save locally under ./screenshots/<session_folder>/widgets/*.png
  6) Upload widgets to Supabase under widgetextractor/<session_folder>/widgets/*.png
  7) Insert widgets (kdh_widget_dim) with FK screengrab_id

One session folder per run (provided by caller). Identical layout to Power BI.
"""

from __future__ import annotations

# ╔═══════════════════════════════════════════════════════════════════════════╗
# ║ Imports & typing                                                          ║
# ╚═══════════════════════════════════════════════════════════════════════════╝
import os
import io
import re
import uuid 
from pathlib import Path
from typing import Dict, List, Optional, Tuple
import  hashlib
from datetime import datetime, timezone
from urllib.parse import urlparse
from provisioning.bootstrap import ensure_playwright_installed
ensure_playwright_installed()

import tableauserverclient as TSC
from PIL import Image
from supabase import create_client, Client

# DB helpers reused from your persist layer (Power BI uses these)
from provisioning.a2_kpidrift_capture.a2_kpidrift_persist import (
    upsert_screengrab,  # Step 4 (parent)
    insert_widgets,     # Step 7 (children)
)

# ╔═══════════════════════════════════════════════════════════════════════════╗
# ║ Config / secrets helpers                                                  ║
# ╚═══════════════════════════════════════════════════════════════════════════╝
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

SUPABASE_URL  = _sget("SUPABASE_URL", "SUPABASE__URL")
SUPABASE_KEY  = _sget("SUPABASE_SERVICE_ROLE_KEY", "SUPABASE_SERVICE_KEY", "SUPABASE_ANON_KEY", "SUPABASE__SUPABASE_SERVICE_KEY")

KDH_BUCKET             = _sget("KDH_BUCKET", default="kpidrifthunter")
KDH_FOLDER_ROOT        = _sget("KDH_FOLDER_ROOT", default="widgetextractor")  # used only in storage keys
KDH_TABLE_SCREENGRABS  = _sget("KDH_TABLE_SCREENGRABS", default="kdh_screengrab_dim")
KDH_TABLE_WIDGETS      = _sget("KDH_TABLE_WIDGETS", default="kdh_widget_dim")

TABLEAU_SERVER_URL = _sget("TABLEAU_SERVER_URL")
TABLEAU_SITE_ID    = _sget("TABLEAU_SITE_ID")
TABLEAU_USERNAME   = _sget("TABLEAU_USERNAME")
TABLEAU_PASSWORD   = _sget("TABLEAU_PASSWORD")

sb: Optional[Client] = None
if SUPABASE_URL and SUPABASE_KEY:
    try:
        sb = create_client(SUPABASE_URL, SUPABASE_KEY)
    except Exception:
        sb = None

# ╔═══════════════════════════════════════════════════════════════════════════╗
# ║ Local FS + naming utilities (same behavior as your working code)          ║
# ╚═══════════════════════════════════════════════════════════════════════════╝
def _nowstamp() -> str:
    from datetime import datetime
    return datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")

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
    return re.sub(r"[^a-z0-9]+", "", (s or "").lower())

def _log(msg: str):
    print(f"[TABLEAU-INTRIAL] {msg}")

def _sha256_hex(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()

def _host(u: str) -> str | None:
    try:
        return urlparse(u).hostname
    except Exception:
        return None

# ╔═══════════════════════════════════════════════════════════════════════════╗
# ║ Supabase storage helper (identical semantics to Power BI)                 ║
# ╚═══════════════════════════════════════════════════════════════════════════╝
def _storage_upload_bytes(bucket: str, key: str, data: bytes, content_type="image/png") -> Dict[str, str]:
    """
    Upload bytes to Supabase Storage; return {'key', 'public_url'}.
    Key must NOT include bucket name; mirrors Power BI helper behavior.
    """
    if not sb:
        raise RuntimeError("Supabase client not initialized (check SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY).")
    key = key.lstrip("/")
    try:
        sb.storage.from_(bucket).upload(path=key, file=data, file_options={"content-type": content_type, "upsert": True})
    except Exception:
        try:  # replace-and-retry path for immutable storage configs
            sb.storage.from_(bucket).remove([key])
        except Exception:
            pass
        sb.storage.from_(bucket).upload(path=key, file=data, file_options={"content-type": content_type})
    try:
        url = sb.storage.from_(bucket).get_public_url(key)
    except Exception:
        url = ""
    return {"key": key, "public_url": url}

# ╔═══════════════════════════════════════════════════════════════════════════╗
# ║ Tableau (TSC) utilities                                                    ║
# ╚═══════════════════════════════════════════════════════════════════════════╝
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
    desired_project_id = _find_project_id(server, project_name)

    try:
        wbs = list(TSC.Pager(server.workbooks))
    except Exception:
        wbs, _ = server.workbooks.get()

    if desired_project_id:
        wbs = [wb for wb in wbs if getattr(wb, "project_id", None) == desired_project_id]

    n_slug = _norm(slug or "")
    n_name = _norm(name or "")

    if n_slug:
        for wb in wbs:
            if _norm(getattr(wb, "content_url", "")) == n_slug:
                return wb
    if n_name:
        for wb in wbs:
            if _norm(wb.name) == n_name:
                return wb
    if n_slug:
        for wb in wbs:
            if _norm(wb.name) == n_slug:
                return wb
    return None

def _populate_workbook_views(server: TSC.Server, wb: TSC.WorkbookItem):
    try:
        server.workbooks.populate_views(wb)  # newer TSC
        return
    except Exception:
        try:
            server.workbooks.populate(wb)    # older fallback
            return
        except Exception:
            pass
    _log("Warning: could not populate workbook views; proceeding anyway.")

def _export_view_png(server: TSC.Server, v: TSC.ViewItem, out_path: Path) -> Tuple[int, int, bytes]:
    """
    Export a view image (prefer high-res) and write to out_path.
    Returns (w, h, png_bytes).
    """
    img_bytes = None
    try:
        req = TSC.ImageRequestOptions(imageresolution=TSC.ImageRequestOptions.Resolution.High)
        server.views.populate_image(v, req)
        img_bytes = getattr(v, "image", None)
        if img_bytes:
            _log(f"Export via populate_image: {v.name}")
    except Exception:
        pass

    if not img_bytes:
        try:
            server.views.populate_preview_image(v)
            img_bytes = getattr(v, "preview_image", None)
            if img_bytes:
                _log(f"Export via populate_preview_image: {v.name}")
        except Exception:
            pass

    if not img_bytes:
        try:
            server.views.populate(v)
            img_bytes = getattr(v, "image", None) or getattr(v, "preview_image", None)
            if img_bytes:
                _log(f"Export via legacy populate: {v.name}")
        except Exception:
            pass

    if not img_bytes:
        raise RuntimeError("Could not obtain image for view (no supported populate method)")

    out_path.write_bytes(img_bytes)
    with Image.open(io.BytesIO(img_bytes)) as im:
        w, h = int(im.width), int(im.height)
    return w, h, img_bytes


def _db_insert(table: str, row: Dict) -> Dict:
            if not sb:
                return {"ok": False, "error": "no supabase client"}
            try:
                res = sb.table(table).insert(row).execute()
                return {"ok": True, "data": getattr(res, "data", None)}
            except Exception as e:
                return {"ok": False, "error": str(e)}

# ╔═══════════════════════════════════════════════════════════════════════════╗
# ║ PUBLIC ENTRY — mirrors Power BI flow                                      ║
# ╚═══════════════════════════════════════════════════════════════════════════╝
def capture_tableau_api(
    *,
    # REQUIRED: single folder per run; caller must create/provide it (e.g., 'tableau_20250909T025207Z')
    session_folder: str,
    # Optional workbook filters
    workbook_name: Optional[str] = None,
    workbook_slug: Optional[str] = None,  # content_url-ish
    project_name: Optional[str] = None,
    limit_views: Optional[int] = None,
) -> Dict:
    """
    Capture full + widgets for a Tableau workbook via TSC with Power BI parity.
    Uses ONE session folder for the entire run (no internal timestamping).
    """
    # Basic config guards
    if not all([TABLEAU_SERVER_URL, TABLEAU_SITE_ID, TABLEAU_USERNAME, TABLEAU_PASSWORD]):
        raise RuntimeError("Tableau Cloud credentials are not configured.")
    if not sb:
        raise RuntimeError("Supabase client not initialized (check SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY).")

    # ── ONE session folder per run (local) ───────────────────────────────────
    base_local = _ensure_dir(Path("./screenshots") / session_folder)
    widgets_local = _ensure_dir(base_local / "widgets")

    # Storage prefix mirrors Power BI's: widgetextractor/<session_folder>/...
    session_prefix = f"{KDH_FOLDER_ROOT}/{session_folder}"

    # ── Sign-in & resolve workbook ───────────────────────────────────────────
    server, auth = _sign_in()
    try:
        _log("Signed in. Looking for workbook...")
        wb = _find_workbook(server, workbook_name, workbook_slug, project_name)
        if not wb:
            raise RuntimeError(
                f"Workbook not found (name={workbook_name!r} slug={workbook_slug!r} project={project_name!r})"
            )

        _populate_workbook_views(server, wb)
        views: List[TSC.ViewItem] = list(getattr(wb, "views", []) or [])
        if not views:
            raise RuntimeError("No views found on workbook after population.")

        if limit_views is not None:
            views = views[:int(limit_views)]

        wb_title = wb.name or (workbook_name or "Tableau Workbook")
        wb_slug_eff = _slugify(wb_title)

        exported: List[Dict] = []
        crops_payload: List[Dict] = []
                
        
        # ╔═══════════════════════════════════════════════════════════════════╗
        # ║ Step 1–4: FULL image (first view) → local + upload + screengrab  ║
        # ╚═══════════════════════════════════════════════════════════════════╝
        v0 = views[0]
        full_fname_local = f"{wb_slug_eff}_full.png"  # ./screenshots/<session_folder>/<workbook_slug>_full.png
        full_local_path = base_local / full_fname_local

        w0, h0, full_bytes = _export_view_png(server, v0, full_local_path)

        full_key = f"{session_prefix}/{full_fname_local}"  # widgetextractor/<session_folder>/<workbook_slug>_full.png
        uploaded_full = _storage_upload_bytes(KDH_BUCKET, full_key, full_bytes)

        # Build traceable URL (prefer raw content_url; fallback to slugified title)
        base_url = (TABLEAU_SERVER_URL or "").rstrip("/")
        site     = (TABLEAU_SITE_ID or "").strip()
        wb_content_url = getattr(wb, "content_url", None)
        wb_slug_raw = wb_content_url if wb_content_url else wb_slug_eff
        tableau_url = f"{base_url}/#/site/{site}/workbooks/{wb_slug_raw}" if site else f"{base_url}/#/workbooks/{wb_slug_raw}"
        screengrab_id = str(uuid.uuid4())
        ts = _nowstamp()
        screengrab_id = str(uuid.uuid4())
        capture_session_id = str(uuid.uuid4())   # one per run; keep this if you want to query by session
        ts_utc = datetime.now(timezone.utc)      # timestamptz

        # 2) Required 64-char hash of the FULL PNG bytes
        sg_hash = _sha256_hex(full_bytes)
        screengrab_row = {
            "screengrab_id": screengrab_id,
            "capture_session_id": capture_session_id,
            "url": tableau_url,
            "platform": "tableau",
            "detected_via": "api",
            "platform_confidence": 0.990,
            "screengrab_hashvalue": sg_hash,
            "storage_bucket": KDH_BUCKET,
            "storage_path_full": uploaded_full.get("key", full_key),
            "wrapper_host": "test",
            "user_id": None,
            "captured_at": ts_utc.isoformat(),
            }
        
        #res = _db_insert(KDH_TABLE_SCREENGRABS, screengrab_row)
        res = sb.table("kdh_screengrab_dim").insert(screengrab_row, returning="representation").execute()
        '''
        try:
            res = (
        sb.table("kdh_screengrab_dim")
          .insert(screengrab_row)
          .select("screengrab_id")
          .single()
                  .execute()
                 )
            if not getattr(res, "data", None) or not res.data.get("screengrab_id"):
                raise RuntimeError(f"Insert returned no data: {getattr(res, 'data', None)}")
        except Exception as e:
            raise RuntimeError(f"Failed to insert screengrab row into kdh_screengrab_dim: {e}")
        '''
        if not res.data:
            raise RuntimeError(f"Insert returned no data: {res.data}")
            screengrab_id = res.data[0]["screengrab_id"]
        # ╔═══════════════════════════════════════════════════════════════════╗
        # ║ Step 5–6: Widgets → local ./screenshots/<session>/widgets/*.png  ║
        # ╚═══════════════════════════════════════════════════════════════════╝
        for idx, v in enumerate(views, start=1):
            title = v.name or f"View_{idx}"                 # preserve your working naming
            base = f"{_sanitize(title)}_{idx:02d}.png"
            local_path = widgets_local / base

            try:
                w, h, png_bytes = _export_view_png(server, v, local_path)

                key = f"{session_prefix}/widgets/{base}"    # widgetextractor/<session_folder>/widgets/<...>.png
                _ = _storage_upload_bytes(KDH_BUCKET, key, png_bytes)

                exported.append({
                    "view_id": v.id,
                    "view_name": title,
                    "path": key,
                    "w": w, "h": h
                })
                crops_payload.append({"path": key, "bbox": [0, 0, int(w), int(h)]})

            except Exception as e:
                _log(f"Failed exporting view '{title}': {e}")

        # ╔═══════════════════════════════════════════════════════════════════╗
        # ║ Step 7: Insert widgets with FK to screengrab_id                  ║
        # ╚═══════════════════════════════════════════════════════════════════╝
        if crops_payload:
            insert_widgets(
                sb=sb,
                screengrab_id=screengrab_id,               # REAL FK from Step 4
                storage_bucket=KDH_BUCKET,
                crops=crops_payload,
            )

        return {
            "workbook": wb_title,
            "exported": exported,
            "session_folder": session_folder,              # local folder name
            "session_prefix": session_prefix,              # storage prefix
            "full_local_path": str(full_local_path),
            "full_storage_key": uploaded_full.get("key", full_key),
            "screengrab_id": screengrab_id,
        }

    finally:
        try:
            server.auth.sign_out()
        except Exception:
            pass
