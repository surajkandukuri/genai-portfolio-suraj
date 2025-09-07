# pages/22_kpidrift_widgetextractor.py
# Orchestrator Page (Streamlit) for Widget Extraction
# - UI for selecting URLs
# - Classifies URL → dispatches to Power BI or Tableau extractor
# - Keeps your local manifest saving & output display
# - No feature loss vs. your previous single-file version

from __future__ import annotations

import os, json
from pathlib import Path
from typing import Dict, List, Optional

import streamlit as st
import pandas as pd
from supabase import create_client, Client

# === NEW: provider dispatch imports ==========================================
from provisioning.a2_kpidrift_widgetextractor_power_bi import extract as extract_pbi
from provisioning.a2_kpidrift_widgetextractor_tableau import extract as extract_tbl

# ──────────────────────────────────────────────────────────────────────────────
st.set_page_config(page_title="KPI Drift — Widget Extractor", page_icon="🧩", layout="wide")
st.title("🧩 KPI Drift — Widget Extractor")
st.caption("Clips per-chart crops from live dashboards. Uploads to Supabase and logs DB rows.")

# ── Config & Secrets ─────────────────────────────────────────────────────────
def _sget(*keys, default=None):
    """Prefer st.secrets, then env vars."""
    for k in keys:
        try:
            if k in st.secrets:
                return st.secrets[k]
        except Exception:
            pass
        v = os.getenv(k)
        if v:
            return v
    return default

SUPABASE_URL  = _sget("SUPABASE_URL", "SUPABASE__URL")
# Prefer SERVICE_ROLE for inserts with RLS; fallback to ANON if your policies allow
SUPABASE_KEY  = _sget("SUPABASE_SERVICE_ROLE_KEY", "SUPABASE_SERVICE_KEY", "SUPABASE_ANON_KEY", "SUPABASE__SUPABASE_SERVICE_KEY")

KDH_BUCKET             = _sget("KDH_BUCKET", default="kpidrifthunter")
KDH_FOLDER_ROOT        = _sget("KDH_FOLDER_ROOT", default="widgetextractor")
KDH_TABLE_SCREENGRABS  = _sget("KDH_TABLE_SCREENGRABS", default="kdh_screengrab_dim")
KDH_TABLE_WIDGETS      = _sget("KDH_TABLE_WIDGETS", default="kdh_widget_dim")

sb: Optional[Client] = None
if SUPABASE_URL and SUPABASE_KEY:
    try:
        sb = create_client(SUPABASE_URL, SUPABASE_KEY)
    except Exception as e:
        st.warning(f"Supabase client not available: {e}")

# ── Small helpers ─────────────────────────────────────────────────────────────
def _nowstamp() -> str:
    from datetime import datetime
    return datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")

def _ensure_outdir(p: Path) -> Path:
    p.mkdir(parents=True, exist_ok=True)
    return p

def _detect_platform(url: str) -> str:
    """URL-based classifier: 'powerbi' | 'tableau' | 'unknown'."""
    u = (url or "").lower()
    if "powerbi.com" in u:
        return "powerbi"
    if "tableau" in u or "public.tableau.com" in u:
        return "tableau"
    return "unknown"

# ── Recent Screengrabs (for the top table) ────────────────────────────────────
@st.cache_data(ttl=60)
def load_recent_screengrabs(limit=200):
    if not sb: return []
    q = (
        sb.table(KDH_TABLE_SCREENGRABS)
          .select("screengrab_id, url, platform, storage_path_full, captured_at")
          .order("captured_at", desc=True)
          .limit(limit)
          .execute()
    )
    return q.data or []

rows = load_recent_screengrabs()
df = pd.DataFrame(rows) if rows else pd.DataFrame(
    columns=["screengrab_id","url","platform","captured_at","storage_path_full"]
)
df["Select"] = False

st.subheader("Recent Screengrabs (from DB)")
edited = st.data_editor(
    df[["Select", "url", "platform", "captured_at", "screengrab_id", "storage_path_full"]],
    column_config={
        "Select": st.column_config.CheckboxColumn("Select"),
        "url": st.column_config.LinkColumn("URL"),
        "platform": "Platform",
        "captured_at": "Captured At (UTC)",
        "screengrab_id": st.column_config.TextColumn("ID"),
        "storage_path_full": st.column_config.TextColumn("Storage Path"),
    },
    use_container_width=True,
    hide_index=True,
    num_rows="dynamic",
    height=420,
)
selected = edited[edited["Select"] == True]  # noqa: E712

st.markdown("— or —")
manual_url = st.text_input("Manual URL (Power BI / Tableau public link)")

c1, c2, c3 = st.columns([1,1,6])
go_btn = c1.button("▶️ Extract + Upload", type="primary", use_container_width=True,
                   disabled=(selected.empty and not manual_url.strip()))
refresh_btn = c2.button("↻ Refresh DB list", use_container_width=True)
save_local  = c3.toggle("Also save local copy (manifest.json)", value=True)

if refresh_btn:
    st.cache_data.clear()
    st.rerun()

# ── Dispatcher that calls the right provider ─────────────────────────────────
def _dispatch_extract(url: str, platform_hint: str, session_folder: str,
                      viewport=(1920,1080), scale=2.0, max_widgets=80) -> Dict:
    """
    Calls the correct provider's extract() and returns its manifest.
    - Providers already: take full-page + widget crops, upload to Supabase,
      and insert rows into kdh_screengrab_dim / kdh_widget_dim.
    """
    # Fallback: default to Power BI if unknown (least risky; previous code didn't block unknowns)
    platform = platform_hint or _detect_platform(url)
    if platform == "tableau":
        return extract_tbl(url=url, session_folder=session_folder,
                           viewport=viewport, scale=scale, max_widgets=max_widgets)
    # default and 'powerbi'
    return extract_pbi(url=url, session_folder=session_folder,
                       viewport=viewport, scale=scale, max_widgets=max_widgets)

# ── Core UI Action ────────────────────────────────────────────────────────────
if go_btn:
    targets: List[str] = []

    if not selected.empty:
        targets.extend(selected["url"].tolist())

    if manual_url.strip():
        targets.append(manual_url.strip())

    if not targets:
        st.warning("No rows selected and no manual URL provided.")
    else:
        for url in targets:
            platform_hint = _detect_platform(url)
            session = f"{platform_hint if platform_hint!='unknown' else 'auto'}_{_nowstamp()}"

            st.subheader(f"Extracting → {url}")
            manifest = _dispatch_extract(
                url=url,
                platform_hint=platform_hint,
                session_folder=session,
                viewport=(1920,1080),
                scale=2.0,
                max_widgets=80,
            )

            # Optional manifest file locally (unchanged behavior)
            if save_local:
                outdir = _ensure_outdir(Path("./screenshots")/session)
                (outdir / f"manifest_{_nowstamp()}.json").write_text(
                    json.dumps(manifest, indent=2), encoding="utf-8"
                )

            # Success UI (unchanged)
            st.success(
                f"Uploaded {manifest.get('widgets_count', 0)} widgets to Storage at: "
                f"{manifest.get('storage_prefix','')} (bucket: {KDH_BUCKET})"
            )
            st.code(f"{manifest.get('storage_prefix','')}", language="text")

            # Path to local widgets folder (unchanged)
            st.code(str(Path('./screenshots')/session/'widgets'), language="text")
