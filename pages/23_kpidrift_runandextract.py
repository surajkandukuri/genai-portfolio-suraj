# pages/23_kpidrift_runandextract.py
# KPI Drift Hunter — 2×2 Grid with table-like separators (Public | Via API) × (Power BI | Tableau)
# Adds explicit "WIP" labeling ONLY for Cloud • Power BI (buttons visible but disabled)

from __future__ import annotations
import os, re, json, uuid, traceback
from pathlib import Path
from typing import Dict, List, Tuple, Optional

import streamlit as st
from supabase import create_client, Client

# --- Capture providers --------------------------------------------------------
from provisioning.a2_kpidrift_capture.a2_kpidrift_powerbi import capture_powerbi
from provisioning.a2_kpidrift_capture.a2_kpidrift_tableau import capture_tableau

# --- Persist helpers (upload → DB) -------------------------------------------
from provisioning.a2_kpidrift_capture.a2_kpidrift_persist import (
    insert_widgets,
    image_wh,
)

# --- Widget extractors --------------------------------------------------------
from provisioning.a2_kpidrift_widgetextractor_power_bi import extract as extract_pbi
from provisioning.a2_kpidrift_widgetextractor_tableau import extract as extract_tbl


# ────────────────────────── Setup & config helpers ────────────────────────────
def _sget(*keys, default=None):
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
SUPABASE_KEY  = _sget("SUPABASE_SERVICE_ROLE_KEY", "SUPABASE_SERVICE_KEY", "SUPABASE_ANON_KEY", "SUPABASE__SUPABASE_SERVICE_KEY")
KDH_BUCKET    = _sget("KDH_BUCKET", default="kpidrifthunter")

@st.cache_resource
def _sb() -> Client:
    if not SUPABASE_URL or not SUPABASE_KEY:
        st.error("Missing Supabase config. Add SUPABASE_URL and a SERVICE_ROLE/ANON key in .streamlit/secrets.toml")
        st.stop()
    try:
        return create_client(SUPABASE_URL, SUPABASE_KEY)
    except Exception as e:
        st.error(f"Could not connect to Supabase: {e}")
        st.stop()

sb: Client = _sb()

# Rolling log + status dock
if "kdh_log" not in st.session_state:
    st.session_state["kdh_log"] = []
if "kdh_status" not in st.session_state:
    st.session_state["kdh_status"] = {"headline": "", "summary": "", "storage_prefix": "", "full_image_url": ""}

def _log(msg: str):
    from datetime import datetime, timezone
    ts = datetime.now(timezone.utc).strftime("%H:%M:%S")
    st.session_state["kdh_log"].append(f"{ts} {msg}")
    if len(st.session_state["kdh_log"]) > 300:
        st.session_state["kdh_log"] = st.session_state["kdh_log"][-300:]

def _nowstamp() -> str:
    from datetime import datetime, timezone
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")

def _slugify_url(url: str) -> str:
    t = (url or "").strip().lower()
    t = re.sub(r"https?://", "", t)
    t = re.sub(r"[^a-z0-9]+", "-", t).strip("-")
    return t[:80] or "report"

def _day_prefix(session_id: str, report_slug: str) -> str:
    from datetime import datetime, timezone
    now = datetime.now(timezone.utc)
    return f"{KDH_BUCKET}/{now:%Y/%m/%d}/{session_id}/{report_slug}"

def _storage_upload_bytes(bucket: str, key: str, data: bytes, content_type="image/png") -> None:
    options = {"contentType": content_type, "upsert": "true"}
    key_only = "/".join(key.split("/")[1:]) if key.startswith(bucket + "/") else key
    sb.storage.from_(bucket).upload(key_only, data, options)

def _storage_signed_url(bucket: str, key: str, ttl_sec=3600) -> str:
    key_only = "/".join(key.split("/")[1:]) if key.startswith(bucket + "/") else key
    try:
        return sb.storage.from_(bucket).create_signed_url(key_only, ttl_sec)["signedURL"]
    except Exception:
        return ""

# ───────────────────────────── Safe screengrab upsert ────────────────────────
def _safe_upsert_screengrab(row: Dict) -> Dict:
    try:
        res = sb.table("kdh_screengrab_dim").insert(row).execute()
        if getattr(res, "data", None):
            return res.data[0]
    except Exception as e:
        url = row.get("url"); sg_hash = row.get("screengrab_hashvalue")
        if url:
            try:
                data = sb.table("kdh_screengrab_dim").select("*").eq("url", url).limit(1).execute().data
                if data: return data[0]
            except Exception: pass
        if sg_hash:
            try:
                data = sb.table("kdh_screengrab_dim").select("*").eq("screengrab_hashvalue", sg_hash).limit(1).execute().data
                if data: return data[0]
            except Exception: pass
        raise e
    return row

# ───────────────────────────── Grading helpers ───────────────────────────────
def _grade_from_quality(q: Optional[float]) -> Tuple[str, float]:
    if q is None: return "A", 0.90
    if q >= 0.97: return "A+", 1.00
    if q >= 0.90: return "A", 0.95
    if q >= 0.80: return "B", 0.80
    if q >= 0.65: return "C", 0.65
    if q >= 0.50: return "D", 0.50
    return "F", 0.30

def _summarize_grades(widgets: List[Dict]) -> Dict:
    counts = {"A+":0,"A":0,"B":0,"C":0,"D":0,"F":0}
    total_weight = 0.0; weighted = 0.0
    for w in widgets or []:
        q_letter = w.get("quality"); q_score = w.get("quality_score")
        if q_letter and q_letter in counts:
            counts[q_letter] += 1
            numeric = q_score if isinstance(q_score, (int,float)) else 0.90
        else:
            numeric = 0.90; counts["A"] += 1
        area = 1.0
        if "bbox" in w and isinstance(w["bbox"], list) and len(w["bbox"])==4:
            _, _, ww, hh = w["bbox"]; area = max(1.0, float(ww) * float(hh))
        elif "w" in w and "h" in w:
            area = max(1.0, float(w["w"]) * float(w["h"]))
        total_weight += area; weighted += area * numeric
    run_letter, _ = _grade_from_quality(weighted/total_weight if total_weight>0 else None)
    return {"counts": counts, "run_grade": run_letter, "total": sum(counts.values())}

# ────────────────────────── Action handlers (core) ───────────────────────────
def _run_public_powerbi(url: str, session_id: str) -> Dict:
    _log("Validating Power BI public URL…")
    if "/view?" not in (url or ""):
        raise ValueError("Power BI public link must contain '/view?'.")
    outdir = Path("./screenshots"); outdir.mkdir(parents=True, exist_ok=True)
    _log("Launching browser & loading report…")
    result = capture_powerbi(url, outdir)

    report_slug = _slugify_url(url); prefix = _day_prefix(session_id, report_slug)

    _log("Uploading full image…")
    full_png = result.artifacts.full.read_bytes()
    full_key = f"{prefix}/full.png"
    _storage_upload_bytes(KDH_BUCKET, full_key, full_png)
    full_signed = _storage_signed_url(KDH_BUCKET, full_key)

    crop_items: List[dict] = []
    if result.artifacts.report and result.artifacts.report.exists():
        crop_png = result.artifacts.report.read_bytes()
        crop_key = f"{prefix}/widgets/report_crop.png"
        _storage_upload_bytes(KDH_BUCKET, crop_key, crop_png)
        w, h = image_wh(crop_png)
        crop_items.append({"bytes": crop_png, "path": crop_key, "bbox": [0, 0, w, h]})

    _log("Writing DB rows (safe upsert)…")
    sg_db = _safe_upsert_screengrab({
        "session_id": session_id, "url": url, "platform": "powerbi",
        "screengrab_hashvalue": "", "storage_bucket": KDH_BUCKET, "storage_path_full": full_key,
    })
    if crop_items:
        insert_widgets(sb, screengrab_id=sg_db["screengrab_id"], storage_bucket=KDH_BUCKET, crops=crop_items)

    return {"full_signed": full_signed, "storage_prefix": "/".join(full_key.split("/")[:-1]), "widgets": crop_items}

def _run_public_tableau(url: str, session_id: str) -> Dict:
    outdir = Path("./screenshots"); outdir.mkdir(parents=True, exist_ok=True)
    _log("Launching browser & loading Tableau Public view…")
    result = capture_tableau(url, outdir)

    report_slug = _slugify_url(url); prefix = _day_prefix(session_id, report_slug)

    _log("Uploading full image…")
    full_png = result.artifacts.full.read_bytes()
    full_key = f"{prefix}/full.png"
    _storage_upload_bytes(KDH_BUCKET, full_key, full_png)
    full_signed = _storage_signed_url(KDH_BUCKET, full_key)

    crop_items: List[dict] = []
    if result.artifacts.report and result.artifacts.report.exists():
        crop_png = result.artifacts.report.read_bytes()
        crop_key = f"{prefix}/widgets/report_crop.png"
        _storage_upload_bytes(KDH_BUCKET, crop_key, crop_png)
        w, h = image_wh(crop_png)
        crop_items.append({"bytes": crop_png, "path": crop_key, "bbox": [0, 0, w, h]})

    _log("Writing DB rows (safe upsert)…")
    sg_db = _safe_upsert_screengrab({
        "session_id": session_id, "url": url, "platform": "tableau",
        "screengrab_hashvalue": "", "storage_bucket": KDH_BUCKET, "storage_path_full": full_key,
    })
    if crop_items:
        insert_widgets(sb, screengrab_id=sg_db["screengrab_id"], storage_bucket=KDH_BUCKET, crops=crop_items)

    return {"full_signed": full_signed, "storage_prefix": "/".join(full_key.split("/")[:-1]), "widgets": crop_items}

def _extract_public_powerbi(url: str, session_prefix: str) -> Dict:
    return extract_pbi(url=url, session_folder=session_prefix, viewport=(1920,1080), scale=2.0, max_widgets=80)

def _extract_public_tableau(url: str, session_prefix: str) -> Dict:
    return extract_tbl(url=url, session_folder=session_prefix, viewport=(1920,1080), scale=2.0, max_widgets=80)

def _extract_cloud_tableau(url: str, session_prefix: str) -> Dict:
    return extract_tbl(url=url, session_folder=session_prefix, viewport=(1920,1080), scale=2.0, max_widgets=80)

# ─────────────────────────────── UI rendering ────────────────────────────────
st.set_page_config(page_title="KPI Drift — Run & Extract", page_icon="🧩", layout="wide")
st.title("🧩 KPI Drift — Run & Extract")
st.caption("2×2 grid: Public / Via API × Power BI / Tableau. Every box supports Run & Extract.")

# CSS for crisp table lines + badges
st.markdown("""
<style>
.table-hline { height:1px; background:#E2E8F0; margin: 0.5rem 0 1rem 0; }
.vline-wrap { display:flex; align-items:stretch; justify-content:center; }
.vline { width:1px; background:#E2E8F0; min-height: 130px; margin-top: 0.5rem; }
.cell-pad { padding-top: .25rem; }
.badge { display:inline-block; padding:2px 8px; border-radius:999px; font-size:12px; line-height:18px; border:1px solid #CBD5E1; background:#F8FAFC; color:#334155; }
.badge-wip { background:#F3F4F6; color:#6B7280; border-style:dashed; }
.cell-title { font-weight: 600; margin-bottom: .35rem; }
.hint { color:#64748B; font-size:12px; margin-top:.25rem; }
</style>
""", unsafe_allow_html=True)

# One session id
session_id = st.session_state.get("kdh_session") or str(uuid.uuid4())
st.session_state["kdh_session"] = session_id

# Secrets status (Tableau)
tbl_ok = all([
    bool(_sget("TABLEAU_SERVER_URL")),
    bool(_sget("TABLEAU_SITE_ID")),
    bool(_sget("TABLEAU_USERNAME")),
    bool(_sget("TABLEAU_PASSWORD")),
])
with st.container():
    c1, c2 = st.columns([1,3])
    with c1: st.markdown("**Secrets**")
    with c2:
        if tbl_ok:
            st.success("Tableau API • Connected", icon="🔐")
        else:
            missing = [k for k in ["TABLEAU_SERVER_URL","TABLEAU_SITE_ID","TABLEAU_USERNAME","TABLEAU_PASSWORD"] if not _sget(k)]
            st.warning(f"Tableau API • Missing: {', '.join(missing)}", icon="⚠️")

st.divider()

# Header row (adds a vertical separator + WIP badge note ONLY for Power BI)
lab, hdr_pub, sep, hdr_api = st.columns([0.8, 2, 0.07, 2])
lab.write("")  # spacer
hdr_pub.markdown("### Public")
with sep:
    st.markdown("<div class='vline-wrap'><div class='vline' style='min-height:32px;'></div></div>", unsafe_allow_html=True)
hdr_api.markdown("### Via API  <span class='badge badge-wip'>Power BI is WIP</span>", unsafe_allow_html=True)

# Horizontal line under header
st.markdown("<div class='table-hline'></div>", unsafe_allow_html=True)

# ---- Helper: single cell ----------------------------------------------------
def render_cell(cell_key: str, title: str, is_cloud: bool, provider: str,
                *, disable_actions: bool=False, wip_hint: Optional[str]=None):
    st.markdown(f"<div class='cell-title'>{title}</div>", unsafe_allow_html=True)
    if wip_hint:
        st.markdown(f"<span class='badge badge-wip'>WIP</span> <span class='hint'>{wip_hint}</span>", unsafe_allow_html=True)
    url = st.text_input("URL", key=f"url_{cell_key}", placeholder="https://…", label_visibility="collapsed")

    prog = st.empty(); step = st.empty(); grade_badge = st.empty(); outbox = st.empty()

    b1, b2 = st.columns([1,1])
    run_clicked = b1.button("Run", key=f"run_{cell_key}", use_container_width=True, disabled=disable_actions)
    ext_clicked = b2.button("Extract", key=f"ext_{cell_key}", type="primary", use_container_width=True, disabled=disable_actions)

    def _advance(i, n, txt): prog.progress(int((i/n)*100)); step.caption(txt)

    def _finalize(summary_html: str, widgets: List[Dict], storage_prefix: str, full_image_url: str = ""):
        g = _summarize_grades(widgets)
        grade_badge.success(f"Run Grade: **{g['run_grade']}**  •  Widgets: {g['total']}  •  A:{g['counts']['A']+g['counts']['A+']}  B:{g['counts']['B']}  C:{g['counts']['C']}  D:{g['counts']['D']}  F:{g['counts']['F']}")
        outbox.markdown(summary_html, unsafe_allow_html=True)
        st.session_state["kdh_status"] = {"headline": title, "summary": summary_html,
                                          "storage_prefix": storage_prefix, "full_image_url": full_image_url}

    if (run_clicked or ext_clicked) and not disable_actions:
        if not url.strip():
            st.warning("Paste a URL first."); return
        if provider == "powerbi" and "powerbi.com" not in url.lower():
            st.warning("This looks like a non-Power BI link. Consider the Tableau row.")
        if provider == "tableau" and "tableau" not in url.lower():
            st.warning("This looks like a non-Tableau link. Consider the Power BI row.")
        try:
            N = 9; _advance(1, N, "Validating & preparing…"); _log(f"[{title}] start")
            if run_clicked:
                if provider == "powerbi" and not is_cloud:
                    _advance(2, N, "Launching browser…"); res = _run_public_powerbi(url, session_id)
                    _advance(8, N, "Grading…")
                    summ = f"✅ **RUN complete** → `{res['storage_prefix']}` (bucket: {KDH_BUCKET})"
                    link = f"<div><a href='{res['full_signed']}' target='_blank'>Open full image</a></div>" if res.get("full_signed") else ""
                    _finalize(summ+link, res.get("widgets", []), res["storage_prefix"], res.get("full_signed","")); _advance(9, N, "Done")
                elif provider == "tableau" and not is_cloud:
                    _advance(2, N, "Launching browser…"); res = _run_public_tableau(url, session_id)
                    _advance(8, N, "Grading…")
                    summ = f"✅ **RUN complete** → `{res['storage_prefix']}` (bucket: {KDH_BUCKET})"
                    link = f"<div><a href='{res['full_signed']}' target='_blank'>Open full image</a></div>" if res.get("full_signed") else ""
                    _finalize(summ+link, res.get("widgets", []), res["storage_prefix"], res.get("full_signed","")); _advance(9, N, "Done")
                else:  # Tableau Cloud RUN (API)
                    tbl_ok_local = all([
                        bool(_sget("TABLEAU_SERVER_URL")), bool(_sget("TABLEAU_SITE_ID")),
                        bool(_sget("TABLEAU_USERNAME")), bool(_sget("TABLEAU_PASSWORD")),
                    ])
                    if not tbl_ok_local:
                        _finalize("⚠️ Tableau API creds missing (TABLEAU_SERVER_URL, SITE_ID, USERNAME, PASSWORD).", [], "", ""); return
                    _advance(2, N, "Signing in to Tableau Cloud…"); res = _extract_cloud_tableau(url=url, session_prefix=f"tableau_{_nowstamp()}")
                    widgets = res.get("widgets", []); storage_prefix = res.get("storage_prefix") or res.get("session_folder","")
                    _advance(8, N, "Grading…"); summ = f"✅ **RUN (API) complete** → `{storage_prefix}` (bucket: {KDH_BUCKET})"
                    _finalize(summ, widgets, storage_prefix, ""); _advance(9, N, "Done")
            if ext_clicked:
                sess = f"{provider}_{_nowstamp()}"
                if provider == "powerbi" and not is_cloud:
                    _advance(2, N, "Launching browser…"); res = _extract_public_powerbi(url, sess)
                elif provider == "tableau" and not is_cloud:
                    _advance(2, N, "Launching browser…"); res = _extract_public_tableau(url, sess)
                else:
                    tbl_ok_local = all([
                        bool(_sget("TABLEAU_SERVER_URL")), bool(_sget("TABLEAU_SITE_ID")),
                        bool(_sget("TABLEAU_USERNAME")), bool(_sget("TABLEAU_PASSWORD")),
                    ])
                    if not tbl_ok_local:
                        _finalize("⚠️ Tableau API creds missing (TABLEAU_SERVER_URL, SITE_ID, USERNAME, PASSWORD).", [], "", ""); return
                    _advance(2, N, "Signing in to Tableau Cloud…"); res = _extract_cloud_tableau(url=url, session_prefix=sess)
                _advance(8, N, "Grading…"); widgets = res.get("widgets", []); storage_prefix = res.get("storage_prefix") or res.get("session_folder","")
                summ = f"✅ **EXTRACT complete** → `{storage_prefix}` (widgets: {len(widgets)})"
                _finalize(summ, widgets, storage_prefix, ""); _advance(9, N, "Done")
        except Exception as e:
            tb = traceback.format_exc(); st.error(f"Failed: {e}\n\n```traceback\n{tb}\n```"); _log(f"ERROR: {e}")

# ───────────── Rows with vertical & horizontal separators ────────────────────

# Row: Power BI
lab, pub, sep, api = st.columns([0.8, 2, 0.07, 2])
lab.markdown("#### Power BI")
with pub:
    st.markdown("<div class='cell-pad'>", unsafe_allow_html=True)
    render_cell("pub_pbi", " ", is_cloud=False, provider="powerbi")
    st.markdown("</div>", unsafe_allow_html=True)
with sep:
    st.markdown("<div class='vline-wrap'><div class='vline'></div></div>", unsafe_allow_html=True)
with api:
    st.markdown("<div class='cell-pad'>", unsafe_allow_html=True)
    # 👇 Explicit WIP for Cloud • Power BI
    render_cell(
        "api_pbi", " ", is_cloud=True, provider="powerbi",
        disable_actions=True,
        wip_hint="Power BI via API is Work In Progress."
    )
    st.markdown("</div>", unsafe_allow_html=True)

# Horizontal separator
st.markdown("<div class='table-hline'></div>", unsafe_allow_html=True)

# Row: Tableau
lab2, pub2, sep2, api2 = st.columns([0.8, 2, 0.07, 2])
lab2.markdown("#### Tableau")
with pub2:
    st.markdown("<div class='cell-pad'>", unsafe_allow_html=True)
    render_cell("pub_tbl", " ", is_cloud=False, provider="tableau")
    st.markdown("</div>", unsafe_allow_html=True)
with sep2:
    st.markdown("<div class='vline-wrap'><div class='vline'></div></div>", unsafe_allow_html=True)
with api2:
    st.markdown("<div class='cell-pad'>", unsafe_allow_html=True)
    render_cell("api_tbl", " ", is_cloud=True, provider="tableau")
    st.markdown("</div>", unsafe_allow_html=True)

# Bottom border
st.markdown("<div class='table-hline'></div>", unsafe_allow_html=True)

st.divider()

# ───────────────────────────── Global Status Dock ────────────────────────────
st.markdown("### Run Status")
s = st.session_state["kdh_status"]
if s["headline"]:
    st.write(f"**Latest:** {s['headline']}")
if s["summary"]:
    st.info(s["summary"])
    if s["full_image_url"]:
        st.link_button("Open full image", s["full_image_url"], use_container_width=False)
    if s["storage_prefix"]:
        st.code(s["storage_prefix"], language="text")

st.caption("Recent Log (tail)")
log_tail = "\n".join(st.session_state["kdh_log"][-30:])
st.code(log_tail or "(no log yet)", language="text")
