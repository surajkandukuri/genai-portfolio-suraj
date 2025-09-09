# pages/24_kpidrift_parseandcompare.py
from __future__ import annotations

import os, re, json, uuid, base64
from datetime import datetime, timezone
from typing import Dict, List, Optional, Set, Tuple

import pandas as pd
import streamlit as st
from supabase import create_client, Client
from postgrest.exceptions import APIError
from mistralai import Mistral

# ─────────────────────────────────────────────────────────────────────────────
# Config / Secrets
# ─────────────────────────────────────────────────────────────────────────────
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
SUPABASE_KEY  = _sget("SUPABASE_SERVICE_ROLE_KEY", "SUPABASE_SERVICE_KEY", "SUPABASE_ANON_KEY", "SUPABASE__SUPABASE_SERVICE_KEY")

KDH_BUCKET     = _sget("KDH_BUCKET", default="kpidrifthunter")
JSONS_ROOT     = "jsons_from_wigetsimages"  # keep spelling as requested
TBL_SG         = _sget("KDH_TABLE_SCREENGRABS", default="kdh_screengrab_dim")
TBL_WIDGETS    = _sget("KDH_TABLE_WIDGETS", default="kdh_widget_dim")
TBL_XFACT      = _sget("KDH_TABLE_WIDGET_EXTRACT", default="kdh_widget_extract_fact")

MISTRAL_API_KEY = _sget("MISTRAL_API_KEY")
MISTRAL_MODEL   = _sget("MISTRAL_MODEL", default="pixtral-12b-2409")

if not SUPABASE_URL or not SUPABASE_KEY:
    st.error("Missing Supabase config. Add SUPABASE_URL and a SERVICE_ROLE/ANON key in .streamlit/secrets.toml")
    st.stop()

@st.cache_resource
def get_sb() -> Client:
    try:
        return create_client(SUPABASE_URL, SUPABASE_KEY)
    except Exception as e:
        st.error(f"Could not connect to Supabase: {e}")
        st.stop()

sb: Client = get_sb()

# ─────────────────────────────────────────────────────────────────────────────
# Generic helpers (schema-aware + storage)
# ─────────────────────────────────────────────────────────────────────────────
def _nowstamp_z() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")

def _sanitize(s: str, max_len=160) -> str:
    s = (s or "").strip()
    s = re.sub(r"[^\w\-. ]+", "_", s)
    s = re.sub(r"\s+", "_", s)
    return (s[:max_len] or "untitled").rstrip("._-")

def _get_columns(table: str) -> Set[str]:
    """Infer available columns by fetching 1 row. Works with RLS-enabled tables."""
    try:
        res = sb.table(table).select("*").limit(1).execute()
        rows = res.data or []
        return set(rows[0].keys()) if rows else set()
    except Exception:
        return set()

def storage_download_bytes(bucket: str, key: str) -> bytes:
    key = (key or "").lstrip("/")
    return sb.storage.from_(bucket).download(key)

def storage_public_url(bucket: str, key: str) -> str:
    key = (key or "").lstrip("/")
    try:
        return sb.storage.from_(bucket).get_public_url(key)
    except Exception:
        return ""

def storage_upload_bytes(bucket: str, key: str, data: bytes, content_type="application/json") -> Dict[str, str]:
    key = key.lstrip("/")
    try:
        sb.storage.from_(bucket).upload(path=key, file=data,
                                        file_options={"content-type": content_type, "upsert": True})
    except Exception:
        try:
            sb.storage.from_(bucket).remove([key])
        except Exception:
            pass
        sb.storage.from_(bucket).upload(path=key, file=data, file_options={"content-type": content_type})
    try:
        url = sb.storage.from_(bucket).get_public_url(key)
    except Exception:
        url = ""
    return {"key": key, "public_url": url}

def json_storage_key(session_id: str, image_name: str) -> str:
    base = _sanitize(image_name.rsplit(".", 1)[0])
    ts = _nowstamp_z()
    return f"{JSONS_ROOT}/{session_id}/{base}_{ts}.json"

def extract_session_from_path(path: str) -> Optional[str]:
    """
    Parse session ID from storage path:
      widgetextractor/<session>/widgets/<file>
    """
    if not path:
        return None
    parts = path.strip("/").split("/")
    if len(parts) >= 3 and parts[0].lower().startswith("widgetextractor"):
        return parts[1]
    for p in parts:
        if re.search(r"\d{8}t\d{6}z", p, flags=re.I):
            return p
    return None

# ─────────────────────────────────────────────────────────────────────────────
# Data access — adapt to whatever schema exists
# ─────────────────────────────────────────────────────────────────────────────
WIDGET_COLS = _get_columns(TBL_WIDGETS)
SG_COLS     = _get_columns(TBL_SG)
XF_COLS     = _get_columns(TBL_XFACT)

DEFAULT_WIDGET_ORDER_COL = (
    "insrt_dttm" if "insrt_dttm" in WIDGET_COLS else
    ("rec_eff_strt_dt" if "rec_eff_strt_dt" in WIDGET_COLS else "widget_id")
)

@st.cache_data(ttl=120)
def load_recent_sessions(limit=400) -> List[str]:
    """Derive sessions from widget storage paths (works regardless of DB columns)."""
    sel_cols = ["storage_path_crop"]
    if DEFAULT_WIDGET_ORDER_COL:
        order_col = DEFAULT_WIDGET_ORDER_COL
    else:
        order_col = "widget_id"

    q = sb.table(TBL_WIDGETS).select(",".join(sel_cols)).order(order_col, desc=True).limit(limit).execute()
    rows = q.data or []
    sessions = []
    seen = set()
    for r in rows:
        sess = extract_session_from_path(r.get("storage_path_crop"))
        if sess and sess not in seen:
            seen.add(sess)
            sessions.append(sess)
    return sessions

@st.cache_data(ttl=180)
def load_widgets_for_session(session_id: str) -> List[Dict]:
    """
    Load widgets whose storage_path_crop contains /<session_id>/.
    Map screengrab metadata if present (fetched separately to avoid embed assumptions).
    """
    # 1) Pull widgets by storage path
    sel_cols = ["widget_id", "screengrab_id", "storage_path_crop"]
    for c in ["widget_title", "widget_type", "quality", "quality_score", "bbox_xywh",
              "insrt_dttm", "extraction_stage", "area_px"]:
        if c in WIDGET_COLS: sel_cols.append(c)

    q = (sb.table(TBL_WIDGETS)
            .select(",".join(sel_cols))
            .ilike("storage_path_crop", f"%/{session_id}/%")
            .order(DEFAULT_WIDGET_ORDER_COL, desc=False)
            .execute())
    widgets = q.data or []

    # 2) Fetch parent screengrabs (optional)
    url_by_sg, cap_by_sg, sess_by_sg, rn_by_sg = {}, {}, {}, {}
    sg_ids = list({w["screengrab_id"] for w in widgets if w.get("screengrab_id")})
    if sg_ids:
        sg_sel = ["screengrab_id"]
        for c in ["url", "captured_at", "capture_session_id", "report_name", "report_slug"]:
            if c in SG_COLS: sg_sel.append(c)
        B = 200
        rows_all = []
        for i in range(0, len(sg_ids), B):
            batch = sg_ids[i:i+B]
            rr = sb.table(TBL_SG).select(",".join(sg_sel)).in_("screengrab_id", batch).execute()
            rows_all.extend(rr.data or [])
        for s in rows_all:
            sgid = s.get("screengrab_id")
            if "url" in s: url_by_sg[sgid] = s.get("url")
            if "captured_at" in s: cap_by_sg[sgid] = s.get("captured_at")
            if "capture_session_id" in s: sess_by_sg[sgid] = s.get("capture_session_id")
            if "report_name" in s: rn_by_sg[sgid] = s.get("report_name")

    # 3) Normalize fields for UI and parser
    for r in widgets:
        r["storage_path_widget"] = r.get("storage_path_crop")  # alias used by parser
        sgid = r.get("screengrab_id")
        r["url"]         = url_by_sg.get(sgid)
        r["captured_at"] = cap_by_sg.get(sgid)
        r["session_key"] = sess_by_sg.get(sgid) or extract_session_from_path(r.get("storage_path_crop"))
        r["report_name"] = rn_by_sg.get(sgid)
        r["public_url"]  = storage_public_url(KDH_BUCKET, r["storage_path_widget"])
    return widgets

@st.cache_data(ttl=120)
def load_recent_extracts(limit=300) -> List[Dict]:
    q = sb.table(TBL_XFACT).select("*").order("created_at", desc=True).limit(limit).execute()
    return q.data or []

# ─────────────────────────────────────────────────────────────────────────────
# Mistral extraction (image bytes → JSON)
# ─────────────────────────────────────────────────────────────────────────────
GRAPH_PROMPT = (
  "You are an expert extraction engine for charts (line/bar/pie). "
  "Given an image, extract structured values as JSON with fields: "
  "title, x_axis_label, y_axis_label, data_points (list of {x, y})."
)

def extract_graph_json_from_png_bytes(png_bytes: bytes) -> Dict:
    if not MISTRAL_API_KEY:
        raise RuntimeError("MISTRAL_API_KEY not configured.")
    client = Mistral(api_key=MISTRAL_API_KEY)
    encoded = base64.b64encode(png_bytes).decode("utf-8")
    messages = [
        {"role": "system", "content": GRAPH_PROMPT},
        {
            "role": "user",
            "content": [
                {"type": "text", "text": "Extract data from this chart image."},
                {"type": "image_url", "image_url": f"data:image/png;base64,{encoded}"}
            ]
        }
    ]
    resp = client.chat.complete(
        model=MISTRAL_MODEL,
        messages=messages,
        response_format={"type": "json_object"},
    )
    raw = resp.choices[0].message.content
    try:
        return json.loads(raw)
    except Exception:
        start = raw.find("{"); end = raw.rfind("}")
        return json.loads(raw[start:end+1])

# ─────────────────────────────────────────────────────────────────────────────
# Comparison math
# ─────────────────────────────────────────────────────────────────────────────
def _to_df(vals: Dict) -> pd.DataFrame:
    dpts = (vals or {}).get("data_points") or []
    rows = []
    for p in dpts:
        x = p.get("x")
        y = p.get("y")
        if y is not None:
            rows.append({"x": str(x), "y": float(y)})
    return pd.DataFrame(rows)

def compare_json_values(vals_a: Dict, vals_b: Dict) -> Dict:
    df_a = _to_df(vals_a).rename(columns={"y": "value_a"})
    df_b = _to_df(vals_b).rename(columns={"y": "value_b"})
    df = pd.merge(df_a, df_b, on="x", how="inner")
    if df.empty:
        return {"corr": None, "mape": None, "n": 0, "verdict": "no_overlap", "aligned": df}

    corr = float(df[["value_a", "value_b"]].corr().iloc[0,1])
    eps = 1e-9
    mape = float((abs(df.value_a - df.value_b) / (abs(df.value_b)+eps)).mean())

    verdict = "consistent" if (corr > 0.95 and mape < 0.02) else ("likely_mismatch" if corr > 0.80 else "conflict")
    return {"corr": corr, "mape": mape, "n": int(len(df)), "verdict": verdict, "aligned": df}

# ─────────────────────────────────────────────────────────────────────────────
# UI
# ─────────────────────────────────────────────────────────────────────────────
st.set_page_config(page_title="KPI Drift — Parse & Compare", page_icon="📊", layout="wide")
st.title("📊 KPI Drift — Parse & Compare")
st.caption("Schema-aware: detects actual columns. Choose a session, then **pick the exact widget** to parse.")

# =============================================================================
# ① PARSE
# =============================================================================
st.header("① Parse")

sessions = load_recent_sessions()
left, right = st.columns([1.4, 2.6])
session_choice = left.selectbox("Session (derived from storage path)", options=["— choose —"] + sessions, index=0)

widgets = []
if session_choice and session_choice != "— choose —":
    widgets = load_widgets_for_session(session_choice)

# --- User-friendly, professional widget picker
if widgets:
    dfw = pd.DataFrame(widgets)

    # Build a readable label with sensible fallbacks
    def make_label(r: pd.Series) -> str:
        fname = (r.get("storage_path_widget") or "").split("/")[-1]
        title = r.get("widget_title") or "Untitled"
        wtype = r.get("widget_type") or "chart"
        quality = r.get("quality") or "unknown"
        when = r.get("captured_at") or ""
        rname = r.get("report_name") or "report"
        return f"{rname} • {title} ({wtype}, {quality}) • {fname} • {when}"

    dfw["__label__"] = dfw.apply(make_label, axis=1)

    # Searchable selectbox + live preview card
    pick = right.selectbox(
        "Choose a widget to parse",
        options=dfw["__label__"].tolist(),
        index=0 if len(dfw) else None,
        help="Select which widget image to send to the parser."
    )

    # Preview card
    sel = dfw[dfw["__label__"] == pick].iloc[0].to_dict()
    with st.container(border=True):
        cimg, cmeta = st.columns([1.2, 2.0])
        if sel.get("public_url"):
            cimg.image(sel["public_url"], caption="Widget preview", use_column_width=True)
        else:
            cimg.info("No public URL available for this image.")

        # Show concise metadata
        cmeta.subheader(sel.get("widget_title") or "Untitled")
        meta_rows = []
        for key in ["widget_type", "quality", "quality_score", "url", "storage_path_widget", "captured_at", "session_key"]:
            if key in dfw.columns and sel.get(key) not in [None, "", []]:
                meta_rows.append((key.replace("_"," ").title(), str(sel.get(key))))
        meta_df = pd.DataFrame(meta_rows, columns=["Field", "Value"])
        cmeta.table(meta_df)

else:
    dfw = pd.DataFrame()
    pick = None

# Controls
col_parse, col_save, _ = st.columns([1,1,5])
save_json_files = col_save.toggle(
    "Save JSON to Storage",
    value=True,
    help=f"Writes an artifact under `{JSONS_ROOT}/{{session}}/…json` for audit & sharing."
)
do_parse = col_parse.button("Parse Selected Widget", type="primary", use_container_width=True, disabled=not (len(widgets) and pick))

# Execute parse
if do_parse and pick:
    row = dfw[dfw["__label__"] == pick].iloc[0].to_dict()
    img_path = (row.get("storage_path_widget") or "").lstrip("/")
    image_name = img_path.split("/")[-1] if img_path else "widget.png"

    try:
        png_bytes = storage_download_bytes(KDH_BUCKET, img_path)
    except Exception as e:
        st.error(f"Download failed for {img_path}: {e}")
        st.stop()

    # LLM extraction
    try:
        values = extract_graph_json_from_png_bytes(png_bytes)
    except Exception as e:
        st.error(f"LLM extraction failed: {e}")
        st.stop()

    session_key = row.get("session_key") or extract_session_from_path(row.get("storage_path_widget")) or "session"

    # Optional JSON artifact
    json_key = None
    if save_json_files:
        json_key = json_storage_key(session_key, image_name)
        storage_upload_bytes(KDH_BUCKET, json_key, json.dumps(values).encode("utf-8"))

    # Insert into fact table — only columns that exist
    base_payload = {
        "extraction_id": str(uuid.uuid4()),
        "widget_id": row.get("widget_id"),
        "screengrab_id": row.get("screengrab_id"),
        "url": row.get("url"),
        "image_storage_path": img_path,
        "json_storage_path": json_key,
        "values": values,
        "created_at": datetime.now(timezone.utc).isoformat()
    }
    if "capture_session_id" in XF_COLS:
        base_payload["capture_session_id"] = session_key
    elif "session_folder" in XF_COLS:
        base_payload["session_folder"] = session_key

    payload = {k: v for k, v in base_payload.items() if (k in XF_COLS) or (k == "values")}

    try:
        ins = sb.table(TBL_XFACT).insert(payload).execute()
        st.success("Parsed and saved ✅")
        with st.expander("Extracted JSON", expanded=False):
            st.json(values)
        st.caption(f"DB insert: {ins.data}")
    except APIError as e:
        st.error(f"Insert failed (schema mismatch): {e}")
    except Exception as e:
        st.error(f"Insert failed: {e}")

# =============================================================================
# ② COMPARE
# =============================================================================
st.header("② Compare")

extract_rows = load_recent_extracts()
if not extract_rows:
    st.info("No parsed rows yet. Use the Parse section above.")
else:
    dfe = pd.DataFrame(extract_rows)

    def pick_session(r):
        for k in ["capture_session_id","session_folder","session_key"]:
            if k in r and r[k]:
                return r[k]
        p = r.get("image_storage_path") if isinstance(r, dict) else r["image_storage_path"]
        return extract_session_from_path(p) or "—"

    def pick_img(r):
        p = r.get("image_storage_path") if isinstance(r, dict) else r["image_storage_path"]
        return (p or "").split("/")[-1] or "—"

    if "created_at" not in dfe.columns:
        dfe["created_at"] = ""
    if "image_storage_path" not in dfe.columns:
        dfe["image_storage_path"] = ""

    dfe["label"] = dfe.apply(lambda r: f"{pick_session(r)} • {pick_img(r)} • {r.get('created_at')}", axis=1)

    c1, c2 = st.columns(2)
    left_pick  = c1.selectbox("Left series", options=dfe["label"].tolist())
    right_pick = c2.selectbox("Right series", options=dfe["label"].tolist(),
                              index=min(1, len(dfe)-1) if len(dfe) > 1 else 0)

    go = st.button("Compare Now", type="primary")
    if go:
        la = dfe[dfe["label"]==left_pick].iloc[0]
        rb = dfe[dfe["label"]==right_pick].iloc[0]

        vals_a = la.get("values") or {}
        vals_b = rb.get("values") or {}

        res = compare_json_values(vals_a, vals_b)

        st.subheader("Verdict")
        st.write(f"**Verdict:** `{res['verdict']}`")
        st.write(f"Aligned points: n={res['n']}, corr={res['corr']}, mape={res['mape']}")

        if isinstance(res.get("aligned"), pd.DataFrame) and not res["aligned"].empty:
            st.subheader("Aligned values")
            st.dataframe(res["aligned"], use_container_width=True)
