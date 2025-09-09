# pages/24_kpidrift_parseandcompare.py
from __future__ import annotations

import os, re, json, uuid, base64
from datetime import datetime, timezone
from typing import Dict, List, Optional

import pandas as pd
import streamlit as st
from supabase import create_client, Client
from mistralai import Mistral
from postgrest.exceptions import APIError

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
JSONS_ROOT     = "jsons_from_wigetsimages"  # keep your spelling
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
# Helpers
# ─────────────────────────────────────────────────────────────────────────────
def _nowstamp_z() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")

def _sanitize(s: str, max_len=160) -> str:
    s = (s or "").strip()
    s = re.sub(r"[^\w\-. ]+", "_", s)
    s = re.sub(r"\s+", "_", s)
    return (s[:max_len] or "untitled").rstrip("._-")

def storage_download_bytes(bucket: str, key: str) -> bytes:
    key = (key or "").lstrip("/")
    return sb.storage.from_(bucket).download(key)

def storage_upload_bytes(bucket: str, key: str, data: bytes, content_type="application/json") -> Dict[str,str]:
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

# ─────────────────────────────────────────────────────────────────────────────
# Data access (FK embed; NO schema changes)
# ─────────────────────────────────────────────────────────────────────────────
@st.cache_data(ttl=120)
def load_recent_sessions(limit=200) -> List[str]:
    """
    Sessions come from kdh_screengrab_dim.capture_session_id (NOT session_folder).
    """
    q = (
        sb.table(TBL_SG)
          .select("capture_session_id, captured_at")
          .order("captured_at", desc=True)
          .limit(limit)
          .execute()
    )
    rows = q.data or []
    seen, out = set(), []
    for r in rows:
        s = r.get("capture_session_id")
        if s and s not in seen:
            seen.add(s); out.append(s)
    return out

@st.cache_data(ttl=180)
def load_widgets_for_session(session_id: str) -> List[Dict]:
    """
    Pull widgets joined with parent screengrab fields via FK embed.
    widget table does NOT have session info; we alias storage_path_crop -> storage_path_widget.
    """
    q = (
        sb.table(TBL_WIDGETS)
          .select(
              "widget_id, screengrab_id, widget_title, widget_type, "
              "quality, quality_score, bbox_xywh, storage_path_crop, "
              f"{TBL_SG}(capture_session_id, url, captured_at, platform, report_name, report_slug)"
          )
          .eq(f"{TBL_SG}.capture_session_id", session_id)
          .order("screengrab_id", desc=False)
          .execute()
    )
    rows = q.data or []
    for r in rows:
        parent = r.get(TBL_SG) or {}
        # normalize expected fields for UI
        r["url"]                 = parent.get("url")
        r["captured_at"]         = parent.get("captured_at")
        r["report_name"]         = parent.get("report_name")
        r["report_slug"]         = parent.get("report_slug")
        r["capture_session_id"]  = parent.get("capture_session_id")
        r["storage_path_widget"] = r.get("storage_path_crop")  # alias for the parser
    return rows

@st.cache_data(ttl=120)
def load_recent_extracts(limit=300) -> List[Dict]:
    # We don't know your exact columns; select a superset and handle missing keys in code.
    q = (
        sb.table(TBL_XFACT)
          .select("*")
          .order("created_at", desc=True)
          .limit(limit)
          .execute()
    )
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
st.caption("Pick a widget → Parse (extract JSON into table). Then select two parses to Compare.")

# =============================================================================
# ① PARSE
# =============================================================================
st.header("① Parse")

sessions = load_recent_sessions()
col_s1, col_s2 = st.columns([1,4])
session_choice = col_s1.selectbox("Session (capture_session_id)", options=["— choose —"] + sessions, index=0)

widgets = []
if session_choice and session_choice != "— choose —":
    widgets = load_widgets_for_session(session_choice)

if widgets:
    dfw = pd.DataFrame(widgets)
    dfw["__label__"] = dfw.apply(
        lambda r: f"{r.get('report_name') or 'report'} | {r.get('widget_title') or 'Untitled'} | "
                  f"{(r.get('storage_path_widget') or '').split('/')[-1]} | "
                  f"{(r.get('captured_at') or '')}",
        axis=1
    )

    st.caption("Widgets in this session")
    st.dataframe(
        dfw[["widget_id","widget_title","widget_type","quality","quality_score","url","storage_path_widget","captured_at"]],
        use_container_width=True, height=300
    )

    pick = st.radio(
        "Pick a widget to parse",
        options=dfw["__label__"].tolist(),
        index=0 if len(dfw) else None
    )
else:
    dfw = pd.DataFrame()
    pick = None

cpa1, cpa2, _ = st.columns([1,1,6])
save_json_files = cpa2.toggle("Also save JSON to Storage", value=True,
                              help="Writes an audit JSON at kpidrifthunter/jsons_from_wigetsimages/{session}/...")

do_parse = cpa1.button("Parse", type="primary", use_container_width=True, disabled=not (len(widgets) and pick))

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

    # Optional JSON artifact
    json_key = None
    if save_json_files:
        json_key = json_storage_key(row.get("capture_session_id") or "session", image_name)
        storage_upload_bytes(KDH_BUCKET, json_key, json.dumps(values).encode("utf-8"))

    # Insert into fact table (be tolerant of column differences)
    payload_base = {
        "extraction_id": str(uuid.uuid4()),
        "widget_id": row.get("widget_id"),
        "screengrab_id": row.get("screengrab_id"),
        "url": row.get("url"),
        "image_storage_path": img_path,
        "json_storage_path": json_key,
        "values": values,
        "created_at": datetime.now(timezone.utc).isoformat()
    }

    # Try with capture_session_id; if 42703, fallback without it; then try 'session_folder'
    tried = []
    last_err = None
    for variant in [
        {**payload_base, "capture_session_id": row.get("capture_session_id")},
        payload_base,
        {**payload_base, "session_folder": row.get("capture_session_id")},
    ]:
        keyset = tuple(sorted(variant.keys()))
        if keyset in tried:
            continue
        tried.append(keyset)
        try:
            ins = sb.table(TBL_XFACT).insert(variant).execute()
            st.success("Parsed and saved ✅")
            st.json(values, expanded=False)
            st.write("DB insert:", ins.data)
            last_err = None
            break
        except APIError as e:
            # 42703 = undefined_column; try next variant
            last_err = e
        except Exception as e:
            last_err = e
    if last_err:
        st.error(f"Insert failed. Last error:\n\n{last_err}")

# =============================================================================
# ② COMPARE
# =============================================================================
st.header("② Compare")

extract_rows = load_recent_extracts()
if not extract_rows:
    st.info("No parsed rows yet. Use the Parse section above.")
else:
    dfe = pd.DataFrame(extract_rows)
    # pick a session-like label from either capture_session_id or session_folder if present
    def _sess(r):
        return r.get("capture_session_id") or r.get("session_folder") or "—"
    def _img(r):
        p = r.get("image_storage_path") or ""
        return p.split("/")[-1] if p else "—"

    dfe["label"] = dfe.apply(lambda r: f"{_sess(r)} | {_img(r)} | {r.get('created_at')}", axis=1)

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
        st.write(f"n={res['n']}, corr={res['corr']}, mape={res['mape']}")

        if isinstance(res.get("aligned"), pd.DataFrame) and not res["aligned"].empty:
            st.subheader("Aligned values")
            st.dataframe(res["aligned"], use_container_width=True)
