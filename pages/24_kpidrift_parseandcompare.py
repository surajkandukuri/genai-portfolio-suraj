# pages/24_kpidrift_parseandcompare.py
from __future__ import annotations

import os, re, json, uuid, base64, time, math
from datetime import datetime, timezone
from typing import Dict, List, Optional, Set, Tuple

import pandas as pd
import streamlit as st
from supabase import create_client, Client
from postgrest.exceptions import APIError
from mistralai import Mistral
import importlib.util
#from provisioning.a2_kpidrift_capture.a2_kpidrift_powerbi import capture_powerbi
from provisioning.a2_kpidrift_capture.a2_kpidrift_pair_compare import PairCompareLLM
# ─────────────────────────────────────────────────────────────────────────────
# Config / Secrets
# ─────────────────────────────────────────────────────────────────────────────
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

KDH_BUCKET     = _sget("KDH_BUCKET", default="kpidrifthunter")
JSONS_ROOT     = "jsons_from_wigetsimages"  # spelling kept per request
TBL_SG         = _sget("KDH_TABLE_SCREENGRABS", default="kdh_screengrab_dim")
TBL_WIDGETS    = _sget("KDH_TABLE_WIDGETS", default="kdh_widget_dim")
TBL_XFACT      = _sget("KDH_TABLE_WIDGET_EXTRACT", default="kdh_widget_extract_fact")
TBL_PAIR       = _sget("KDH_TABLE_PAIR_MAP", default="kdh_pair_map_dim")  # SCD-2 mapping table

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

# NEW: cache the LLM-only pair comparator
@st.cache_resource
def _get_pair_comparator():
    return PairCompareLLM(
        supabase_client=sb,
        secrets=st.secrets,                 # reads MISTRAL_API_KEY / MISTRAL_MODEL if present
        tbl_widgets=TBL_WIDGETS,
        tbl_extract=TBL_XFACT,
        tbl_pairs=TBL_PAIR,
        tbl_compare="kdh_compare_fact",
    )

cmp_llm = _get_pair_comparator()

# ─────────────────────────────────────────────────────────────────────────────
# Helpers (schema-aware + storage)
# ─────────────────────────────────────────────────────────────────────────────
def _nowstamp_z() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")

def _sanitize(s: str, max_len=160) -> str:
    s = (s or "").strip()
    s = re.sub(r"[^\w\-. ]+", "_", s)
    s = re.sub(r"\s+", "_", s)
    return (s[:max_len] or "untitled").rstrip("._-")

def _get_columns(table: str) -> Set[str]:
    try:
        res = sb.table(table).select("*").limit(1).execute()
        rows = res.data or []
        return set(rows[0].keys()) if rows else set()
    except Exception:
        return set()

@st.cache_data(ttl=300, show_spinner=False)
def fetch_widget_image_bytes(bucket: str, key: str) -> bytes:
    key = (key or "").lstrip("/")
    resp = sb.storage.from_(bucket).download(key)
    if isinstance(resp, (bytes, bytearray)):
        return bytes(resp)
    content = getattr(resp, "content", None)
    if content is not None:
        return bytes(content)
    if hasattr(resp, "read"):
        return resp.read()
    if isinstance(resp, dict):
        if "data" in resp and isinstance(resp["data"], (bytes, bytearray)):
            return bytes(resp["data"])
        if "data" in resp and hasattr(resp["data"], "read"):
            return resp["data"].read()
    raise RuntimeError(f"Could not decode bytes for storage object: {bucket}/{key}")

def upload_json_to_storage(bucket: str, key: str, data_bytes: bytes):
    # IMPORTANT: values must be strings (httpx header restriction)
    return sb.storage.from_(bucket).upload(
        path=key,
        file=data_bytes,
        file_options={"content_type": "application/json", "upsert": "true"},
    )

def json_storage_key(session_id: str, image_name: str) -> str:
    base = _sanitize(image_name.rsplit(".", 1)[0])
    ts = _nowstamp_z()
    return f"{JSONS_ROOT}/{session_id}/{base}_{ts}.json"

def extract_session_from_path(path: str) -> Optional[str]:
    if not path: return None
    parts = path.strip("/").split("/")
    if len(parts) >= 3 and parts[0].lower().startswith("widgetextractor"):
        return parts[1]
    for p in parts:
        if re.search(r"\d{8}t\d{6}z", p, flags=re.I):
            return p
    return None

# ─────────────────────────────────────────────────────────────────────────────
# Data access
# ─────────────────────────────────────────────────────────────────────────────
WIDGET_COLS = _get_columns(TBL_WIDGETS)
SG_COLS     = _get_columns(TBL_SG)
XF_COLS     = _get_columns(TBL_XFACT)
PAIR_COLS   = _get_columns(TBL_PAIR)

DEFAULT_WIDGET_ORDER_COL = (
    "insrt_dttm" if "insrt_dttm" in WIDGET_COLS else
    ("rec_eff_strt_dt" if "rec_eff_strt_dt" in WIDGET_COLS else "widget_id")
)

@st.cache_data(ttl=120)
def load_recent_sessions(limit=500) -> List[str]:
    q = (sb.table(TBL_WIDGETS)
            .select("storage_path_crop")
            .order(DEFAULT_WIDGET_ORDER_COL, desc=True)
            .limit(limit)
            .execute())
    rows = q.data or []
    sessions, seen = [], set()
    for r in rows:
        sess = extract_session_from_path(r.get("storage_path_crop"))
        if sess and sess not in seen:
            seen.add(sess); sessions.append(sess)
    return sessions

@st.cache_data(ttl=180)
def load_widgets_for_session(session_id: str) -> List[Dict]:
    sel_cols = ["widget_id", "screengrab_id", "storage_path_crop"]
    for c in ["widget_title","widget_type","quality","quality_score","bbox_xywh",
              "insrt_dttm","extraction_stage","area_px"]:
        if c in WIDGET_COLS: sel_cols.append(c)

    q = (sb.table(TBL_WIDGETS)
            .select(",".join(sel_cols))
            .ilike("storage_path_crop", f"%/{session_id}/%")
            .order(DEFAULT_WIDGET_ORDER_COL, desc=False)
            .execute())
    widgets = q.data or []

    url_by_sg, cap_by_sg, sess_by_sg, rn_by_sg = {}, {}, {}, {}
    sg_ids = list({w["screengrab_id"] for w in widgets if w.get("screengrab_id")})
    if sg_ids:
        sg_sel = ["screengrab_id"]
        for c in ["url", "captured_at", "capture_session_id", "report_name", "report_slug"]:
            if c in SG_COLS: sg_sel.append(c)
        rows_all = []
        for i in range(0, len(sg_ids), 200):
            batch = sg_ids[i:i+200]
            rr = sb.table(TBL_SG).select(",".join(sg_sel)).in_("screengrab_id", batch).execute()
            rows_all.extend(rr.data or [])
        for s in rows_all:
            sgid = s.get("screengrab_id")
            url_by_sg[sgid]  = s.get("url")
            cap_by_sg[sgid]  = s.get("captured_at")
            sess_by_sg[sgid] = s.get("capture_session_id")
            rn_by_sg[sgid]   = s.get("report_name")

    for r in widgets:
        r["storage_path_widget"] = r.get("storage_path_crop")
        sgid = r.get("screengrab_id")
        r["url"]         = url_by_sg.get(sgid)
        r["captured_at"] = cap_by_sg.get(sgid)
        r["session_key"] = sess_by_sg.get(sgid) or extract_session_from_path(r.get("storage_path_crop"))
        r["report_name"] = rn_by_sg.get(sgid)
        r["public_url"]  = ""
    return widgets

# pick best timestamp to order by (schema-aware)
def _first_existing(cols: list[str], available: set[str]) -> Optional[str]:
    for c in cols:
        if c in available:
            return c
    return None

def latest_extract_for_widget(widget_id: str) -> Optional[Dict]:
    """Return the latest extract row for a widget, ordering by the best available timestamp column."""
    try:
        info = sb.table(TBL_XFACT).select("*").limit(1).execute()
        cols = set((info.data or [{}])[0].keys())
    except Exception:
        cols = set()

    order_col = _first_existing(["created_at", "insrt_dttm", "rec_eff_strt_dt", "updated_at"], cols) or "extraction_id"

    try:
        res = (
            sb.table(TBL_XFACT)
              .select("*")
              .eq("widget_id", widget_id)
              .order(order_col, desc=True)
              .limit(1)
              .execute()
        )
        rows = res.data or []
        return rows[0] if rows else None
    except Exception:
        return None

def _pick_json_payload(row: Dict) -> Dict:
    # Accept common names; fall back to first JSON-like field
    for key in ["values", "json_values", "payload", "extracted_values"]:
        if key in row and row[key]:
            return row[key]
    for k, v in row.items():
        if isinstance(v, (dict, list)):
            return v
    return {}

@st.cache_data(ttl=120)
def load_recent_extracts(limit=300) -> List[Dict]:
    q = sb.table(TBL_XFACT).select("*").order("created_at", desc=True).limit(limit).execute()
    return q.data or []

# ─────────────────────────────────────────────────────────────────────────────
# SCD-2 helpers for pair mappings
# ─────────────────────────────────────────────────────────────────────────────
def scd2_upsert_pair(widget_left: str, widget_right: str,
                     left_sess: Optional[str], right_sess: Optional[str],
                     pair_number: Optional[int]) -> Dict:
    """
    End-date current row for (widget_left, widget_right) if anything changed,
    then insert a new 'current' row. Returns {"action": "...", "pair_id": "..."}.
    """
    now_iso = datetime.now(timezone.utc).isoformat()

    cur = (sb.table(TBL_PAIR)
             .select("*")
             .eq("widget_id_left", widget_left)
             .eq("widget_id_right", widget_right)
             .eq("curr_rec_ind", True)
             .limit(1)
             .execute())
    cur_rows = cur.data or []

    if cur_rows:
        row = cur_rows[0]
        same = (
            (row.get("left_session_id")  == left_sess) and
            (row.get("right_session_id") == right_sess) and
            (row.get("pair_number")      == pair_number)
        )
        if same:
            return {"action": "unchanged", "pair_id": row.get("pair_id")}
        sb.table(TBL_PAIR).update({"curr_rec_ind": False, "rec_eff_end_dt": now_iso}) \
            .eq("pair_id", row["pair_id"]).execute()

    payload = {
        "widget_id_left":  widget_left,
        "widget_id_right": widget_right,
        "left_session_id":  left_sess,
        "right_session_id": right_sess,
        "pair_number":      pair_number,
        "insrt_dttm":      now_iso,
        "rec_eff_strt_dt": now_iso,
        "curr_rec_ind":    True,
        "status":          "active",
    }
    ins = sb.table(TBL_PAIR).insert(payload).execute()
    pid = (ins.data or [{}])[0].get("pair_id")
    return {"action": "inserted", "pair_id": pid}

@st.cache_data(ttl=60)
def load_current_pairs() -> List[Dict]:
    try:
        res = (sb.table(TBL_PAIR).select("*")
               .eq("curr_rec_ind", True)
               .order("insrt_dttm", desc=True)
               .execute())
        return res.data or []
    except Exception:
        return []

def widget_titles(widget_ids: List[str]) -> Dict[str, str]:
    if not widget_ids: return {}
    rows = (sb.table(TBL_WIDGETS)
              .select("widget_id,widget_title")
              .in_("widget_id", widget_ids)
              .execute()).data or []
    return {r["widget_id"]: r.get("widget_title") or "" for r in rows}

# ─────────────────────────────────────────────────────────────────────────────
# LLM extract + compare (Parse step uses this extractor)
# ─────────────────────────────────────────────────────────────────────────────
GRAPH_PROMPT = (
  "You are an expert extraction engine for charts (line/bar/pie). "
  "Given an image, extract structured values as JSON with fields: "
  "title, x_axis_label, y_axis_label, data_points (list of {x, y})."
)

def build_llm_messages_from_bytes(png_bytes: bytes) -> list:
    encoded = base64.b64encode(png_bytes).decode("utf-8")
    return [
        {"role": "system", "content": GRAPH_PROMPT},
        {"role": "user", "content": [
            {"type": "text", "text": "Extract data from this chart image."},
            {"type": "image_url", "image_url": f"data:image/png;base64,{encoded}"},
        ]},
    ]

def call_mistral(messages: list) -> str:
    if not MISTRAL_API_KEY:
        raise RuntimeError("MISTRAL_API_KEY not configured.")
    client = Mistral(api_key=MISTRAL_API_KEY)
    resp = client.chat.complete(
        model=MISTRAL_MODEL,
        messages=messages,
        response_format={"type": "json_object"},
    )
    return resp.choices[0].message.content

def parse_llm_json(raw_text: str) -> Dict:
    try:
        return json.loads(raw_text)
    except Exception:
        start = raw_text.find("{"); end = raw_text.rfind("}")
        return json.loads(raw_text[start:end+1])

def _to_df(vals: Dict) -> pd.DataFrame:
    dpts = (vals or {}).get("data_points") or []
    rows = []
    for p in dpts:
        x = p.get("x"); y = p.get("y")
        if y is not None:
            rows.append({"x": str(x), "y": float(y)})
    return pd.DataFrame(rows)

def compare_json_values(vals_a: Dict, vals_b: Dict) -> Dict:
    df_a = _to_df(vals_a).rename(columns={"y":"value_a"})
    df_b = _to_df(vals_b).rename(columns={"y":"value_b"})
    df = pd.merge(df_a, df_b, on="x", how="inner")
    if df.empty:
        return {"corr": None, "mape": None, "n": 0, "verdict": "no_overlap", "aligned": df}
    corr = float(df[["value_a","value_b"]].corr().iloc[0,1])
    mape = float((abs(df.value_a - df.value_b) / (abs(df.value_b)+1e-9)).mean())
    verdict = "consistent" if (corr>0.95 and mape<0.02) else ("likely_mismatch" if corr>0.80 else "conflict")
    return {"corr":corr, "mape":mape, "n":int(len(df)), "verdict":verdict, "aligned":df}

# ─────────────────────────────────────────────────────────────────────────────
# Debug expanders (Collapse/Expand All)
# ─────────────────────────────────────────────────────────────────────────────
if "kdh_debug_keys" not in st.session_state:
    st.session_state.kdh_debug_keys: List[str] = []

def debug_expander(title: str, key: str):
    if key not in st.session_state.kdh_debug_keys:
        st.session_state.kdh_debug_keys.append(key)
    expanded = st.session_state.get(key, False)
    return st.expander(title, expanded=expanded)

def expand_all_debug():
    for k in st.session_state.kdh_debug_keys:
        st.session_state[k] = True

def collapse_all_debug():
    for k in st.session_state.kdh_debug_keys:
        st.session_state[k] = False

# ─────────────────────────────────────────────────────────────────────────────
# UI
# ─────────────────────────────────────────────────────────────────────────────
st.set_page_config(page_title="KPI Drift — Parse → Map → Compare", page_icon="📊", layout="wide")
st.title("📊 KPI Drift — Parse → Map → Compare")
st.caption("Sequential flow: **① Parse**, then **② Map (only parsed widgets)**, then **③ Compare by saved pairs**.")

hdr_c1, hdr_c2, hdr_c3 = st.columns([1, 0.16, 0.16])
with hdr_c1:
    show_debug = st.toggle("Show debug details", value=False, help="Show LLM request/response and DB payloads")
with hdr_c2:
    if st.button("Expand All", use_container_width=True, disabled=not show_debug):
        expand_all_debug()
with hdr_c3:
    if st.button("Collapse All", use_container_width=True, disabled=not show_debug):
        collapse_all_debug()

# =============================================================================
# ① PARSE  (MUST happen first)
# =============================================================================
st.header("① Parse")
sessions = load_recent_sessions()
left, right = st.columns([1.6, 2.4])
session_choice = left.selectbox("Session (derived from storage path)", options=["— choose —"] + sessions, index=0)

widgets = []
if session_choice and session_choice != "— choose —":
    widgets = load_widgets_for_session(session_choice)

if widgets:
    dfw = pd.DataFrame(widgets)

    def make_label(r: pd.Series) -> str:
        fname = (r.get("storage_path_widget") or "").split("/")[-1]
        title = r.get("widget_title") or "Untitled"
        wtype = r.get("widget_type") or "chart"
        quality = r.get("quality") or "unknown"
        when = r.get("captured_at") or ""
        rname = r.get("report_name") or "report"
        return f"{rname} • {title} ({wtype}, {quality}) • {fname} • {when}"

    dfw["__label__"] = dfw.apply(make_label, axis=1)

    selected_labels = right.multiselect(
        "Choose widget(s) to parse",
        options=dfw["__label__"].tolist(),
        default=dfw["__label__"].tolist()[:1],
        help="Pick one or many widgets to send to the parser."
    )

    # Dynamic preview
    if selected_labels:
        sel_df = dfw.set_index("__label__").loc[selected_labels].reset_index()
        sel_count = len(selected_labels)

        if sel_count == 1:
            first = sel_df.iloc[0].to_dict()
            with st.container(border=True):
                cimg, cmeta = st.columns([1.1, 2.0])
                try:
                    first_bytes = fetch_widget_image_bytes(
                        KDH_BUCKET, (first.get("storage_path_widget") or "").lstrip("/")
                    )
                    cimg.image(first_bytes, caption="Widget preview", use_container_width=True)
                except Exception:
                    cimg.info("No preview for the selected widget.")
                cmeta.subheader(first.get("widget_title") or "Untitled")
                meta_rows = []
                for key in ["widget_id","widget_type","quality","quality_score","url",
                            "storage_path_widget","captured_at","session_key"]:
                    if key in dfw.columns and first.get(key) not in [None, "", []]:
                        meta_rows.append((key.replace("_"," ").title(), str(first.get(key))))
                cmeta.dataframe(pd.DataFrame(meta_rows, columns=["Field","Value"]),
                                use_container_width=True, hide_index=True)
        else:
            st.subheader("Selected widget cards")
            cards_per_row = st.slider("Cards per row", min_value=2, max_value=6, value=min(4, sel_count), key="cards_per_row")
            n = len(sel_df)
            rows = math.ceil(n / cards_per_row)
            idx = 0
            for _ in range(rows):
                cols = st.columns(cards_per_row, gap="medium")
                for c in cols:
                    if idx >= n:
                        break
                    row = sel_df.iloc[idx].to_dict()
                    with c.container(border=True):
                        img_key = (row.get("storage_path_widget") or "").lstrip("/")
                        try:
                            png_bytes = fetch_widget_image_bytes(KDH_BUCKET, img_key)
                            title   = row.get("widget_title") or "Untitled"
                            fname   = (row.get("storage_path_widget") or "").split("/")[-1]
                            c.image(png_bytes, caption=f"{title} · {fname}", use_container_width=True)
                        except Exception:
                            c.warning("Image not available")
                        meta_rows = []
                        for key in ["widget_id","widget_type","quality","quality_score","url",
                                    "storage_path_widget","captured_at","session_key"]:
                            if key in dfw.columns and row.get(key) not in [None, "", []]:
                                meta_rows.append((key.replace("_"," ").title(), str(row.get(key))))
                        if meta_rows:
                            c.dataframe(pd.DataFrame(meta_rows, columns=["Field","Value"]),
                                        use_container_width=True, hide_index=True)
                    idx += 1

    c1, c2, _ = st.columns([1,1,5])
    save_json_files = c2.toggle(
        "Save JSON to Storage",
        value=True,
        help=f"Writes audit artifacts to `{JSONS_ROOT}/{{session}}/…json`."
    )
    do_parse = c1.button(
        f"Parse {len(selected_labels) if selected_labels else 0} Selected",
        type="primary",
        use_container_width=True,
        disabled=not selected_labels
    )

    if do_parse:
        results = []
        progress = st.progress(0)
        for i, lab in enumerate(selected_labels, start=1):
            row = dfw[dfw["__label__"] == lab].iloc[0].to_dict()
            widget_id = row.get("widget_id")
            if not widget_id:
                st.error(f"'{lab}' has no widget_id — cannot insert into fact table.")
                progress.progress(min(i/len(selected_labels), 1.0))
                continue

            img_path = (row.get("storage_path_widget") or "").lstrip("/")
            image_name = img_path.split("/")[-1] if img_path else f"widget_{i}.png"

            try:
                png_bytes = fetch_widget_image_bytes(KDH_BUCKET, img_path)
            except Exception as e:
                st.error(f"Download failed for {img_path}: {e}")
                progress.progress(min(i/len(selected_labels), 1.0))
                continue

            messages = build_llm_messages_from_bytes(png_bytes)
            if show_debug:
                with debug_expander(f"🔎 Debug: LLM request for {image_name}", key=f"dbg_req_{i}"):
                    st.write({"image_name": image_name, "image_size_bytes": len(png_bytes)})
                    st.code(GRAPH_PROMPT, language="text")

            raw_text = None
            for attempt in range(3):
                try:
                    raw_text = call_mistral(messages)
                    break
                except Exception as e:
                    if attempt == 2:
                        st.error(f"LLM extraction failed for {image_name}: {e}")
                    else:
                        time.sleep(1.5)
            if raw_text is None:
                progress.progress(min(i/len(selected_labels), 1.0))
                continue

            if show_debug:
                with debug_expander(f"🔎 Debug: LLM raw response for {image_name}", key=f"dbg_raw_{i}"):
                    st.code(raw_text or "<empty>", language="json")

            try:
                values = parse_llm_json(raw_text)
            except Exception as e:
                st.error(f"LLM returned non-JSON for {image_name}: {e}")
                progress.progress(min(i/len(selected_labels), 1.0))
                continue

            if show_debug:
                with debug_expander(f"🔎 Debug: Parsed JSON for {image_name}", key=f"dbg_parsed_{i}"):
                    st.json(values)

            session_key = row.get("session_key") or extract_session_from_path(row.get("storage_path_widget")) or "session"

            json_key = None
            if save_json_files:
                json_key = json_storage_key(session_key, image_name)
                data = json.dumps(values).encode("utf-8")
                upload_json_to_storage(KDH_BUCKET, json_key, data)

            base_payload = {
                "extraction_id": str(uuid.uuid4()),
                "widget_id": widget_id,
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

            payload = base_payload if not XF_COLS else {k: v for k, v in base_payload.items() if (k in XF_COLS) or (k == "values")}

            if show_debug:
                with debug_expander(f"🔎 Debug: DB payload for {image_name}", key=f"dbg_dbp_{i}"):
                    st.json(payload)

            try:
                ins = sb.table(TBL_XFACT).insert(payload).execute()
                if show_debug:
                    with debug_expander(f"🔎 Debug: DB insert result for {image_name}", key=f"dbg_dbres_{i}"):
                        st.json(ins.data)
                results.append({"widget_id": widget_id, "status": "ok", "json_path": json_key, "db_row": ins.data})
            except Exception as e:
                st.error(f"Insert failed for widget {widget_id}: {e}")
                results.append({"widget_id": widget_id, "status": "failed", "error": str(e)})

            progress.progress(min(i/len(selected_labels), 1.0))

        st.success(f"Parsed {sum(1 for r in results if r['status']=='ok')} / {len(results)} widgets.")
        with st.expander("Run details", expanded=False):
            st.json(results)

        # IMPORTANT: clear caches so Map/Compare can see fresh extracts
        load_recent_extracts.clear()

else:
    st.info("Choose a session to list its widgets.")

# =============================================================================
# Gate: only proceed to Map if there is at least one parsed widget in the system
# (We don't block rendering entirely, but we give a clear hint.)
# =============================================================================
_any_extract = bool(load_recent_extracts())
if not _any_extract:
    st.warning("No parsed widgets yet. Complete **① Parse** to proceed to **② Map** and **③ Compare**.")

# =============================================================================
# ② MAP (Human-in-the-Loop) — only show widgets that ALREADY HAVE parsed JSON
#     - No reruns while typing (form)
#     - "Persist mapping" writes SCD-2 rows
#     - "Fetch mapping from database" pulls SCD-2 current rows
# =============================================================================
st.header("② Map (only parsed widgets) & Save (SCD-2)")

st.caption(
    "Pick two sessions. Only widgets with a parsed JSON appear here. "
    "Type the same **Pair #** on both sides, then click **Persist mapping**. "
    "You can also **Fetch mapping from database** to see what is already saved."
)

# ---------- helpers (define if missing) ---------------------------------------
if "widget_is_parsed" not in globals():
    def widget_is_parsed(widget_id: str) -> tuple[bool, Optional[Dict]]:
        """Returns (True,row) if latest extract exists and has JSON payload, else (False,None)."""
        row = latest_extract_for_widget(widget_id)
        if not row:
            return (False, None)
        payload = _pick_json_payload(row)
        return (bool(payload), row)

if "only_parsed_widgets" not in globals():
    def only_parsed_widgets(widgets: List[Dict]) -> List[Dict]:
        """Filter widgets list to those that have a parsed JSON available."""
        out: List[Dict] = []
        for w in widgets:
            ok, _ = widget_is_parsed(w.get("widget_id"))
            if ok:
                out.append(w)
        return out

# ---------- session pickers ---------------------------------------------------
mcol1, mcol2 = st.columns(2)
left_session  = mcol1.selectbox(
    "Left session",
    options=["— choose —"] + sessions,
    index=0,
    key="map_left_sess",
)
right_session = mcol2.selectbox(
    "Right session",
    options=["— choose —"] + sessions,
    index=0,
    key="map_right_sess",
)

# ---------- stable scratch dicts (form-local state) ---------------------------
if "map_scratch_left" not in st.session_state:
    st.session_state.map_scratch_left = {}
if "map_scratch_right" not in st.session_state:
    st.session_state.map_scratch_right = {}

def _reset_scratch_for_visible(left_ids: List[str], right_ids: List[str]):
    """
    Keep only keys that are visible in the current left/right session selections.
    This keeps the form stable when user changes sessions.
    """
    L = st.session_state.map_scratch_left
    R = st.session_state.map_scratch_right
    st.session_state.map_scratch_left  = {k: v for k, v in L.items() if k in left_ids}
    st.session_state.map_scratch_right = {k: v for k, v in R.items() if k in right_ids}

# ---------- load widgets for chosen sessions (parsed only) --------------------
left_widgets_all:  List[Dict] = []
right_widgets_all: List[Dict] = []
if left_session and left_session != "— choose —":
    left_widgets_all  = load_widgets_for_session(left_session)
if right_session and right_session != "— choose —":
    right_widgets_all = load_widgets_for_session(right_session)

left_widgets  = only_parsed_widgets(left_widgets_all)
right_widgets = only_parsed_widgets(right_widgets_all)

left_ids  = [w["widget_id"] for w in left_widgets]
right_ids = [w["widget_id"] for w in right_widgets]
_reset_scratch_for_visible(left_ids, right_ids)

# ---------- top action buttons ------------------------------------------------
bcol1, bcol2, _ = st.columns([0.25, 0.25, 1])
fetch_clicked = bcol1.button("Fetch mapping from database", use_container_width=True)

# Grid placeholder; we only show grids after Fetch/Persist (no flicker while typing)
_grid_placeholder = st.empty()

# ---------- fetch mappings & show grid ---------------------------------------
if fetch_clicked:
    db_pairs = load_current_pairs() or []
    if not db_pairs:
        _grid_placeholder.info("No current mappings in SCD-2 table.")
    else:
        # Optionally back-fill the scratch values to match DB (if those widgets are visible here)
        for row in db_pairs:
            pn = row.get("pair_number")
            if pn is None:
                continue
            pn = int(pn)
            l_id, r_id = row.get("widget_id_left"), row.get("widget_id_right")
            if l_id in left_ids:
                st.session_state.map_scratch_left[l_id] = pn
            if r_id in right_ids:
                st.session_state.map_scratch_right[r_id] = pn

        df_rows = []
        for r in db_pairs:
            df_rows.append({
                "Pair #": r.get("pair_number"),
                "Left ID": r.get("widget_id_left"),
                "Right ID": r.get("widget_id_right"),
                "Left Session": r.get("left_session_id"),
                "Right Session": r.get("right_session_id"),
                "Status": r.get("status"),
                "Current?": "✅" if r.get("curr_rec_ind") else "—",
            })
        _grid_placeholder.dataframe(pd.DataFrame(df_rows), use_container_width=True)

# ---------- pairing UI in a form (NO reruns while typing) --------------------
st.markdown("#### Pair the widgets (type numbers; no refresh until you click **Persist mapping**)")

def _render_pair_column(title: str, widgets: List[Dict], scratch_key: str, bg_hex: str):
    """
    Render 2-up cards with image + number inputs.
    Values are buffered in st.session_state[scratch_key] and do NOT trigger reruns (inside form).
    """
    st.markdown(
        f'<div style="padding:10px 12px;border-radius:12px;border:1px solid #eee;background:{bg_hex};margin-bottom:8px;font-weight:700">{title}</div>',
        unsafe_allow_html=True,
    )
    if not widgets:
        st.info("No parsed widgets in this session yet.")
        return

    for i in range(0, len(widgets), 2):
        rc = st.columns(2)
        for j in range(2):
            if i + j >= len(widgets):
                continue
            w = widgets[i + j]
            with rc[j].container(border=True):
                img_key = (w.get("storage_path_widget") or "").lstrip("/")
                try:
                    png_bytes = fetch_widget_image_bytes(KDH_BUCKET, img_key)
                    title = w.get("widget_title") or "Untitled"
                    fname = (w.get("storage_path_widget") or "").split("/")[-1]
                    st.image(png_bytes, caption=f"{title} · {fname}", use_container_width=True)
                except Exception:
                    st.warning("Image not available")

                st.caption(f"`{w.get('widget_id')}` (parsed ✅)")
                current_val = int(st.session_state[scratch_key].get(w["widget_id"], 0) or 0)
                # Important: keys must be unique and stable inside the form
                val = st.number_input(
                    f"Pair # — {w['widget_id']}",
                    key=f"pair_{scratch_key}_{w['widget_id']}",
                    min_value=0,
                    step=1,
                    value=current_val,
                    help="Use the same number on left & right to link. 0 = unpaired.",
                )
                st.session_state[scratch_key][w["widget_id"]] = int(val)

# Use a form to buffer inputs until the user clicks "Persist mapping"
with st.form("map_form", clear_on_submit=False):
    mc1, mc2 = st.columns(2)
    with mc1:
        _render_pair_column("Left widgets (parsed only)", left_widgets, "map_scratch_left", "#f2f7ff")
    with mc2:
        _render_pair_column("Right widgets (parsed only)", right_widgets, "map_scratch_right", "#fff2e8")

    persist_clicked = st.form_submit_button("💾 Persist mapping (SCD-2)", type="primary", use_container_width=True)

# ---------- persist logic (runs ONCE after form submit) -----------------------
if persist_clicked:
    # Build {pair_no: {"left":[ids], "right":[ids]}}
    pairs_scratch: Dict[str, Dict[str, List[str]]] = {}

    def _add(side: str, wid: str, num: int):
        if num and int(num) > 0:
            k = str(int(num))
            pairs_scratch.setdefault(k, {"left": [], "right": []})
            pairs_scratch[k][side].append(wid)

    for wid, num in st.session_state.map_scratch_left.items():
        _add("left", wid, num)
    for wid, num in st.session_state.map_scratch_right.items():
        _add("right", wid, num)

    save_rows = []
    issues = []
    for k in sorted(pairs_scratch, key=lambda x: int(x)):
        L, R = pairs_scratch[k]["left"], pairs_scratch[k]["right"]
        if len(L) == 1 and len(R) == 1:
            res = scd2_upsert_pair(
                widget_left=L[0],
                widget_right=R[0],
                left_sess=left_session if left_session != "— choose —" else None,
                right_sess=right_session if right_session != "— choose —" else None,
                pair_number=int(k),
            )
            save_rows.append({"pair_number": int(k), "left": L[0], "right": R[0], **res})
        else:
            issues.append({"pair_number": int(k), "left_ids": L, "right_ids": R, "status": "⚠ check (expect 1:1)"})

    if save_rows:
        st.success(f"Saved {len(save_rows)} mapping(s).")
        df_saved = pd.DataFrame(save_rows).sort_values("pair_number")
        _grid_placeholder.dataframe(df_saved, use_container_width=True)
        load_current_pairs.clear()

    if issues:
        st.warning("Some pairs were not 1:1 and were not persisted.")
        st.dataframe(pd.DataFrame(issues).sort_values("pair_number"), use_container_width=True)

# Note: We do NOT render a “preview while typing” grid anymore.
#       The preview grids appear ONLY after “Fetch mapping from database”
#       or after “Persist mapping” (saved results), which keeps UI stable
#       and avoids flicker.


# =============================================================================
# ③ COMPARE — only on mapped pairs (SCD-2 current rows)
# =============================================================================
st.header("③ Compare by Pair (SCD-2)")

curr_pairs = load_current_pairs()
if not curr_pairs:
    st.info("No current pairs in SCD-2 table. Save mappings above in **② Map**.")
else:
    wid_titles = widget_titles([r["widget_id_left"] for r in curr_pairs] + [r["widget_id_right"] for r in curr_pairs])

    # Keep only pairs whose both sides still have parsed JSON (should be true by construction, but guard anyway)
    ready_pairs = []
    for r in curr_pairs:
        l_ok, _ = widget_is_parsed(r["widget_id_left"])
        r_ok, _ = widget_is_parsed(r["widget_id_right"])
        if l_ok and r_ok:
            ready_pairs.append(r)

    if not ready_pairs:
        st.warning("All current pairs are missing parses on one/both sides. Re-parse in **①** or re-map in **②**.")
    else:
        def label_for_pair(r: Dict) -> str:
            l_id, r_id = r.get("widget_id_left"), r.get("widget_id_right")
            return f"{r.get('pair_number') or '—'} • {wid_titles.get(l_id,'')} ({l_id[:8]}) ⟂ {wid_titles.get(r_id,'')} ({r_id[:8]}) • {r.get('left_session_id','?')} ↔ {r.get('right_session_id','?')}"

        options = {label_for_pair(r): r for r in ready_pairs}
        pick_label = st.selectbox("Pick a mapped pair (both sides parsed)", options=list(options.keys()))
        chosen = options[pick_label]

        # show the two images
        cc1, cc2 = st.columns(2)
        with cc1:
            try:
                l_img_path = (sb.table(TBL_WIDGETS).select("storage_path_crop").eq("widget_id", chosen["widget_id_left"]).limit(1).execute().data or [{}])[0].get("storage_path_crop","")
                l_bytes = fetch_widget_image_bytes(KDH_BUCKET, l_img_path.lstrip("/"))
                st.image(l_bytes, caption=f"Left · {wid_titles.get(chosen['widget_id_left'],'')} ({chosen['widget_id_left']})", use_container_width=True)
            except Exception:
                st.warning("Left image not available")
        with cc2:
            try:
                r_img_path = (sb.table(TBL_WIDGETS).select("storage_path_crop").eq("widget_id", chosen["widget_id_right"]).limit(1).execute().data or [{}])[0].get("storage_path_crop","")
                r_bytes = fetch_widget_image_bytes(KDH_BUCKET, r_img_path.lstrip("/"))
                st.image(r_bytes, caption=f"Right · {wid_titles.get(chosen['widget_id_right'],'')} ({chosen['widget_id_right']})", use_container_width=True)
            except Exception:
                st.warning("Right image not available")

        # Compare button (LLM-only)
        left_ok, _  = widget_is_parsed(chosen["widget_id_left"])
        right_ok, _ = widget_is_parsed(chosen["widget_id_right"])
        run_cmp = st.button("Run Compare for this Pair (LLM)", type="primary", disabled=not (left_ok and right_ok))

        if not (left_ok and right_ok):
            st.info("This pair includes an unparsed widget. Parse both in **①**.")

        if run_cmp:
            # Use the LLM-only comparator; it will:
            # - load the latest extracts for both widgets
            # - call the LLM to compare VALUES ONLY
            # - persist SCD-2 row into kdh_compare_fact
            # - return {"result": {...}, "db_row": {...}}
            out = cmp_llm.compare_pair_by_row(chosen)

            if "error" in out:
                st.error(out["error"])
            else:
                res = out["result"]   # {verdict: Matched|NotMatched, confidence, why[], numbers_used{...}}
                db  = out["db_row"]   # persisted SCD-2 row

                # Verdict banner
                v = res.get("verdict", "NotMatched")
                conf = res.get("confidence", 0.0)
                badge = "✅ Matched" if v == "Matched" else "❌ Not Matched"
                st.subheader("Verdict (LLM)")
                st.write(f"{badge}  ·  confidence={conf:.2f}")

                # Short reasons
                why = res.get("why") or []
                if why:
                    st.caption(" · ".join(why))

                # Audit: what numbers the LLM actually compared (left/right normalized)
                with st.expander("Numbers used (LLM-normalized)", expanded=False):
                    st.json(res.get("numbers_used", {}))

                # DB row persisted (SCD-2)
                if show_debug:
                    with debug_expander("🔎 Debug: Compare SCD-2 row", key="dbg_cmp_db_row"):
                        st.json(db)
