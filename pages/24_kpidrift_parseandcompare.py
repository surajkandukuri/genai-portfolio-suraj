# pages/24_kpidrift_parseandcompare.py
from __future__ import annotations

import os, re, json, uuid, base64, time, math
from datetime import datetime, timezone
from typing import Dict, List, Optional, Set

import pandas as pd
import streamlit as st
from supabase import create_client, Client
from postgrest.exceptions import APIError
from mistralai import Mistral

# ─────────────────────────────────────────────────────────────────────────────
# Config / Secrets (kept as-is)
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
    """Infer available columns by fetching 1 row. If table is empty, returns empty set."""
    try:
        res = sb.table(table).select("*").limit(1).execute()
        rows = res.data or []
        return set(rows[0].keys()) if rows else set()
    except Exception:
        return set()

@st.cache_data(ttl=300, show_spinner=False)
def fetch_widget_image_bytes(bucket: str, key: str) -> bytes:
    """
    Download the stored widget image from Supabase Storage and return raw bytes.
    Works with private buckets. Handles multiple SDK return shapes.
    """
    key = (key or "").lstrip("/")
    resp = sb.storage.from_(bucket).download(key)

    # Already bytes?
    if isinstance(resp, (bytes, bytearray)):
        return bytes(resp)

    # Response-like with .content
    content = getattr(resp, "content", None)
    if content is not None:
        return bytes(content)

    # file-like with .read()
    if hasattr(resp, "read"):
        return resp.read()

    # dict-ish shapes: {'data': b'...'} or {'data': <file-like>}
    if isinstance(resp, dict):
        if "data" in resp and isinstance(resp["data"], (bytes, bytearray)):
            return bytes(resp["data"])
        if "data" in resp and hasattr(resp["data"], "read"):
            return resp["data"].read()

    raise RuntimeError(f"Could not decode bytes for storage object: {bucket}/{key}")

def json_storage_key(session_id: str, image_name: str) -> str:
    base = _sanitize(image_name.rsplit(".", 1)[0])
    ts = _nowstamp_z()
    return f"{JSONS_ROOT}/{session_id}/{base}_{ts}.json"

def extract_session_from_path(path: str) -> Optional[str]:
    """Parse session ID from storage path: widgetextractor/<session>/widgets/<file>"""
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
XF_COLS     = _get_columns(TBL_XFACT)  # may be empty if table has no rows yet

DEFAULT_WIDGET_ORDER_COL = (
    "insrt_dttm" if "insrt_dttm" in WIDGET_COLS else
    ("rec_eff_strt_dt" if "rec_eff_strt_dt" in WIDGET_COLS else "widget_id")
)

@st.cache_data(ttl=120)
def load_recent_sessions(limit=500) -> List[str]:
    """Derive sessions from widget storage paths (works regardless of DB columns)."""
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
    """Load widgets where storage_path_crop contains /<session_id>/; map optional screengrab metadata."""
    # 1) widgets
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

    # 2) screengrab metadata (optional)
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

    # 3) normalize
    for r in widgets:
        r["storage_path_widget"] = r.get("storage_path_crop")
        sgid = r.get("screengrab_id")
        r["url"]         = url_by_sg.get(sgid)
        r["captured_at"] = cap_by_sg.get(sgid)
        r["session_key"] = sess_by_sg.get(sgid) or extract_session_from_path(r.get("storage_path_crop"))
        r["report_name"] = rn_by_sg.get(sgid)
        r["public_url"]  = ""  # we don't fetch from online for preview
    return widgets

@st.cache_data(ttl=120)
def load_recent_extracts(limit=300) -> List[Dict]:
    q = sb.table(TBL_XFACT).select("*").order("created_at", desc=True).limit(limit).execute()
    return q.data or []

# ─────────────────────────────────────────────────────────────────────────────
# LLM (Mistral) — image bytes → JSON + DEBUG
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
        {
            "role": "user",
            "content": [
                {"type": "text", "text": "Extract data from this chart image."},
                {"type": "image_url", "image_url": f"data:image/png;base64,{encoded}"}
            ]
        }
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

# ─────────────────────────────────────────────────────────────────────────────
# Compare math
# ─────────────────────────────────────────────────────────────────────────────
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
# UI
# ─────────────────────────────────────────────────────────────────────────────
st.set_page_config(page_title="KPI Drift — Parse & Compare", page_icon="📊", layout="wide")
st.title("📊 KPI Drift — Parse & Compare")
st.caption("Schema-aware: detects actual columns. Choose a session, then **pick the exact widget(s)** to parse.")

# Debug toggle
show_debug = st.toggle("Show debug details", value=False, help="Show LLM request/response and DB payloads")

# =============================================================================
# ① PARSE
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

    # Display-friendly label
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
        help="Pick one or many widgets from this session to send to the parser."
    )

    # ── Dynamic preview:
    #    - If 1 selected: show a single large preview + metadata (no gallery)
    #    - If >1 selected: show N cards (each widget) with image + its own metadata; choose cards/row with slider
    if selected_labels:
        sel_df = dfw.set_index("__label__").loc[selected_labels].reset_index()
        sel_count = len(selected_labels)

        if sel_count == 1:
            # Single card: big image + metadata
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
                meta_df = pd.DataFrame(meta_rows, columns=["Field","Value"])
                cmeta.dataframe(meta_df, use_container_width=True, hide_index=True)

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
                        # Image
                        img_key = (row.get("storage_path_widget") or "").lstrip("/")
                        try:
                            png_bytes = fetch_widget_image_bytes(KDH_BUCKET, img_key)
                            title   = row.get("widget_title") or "Untitled"
                            fname   = (row.get("storage_path_widget") or "").split("/")[-1]
                            c.image(png_bytes, caption=f"{title} · {fname}", use_container_width=True)
                        except Exception:
                            c.warning("Image not available")

                        # Metadata (directly below the image in the same card)
                        meta_rows = []
                        for key in ["widget_id","widget_type","quality","quality_score","url",
                                    "storage_path_widget","captured_at","session_key"]:
                            if key in dfw.columns and row.get(key) not in [None, "", []]:
                                meta_rows.append((key.replace("_"," ").title(), str(row.get(key))))
                        if meta_rows:
                            meta_df = pd.DataFrame(meta_rows, columns=["Field","Value"])
                            c.dataframe(meta_df, use_container_width=True, hide_index=True)
                    idx += 1

    # Controls
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
                st.error(f"Selected item '{lab}' has no widget_id — cannot insert into fact table.")
                progress.progress(min(i/len(selected_labels), 1.0))
                continue

            img_path = (row.get("storage_path_widget") or "").lstrip("/")
            image_name = img_path.split("/")[-1] if img_path else f"widget_{i}.png"

            # Download image bytes ONCE and reuse for LLM
            try:
                png_bytes = fetch_widget_image_bytes(KDH_BUCKET, img_path)
            except Exception as e:
                st.error(f"Download failed for {img_path}: {e}")
                progress.progress(min(i/len(selected_labels), 1.0))
                continue

            # Build LLM request (and debug)
            messages = build_llm_messages_from_bytes(png_bytes)
            if show_debug:
                st.subheader(f"🔍 Debug: LLM request for {image_name}")
                st.write({"image_name": image_name, "image_size_bytes": len(png_bytes)})
                st.code(GRAPH_PROMPT[:800] + ("..." if len(GRAPH_PROMPT) > 800 else ""), language="text")

            # Call LLM with light retry
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

            # Parse LLM JSON (and debug)
            if show_debug:
                st.subheader(f"🔍 Debug: LLM raw response for {image_name}")
                st.code((raw_text[:1500] + ("..." if len(raw_text) > 1500 else "")) or "<empty>", language="json")

            try:
                values = parse_llm_json(raw_text)
            except Exception as e:
                st.error(f"LLM returned non-JSON for {image_name}: {e}")
                if show_debug:
                    st.exception(e)
                progress.progress(min(i/len(selected_labels), 1.0))
                continue

            if show_debug:
                st.subheader(f"🔍 Debug: Parsed JSON for {image_name}")
                st.json(values)

            session_key = row.get("session_key") or extract_session_from_path(row.get("storage_path_widget")) or "session"

            # Optional JSON artifact
            json_key = None
            if save_json_files:
                json_key = json_storage_key(session_key, image_name)
                data = json.dumps(values).encode("utf-8")
                sb.storage.from_(KDH_BUCKET).upload(
                    path=json_key, file=data,
                    file_options={"content_type": "application/json", "upsert": "true"}
                )

            # Build DB payload — schema-aware
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

            # If fact table is empty (XF_COLS = set()), don't filter keys.
            if XF_COLS:
                payload = {k: v for k, v in base_payload.items() if (k in XF_COLS) or (k == "values")}
            else:
                payload = base_payload.copy()

            if show_debug:
                st.subheader(f"🔍 Debug: DB payload for {image_name}")
                st.json(payload)

            # Insert
            try:
                ins = sb.table(TBL_XFACT).insert(payload).execute()
                if show_debug:
                    st.subheader(f"🔍 Debug: DB insert result for {image_name}")
                    st.json(ins.data)
                results.append({"widget_id": widget_id, "status": "ok", "json_path": json_key, "db_row": ins.data})
            except APIError as e:
                st.error(f"Insert failed for widget {widget_id}: {e}")
                if show_debug:
                    st.exception(e)
                results.append({"widget_id": widget_id, "status": "failed", "error": str(e)})
            except Exception as e:
                st.error(f"Insert failed for widget {widget_id}: {e}")
                if show_debug:
                    st.exception(e)
                results.append({"widget_id": widget_id, "status": "failed", "error": str(e)})

            progress.progress(min(i/len(selected_labels), 1.0))

        st.success(f"Parsed {sum(1 for r in results if r['status']=='ok')} / {len(results)} widgets.")
        with st.expander("Run details", expanded=False):
            st.json(results)

else:
    st.info("Choose a session to list its widgets.")

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
