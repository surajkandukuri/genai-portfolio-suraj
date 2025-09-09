# provisioning/kdh_widget_value_extractor.py
from __future__ import annotations

import os, io, json, base64, re, uuid
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple

from dotenv import load_dotenv
from supabase import create_client, Client
from mistralai import Mistral

# ─────────────────────────────────────────────────────────────────────────────
# Env & clients
# ─────────────────────────────────────────────────────────────────────────────
load_dotenv()

def _sget(*keys, default=None):
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
KDH_BUCKET    = _sget("KDH_BUCKET", default="kpidrifthunter")

# storage “roots” used by your other pages
WIDGETS_ROOT  = _sget("KDH_FOLDER_ROOT", default="widgetextractor")   # where images live: widgetextractor/{session}/widgets/...
JSONS_ROOT    = "jsons_from_wigetsimages"  # per your requested spelling

# tables
TBL_WIDGETS   = _sget("KDH_TABLE_WIDGETS", default="kdh_widget_dim")
TBL_XFACT     = _sget("KDH_TABLE_WIDGET_EXTRACT", default="kdh_widget_extract_fact")  # new fact table (see DDL below)

MISTRAL_API_KEY = _sget("MISTRAL_API_KEY")
MISTRAL_MODEL   = _sget("MISTRAL_MODEL", default="pixtral-12b-2409")

if not SUPABASE_URL or not SUPABASE_KEY:
    raise RuntimeError("Missing Supabase config (SUPABASE_URL / SUPABASE_SERVICE_ROLE_KEY).")

sb: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────
def _nowstamp_z() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")

def _sanitize_filename(s: str, max_len=160) -> str:
    s = (s or "").strip()
    s = re.sub(r"[^\w\-. ]+", "_", s)
    s = re.sub(r"\s+", "_", s)
    return (s[:max_len] or "untitled").rstrip("._-")

def _b64_from_bytes(b: bytes) -> str:
    return base64.b64encode(b).decode("utf-8")

def _storage_upload_bytes(bucket: str, key: str, data: bytes, content_type="application/json") -> Dict[str,str]:
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

def _storage_download_bytes(bucket: str, key: str) -> bytes:
    key = key.lstrip("/")
    # supabase-py v2: .download returns bytes
    return sb.storage.from_(bucket).download(key)

# ─────────────────────────────────────────────────────────────────────────────
# LLM extraction (combined from your graph_extraction.py)
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

    encoded = _b64_from_bytes(png_bytes)
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
        response_format={"type": "json_object"}
    )
    raw = resp.choices[0].message.content
    try:
        return json.loads(raw)
    except Exception:
        # if model wrapped JSON in text, try to strip
        start = raw.find("{"); end = raw.rfind("}")
        return json.loads(raw[start:end+1])

# ─────────────────────────────────────────────────────────────────────────────
# Core utility
# ─────────────────────────────────────────────────────────────────────────────
def list_widget_rows_for_session(session_folder: str) -> List[Dict]:
    """
    Pull the widget rows we created during extraction for this session.
    We only process images ending with '_good.png'.
    """
    q = (
        sb.table(TBL_WIDGETS)
          .select("*")
          .eq("session_folder", session_folder)
          .ilike("storage_path_widget", "%_good.png")
          .order("widget_index", desc=False)
          .execute()
    )
    return q.data or []

def storage_key_for_json(session_folder: str, image_name: str) -> str:
    """
    kpidrifthunter/jsons_from_wigetsimages/{session}/{image_name}_{timestamp}.json
    NOTE: Storage uses '/' separators; keep Windows backslashes out of keys.
    """
    ts = _nowstamp_z()
    base = _sanitize_filename(image_name.rsplit(".", 1)[0])
    return f"{JSONS_ROOT}/{session_folder}/{base}_{ts}.json"

def process_session(session_folder: str, limit: Optional[int] = None) -> Dict:
    """
    For a given session, fetch widget images from DB, download each from Storage,
    extract values, save JSON file to Storage, and insert a fact row linking it all.
    Returns a manifest of processed items.
    """
    rows = list_widget_rows_for_session(session_folder)
    if limit is not None:
        rows = rows[:int(limit)]

    processed = []
    for r in rows:
        widget_id = r.get("widget_id")
        url       = r.get("url")
        img_path  = (r.get("storage_path_widget") or "").lstrip("/")  # e.g., widgetextractor/<session>/widgets/....
        image_name = img_path.split("/")[-1] or f"widget_{r.get('widget_index','')}"

        # Download the PNG bytes
        png_bytes = _storage_download_bytes(KDH_BUCKET, img_path)

        # Run extraction
        values = extract_graph_json_from_png_bytes(png_bytes)

        # Save JSON to Storage (audit-friendly)
        json_key = storage_key_for_json(session_folder, image_name)
        _ = _storage_upload_bytes(KDH_BUCKET, json_key, json.dumps(values).encode("utf-8"))

        # Insert log row in the fact table
        payload = {
            "extraction_id": str(uuid.uuid4()),
            "widget_id": widget_id,
            "url": url,
            "screengrab_id": r.get("screengrab_id"),   # may be NULL depending on your pbi extractor
            "session_folder": session_folder,
            "image_storage_path": img_path,
            "json_storage_path": json_key,
            "values": values,                           # JSONB column
            "created_at": datetime.now(timezone.utc).isoformat()
        }
        res = sb.table(TBL_XFACT).insert(payload).execute()

        processed.append({
            "widget_id": widget_id,
            "image": img_path,
            "json": json_key,
            "insert_ok": bool(getattr(res, "data", None)),
        })

    return {
        "session_folder": session_folder,
        "count": len(processed),
        "items": processed
    }

# ─────────────────────────────────────────────────────────────────────────────
# CLI entry
# ─────────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser(description="Extract values from widget images and persist JSON + fact rows.")
    ap.add_argument("--session", required=True, help="Session folder used by widget extractor (e.g., powerbi_20250909T120000Z)")
    ap.add_argument("--limit", type=int, default=None, help="Process at most N widgets")
    args = ap.parse_args()

    out = process_session(args.session, limit=args.limit)
    print(json.dumps(out, indent=2))
