# provisioning/2_kpidrift_capture/2_kpidrift_persist.py
import uuid, datetime as dt, hashlib
from io import BytesIO
from urllib.parse import urlparse
from typing import List, Dict, Tuple, Optional

from PIL import Image  # pip install pillow

def sha256_hex(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()  # 64-char lowercase hex

def image_wh(png_bytes: bytes) -> Tuple[int, int]:
    with Image.open(BytesIO(png_bytes)) as im:
        return int(im.width), int(im.height)

def url_host(url: str) -> Optional[str]:
    try:
        return urlparse(url).hostname
    except Exception:
        return None

def upsert_screengrab(
    sb, *,
    session_id: str,
    url: str,
    platform: str,
    full_png_bytes: bytes,
    storage_bucket: str,
    storage_path_full: str,
    user_id: Optional[str] = None,
) -> Dict:
    """
    Insert or fetch screengrab by content hash (kdh_screengrab_dim).
    Matches your table's constraints: char(64) hash, not-null fields, etc.
    """
    now = dt.datetime.utcnow()
    row = {
        "screengrab_id": str(uuid.uuid4()),
        "capture_session_id": session_id,
        "url": url,
        "platform": platform or "unknown",
        "detected_via": "url",
        "platform_confidence": 0.990 if platform in ("powerbi", "tableau") else 0.500,
        "screengrab_hashvalue": sha256_hex(full_png_bytes),
        "storage_bucket": storage_bucket,
        "storage_path_full": storage_path_full,
        "wrapper_host": url_host(url),
        "user_id": user_id,
        "captured_at": now.isoformat(),
        "rec_eff_strt_dt": now.isoformat(),
        "curr_rec_ind": True,
    }
    try:
        res = sb.table("kdh_screengrab_dim").insert(row).execute()
        return res.data[0]
    except Exception:
        # idempotency via unique(hash): fetch existing
        res = (sb.table("kdh_screengrab_dim")
               .select("*")
               .eq("screengrab_hashvalue", row["screengrab_hashvalue"])
               .limit(1).execute())
        return res.data[0] if res.data else row

def insert_widgets(
    sb,
    *,
    screengrab_id: str,
    storage_bucket: str,
    crops: List[Dict],   # each: {"path": ".../widgets/w_0.png", "bytes": b"...", "bbox": [x,y,w,h] | None}
) -> None:
    """
    Insert widget crops (kdh_widget_dim). Ensures bbox_xywh meets CHECK (w>0,h>0).
    """
    now = dt.datetime.utcnow().isoformat()
    rows = []

    for c in crops:
        if c.get("bbox"):
            x, y, w, h = c["bbox"]
        else:
            w, h = image_wh(c["bytes"])
            x, y = 0, 0
        rows.append({
            "widget_id": str(uuid.uuid4()),
            "screengrab_id": screengrab_id,
            "bbox_xywh": [int(x), int(y), int(w), int(h)],
            "storage_bucket": storage_bucket,
            "storage_path_crop": c["path"],
            "extraction_stage": "captured",
            "extraction_notes": None,
            "widget_type": None,
            "widget_title": None,
            "unit": None,
            "agg": "unknown",
            "ocr_confidence": None,
            "classification_confidence": None,
            "parsed_to_fact": False,
            "insrt_dttm": now,
            "rec_eff_strt_dt": now,
            "curr_rec_ind": True,
        })

    if rows:
        sb.table("kdh_widget_dim").insert(rows).execute()
