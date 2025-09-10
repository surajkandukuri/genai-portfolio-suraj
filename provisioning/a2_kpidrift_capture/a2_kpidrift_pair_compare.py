# provisioning/a2_kpidriftcapture/a2_kpidrift_pair_compare.py
from __future__ import annotations

import json
import os
import re
import uuid
from datetime import datetime, timezone
from typing import Dict, Optional, Any, Tuple

from postgrest.exceptions import APIError
from mistralai import Mistral


# ----------------------------- Utilities -------------------------------------

def _sget(secrets: dict, *keys, default=None):
    for k in keys:
        if secrets and k in secrets:
            return secrets[k]
        v = os.getenv(k)
        if v:
            return v
    return default

def _now_iso_z() -> str:
    return datetime.now(timezone.utc).isoformat()

def _safe_json_loads(s: str) -> dict:
    try:
        return json.loads(s)
    except Exception:
        # ultra-robust: slice first balanced JSON block
        start = s.find("{")
        end = s.rfind("}")
        if start >= 0 and end >= 0 and end > start:
            return json.loads(s[start:end+1])
        raise


# ----------------------------- LLM Comparator --------------------------------

LLM_COMPARE_SYSTEM = (
    "You are a KPI comparison assistant. Two widgets are ALREADY paired, meaning they "
    "represent the SAME KPI. Your job is to compare VALUES ONLY (ignore names/titles).\n"
    "Normalize numbers (commas, K/M/B/%), normalize x labels (case/spacing), align series.\n"
    "Rules:\n"
    "- If both are single KPI values, compare the scalars.\n"
    "- If both are lists/series, align on x labels and compare aligned y values.\n"
    "- Return 'Matched' ONLY if values are equal up to normal rounding; otherwise 'NotMatched'.\n"
    "- Be strict and deterministic.\n"
    "Return STRICT JSON only with keys:\n"
    "{\n"
    '  "verdict": "Matched" | "NotMatched",\n'
    '  "confidence": <float 0..1>,\n'
    '  "why": [<short reasons>],\n'
    '  "numbers_used": {\n'
    '     "left":  {"unit": "usd|%|none", "points": [{"x": "...", "y": <number>}, ...]} OR {"value": <number>, "unit": "..."} ,\n'
    '     "right": {"unit": "usd|%|none", "points": [{"x": "...", "y": <number>}, ...]} OR {"value": <number>, "unit": "..."}\n'
    "  }\n"
    "}\n"
)

LLM_COMPARE_USER_TEMPLATE = (
    "Compare VALUES ONLY. Ignore titles/names.\n"
    "Return the STRICT JSON schema requested.\n"
    "JSON A:\n{json_a}\n\n"
    "JSON B:\n{json_b}\n"
)


class PairCompareLLM:
    """
    LLM-only comparator:
      - Fetch latest extracts for each widget in a mapped pair
      - Ask LLM to normalize + compare values only
      - Persist result into kdh_compare_fact as SCD-2
    """

    def __init__(
        self,
        supabase_client,
        secrets: Optional[dict] = None,
        *,
        tbl_widgets: str = "kdh_widget_dim",
        tbl_extract: str = "kdh_widget_extract_fact",
        tbl_pairs: str   = "kdh_pair_map_dim",
        tbl_compare: str = "kdh_compare_fact",
        model: Optional[str] = None,
        api_key: Optional[str] = None,
    ):
        self.sb = supabase_client
        self.TBL_WIDGETS = tbl_widgets
        self.TBL_EXTRACT = tbl_extract
        self.TBL_PAIRS   = tbl_pairs
        self.TBL_COMPARE = tbl_compare

        self.model   = model or _sget(secrets, "MISTRAL_MODEL", default="pixtral-12b-2409")
        self.api_key = api_key or _sget(secrets, "MISTRAL_API_KEY")

        if not self.api_key:
            raise RuntimeError("MISTRAL_API_KEY is required for LLM-only compare.")

        self._extract_cols = self._discover_cols(self.TBL_EXTRACT)

    # ------------------------- discovery / loading ----------------------------

    def _discover_cols(self, table: str) -> set[str]:
        try:
            res = self.sb.table(table).select("*").limit(1).execute()
            rows = res.data or []
            return set(rows[0].keys()) if rows else set()
        except Exception:
            return set()

    def _order_col_for_extract(self) -> str:
        cols = self._extract_cols
        for c in ["created_at", "insrt_dttm", "rec_eff_strt_dt", "updated_at"]:
            if c in cols:
                return c
        # fallback (should exist in your fact)
        return "extraction_id"

    def _pick_json_payload(self, row: Dict) -> Dict:
        for key in ["values", "json_values", "payload", "extracted_values"]:
            if key in row and row[key]:
                return row[key]
        # fallback: first dict/list field
        for k, v in row.items():
            if isinstance(v, (dict, list)):
                return v
        return {}

    def load_latest_extract_for_widget(self, widget_id: str) -> Optional[Dict]:
        try:
            res = (
                self.sb.table(self.TBL_EXTRACT)
                .select("*")
                .eq("widget_id", widget_id)
                .order(self._order_col_for_extract(), desc=True)
                .limit(1)
                .execute()
            )
            rows = res.data or []
            return rows[0] if rows else None
        except Exception:
            return None

    # ----------------------------- LLM call -----------------------------------

    def _call_llm_compare(self, json_a: Dict, json_b: Dict) -> Dict:
        client = Mistral(api_key=self.api_key)
        user = LLM_COMPARE_USER_TEMPLATE.format(
            json_a=json.dumps(json_a, ensure_ascii=False),
            json_b=json.dumps(json_b, ensure_ascii=False),
        )
        resp = client.chat.complete(
            model=self.model,
            messages=[
                {"role": "system", "content": LLM_COMPARE_SYSTEM},
                {"role": "user",   "content": user},
            ],
            temperature=0.0,
            response_format={"type": "json_object"},
        )
        raw = resp.choices[0].message.content or "{}"
        out = _safe_json_loads(raw)

        # Minimal normalization
        verdict = out.get("verdict", "")
        if verdict not in ("Matched", "NotMatched"):
            out["verdict"] = "NotMatched"
        try:
            conf = float(out.get("confidence", 0.0))
            out["confidence"] = max(0.0, min(1.0, conf))
        except Exception:
            out["confidence"] = 0.0
        out.setdefault("why", [])
        out.setdefault("numbers_used", {})
        return out

    # ------------------------- SCD2 upsert compare ----------------------------

    def scd2_upsert_compare(
        self,
        *,
        pair_id: str,
        left_extraction_id: Optional[str],
        right_extraction_id: Optional[str],
        left_widget_id: Optional[str],
        right_widget_id: Optional[str],
        verdict: str,
        confidence: float,
        reasons: list,
        numbers_used: dict,
        compare_mode: str = "llm",
        model_name: Optional[str] = None,
        model_params: Optional[dict] = None,
    ) -> Dict:
        now_iso = _now_iso_z()
        model_name = model_name or self.model

        # End-date current row for same natural key (pair + left_ext + right_ext + model), then insert
        try:
            self.sb.table(self.TBL_COMPARE).update(
                {"curr_rec_ind": False, "rec_eff_end_dt": now_iso}
            ).eq("pair_id", pair_id)\
             .eq("left_extraction_id", left_extraction_id)\
             .eq("right_extraction_id", right_extraction_id)\
             .eq("model_name", model_name)\
             .eq("curr_rec_ind", True)\
             .execute()
        except APIError:
            pass

        payload = {
            "pair_id": pair_id,
            "left_extraction_id": left_extraction_id,
            "right_extraction_id": right_extraction_id,
            "left_widget_id": left_widget_id,
            "right_widget_id": right_widget_id,
            "compare_mode": compare_mode,
            "model_name": model_name,
            "model_params": model_params or {},
            "verdict": verdict,
            "confidence": confidence,
            "reasons": reasons or [],
            "numbers_used": numbers_used or {},
            "created_at": now_iso,
            "insrt_dttm": now_iso,
            "rec_eff_strt_dt": now_iso,
            "curr_rec_ind": True,
        }
        ins = self.sb.table(self.TBL_COMPARE).insert(payload).execute()
        return (ins.data or [{}])[0]

    # ------------------------------ Public API --------------------------------

    def compare_pair_by_row(self, pair_row: Dict) -> Dict:
        """
        Given a current row from kdh_pair_map_dim (mapped pair), run LLM compare:
          - loads latest extracts for left/right widget ids
          - calls LLM to compare values only
          - saves SCD-2 row into kdh_compare_fact
          - returns the compare result (dict) + db_row
        """
        pair_id = pair_row.get("pair_id")
        left_wid  = pair_row.get("widget_id_left")
        right_wid = pair_row.get("widget_id_right")

        if not left_wid or not right_wid:
            return {"error": "pair row missing widget ids", "pair_id": pair_id}

        left_ext  = self.load_latest_extract_for_widget(left_wid)
        right_ext = self.load_latest_extract_for_widget(right_wid)
        if not left_ext or not right_ext:
            return {"error": "missing latest extracts for one or both widgets", "pair_id": pair_id}

        vals_a = self._pick_json_payload(left_ext)
        vals_b = self._pick_json_payload(right_ext)
        if not vals_a or not vals_b:
            return {"error": "missing JSON values in one or both extracts", "pair_id": pair_id}

        # LLM compare (values only)
        out = self._call_llm_compare(vals_a, vals_b)

        # persist (SCD2)
        db_row = self.scd2_upsert_compare(
            pair_id=pair_id,
            left_extraction_id=left_ext.get("extraction_id"),
            right_extraction_id=right_ext.get("extraction_id"),
            left_widget_id=left_wid,
            right_widget_id=right_wid,
            verdict=out.get("verdict", "NotMatched"),
            confidence=out.get("confidence", 0.0),
            reasons=out.get("why", []),
            numbers_used=out.get("numbers_used", {}),
            compare_mode="llm",
            model_name=self.model,
            model_params={"temperature": 0.0},
        )

        return {"result": out, "db_row": db_row}

