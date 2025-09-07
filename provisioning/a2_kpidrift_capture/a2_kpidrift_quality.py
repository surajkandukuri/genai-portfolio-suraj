from __future__ import annotations
from typing import Dict, List, Tuple

# Base detection gates
MIN_W, MIN_H = 150, 100

# Final threshold for "good"
QUALITY_THRESHOLD = 0.60

# IoU rule exported for possible use elsewhere
IOU_DROP_SAME_KIND = 0.72

# ---- New, stricter “good” constraints ----
# Anything outside HARD_AR_* is junk unless rescued by very strong evidence.
HARD_AR_LOW  = 0.60   # extremely tall/narrow
HARD_AR_HIGH = 3.50   # extremely wide/short

# Even inside hard bounds, we still penalize if outside these “pleasant” bounds.
SOFT_AR_LOW  = 0.80
SOFT_AR_HIGH = 2.20

# To be “good” you generally need at least this much size, unless you’re a
# container WITH a nearby title AND not wildly skinny/wide.
MIN_GOOD_W    = 220
MIN_GOOD_H    = 160
MIN_GOOD_AREA = 160_000

def score_widget(selector_kind: str, bbox_xywh: Tuple[int,int,int,int], title_present: bool) -> Dict:
    """
    Returns a dict with: quality ('good'|'junk'), quality_score (0..1),
    quality_reason (csv), area_px, selector_kind, title_present, w, h, ar
    """
    x, y, w, h = bbox_xywh
    area = max(1, w) * max(1, h)
    ar   = w / max(1, h)

    score = 0.0
    reasons: List[str] = []

    # --- size gate (absolute minimums just to be considered)
    if w < MIN_W or h < MIN_H:
        reasons.append("too_small")
        score -= 0.7
    else:
        score += 0.2

    # --- selector priority
    if selector_kind in ("container", "tableau"):
        score += 0.6
    elif selector_kind == "role":
        score += 0.3
    else:  # primitive
        reasons.append("primitive_selector")
        score += 0.15

    # --- title proximity
    if title_present:
        score += 0.35
    else:
        reasons.append("no_title")

    # --- HARD aspect ratio rule:
    # outside these → usually navigation rails / separators → junk unless rescued
    hard_ar_violation = (ar < HARD_AR_LOW) or (ar > HARD_AR_HIGH)
    if hard_ar_violation:
        reasons.append("hard_ar_violation")
        score -= 0.6

    # --- SOFT aspect ratio penalty (skinny strips)
    if (ar < SOFT_AR_LOW) or (ar > SOFT_AR_HIGH):
        reasons.append("bad_aspect_ratio")
        score -= 0.35

    # --- MIN "good" size: require decent size for confidence
    # If you're not a container+title, you must meet the "good size".
    good_size_ok = (w >= MIN_GOOD_W and h >= MIN_GOOD_H and area >= MIN_GOOD_AREA)
    is_container_with_title = (selector_kind in ("container", "tableau")) and title_present

    if not good_size_ok and not is_container_with_title:
        reasons.append("too_small_for_good")
        score -= 0.35

    # --- Rescue rule:
    # Allow container+title to pass *only* if not extremely skinny/wide and not tiny.
    if is_container_with_title:
        if hard_ar_violation:
            reasons.append("no_rescue_hard_ar")
            score -= 0.25
        # still require some minimal substance
        if w < 180 or h < 140 or area < 120_000:
            reasons.append("container_title_but_too_small")
            score -= 0.2

    # clamp & decide
    score = max(0.0, min(1.0, score))
    quality = "good" if score >= QUALITY_THRESHOLD else "junk"

    return {
        "quality": quality,
        "quality_score": round(score, 3),
        "quality_reason": ",".join(reasons) or None,
        "area_px": area,
        "selector_kind": selector_kind,
        "title_present": bool(title_present),
        # debug
        "w": w,
        "h": h,
        "ar": round(ar, 3),
    }

def append_quality_suffix(filename: str, quality: str) -> str:
    """
    Insert _good/_junk before the extension. Example:
    'foo.png' -> 'foo_good.png'
    """
    q = "good" if quality == "good" else "junk"
    if "." in filename:
        stem, ext = filename.rsplit(".", 1)
        return f"{stem}_{q}.{ext}"
    return f"{filename}_{q}"

def iou(a: Tuple[int,int,int,int], b: Tuple[int,int,int,int]) -> float:
    ax, ay, aw, ah = a; bx, by, bw, bh = b
    x1, y1 = max(ax, bx), max(ay, by)
    x2, y2 = min(ax+aw, bx+bw), min(ay+ah, by+bh)
    inter = max(0, x2-x1) * max(0, y2-y1)
    if inter <= 0: return 0.0
    ua = aw*ah + bw*bh - inter
    return inter / max(1, ua)

__all__ = [
    "MIN_W", "MIN_H", "QUALITY_THRESHOLD", "IOU_DROP_SAME_KIND",
    "score_widget", "append_quality_suffix", "iou",
]
