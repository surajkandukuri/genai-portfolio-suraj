# matrix_numeric_with_images_styled.py
import streamlit as st
import pandas as pd
from typing import List, Dict

st.set_page_config(page_title="KPI Pairing — Numeric + Images", layout="wide")

# --- Instruction (big & clear) ------------------------------------------------
st.markdown(
    """
    <div style="
      padding:14px 16px;
      background:linear-gradient(90deg,#eef6ff, #fff4ec);
      border:1px solid #eaeaea;
      border-radius:12px;
      font-size:18px; font-weight:600; color:#333;
      ">
      👉 Type the same <b>number</b> on both sides (e.g., <b>1</b>) to link matching widgets.
      Each card shows a widget image.
    </div>
    """,
    unsafe_allow_html=True,
)

st.write("")

# --- Demo data (replace `img` with Supabase public URLs or local file paths) --
pbi_widgets: List[Dict] = [
    {"id": "pbi_w1", "title": "Total Sales",      "img": "https://picsum.photos/seed/pbi1/420/260"},
    {"id": "pbi_w2", "title": "Orders by Month",  "img": "https://picsum.photos/seed/pbi2/420/260"},
    {"id": "pbi_w3", "title": "Profit %",         "img": "https://picsum.photos/seed/pbi3/420/260"},
    {"id": "pbi_w4", "title": "Region Split",     "img": "https://picsum.photos/seed/pbi4/420/260"},
]
tbl_widgets: List[Dict] = [
    {"id": "tbl_w1", "title": "Sales Total",      "img": "https://picsum.photos/seed/tbl1/420/260"},
    {"id": "tbl_w2", "title": "Monthly Orders",   "img": "https://picsum.photos/seed/tbl2/420/260"},
    {"id": "tbl_w3", "title": "Profit Margin",    "img": "https://picsum.photos/seed/tbl3/420/260"},
    {"id": "tbl_w4", "title": "Sales by Region",  "img": "https://picsum.photos/seed/tbl4/420/260"},
]

# --- Session state for pair numbers ------------------------------------------
if "pair_pbi" not in st.session_state:
    st.session_state.pair_pbi = {w["id"]: 0 for w in pbi_widgets}
if "pair_tbl" not in st.session_state:
    st.session_state.pair_tbl = {w["id"]: 0 for w in tbl_widgets}

# --- Some lightweight CSS for card backgrounds --------------------------------
st.markdown(
    """
    <style>
      .kdh-col-title { font-size:18px; font-weight:700; margin: 2px 0 10px 2px; }
      .kdh-col {
        border-radius: 14px; padding: 14px; border: 1px solid #eaeaea;
      }
      .kdh-col.pbi { background: #f2f7ff; }      /* light blue */
      .kdh-col.tbl { background: #fff2e8; }      /* light orange */
      .kdh-card {
        background: #fff; border: 1px solid #eee; border-radius: 12px;
        padding: 10px; margin-bottom: 12px;
        box-shadow: 0 1px 0 rgba(0,0,0,0.03);
      }
      .kdh-caption { font-weight:600; margin-top:6px; color:#333; }
      .kdh-id { font-family: ui-monospace, SFMono-Regular, Menlo, monospace; color:#888; font-size:12px; }
    </style>
    """,
    unsafe_allow_html=True,
)

# --- Render columns -----------------------------------------------------------
left, right = st.columns(2, vertical_alignment="top")

def render_column(col, title, widgets, state_key_prefix, state_dict_key, bg_class):
    with col:
        st.markdown(f'<div class="kdh-col {bg_class}">', unsafe_allow_html=True)
        st.markdown(f'<div class="kdh-col-title">{title}</div>', unsafe_allow_html=True)

        # 2-up grid of cards
        for i in range(0, len(widgets), 2):
            row = st.columns(2, vertical_alignment="top")
            for j in range(2):
                if i + j >= len(widgets):
                    continue
                w = widgets[i + j]
                with row[j]:
                    st.markdown('<div class="kdh-card">', unsafe_allow_html=True)
                    st.image(w["img"], use_container_width=True)
                    st.markdown(f'<div class="kdh-caption">{w["title"]}</div>', unsafe_allow_html=True)
                    st.markdown(f'<div class="kdh-id">{w["id"]}</div>', unsafe_allow_html=True)
                    val = st.number_input(
                        f"Pair # — {w['id']}",
                        key=f"{state_key_prefix}_{w['id']}",
                        min_value=0, step=1, value=int(st.session_state[state_dict_key][w["id"]]),
                        help="Use the same number on left & right to link them. 0 = unpaired."
                    )
                    st.session_state[state_dict_key][w["id"]] = int(val)
                    st.markdown('</div>', unsafe_allow_html=True)  # .kdh-card

        st.markdown('</div>', unsafe_allow_html=True)  # .kdh-col

render_column(left,  "Power BI Widgets", pbi_widgets, "pbi", "pair_pbi", "pbi")
render_column(right, "Tableau Widgets",  tbl_widgets, "tbl", "pair_tbl", "tbl")

# --- Build pairs dict: { "1": {"powerbi":[ids], "tableau":[ids]} } -----------
pairs = {}
def add_pair(side, wid, num):
    if num and int(num) > 0:
        k = str(int(num))
        pairs.setdefault(k, {"powerbi": [], "tableau": []})
        pairs[k][side].append(wid)

for wid, num in st.session_state.pair_pbi.items(): add_pair("powerbi", wid, num)
for wid, num in st.session_state.pair_tbl.items(): add_pair("tableau", wid, num)

# --- Preview (one row per pair number) ---------------------------------------
st.write("")
st.markdown("### Pairing Preview")

def meta_by_id(side_list, wid):
    pool = pbi_widgets if side_list == "powerbi" else tbl_widgets
    for w in pool:
        if w["id"] == wid: return w
    return None

if not pairs:
    st.info("No pairs yet. Set a Pair # on both sides.")
else:
    rows = []
    for k in sorted(pairs, key=lambda x: int(x)):
        left_ids  = pairs[k]["powerbi"]
        right_ids = pairs[k]["tableau"]
        status = "✅ 1:1 ready" if (len(left_ids) == 1 and len(right_ids) == 1) else "⚠️ check"
        rows.append({"Pair #": k, "Power BI IDs": ", ".join(left_ids) or "—", "Tableau IDs": ", ".join(right_ids) or "—", "Status": status})
    st.dataframe(pd.DataFrame(rows), use_container_width=True)

    # thumbnail strip per pair (nice visual confirmation)
    for k in sorted(pairs, key=lambda x: int(x)):
        st.markdown(f"**Pair {k}** — { '✅ 1:1 ready' if (len(pairs[k]['powerbi'])==1 and len(pairs[k]['tableau'])==1) else '⚠️ check' }")
        c1, c2 = st.columns(2)
        with c1:
            st.markdown("_Power BI_")
            if pairs[k]["powerbi"]:
                for wid in pairs[k]["powerbi"]:
                    m = meta_by_id("powerbi", wid)
                    st.image(m["img"], caption=f"{m['title']} · `{wid}`", use_container_width=True)
            else:
                st.write("—")
        with c2:
            st.markdown("_Tableau_")
            if pairs[k]["tableau"]:
                for wid in pairs[k]["tableau"]:
                    m = meta_by_id("tableau", wid)
                    st.image(m["img"], caption=f"{m['title']} · `{wid}`", use_container_width=True)
            else:
                st.write("—")

# --- Mapping object to feed your Compare step --------------------------------
st.markdown("### Mapping object (to feed Compare)")
st.code(pairs, language="python")
