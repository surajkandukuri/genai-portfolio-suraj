# pages/26_kpidrift_reports.py
from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone, date
from typing import Dict, List, Optional, Set

import pandas as pd
import streamlit as st
import altair as alt
from supabase import create_client, Client

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

SUPABASE_URL = _sget("SUPABASE_URL", "SUPABASE__URL")
SUPABASE_KEY = _sget(
    "SUPABASE_SERVICE_ROLE_KEY",
    "SUPABASE_SERVICE_KEY",
    "SUPABASE_ANON_KEY",
    "SUPABASE__SUPABASE_SERVICE_KEY",
)
TBL_XFACT   = _sget("KDH_TABLE_WIDGET_EXTRACT", default="kdh_widget_extract_fact")
TBL_PAIR    = _sget("KDH_TABLE_PAIR_MAP", default="kdh_pair_map_dim")
TBL_CMP     = _sget("KDH_TABLE_COMPARE_FACT", default="kdh_compare_fact")  # you created earlier

if not SUPABASE_URL or not SUPABASE_KEY:
    st.error("Missing Supabase config. Add SUPABASE_URL and a key in .streamlit/secrets.toml")
    st.stop()

@st.cache_resource
def get_sb() -> Client:
    return create_client(SUPABASE_URL, SUPABASE_KEY)

sb: Client = get_sb()

# ─────────────────────────────────────────────────────────────────────────────
# Utilities (schema-aware safe reads)
# ─────────────────────────────────────────────────────────────────────────────
def _get_columns(table: str) -> Set[str]:
    try:
        res = sb.table(table).select("*").limit(1).execute()
        rows = res.data or []
        return set(rows[0].keys()) if rows else set()
    except Exception:
        return set()

def _first_existing(candidates: List[str], have: Set[str], default=None) -> Optional[str]:
    for c in candidates:
        if c in have:
            return c
    return default

@st.cache_data(ttl=120)
def fetch_table(table: str, limit: int = 5000) -> pd.DataFrame:
    """Load a table into a DataFrame with a sensible limit, handling pagination lightly."""
    cols = _get_columns(table)
    if not cols:
        return pd.DataFrame()

    # a simple “page” loop (keep it light)
    out_rows: List[Dict] = []
    last_count = 0
    page = 0
    page_size = min(2000, limit)
    while len(out_rows) < limit:
        q = sb.table(table).select("*").range(page * page_size, (page + 1) * page_size - 1).execute()
        batch = q.data or []
        out_rows.extend(batch)
        last_count = len(batch)
        page += 1
        if last_count < page_size:  # no more rows
            break

    df = pd.DataFrame(out_rows)
    return df

def parse_date_col(df: pd.DataFrame, candidates: List[str]) -> Optional[str]:
    """Pick a best date column and coerce to pandas datetime (UTC). Returns column name or None."""
    if df.empty:
        return None
    have = set(df.columns)
    col = _first_existing(candidates, have)
    if not col:
        return None
    try:
        df[col] = pd.to_datetime(df[col], errors="coerce", utc=True)
        return col
    except Exception:
        return None

# ─────────────────────────────────────────────────────────────────────────────
# Page UI
# ─────────────────────────────────────────────────────────────────────────────
st.set_page_config(page_title="KPI Drift — Reports", page_icon="📈", layout="wide")
st.title("📈 KPI Drift — Reports")
st.caption("Basic analytics across Parse → Map → Compare pipeline.")

# Filters row
fl1, fl2, fl3, fl4 = st.columns([0.26, 0.26, 0.26, 0.22])
today = date.today()
default_start = today - timedelta(days=30)
dr = fl1.date_input("Date range (UTC)", value=(default_start, today))
match_filter = fl2.selectbox("Match status", options=["All", "Matched", "Not Matched", "Other"], index=0)
show_raw = fl3.toggle("Show raw tables", value=False)
limit_rows = fl4.number_input("Max rows to fetch", min_value=500, max_value=20000, step=500, value=5000)

# ─────────────────────────────────────────────────────────────────────────────
# Load data
# ─────────────────────────────────────────────────────────────────────────────
with st.spinner("Loading data..."):
    df_x = fetch_table(TBL_XFACT, limit_rows)   # parsed widgets
    df_p = fetch_table(TBL_PAIR, limit_rows)    # SCD-2 pairs
    df_c = fetch_table(TBL_CMP, limit_rows)     # compare fact (LLM or numeric compare)

# Normalize date columns
x_date_col = parse_date_col(df_x, ["created_at", "insrt_dttm", "rec_eff_strt_dt", "updated_at"])
p_date_col = parse_date_col(df_p, ["insrt_dttm", "rec_eff_strt_dt", "updated_at"])
c_date_col = parse_date_col(df_c, ["compared_at", "created_at", "insrt_dttm", "rec_eff_strt_dt"])

# Apply date filter
def within_range(df: pd.DataFrame, date_col: Optional[str]) -> pd.DataFrame:
    if df.empty or (not date_col):
        return df
    if isinstance(dr, tuple) and len(dr) == 2:
        start_dt = datetime.combine(dr[0], datetime.min.time()).replace(tzinfo=timezone.utc)
        end_dt   = datetime.combine(dr[1], datetime.max.time()).replace(tzinfo=timezone.utc)
        return df[(df[date_col] >= start_dt) & (df[date_col] <= end_dt)]
    return df

df_x_f = within_range(df_x.copy(), x_date_col)
df_p_f = within_range(df_p.copy(), p_date_col)
df_c_f = within_range(df_c.copy(), c_date_col)

# Optional match filter on compare fact
if not df_c_f.empty and "verdict" in df_c_f.columns and match_filter != "All":
    if match_filter == "Matched":
        df_c_f = df_c_f[df_c_f["verdict"].astype(str).str.lower().isin(["matched", "consistent", "ok", "100%"])]
    elif match_filter == "Not Matched":
        df_c_f = df_c_f[df_c_f["verdict"].astype(str).str.lower().isin(["not matched", "mismatch", "conflict", "likely_mismatch"])]
    else:
        # "Other"
        df_c_f = df_c_f[~df_c_f["verdict"].astype(str).str.lower().isin(
            ["matched", "consistent", "ok", "100%", "not matched", "mismatch", "conflict", "likely_mismatch"]
        )]

# ─────────────────────────────────────────────────────────────────────────────
# KPIs
# ─────────────────────────────────────────────────────────────────────────────
k1, k2, k3, k4 = st.columns(4)

# Parsed widgets
parsed_n = int(len(df_x_f))
k1.metric("Parsed widgets (fact)", f"{parsed_n:,}")

# Current pairs (SCD-2)
pairs_curr_n = 0
if not df_p_f.empty and "curr_rec_ind" in df_p_f.columns:
    pairs_curr_n = int(df_p_f[df_p_f["curr_rec_ind"] == True].shape[0])  # noqa: E712
else:
    pairs_curr_n = int(df_p_f.shape[0])
k2.metric("Current pairs (SCD-2)", f"{pairs_curr_n:,}")

# Compare runs
cmps_n = int(len(df_c_f))
k3.metric("Compare runs", f"{cmps_n:,}")

# Match rate
match_rate = None
if not df_c_f.empty and "verdict" in df_c_f.columns:
    v = df_c_f["verdict"].astype(str).str.lower()
    matched = v.isin(["matched", "consistent", "ok", "100%"]).sum()
    match_rate = (matched / len(v)) * 100 if len(v) else None
k4.metric("Match rate", f"{match_rate:.1f}%" if match_rate is not None else "—")

st.divider()

# ─────────────────────────────────────────────────────────────────────────────
# Trends: parsed, pairs, compares
# ─────────────────────────────────────────────────────────────────────────────
st.subheader("Activity trends")

def daily_counts(df: pd.DataFrame, date_col: Optional[str], label: str) -> pd.DataFrame:
    if df.empty or not date_col:
        return pd.DataFrame(columns=["day", "metric", "count"])
    tmp = df.copy()
    tmp["day"] = tmp[date_col].dt.floor("D")
    out = tmp.groupby("day").size().reset_index(name="count")
    out["metric"] = label
    return out

df_trend = pd.concat(
    [
        daily_counts(df_x_f, x_date_col, "parsed"),
        daily_counts(df_p_f.query("curr_rec_ind == True") if "curr_rec_ind" in df_p_f.columns else df_p_f, p_date_col, "pairs"),
        daily_counts(df_c_f, c_date_col, "compares"),
    ],
    ignore_index=True,
)

if df_trend.empty:
    st.info("No data in the selected range.")
else:
    chart = (
        alt.Chart(df_trend)
        .mark_line(point=True)
        .encode(
            x=alt.X("day:T", title="Day"),
            y=alt.Y("count:Q", title="Count"),
            color=alt.Color("metric:N", title="Metric"),
            tooltip=["day:T", "metric:N", "count:Q"],
        )
        .properties(height=280)
    )
    st.altair_chart(chart, use_container_width=True)

# ─────────────────────────────────────────────────────────────────────────────
# Compare outcomes & quality
# ─────────────────────────────────────────────────────────────────────────────
st.subheader("Compare outcomes")

colA, colB = st.columns([0.55, 0.45])

with colA:
    if df_c_f.empty or "verdict" not in df_c_f.columns:
        st.info("No compare data to show.")
    else:
        counts = df_c_f["verdict"].astype(str).str.title().value_counts().reset_index()
        counts.columns = ["Verdict", "Count"]
        bar = (
            alt.Chart(counts)
            .mark_bar()
            .encode(
                x=alt.X("Verdict:N", sort="-y"),
                y=alt.Y("Count:Q"),
                tooltip=["Verdict:N", "Count:Q"],
                color=alt.Color("Verdict:N", legend=None),
            )
            .properties(height=260)
        )
        st.altair_chart(bar, use_container_width=True)

with colB:
    if df_c_f.empty:
        st.info("No compare data to show.")
    else:
        # If your compare fact stores corr/mape, summarize here (schema-aware)
        cards = []
        if "corr" in df_c_f.columns:
            cards.append(("Mean corr", df_c_f["corr"].dropna().mean()))
        if "mape" in df_c_f.columns:
            cards.append(("Mean MAPE", df_c_f["mape"].dropna().mean()))
        if cards:
            m1, m2 = st.columns(len(cards)) if len(cards) >= 2 else st.columns(2)
            cols = [m1, m2] if len(cards) >= 2 else [m1]
            for (label, val), c in zip(cards, cols):
                if val is not None:
                    if "mape" in label.lower():
                        c.metric(label, f"{float(val)*100:.2f}%")
                    else:
                        c.metric(label, f"{float(val):.3f}")
        else:
            st.caption("No corr/MAPE columns found; skipping quality KPIs.")

# ─────────────────────────────────────────────────────────────────────────────
# Recent failures (or mismatches)
# ─────────────────────────────────────────────────────────────────────────────
st.subheader("Recent mismatches")
if df_c_f.empty or "verdict" not in df_c_f.columns:
    st.info("No compare data.")
else:
    v = df_c_f["verdict"].astype(str).str.lower()
    mask = v.isin(["not matched", "mismatch", "conflict", "likely_mismatch"])
    fails = df_c_f[mask].copy()
    # Pick a recent column to sort by:
    sort_col = _first_existing(["compared_at", "created_at", "insrt_dttm", "rec_eff_strt_dt"], set(fails.columns))
    if sort_col:
        fails = fails.sort_values(sort_col, ascending=False)
    view_cols = [c for c in ["pair_id", "left_widget_id", "right_widget_id", "verdict", "corr", "mape", sort_col] if c in fails.columns]
    if fails.empty:
        st.success("No mismatches in selected range. 🎉")
    else:
        st.dataframe(fails[view_cols].head(50), use_container_width=True)

# ─────────────────────────────────────────────────────────────────────────────
# Downloads
# ─────────────────────────────────────────────────────────────────────────────
st.subheader("Downloads")
d1, d2, d3 = st.columns(3)
d1.download_button("⬇️ Parsed (CSV)", data=df_x_f.to_csv(index=False).encode("utf-8"), file_name="parsed_widgets.csv", mime="text/csv")
d2.download_button("⬇️ Pairs (CSV)",  data=df_p_f.to_csv(index=False).encode("utf-8"), file_name="pairs_scd2.csv", mime="text/csv")
d3.download_button("⬇️ Compares (CSV)", data=df_c_f.to_csv(index=False).encode("utf-8"), file_name="compare_fact.csv", mime="text/csv")

# ─────────────────────────────────────────────────────────────────────────────
# Raw tables (optional)
# ─────────────────────────────────────────────────────────────────────────────
if show_raw:
    st.divider()
    st.subheader("Raw tables")
    st.markdown("**Parsed**")
    st.dataframe(df_x_f, use_container_width=True)
    st.markdown("**Pairs (SCD-2)**")
    st.dataframe(df_p_f, use_container_width=True)
    st.markdown("**Compare fact**")
    st.dataframe(df_c_f, use_container_width=True)
