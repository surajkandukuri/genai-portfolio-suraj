# provisioning/ui.py
from pathlib import Path
from contextlib import contextmanager
import streamlit as st

# ===== Global CSS =====
_CSS = """
<style>
:root{
  --bg:#f1f5f9;
  --panel:#c4d2ea; --panel-grad:#b7c8e6;
  --card:#ffffff;
  --text:#0f172a; --muted:#475569; --brand:#0b2f4e; --accent:#1f8a70;
  --radius:14px;
  --shadow:0 6px 18px rgba(15,76,129,.08), 0 1px 3px rgba(0,0,0,.06);

  /* Sidebar fine-tuning */
  --nav-pad-y:4px;
  --nav-pad-x:8px;
  --nav-gap:2px;

  /* Tree spacing (change these if you want wider/narrower indents) */
  --indent-1-pad:24px;   /* text left pad for level 1 */
  --indent-2-pad:38px;   /* text left pad for level 2 */
  --indent-3-pad:52px;   /* text left pad for level 3 */
  --guide-1-left:6px;    /* guide bar X for level 1 */
  --guide-2-left:20px;   /* guide bar X for level 2 */
  --guide-3-left:34px;   /* guide bar X for level 3 */
  --bullet-1-left:12px;  /* bullet X for level 1 */
  --bullet-2-left:26px;  /* bullet X for level 2 */
  --bullet-3-left:40px;  /* bullet X for level 3 */
}

html, body, .stApp { background: var(--bg); color: var(--text); }
.block-container { max-width: 1150px; padding-top: 1.0rem; }

/* Hide Streamlit’s default page list */
[data-testid="stSidebarNav"] { display: none !important; }

/* Sidebar look (simple tree) */
[data-testid="stSidebar"] {
  background: linear-gradient(0deg, var(--panel), var(--panel-grad));
  border-right: 1px solid rgba(0,0,0,.08);
}
.sidebar-wrap { padding: 12px 14px 18px 14px; }
.menu-title {
  font-weight: 900; font-size: 26px; line-height: 1.1;
  color: var(--brand); margin: 2px 6px 12px 6px; white-space: nowrap;
}
.menu-section { font-weight: 800; margin: 14px 6px 6px 6px; color: var(--brand); }

/* Row wrapper for each page_link */
.navline { margin: var(--nav-gap) 4px !important; position: relative; }

/* HARD RESET: neutralize Streamlit page_link styles */
.navline [data-testid="stPageLink-NavLink"],
.navline [data-testid="stPageLink"]{
  background: transparent !important;
  border: 0 !important;
  box-shadow: none !important;
  padding: 0 !important;
  margin: 0 !important;
}
.navline [data-testid="stPageLink-NavLink"] > button,
.navline [data-testid="stPageLink"] > button{
  all: unset;
  display:block; width:100%; text-align:left; cursor:pointer;
  padding: var(--nav-pad-y) var(--nav-pad-x) !important;
  color:#0b1f38; font-weight:600; line-height:1.2;
  background: transparent !important;
  box-shadow: none !important; border: 0 !important;
}

/* Remove ANY residual “pill” backgrounds (active/hover) */
.navline *[aria-current="page"],
.navline *[aria-current="page"] *,
.navline *:hover,
.navline *:focus{
  background: transparent !important;
  box-shadow: none !important;
}

/* Level-specific indents (tree) */
.navline.indent-1 [data-testid="stPageLink-NavLink"] > button,
.navline.indent-1 [data-testid="stPageLink"] > button { padding-left: var(--indent-1-pad) !important; }
.navline.indent-2 [data-testid="stPageLink-NavLink"] > button,
.navline.indent-2 [data-testid="stPageLink"] > button { padding-left: var(--indent-2-pad) !important; }
.navline.indent-3 [data-testid="stPageLink-NavLink"] > button,
.navline.indent-3 [data-testid="stPageLink"] > button { padding-left: var(--indent-3-pad) !important; }

/* Guide bars */
.navline.indent-1:before,
.navline.indent-2:before,
.navline.indent-3:before {
  content:""; position:absolute; top:0; bottom:0; width:2px;
  background: rgba(11,31,56,.25); border-radius:1px;
}
.navline.indent-1:before { left: var(--guide-1-left); }
.navline.indent-2:before { left: var(--guide-2-left); }
.navline.indent-3:before { left: var(--guide-3-left); }

/* Bullets */
.navline.indent-1 [data-testid="stPageLink-NavLink"] > button::before,
.navline.indent-1 [data-testid="stPageLink"] > button::before,
.navline.indent-2 [data-testid="stPageLink-NavLink"] > button::before,
.navline.indent-2 [data-testid="stPageLink"] > button::before,
.navline.indent-3 [data-testid="stPageLink-NavLink"] > button::before,
.navline.indent-3 [data-testid="stPageLink"] > button::before {
  content:"•"; position:absolute; top:50%; transform:translateY(-50%);
  color:#0b1f38; font-weight:900;
}
.navline.indent-1 [data-testid="stPageLink-NavLink"] > button::before,
.navline.indent-1 [data-testid="stPageLink"] > button::before { left: var(--bullet-1-left); }
.navline.indent-2 [data-testid="stPageLink-NavLink"] > button::before,
.navline.indent-2 [data-testid="stPageLink"] > button::before { left: var(--bullet-2-left); }
.navline.indent-3 [data-testid="stPageLink-NavLink"] > button::before,
.navline.indent-3 [data-testid="stPageLink"] > button::before { left: var(--bullet-3-left); }

/* Hover underline only */
.navline button:hover { text-decoration: underline; }

/* Active item: thin left bar + bolder text; NO pill */
.navline [data-testid="stPageLink-NavLink"] > button[aria-current="page"],
.navline [data-testid="stPageLink"] > button[aria-current="page"] {
  text-decoration: none;
  font-weight: 800; color: var(--brand);
  position: relative;
}
.navline [data-testid="stPageLink-NavLink"] > button[aria-current="page"]::after,
.navline [data-testid="stPageLink"] > button[aria-current="page"]::after {
  content:""; position:absolute; left:0; top:0; bottom:0; width:3px;
  background: var(--brand); border-radius:2px;
}

/* Cards */
.card {
  background: var(--card);
  border-radius: var(--radius);
  border: 1px solid #e5e7eb;
  box-shadow: var(--shadow);
  padding: 16px 18px; margin: 12px 0;
}
.card h4 { margin: 0 0 8px 0; font-weight: 800; color: var(--brand); }
.card .subtle { color: var(--muted); font-size: .92rem; }

/* Hide truly empty blocks */
.card:empty { display: none !important; }
.block-container > div:empty { display: none !important; }

/* Buttons */
.stButton>button {
  border-radius: 12px !important; padding: 8px 16px !important;
  background: var(--accent) !important; color: #fff !important;
  border: none !important; font-weight: 700 !important;
}

/* Headings */
h1, h2 { color: var(--brand); font-weight: 900; }

/* Optional hero (home) */
.hero {
  background: linear-gradient(135deg, #eef5fb 0%, #ffffff 60%);
  border-radius: 22px; box-shadow: var(--shadow);
  padding: 26px 28px; margin: 8px 0 18px 0; border: 1px solid #e6edf5;
}
.hero h1 { font-size: 40px; line-height: 1.1; margin:0 0 6px 0; }
.hero .tagline { color:#526581; margin-bottom:12px; }



/* === SIMPLE OVERRIDE: push Admin children in; no bullets/guide === */
.navline.indent-2:before{ display:none !important; }  /* hide vertical guide */
.navline.indent-2 [data-testid="stPageLink-NavLink"] > button,
.navline.indent-2 [data-testid="stPageLink"] > button{
  padding-left: 72px !important;                     /* <- controls the indent */
}
.navline.indent-2 [data-testid="stPageLink-NavLink"] > button::before,
.navline.indent-2 [data-testid="stPageLink"] > button::before{
  content:"" !important;                              /* hide the • bullet */
}

</style>
"""

def inject_styles():
    st.markdown(_CSS, unsafe_allow_html=True)

# ===== Sidebar (simple tree) =====
ROOT = Path(__file__).resolve().parents[1]  # repo root

def _exists(rel: str) -> bool:
    return (ROOT / rel).exists()

def render_sidebar(active: str = "Home"):
    with st.sidebar:
        st.markdown('<div class="sidebar-wrap">', unsafe_allow_html=True)
        st.markdown('<div class="menu-title">Portal Menu</div>', unsafe_allow_html=True)

        def nav(label: str, path: str, level: int = 0):
            if not _exists(path):
                return
            cls = "navline" + (f" indent-{level}" if level > 0 else "")
            st.markdown(f'<div class="{cls}">', unsafe_allow_html=True)
            st.page_link(path, label=label)
            st.markdown('</div>', unsafe_allow_html=True)

        # Top level
        nav("Home",      "streamlit_app.py", level=0)
        nav("Provision", "pages/1_Provision.py", level=0)

        # Admin group (label only)
        st.markdown('<div class="menu-section">Admin</div>', unsafe_allow_html=True)
        # Children (two indents)
        nav("Reports",   "pages/3_Reports.py",   level=2)
        nav("Console",   "pages/2_Admin.py",     level=2)   # or rename to 2_Console.py if you prefer
        nav("Artifacts", "pages/4_Artifacts.py", level=2)

        # Account
        st.markdown('<div class="menu-section">Account</div>', unsafe_allow_html=True)
        nav("Logout",    "pages/9_Logout.py", level=0)

        st.markdown('</div>', unsafe_allow_html=True)

# ===== Card helper =====
@contextmanager
def card(title: str | None = None, subtitle: str | None = None):
    st.markdown("<div class='card'>", unsafe_allow_html=True)
    if title:
        st.markdown(f"#### {title}")
    if subtitle:
        st.markdown(f"<div class='subtle'>{subtitle}</div>", unsafe_allow_html=True)
    yield
    st.markdown("</div>", unsafe_allow_html=True)
