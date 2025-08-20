# provisioning/nav.py
import streamlit as st
from pathlib import Path
import importlib.util
from typing import Dict, Any, Iterable, List
from urllib.parse import quote, unquote

def _slug(s: str) -> str:
    return s.strip().lower().replace(" ", "-").replace("/", "-")

def _run_script(py_path: str, entry: str = "main") -> None:
    p = Path(py_path)
    if not p.exists():
        st.error(f"File not found: {p.resolve()}")
        return
    spec = importlib.util.spec_from_file_location(f"page_{p.stem.replace('-','_')}", p)
    if not spec or not spec.loader:
        st.error(f"Cannot import {py_path}")
        return
    mod = importlib.util.module_from_spec(spec)  # type: ignore[arg-type]
    spec.loader.exec_module(mod)                 # executes top-level Streamlit code
    fn = getattr(mod, entry, None)
    if callable(fn):
        fn()

def _walk(menu: Iterable[Dict[str, Any]],
          trail: List[str],
          slug_map: Dict[str, str],
          indent: int = 0):
    """Render sidebar and build slug->path map (recursive, any depth)."""
    for item in menu:
        label, path = item["label"], item.get("path")
        node_slug = "/".join([*trail, _slug(label)])
        pad = "&nbsp;" * (indent * 2)

        if path:
            # clickable leaf
            st.sidebar.markdown(f"{pad}- [{label}](?view={quote(node_slug)})")
            slug_map[node_slug] = path
        else:
            # section header (no click)
            st.sidebar.markdown(f"{pad}**{label}**")

        # children
        if item.get("children"):
            _walk(item["children"], [*trail, _slug(label)], slug_map, indent + 1)

def render_sidebar(menu: Iterable[Dict[str, Any]]) -> Dict[str, str]:
    st.sidebar.markdown("### PORTAL MENU")
    slug_map: Dict[str, str] = {}
    _walk(menu, [], slug_map, indent=0)
    return slug_map

def route(slug_map: Dict[str, str], entry_path: str) -> bool:
    """Route to a target; return True if a routed page was rendered."""
    qp = st.query_params
    view = unquote(qp.get("view", "")).lower()
    if not view:
        return False

    target = slug_map.get(view)
    if not target:
        st.error(f"Unknown page key: {view}")
        return True

    # 🔑 If target is the entry script, don't import it—just clear ?view (go home)
    if Path(target).resolve() == Path(entry_path).resolve():
        st.query_params.clear()   # go back to Landing (portfolio_homepage.py)
        return False

    # Mark that we're inside a routed subpage (subpages can check this to skip their own sidebar)
    st.session_state["ROUTED"] = True

    _run_script(target)
    st.sidebar.markdown("[← Back to Home](./)")
    return True
