# pages/1_provision.py
from __future__ import annotations

import os
import re
import json
import stat
import shutil
from datetime import datetime, timezone
from typing import List, Dict
from pathlib import Path
from textwrap import dedent

import streamlit as st
from supabase import create_client, Client

from provisioning.theme import page_header
from provisioning.ui import card

# 🔹 Walkthrough helpers (we'll use only a single tip bubble)
from portfolio_walkthrough import mount, anchor, register

# -----------------------------------------------------------------------------
# Page header
# -----------------------------------------------------------------------------
# st.set_page_config(page_title="Provision", layout="wide")
page_header("PROVISION", "Generate a Docker-ready AI workspace from approved presets.")

# 🔹 Register a single tip + mount with NO tour button
register(
    "provision_agent",
    tips={
        "overview-chooser": (
            "Pick your options to provision your environment: "
            "Environment (DEV/QA/PROD), Core programming/runtime template, "
            "Database, Secrets Manager, and Storage location."
        )
    }
)
mount("provision_agent", show_tour_button=False)

# -----------------------------------------------------------------------------
# Config helpers
# -----------------------------------------------------------------------------
def sget(*keys, default=None):
    """Get config from Streamlit secrets (preferred) or env vars."""
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

SUPABASE_URL = sget("SUPABASE_URL", "SUPABASE__URL")
SUPABASE_KEY = sget("SUPABASE_SERVICE_ROLE_KEY", "SUPABASE_ANON_KEY", "SUPABASE__SUPABASE_SERVICE_KEY")
PROV_BUCKET  = sget("PROV_BUCKET", default="provisional_agent")

@st.cache_resource
def get_sb() -> Client:
    if not SUPABASE_URL or not SUPABASE_KEY:
        st.error("Missing Supabase config. Add SUPABASE_URL and SERVICE_ROLE/ANON key in .streamlit/secrets.toml")
        st.stop()
    try:
        return create_client(SUPABASE_URL, SUPABASE_KEY)
    except Exception as e:
        st.error(f"Could not connect to Supabase: {e}")
        st.stop()

sb = get_sb()

# Health check (DB)
try:
    sb.table("environment").select("*").limit(1).execute()
    st.caption("✅ Connected to Supabase.")
except Exception as e:
    st.error(f"DB check failed. Did you run the DDL? Error: {e}")
    st.stop()

# -----------------------------------------------------------------------------
# Small utils
# -----------------------------------------------------------------------------
EMAIL_RE = re.compile(r"^[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}$", re.I)

def _valid_email(e: str) -> bool:
    return bool(EMAIL_RE.match((e or "").strip()))

def slugify(s: str) -> str:
    import re as _re
    s = _re.sub(r'[^a-zA-Z0-9_]+', '_', s.strip().lower())
    return f"a_{s}" if not s or not s[0].isalpha() else s

def team_env_slug(team_name: str, env_name: str) -> str:
    return f"{slugify(team_name)}_{slugify(env_name)}"

def _ensure_bucket(bucket: str) -> None:
    try:
        sb.storage.create_bucket(bucket)
    except Exception:
        pass

def _upload_bytes(bucket: str, key: str, data: bytes, content_type: str = "application/octet-stream") -> None:
    _ensure_bucket(bucket)
    sb.storage.from_(bucket).upload(
        path=key,
        file=data,
        file_options={"contentType": content_type, "upsert": "true"},
    )

def _signed_url(bucket: str, key: str, ttl_seconds: int = 3600) -> str | None:
    try:
        res = sb.storage.from_(bucket).create_signed_url(key, ttl_seconds)
        return (res or {}).get("signedURL") or (res or {}).get("signed_url")
    except Exception:
        return None

def _show_storage_hint(object_key: str) -> None:
    pics = Path("pictures")
    candidates: list[Path] = []
    if pics.exists():
        for p in pics.iterdir():
            name = p.name.lower()
            if p.is_file() and "supabase" in name and name.endswith((".png", ".jpg", ".jpeg")):
                candidates.append(p)
    candidates = [
        pics / "supabase_storage_hint.png",
        pics / "supabase_hint.png",
        pics / "supabase.png",
        *candidates,
    ]
    for c in candidates:
        if c.exists():
            st.image(str(c), caption="Preview storage location in Supabase (example)")
            return
    parts = object_key.split("/")
    st.markdown("**Storage preview (example path):**")
    st.code(f"{PROV_BUCKET}\n└── " + "/".join(parts[:-1]) + f"/\n    └── {parts[-1]}", language="text")

# -----------------------------------------------------------------------------
# Workspace builder
# -----------------------------------------------------------------------------
def provision_workspace(selection_key: int, team_name: str, env_name: str, runtime_name: str):
    container_port = 8501
    env_port_map = {"dev": 8510, "qa": 8511, "prod": 8512}
    host_port = env_port_map.get(env_name.lower(), 8519)

    team_env = team_env_slug(team_name, env_name)
    table_prefix = f"{team_env}_"
    storage_prefix = f"{team_env}/"

    repo_path = f"apps/{team_env}"
    ws_dir = Path(repo_path)
    ws_dir.mkdir(parents=True, exist_ok=True)

    manifest = {
        "team_env": team_env,
        "env_name": env_name.lower(),
        "runtime": runtime_name.lower(),
        "selection_key": selection_key,
        "table_prefix": table_prefix,
        "storage_prefix": storage_prefix,
    }
    (ws_dir / "provisional_success_manifest.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")

    (ws_dir / "requirements.txt").write_text("streamlit\n", encoding="utf-8")
    (ws_dir / "app.py").write_text(dedent(f"""
        import os, json, streamlit as st
        st.set_page_config(page_title="ProvisionalSuccessfulTestAgent", page_icon="✅", layout="centered")
        st.title("✅ ProvisionalSuccessfulTestAgent")
        try:
            data = json.loads(open("provisional_success_manifest.json").read())
            st.subheader("Workspace Manifest"); st.json(data, expanded=False)
        except Exception as e:
            st.warning(f"Manifest not found: {{e}}")
        st.write("Zero-key demo: environment is alive.")
        st.caption(f"TEAM_ENV={{os.getenv('TEAM_ENV')}} • TABLE_PREFIX={{os.getenv('TABLE_PREFIX')}} • STORAGE_PREFIX={{os.getenv('STORAGE_PREFIX')}}")
        gw = os.getenv("LLM_GATEWAY_URL") or "(disconnected)"
        st.caption(f"LLM_GATEWAY_URL={{gw}}")
    """).strip()+"\n", encoding="utf-8")

    (ws_dir / "Dockerfile").write_text(dedent(f"""
        FROM python:3.11-slim
        WORKDIR /app
        COPY requirements.txt .
        RUN pip install --no-cache-dir -r requirements.txt
        COPY . .
        EXPOSE {container_port}
        CMD ["streamlit", "run", "app.py", "--server.port={container_port}", "--server.address=0.0.0.0"]
    """).strip()+"\n", encoding="utf-8")

    (ws_dir / "docker-compose.yml").write_text(dedent(f"""
        services:
          app:
            build:
              context: .
              dockerfile: Dockerfile
            container_name: {team_env}
            ports:
              - "{host_port}:{container_port}"
            environment:
              TEAM_ENV: "{team_env}"
              TABLE_PREFIX: "{table_prefix}"
              STORAGE_PREFIX: "{storage_prefix}"
              LLM_GATEWAY_URL: ${{{{LLM_GATEWAY_URL:-}}}}
            extra_hosts:
              - "host.docker.internal:host-gateway"
    """).strip()+"\n", encoding="utf-8")

    (ws_dir / ".env").write_text("", encoding="utf-8")

    enable_sh = dedent("""\
        #!/usr/bin/env bash
        set -e
        echo "LLM_GATEWAY_URL=http://host.docker.internal:7000" > .env
        docker compose restart
        echo "Agents ENABLED (gateway URL set)."
    """)
    disable_sh = dedent("""\
        #!/usr/bin/env bash
        set -e
        : > .env
        docker compose restart
        echo "Agents DISABLED (gateway URL cleared)."
    """)
    enable_ps1 = dedent("""\
        Set-Content -Path .env -Value "LLM_GATEWAY_URL=http://host.docker.internal:7000"
        docker compose restart
        Write-Host "Agents ENABLED (gateway URL set)."
    """)
    disable_ps1 = dedent("""\
        Set-Content -Path .env -Value ""
        docker compose restart
        Write-Host "Agents DISABLED (gateway URL cleared)."
    """)

    (ws_dir / "enable_agents.sh").write_text(enable_sh, encoding="utf-8")
    (ws_dir / "disable_agents.sh").write_text(disable_sh, encoding="utf-8")
    (ws_dir / "enable_agents.ps1").write_text(enable_ps1, encoding="utf-8")
    (ws_dir / "disable_agents.ps1").write_text(disable_ps1, encoding="utf-8")

    try:
        for fname in ("enable_agents.sh", "disable_agents.sh"):
            p = ws_dir / fname
            p.chmod(p.stat().st_mode | stat.S_IEXEC)
    except Exception:
        pass

    (ws_dir / "README.md").write_text(dedent(f"""
        # {team_env} — Workspace Starter

        **Environment:** {env_name}
        **Runtime:** {runtime_name}
        **Selection Key:** {selection_key}
        **Tables Prefix:** `{table_prefix}`
        **Storage Prefix:** `{storage_prefix}`
        **Folder:** `{repo_path}`

        ## Run
        ```bash
        cd {repo_path}
        docker compose up -d
        ```
        Open: http://localhost:{host_port}

        ## 🔌 Connect this app to Central Agents (Groq & Checks)
        Connect (uses host.docker.internal — no custom Docker networks):
        ```bash
        # macOS/Linux
        ./enable_agents.sh
        # Windows PowerShell
        .\\enable_agents.ps1
        ```

        Disconnect (isolate the app):
        ```bash
        # macOS/Linux
        ./disable_agents.sh
        # Windows PowerShell
        .\\disable_agents.ps1
        ```

        This sets/clears `LLM_GATEWAY_URL` in `.env` and restarts the container.
        Gateway on host: http://localhost:7000
        In-container URL: http://host.docker.internal:7000
    """).strip()+"\n", encoding="utf-8")

    return repo_path, host_port

# -----------------------------------------------------------------------------
# DB helpers
# -----------------------------------------------------------------------------
def next_unprovisioned_selection(team_id: int):
    res = (
        sb.table("team_selection_batch")
          .select("selection_key, environment_id, target_runtime_id")
          .eq("team_id", team_id)
          .eq("provision_done_ind", "N")
          .order("selection_key", desc=True)
          .limit(1)
          .execute()
          .data
    )
    if not res:
        return None
    batch = res[0]
    env = (sb.table("environment").select("environment_name")
           .eq("environment_id", batch["environment_id"]).single().execute().data)
    rt = (sb.table("target_runtime").select("target_runtime")
          .eq("target_runtime_id", batch["target_runtime_id"]).single().execute().data)
    return {
        "selection_key": batch["selection_key"],
        "env_name": env["environment_name"].lower(),
        "runtime_name": (rt["target_runtime"] or "").lower(),
        "rt_label": (rt["target_runtime"] or "").upper(),
    }

def authenticate(username: str, pwd: str):
    resp = sb.table("teams").select("*").eq("username", username).eq("pwd", pwd).execute()
    if resp.data:
        return resp.data[0]
    return None

@st.cache_data(ttl=60)
def load_environments() -> List[Dict]:
    return sb.table("environment").select("*").order("environment_id").execute().data

@st.cache_data(ttl=60)
def load_artifact_types() -> List[Dict]:
    return sb.table("artifact_type").select("*").order("artifact_type_id").execute().data

@st.cache_data(ttl=60)
def load_artifacts_by_type(artifact_type_id: int) -> List[Dict]:
    return (
        sb.table("artifacts")
          .select("*")
          .eq("artifact_type_id", artifact_type_id)
          .order("artifact_name")
          .execute()
          .data
    )

@st.cache_data(ttl=60)
def load_target_runtimes() -> List[Dict]:
    return sb.table("target_runtime").select("*").order("target_runtime_id").execute().data

# -----------------------------------------------------------------------------
# Session: optional sign-in (preview allowed)
# -----------------------------------------------------------------------------
if "user" not in st.session_state:
    st.session_state.user = None

if st.session_state.user is None:
    st.info(
        "You’re in **preview**. Selections won’t appear in Console/Reports. "
        "Provisioning will upload a preview ZIP to Storage. "
        "**Sign in** to save history and enable direct download."
    )

with st.expander("🔐 Sign in (optional)", expanded=False):
    st.caption("Preview mode lets you try everything. Sign in to save to Console/Reports and enable direct download.")
    with st.form("login_form", clear_on_submit=False):
        col1, col2, col3 = st.columns([1, 1, 1])
        username = col1.text_input("Username", placeholder="team_1")
        pwd = col2.text_input("Password", type="password", placeholder="••••••")
        submitted = col3.form_submit_button("Sign in")
    if submitted:
        user = authenticate(username.strip(), pwd)
        if user:
            st.session_state.user = user
            st.success(f"Welcome, {user['team_name']}! 👋")
            st.rerun()
        else:
            st.error("Invalid credentials. Please try again.")

is_signed_in = st.session_state.user is not None

# -----------------------------------------------------------------------------
# Team Context (signed-in only)
# -----------------------------------------------------------------------------
if is_signed_in:
    user = st.session_state.user
    with card("Team Context"):
        st.write(
            f"**Team:** {user['team_name']} • **POC:** {user.get('team_pointofcontact') or '—'} • "
            f"**DL:** {user.get('team_distributionlist') or '—'} • **User:** {user['username']}"
        )

if is_signed_in and "last_selection" not in st.session_state:
    pending = next_unprovisioned_selection(st.session_state.user["team_id"])
    if pending:
        st.session_state["last_selection"] = pending

# -----------------------------------------------------------------------------
# Target selectors
# -----------------------------------------------------------------------------
envs = load_environments()
env_options = {e["environment_name"].upper(): e["environment_id"] for e in envs}

with card("Target"):
    # 🔹 Single info bubble here
    anchor("overview-chooser")

    colA, colB = st.columns(2)
    env_label = colA.selectbox("Select Environment", list(env_options.keys()))
    environment_id = env_options[env_label]
    env_name_lc = env_label.lower()

    try:
        runtimes = load_target_runtimes()
        if not runtimes:
            st.warning("No target runtimes found. Did you insert rows into target_runtime?")
            st.stop()
        rt_options = {r["target_runtime"].upper(): r["target_runtime_id"] for r in runtimes}
        rt_label = colB.selectbox("Target Environment", list(rt_options.keys()))
        target_runtime_id = rt_options[rt_label]
        runtime_name = rt_label.lower()
    except Exception as e:
        st.error(f"Failed to load target runtimes: {e}")
        st.stop()

# -----------------------------------------------------------------------------
# Artifact selection
# -----------------------------------------------------------------------------
with card("Select Approved Artifacts (one from each)"):
    artifact_types = load_artifact_types()
    picks: Dict[int, int | None] = {}
    for at in artifact_types:
        at_id = at["artifact_type_id"]
        at_name = at["artifact_type"]
        data = load_artifacts_by_type(at_id)
        options = {row["artifact_name"]: row["artifact_id"] for row in data}
        if not options:
            st.warning(f"No artifacts configured for **{at_name}** yet.")
            continue
        selection = st.selectbox(
            f"{at_name.title()}",
            ["-- choose --"] + list(options.keys()),
            key=f"sb_{at_id}",
        )
        picks[at_id] = options.get(selection) if selection != "-- choose --" else None

# Must pick all types
all_chosen = all(picks.get(at["artifact_type_id"]) for at in artifact_types) if artifact_types else False

# -----------------------------------------------------------------------------
# Save + History
# -----------------------------------------------------------------------------
with card("Save Selection & History"):
    col_l, col_r = st.columns([1, 1])

    def create_selection_batch(team_id: int, environment_id: int, username: str, target_runtime_id: int) -> int:
        resp = (
            sb.table("team_selection_batch")
              .insert(
                  {"team_id": team_id, "environment_id": environment_id, "insrt_user_name": username, "target_runtime_id": target_runtime_id},
                  returning="representation"
              )
              .execute()
        )
        if not resp.data or not isinstance(resp.data, list):
            raise RuntimeError(f"Insert did not return a row: {resp}")
        return int(resp.data[0]["selection_key"])

    def save_detail(selection_key: int, artifact_type_id: int, artifact_id: int):
        sb.table("team_selection_detail").insert({
            "selection_key": selection_key, "artifact_type_id": artifact_type_id, "artifact_id": artifact_id
        }).execute()

    with col_l:
        if not is_signed_in:
            st.button("📦 Save Selection", type="primary", disabled=True, help="Sign in to save and audit this selection.")
        else:
            if st.button("📦 Save Selection", type="primary", disabled=not all_chosen):
                try:
                    selection_key = create_selection_batch(
                        st.session_state.user["team_id"], environment_id,
                        st.session_state.user["username"], target_runtime_id
                    )
                    for at in artifact_types:
                        at_id = at["artifact_type_id"]
                        art_id = picks.get(at_id)
                        save_detail(selection_key, at_id, art_id)

                    st.session_state["last_selection"] = {
                        "selection_key": selection_key,
                        "env_name": env_name_lc,
                        "runtime_name": runtime_name,
                        "rt_label": rt_label,
                    }
                    st.success(f"Saved under selection_key **{selection_key}** for **{env_label} / {rt_label}** ✅")
                except Exception as e:
                    st.error(f"Save failed: {e}")

    with col_r:
        with st.expander("📜 View My Past Selections", expanded=False):
            if not is_signed_in:
                st.info("Sign in to view your saved selections and history.")
            else:
                try:
                    resp = (
                        sb.table("v_team_selection_flat")
                          .select("*")
                          .eq("team_id", st.session_state.user["team_id"])
                          .order("selection_key", desc=True)
                          .limit(100)
                          .execute()
                    )
                    rows = resp.data or []
                    if not rows:
                        st.info("No selections yet.")
                    else:
                        st.write([
                            {
                                "selection_key": r["selection_key"],
                                "when": r["insrt_dttm"],
                                "environment": r["environment_name"],
                                "type": r["artifact_type_name"],
                                "artifact": r["artifact_name"],
                                "by": r["insrt_user_name"],
                            }
                            for r in rows
                        ])
                except Exception as e:
                    st.warning(f"Could not load history view: {e}")

# -----------------------------------------------------------------------------
# Provision
# -----------------------------------------------------------------------------
with card("Provision"):
    st.write("Generate a Docker-ready workspace for your latest selection.")
    run = st.button("🚀 Provision Now", type="primary", disabled=not all_chosen)

    if run:
        if is_signed_in:
            sel = st.session_state.get("last_selection") or next_unprovisioned_selection(st.session_state.user["team_id"])
            if not sel:
                st.info("No unprovisioned selections found. Save a new selection first.")
                st.stop()

            repo_path, port = provision_workspace(
                sel["selection_key"], st.session_state.user["team_name"], sel["env_name"], sel["runtime_name"]
            )

            try:
                sb.table("team_selection_batch").update({"provision_done_ind": "Y"}).eq("selection_key", sel["selection_key"]).execute()
                st.success(f"Provisioned and marked selection_key {sel['selection_key']} as 'Y' ✅")
            except Exception as e:
                st.warning(f"Workspace created, but failed to mark provisioned: {e}")

            try:
                team_env = Path(repo_path).name
                stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
                prefix = f"provision/{team_env}/{stamp}/"
                zip_basename = f"{team_env}_{stamp}"
                zip_dir = Path.cwd() / "tmp_zips"
                zip_dir.mkdir(parents=True, exist_ok=True)
                zip_path_without_ext = str(zip_dir / zip_basename)
                shutil.make_archive(base_name=zip_path_without_ext, format="zip", root_dir=repo_path)
                with open(f"{zip_path_without_ext}.zip", "rb") as f:
                    data = f.read()
                object_key = f"{prefix}{zip_basename}.zip"
                _upload_bytes(PROV_BUCKET, object_key, data, content_type="application/zip")
                url = _signed_url(PROV_BUCKET, object_key, ttl_seconds=3600)

                if url:
                    st.link_button("⬇️ Download workspace ZIP (1h)", url, use_container_width=True, type="primary")
                    st.caption("Signed URL is valid for 1 hour.")
                else:
                    st.warning("Uploaded ZIP but could not create a signed URL. Check bucket policies.")
            except Exception as e:
                st.warning(f"Upload skipped/failed: {e}")

            st.code(f"cd {repo_path}\ndocker compose up -d\n# open http://localhost:{port}", language="bash")

        else:
            try:
                preview_team = "preview"
                selection_key = int(datetime.now(timezone.utc).timestamp())

                repo_path, port = provision_workspace(selection_key, preview_team, env_name_lc, runtime_name)

                team_env = Path(repo_path).name
                stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
                prefix = f"preview/{team_env}/{stamp}/"
                zip_basename = f"{team_env}_{stamp}"
                zip_dir = Path.cwd() / "tmp_zips"
                zip_dir.mkdir(parents=True, exist_ok=True)
                zip_path_without_ext = str(zip_dir / zip_basename)
                shutil.make_archive(base_name=zip_path_without_ext, format="zip", root_dir=repo_path)
                with open(f"{zip_path_without_ext}.zip", "rb") as f:
                    data = f.read()
                object_key = f"{prefix}{zip_basename}.zip"
                _upload_bytes(PROV_BUCKET, object_key, data, content_type="application/zip")
                st.success("Preview workspace generated and uploaded to Supabase.")

                _show_storage_hint(object_key)

                masked = object_key.split("/")
                if len(masked) >= 2:
                    masked[-2] = "******"
                st.caption(f"Bucket: **{PROV_BUCKET}** • Key: `{'/'.join(masked)}`")

                with st.container(border=True):
                    st.subheader("Unlock download")
                    st.caption("Enter your email to receive a 1-hour download link for the preview ZIP.")
                    email = st.text_input("Work email", placeholder="you@company.com", key="preview_dl_email")
                    if st.button("Get 1-hour download link"):
                        if _valid_email(email):
                            url = _signed_url(PROV_BUCKET, object_key, ttl_seconds=3600)
                            if url:
                                st.success("Your download link is ready (valid for 1 hour).")
                                st.link_button("⬇️ Download ZIP", url, type="primary")
                                st.caption("Tip: Sign in next time to save to Console/Reports and get direct download automatically.")
                            else:
                                st.error("Could not create a signed URL. Please try again.")
                        else:
                            st.error("Enter a valid email address to continue.")

                st.code(f"cd {repo_path}\ndocker compose up -d\n# open http://localhost:{port}", language="bash")

            except Exception as e:
                st.error(f"Preview provision failed: {e}")

# -----------------------------------------------------------------------------
# Footer
# -----------------------------------------------------------------------------
with card("Session"):
    if st.session_state.user is not None:
        if st.button("Sign out"):
            st.session_state.user = None
            if "last_selection" in st.session_state:
                del st.session_state["last_selection"]
            st.rerun()
    else:
        st.caption("You are browsing in preview. Sign in above to save and enable direct download.")
