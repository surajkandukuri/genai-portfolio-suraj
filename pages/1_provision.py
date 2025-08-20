# pages/1_provision.py
import os
import json
import stat
from typing import List, Dict
from pathlib import Path
from textwrap import dedent

import streamlit as st
from supabase import create_client, Client  # keep if you use Supabase later

# ✅ Our modular theme + components
from provisioning.theme import page_setup, page_header
from provisioning.ui import card

# ── Page bootstrap: theme + sidebar (active highlight) ────────────────────────
page_setup(active="Provision")
page_header("PROVISION", "Generate a Docker-ready AI workspace from approved presets.")

# ── Config helpers ────────────────────────────────────────────────────────────
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

# Health check
try:
    sb.table("environment").select("*").limit(1).execute()
    st.caption("✅ Connected to Supabase.")
except Exception as e:
    st.error(f"DB check failed. Did you run the DDL? Error: {e}")
    st.stop()

# ── Helpers ───────────────────────────────────────────────────────────────────
def slugify(s: str) -> str:
    import re
    s = re.sub(r'[^a-zA-Z0-9_]+', '_', s.strip().lower())
    return f"a_{s}" if not s or not s[0].isalpha() else s

def team_env_slug(team_name: str, env_name: str) -> str:
    return f"{slugify(team_name)}_{slugify(env_name)}"

def provision_workspace(selection_key: int, team_name: str, env_name: str, runtime_name: str):
    """
    Creates apps/<team_env>/ with manifest, docker-compose.yml, README, minimal runnable app,
    plus enable/disable agents scripts and a default .env (empty). Returns (repo_path, host_port).
    """
    container_port = 8501
    env_port_map = {"dev": 8510, "qa": 8511, "prod": 8512}
    host_port = env_port_map.get(env_name.lower(), 8519)

    team_env = team_env_slug(team_name, env_name)
    table_prefix = f"{team_env}_"
    storage_prefix = f"{team_env}/"

    repo_path = f"apps/{team_env}"
    ws_dir = Path(repo_path)
    ws_dir.mkdir(parents=True, exist_ok=True)

    # Manifest
    manifest = {
        "team_env": team_env,
        "env_name": env_name.lower(),
        "runtime": runtime_name.lower(),
        "selection_key": selection_key,
        "table_prefix": table_prefix,
        "storage_prefix": storage_prefix,
    }
    (ws_dir / "provisional_success_manifest.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")

    # Tiny app
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

    # Dockerfile
    (ws_dir / "Dockerfile").write_text(dedent(f"""
        FROM python:3.11-slim
        WORKDIR /app
        COPY requirements.txt .
        RUN pip install --no-cache-dir -r requirements.txt
        COPY . .
        EXPOSE {container_port}
        CMD ["streamlit", "run", "app.py", "--server.port={container_port}", "--server.address=0.0.0.0"]
    """).strip()+"\n", encoding="utf-8")

    # docker-compose.yml (reads LLM_GATEWAY_URL from .env; add extra_hosts for Linux)
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
              LLM_GATEWAY_URL: ${{LLM_GATEWAY_URL:-}}
            extra_hosts:
              - "host.docker.internal:host-gateway"
    """).strip()+"\n", encoding="utf-8")

    # default .env (empty → disconnected)
    (ws_dir / ".env").write_text("", encoding="utf-8")

    # Enable/Disable agent scripts (.env toggle + restart)
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

    # README
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

# ── Data helpers (DB) ─────────────────────────────────────────────────────────
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
    return (sb.table("artifacts").select("*")
            .eq("artifact_type_id", artifact_type_id).order("artifact_name").execute().data)

@st.cache_data(ttl=60)
def load_target_runtimes() -> List[Dict]:
    return sb.table("target_runtime").select("*").order("target_runtime_id").execute().data

# ── UI ────────────────────────────────────────────────────────────────────────

# Session user
if "user" not in st.session_state:
    st.session_state.user = None

# Login (styled)
if st.session_state.user is None:
    with card("Login"):
        with st.form("login_form", clear_on_submit=False):
            col1, col2 = st.columns(2)
            username = col1.text_input("Username", placeholder="team_1")
            pwd = col2.text_input("Password", type="password", placeholder="••••••")
            submitted = st.form_submit_button("Sign in")
        if submitted:
            user = authenticate(username.strip(), pwd)
            if user:
                st.session_state.user = user
                st.success(f"Welcome, {user['team_name']}! 👋")
                st.rerun()
            else:
                st.error("Invalid credentials. Please try again.")
    st.stop()

# After login
user = st.session_state.user
with card("Team Context"):
    st.write(
        f"**Team:** {user['team_name']} • **POC:** {user.get('team_pointofcontact') or '—'} • "
        f"**DL:** {user.get('team_distributionlist') or '—'} • **User:** {user['username']}"
    )

# If session missing, auto-load newest unprovisioned
if "last_selection" not in st.session_state:
    pending = next_unprovisioned_selection(user["team_id"])
    if pending:
        st.session_state["last_selection"] = pending

# Target selectors (styled)
envs = load_environments()
env_options = {e["environment_name"].upper(): e["environment_id"] for e in envs}
with card("Target"):
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

# Artifact selection (styled)
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

# Save + History (styled)
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
        if st.button("📦 Save Selection", type="primary", disabled=not all_chosen):
            try:
                selection_key = create_selection_batch(user["team_id"], environment_id, user["username"], target_runtime_id)
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
            try:
                resp = (
                    sb.table("v_team_selection_flat")
                      .select("*")
                      .eq("team_id", user["team_id"])
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

# Provision (styled)
with card("Provision"):
    st.write("Generate a Docker-ready workspace for your latest selection.")
    if st.button("🚀 Provision Now", type="primary"):
        sel = st.session_state.get("last_selection") or next_unprovisioned_selection(user["team_id"])
        if not sel:
            st.info("No unprovisioned selections found. Save a new selection first.")
            st.stop()

        repo_path, port = provision_workspace(sel["selection_key"], user["team_name"], sel["env_name"], sel["runtime_name"])

        try:
            sb.table("team_selection_batch").update({"provision_done_ind": "Y"}).eq("selection_key", sel["selection_key"]).execute()
            st.success(f"Provisioned and marked selection_key {sel['selection_key']} as 'Y' ✅")
        except Exception as e:
            st.warning(f"Workspace created, but failed to mark provisioned: {e}")

        st.code(f"cd {repo_path}\ndocker compose up -d\n# open http://localhost:{port}", language="bash")

# Footer (styled)
with card("Session"):
    if st.button("Sign out"):
        st.session_state.user = None
        st.rerun()