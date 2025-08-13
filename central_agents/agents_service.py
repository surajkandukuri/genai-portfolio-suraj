# central_agents/agents_service.py
import os, re
from pathlib import Path
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from supabase import create_client, Client
import streamlit as st
'''
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

# ── Config ───────────────────────────────────────────────────────────────────
SUPABASE_URL = sget("SUPABASE_URL", "SUPABASE__URL")
SUPABASE_KEY = sget("SUPABASE_SERVICE_ROLE_KEY", "SUPABASE_ANON_KEY", "SUPABASE__SUPABASE_SERVICE_KEY")

#SUPABASE_URL = os.getenv("SUPABASE_URL")
#SUPABASE_SERVICE_ROLE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")


#if not SUPABASE_URL or not SUPABASE_SERVICE_ROLE_KEY:
#    raise RuntimeError("Set SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY for agents service.")

#sb = create_client(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY)

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
'''



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
REPO_ROOT = Path(os.getenv("REPO_ROOT", os.getcwd()))  # repo root on this machine
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



# ── Helpers ──────────────────────────────────────────────────────────────────
REQUIRED_FILES = [
    "docker-compose.yml",
    "Dockerfile",
    "requirements.txt",
    "provisional_success_manifest.json",
    "app.py",
    "README.md",
]

def _slug(s: str) -> str:
    s = re.sub(r"[^a-zA-Z0-9_]+", "_", (s or "").strip().lower())
    return f"a_{s}" if not s or not s[0].isalpha() else s

def _team_env_from_selection(selection_key: int) -> str:
    batch = (sb.table("team_selection_batch")
               .select("team_id, environment_id")
               .eq("selection_key", selection_key)
               .single()
               .execute()
               .data)
    if not batch:
        raise HTTPException(status_code=404, detail=f"selection_key {selection_key} not found")

    team = (sb.table("teams")
              .select("team_name")
              .eq("team_id", batch["team_id"])
              .single()
              .execute()
              .data)
    env = (sb.table("environment")
             .select("environment_name")
             .eq("environment_id", batch["environment_id"])
             .single()
             .execute()
             .data)

    return f"{_slug(team['team_name'])}_{_slug(env['environment_name'])}"

def _update_provisioning_checks(selection_key: int, status: str):
    sb.table("team_selection_batch") \
      .update({"provisioning_checks": status}) \
      .eq("selection_key", selection_key) \
      .execute()

# ── FastAPI app ───────────────────────────────────────────────────────────────
app = FastAPI(title="Central Agents Service", version="1.0.0")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], allow_methods=["*"], allow_headers=["*"], allow_credentials=True,
)

@app.get("/")
def root():
    return {"service": "central-agents", "hint": "Use /health or /docs"}

class SelectionReq(BaseModel):
    selection_key: int

class PstaReq(BaseModel):
    cuisine: str

@app.get("/health")
def health():
    return {"status": "ok", "repo_root": str(REPO_ROOT)}

# Agent 1: ProvisioningChecksAgent
@app.get("/agents/provisioning-checks/health")
def checks_health():
    return {"status": "ok"}

@app.post("/agents/provisioning-checks/run")
def checks_run(req: SelectionReq):
    team_env = _team_env_from_selection(req.selection_key)
    ws_dir = REPO_ROOT / "apps" / team_env
    missing = [f for f in REQUIRED_FILES if not (ws_dir / f).exists()]
    status = "Pass" if not missing else "Fail"
    _update_provisioning_checks(req.selection_key, status)
    return {"status": status, "team_env": team_env, "repo_path": str(ws_dir), "missing": missing}

# Agent 2: SampleLLMWebCall (internet LLM via Groq Mixtral; key stays centralized)
GROQ_API_KEY = os.getenv("GROQ_API_KEY")
GROQ_MODEL_ID = os.getenv("PSTA_MODEL_ID", "mixtral-8x7b-32768")

def _groq_client():
    if not GROQ_API_KEY:
        return None
    try:
        from groq import Groq
        return Groq(api_key=GROQ_API_KEY)
    except Exception:
        return None

@app.get("/agents/psta/health")
def psta_health():
    return {"status": "ok", "model": GROQ_MODEL_ID, "has_key": bool(GROQ_API_KEY)}

@app.post("/agents/psta/generate")
def psta_generate(req: PstaReq):
    client = _groq_client()
    # Deterministic fallback if key/client missing (keeps demo working)
    if client is None:
        name = {
            "indian": "Saffron Ember",
            "italian": "Vespa & Vine",
            "mexican": "Fuego Verde",
            "thai": "Lotus & Lime",
            "chinese": "Jade Lantern",
        }.get(req.cuisine.lower(), "The Pantry")
        items = [f"{name} Starter", f"{name} Salad", f"{name} Signature", f"{name} Classic", f"{name} Dessert"]
        return {"name": name, "items": items}

    # Live call to Groq Mixtral
    name_resp = client.chat.completions.create(
        model=GROQ_MODEL_ID,
        temperature=0.2,
        max_tokens=64,
        messages=[
            {"role": "system", "content": "You are a concise, creative brand namer. Respond with only the name."},
            {"role": "user", "content": f"I want to open a restaurant for {req.cuisine} food. Recommend exactly ONE creative and fancy name. No lists. No quotes."},
        ],
    )
    name = (name_resp.choices[0].message.content or "The Pantry").strip().splitlines()[0].strip(" -•—\"'")

    items_resp = client.chat.completions.create(
        model=GROQ_MODEL_ID,
        temperature=0.2,
        max_tokens=128,
        messages=[
            {"role": "system", "content": "You are a concise menu designer. Return five short dish names, comma-separated."},
            {"role": "user", "content": f"Give me a sample of menu items for '{name}'. Return exactly FIVE, comma-separated. No explanations."},
        ],
    )
    items_txt = (items_resp.choices[0].message.content or "").strip()
    items = [x.strip(" -•—\"'") for x in items_txt.split(",") if x.strip()]
    while len(items) < 5:
        items.append(f"{name} Special {len(items)+1}")
    return {"name": name, "items": items[:5]}
