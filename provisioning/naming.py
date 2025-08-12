import re

def slugify(s: str) -> str:
    s = re.sub(r'[^a-zA-Z0-9_]+', '_', s.strip().lower())
    return f"a_{s}" if not s or not s[0].isalpha() else s

def team_env_slug(team_name: str, env_name: str) -> str:
    return f"{slugify(team_name)}_{slugify(env_name)}"

def prefixes(team_env: str) -> tuple[str, str]:
    return f"{team_env}_", f"{team_env}/"  # (table_prefix, storage_prefix)

def default_port(runtime: str) -> int:
    return {"django":8000, "flask":8501, ".net":8080, "java":8080, "python":8001}.get(runtime.lower(), 8501)
