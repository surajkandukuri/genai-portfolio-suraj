from typing import Dict, Any
from .supabase_db import get_client

def upsert_workspace(row: Dict[str, Any]) -> Dict[str, Any]:
    """
    Expected keys:
      team_env, selection_key, team_id, team_name, environment_id, environment_name,
      runtime, image_tag, port, table_prefix, storage_prefix, repo_path
    """
    sb = get_client()
    res = sb.table("workspace_registry").upsert(row, on_conflict="team_env").execute()
    return (res.data or [{}])[0]

def fetch_workspace(team_env: str) -> Dict[str, Any]:
    sb = get_client()
    res = sb.table("workspace_registry").select("*").eq("team_env", team_env).single().execute()
    return res.data
