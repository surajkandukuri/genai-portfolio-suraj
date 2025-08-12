from supabase import create_client, Client
from .config import sget

def get_client() -> Client:
    url = sget("SUPABASE_URL", "SUPABASE__URL")
    key = sget("SUPABASE_SERVICE_ROLE_KEY", "SUPABASE_ANON_KEY", "SUPABASE__SUPABASE_SERVICE_KEY")
    if not url or not key:
        raise RuntimeError("Missing Supabase config")
    return create_client(url, key)
