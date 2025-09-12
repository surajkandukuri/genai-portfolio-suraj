# pages/certificates.py
import os
import streamlit as st
from supabase import create_client, Client

st.markdown("### 📚 Certifications & Continuous Learning")
st.caption("Pulled from Supabase Storage → kpidrifthunter/assets")

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

SUPABASE_URL  = _sget("SUPABASE_URL", "SUPABASE__URL")
SUPABASE_KEY  = _sget("SUPABASE_SERVICE_ROLE_KEY", "SUPABASE_SERVICE_KEY", "SUPABASE_ANON_KEY", "SUPABASE__SUPABASE_SERVICE_KEY")
KDH_BUCKET    = _sget("KDH_BUCKET", default="kpidrifthunter")

BUCKET = "kpidrifthunter"
FOLDER = "assets"  # no leading slash

if not SUPABASE_URL or not SUPABASE_KEY:
    st.error("Missing Supabase config. Add SUPABASE_URL and SUPABASE_ANON_KEY (or SERVICE_ROLE) in .streamlit/secrets.toml")
    st.stop()

sb: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# List objects under the folder
try:
    objects = sb.storage.from_(BUCKET).list(FOLDER)
except Exception as e:
    st.error(f"Could not list Supabase Storage objects: {e}")
    st.stop()

# Only .png
pngs = [o for o in (objects or []) if (o.get("name","").lower().endswith(".png"))]
if not pngs:
    st.info(f"No PNG certificates found under {BUCKET}/{FOLDER}")
    st.stop()

# Optional captions
CAPTIONS = {
    "GoogleCertificate_1.png": "✅ Google ADK Certification (Completed, Aug 2025)",
    "GoogleCertificate_2.png": "✅ Google ADK – Course Completion",
}

# Render using SIGNED URLs (works even if bucket/folder is private)
cols = st.columns(2)
for i, obj in enumerate(sorted(pngs, key=lambda o: o["name"])):
    name = obj["name"]
    path = f"{FOLDER}/{name}"  # e.g., assets/GoogleCertificate_1.png

    # create a signed URL (1 hour). Adjust ttl as needed.
    signed = sb.storage.from_(BUCKET).create_signed_url(path, 3600)
    url = signed.get("signedURL") or signed.get("signedUrl") or signed.get("signed_url")

    # Fallback to public URL if signed not available (in case bucket is public)
    if not url:
        pub = sb.storage.from_(BUCKET).get_public_url(path)
        url = pub if isinstance(pub, str) else pub.get("publicUrl") or pub.get("publicURL")

    with cols[i % 2]:
        st.image(url, caption=CAPTIONS.get(name, name), use_container_width=True)
