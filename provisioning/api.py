# provisioning/api.py
from __future__ import annotations
from fastapi import FastAPI, Query
from datetime import datetime

app = FastAPI(title="Provisioning API")

@app.get("/health")
def root_health():
    return {"ok": True, "service": "provisioning-api", "time": datetime.utcnow().isoformat() + "Z"}

@app.get("/gateway/health")
def gateway_health():
    # This is the same process our autostart spins up
    return {"ok": True, "component": "gateway", "message": "FastAPI gateway reachable"}

@app.get("/checks/health")
def checks_health():
    # Stubbed example – replace with real checks as needed
    return {
        "ok": True,
        "checks": {
            "db": "ok",
            "secrets": "ok",
            "llm_gateway": "disconnected",  # flip to "ok" when wired
        },
        "time": datetime.utcnow().isoformat() + "Z",
    }

@app.get("/agents/postprovision/health")
def postprovision_agent_health():
    return {"ok": True, "agent": "postprovision", "status": "ready"}

@app.get("/agents/postprovision/try")
def postprovision_agent_try():
    # Minimal demo payload
    return {
        "ok": True,
        "agent": "postprovision",
        "result": "Sample check complete.",
        "time": datetime.utcnow().isoformat() + "Z",
    }

SAMPLES = {
    "indian": ["Chana Masala", "Palak Paneer", "Masala Dosa"],
    "italian": ["Margherita Pizza", "Penne Arrabbiata", "Tiramisu"],
    "mexican": ["Tacos al Pastor", "Chilaquiles", "Elote"],
    "japanese": ["Chicken Teriyaki", "Katsu Curry", "Miso Soup"],
}

@app.get("/agents/sample-menu")
def sample_menu(cuisine: str = Query("indian", min_length=2)):
    items = SAMPLES.get(cuisine.strip().lower())
    if not items:
        # default fallback
        items = SAMPLES["indian"]
    return {"ok": True, "cuisine": cuisine, "items": items}
