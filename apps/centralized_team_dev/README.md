# centralized_team_dev — Workspace Starter

**Environment:** dev
**Runtime:** flask
**Selection Key:** 12
**Tables Prefix:** `centralized_team_dev_`
**Storage Prefix:** `centralized_team_dev/`
**Folder:** `apps/centralized_team_dev`

## Run
```bash
cd apps/centralized_team_dev
docker compose up -d
```
Open: http://localhost:8510

## 🔌 Connect this app to Central Agents (Groq & Checks)

Connect (uses host.docker.internal — no custom Docker networks):
```bash
# macOS/Linux
./enable_agents.sh
# Windows PowerShell
.\enable_agents.ps1
```

Disconnect (isolate the app):
```bash
# macOS/Linux
./disable_agents.sh
# Windows PowerShell
.\disable_agents.ps1
```

This simply sets/clears `LLM_GATEWAY_URL` in `.env` and restarts the container.
The gateway runs on your host at: http://localhost:7000
From inside the container, the app reaches it at: http://host.docker.internal:7000
