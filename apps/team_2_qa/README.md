# team_2_qa — Workspace Starter

**Environment:** qa
**Runtime:** python
**Selection Key:** 13
**Tables Prefix:** `team_2_qa_`
**Storage Prefix:** `team_2_qa/`
**Folder:** `apps/team_2_qa`

## Run
```bash
cd apps/team_2_qa
docker compose up -d
```
Open: http://localhost:8511

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

This sets/clears `LLM_GATEWAY_URL` in `.env` and restarts the container.
Gateway on host: http://localhost:7000
In-container URL: http://host.docker.internal:7000
