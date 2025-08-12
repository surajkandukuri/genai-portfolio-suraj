from pathlib import Path
from textwrap import dedent
import json
import shutil

# Compose template (single-service with build; change to image: if you publish prebuilt)
COMPOSE_TMPL = """\
version: "3.9"
services:
  app:
    build:
      context: .
      dockerfile: Dockerfile
    container_name: {team_env}
    ports:
      - "{port}:{port}"
    environment:
      TEAM_ENV: "{team_env}"
      TABLE_PREFIX: "{table_prefix}"
      STORAGE_PREFIX: "{storage_prefix}"
"""

# Minimal README with exact steps
README_TMPL = """\
# {team_env} — Workspace Starter

**Environment:** {environment_name}  
**Runtime:** {runtime}  
**Selection Key:** {selection_key}  
**Tables Prefix:** `{table_prefix}`  
**Storage Prefix:** `{storage_prefix}`  
**Folder:** `{repo_path}`

## 1) Install Docker
- Windows/Mac: Docker Desktop
- Linux: Docker Engine
Verify: `docker --version`

## 2) Run locally (build from this folder)
```bash
cd {repo_path}
docker compose up -d
