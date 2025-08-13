Set-Content -Path .env -Value "LLM_GATEWAY_URL=http://host.docker.internal:7000"
docker compose restart
Write-Host "Agents ENABLED (gateway URL set)."
