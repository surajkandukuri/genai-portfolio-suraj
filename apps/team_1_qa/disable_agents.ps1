Set-Content -Path .env -Value ""
docker compose restart
Write-Host "Agents DISABLED (gateway URL cleared)."
