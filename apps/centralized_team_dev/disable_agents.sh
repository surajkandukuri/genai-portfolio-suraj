#!/usr/bin/env bash
set -e
: > .env
docker compose restart
echo "Agents DISABLED (gateway URL cleared)."
