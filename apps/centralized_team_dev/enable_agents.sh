#!/usr/bin/env bash
set -e
echo "LLM_GATEWAY_URL=http://host.docker.internal:7000" > .env
docker compose restart
echo "Agents ENABLED (gateway URL set)."
