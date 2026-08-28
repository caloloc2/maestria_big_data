# ─────────────────────────────────────────────────────────────
# Detiene TODO con un solo comando:
#   1) Whisper worker nativo del host
#   2) Docker (infra + Dagster + tablero + runner de streaming)
#
# NO usa `-v`: los volúmenes (Postgres, MinIO, Kafka) se conservan.
#
#   .\scripts\down.ps1
# ─────────────────────────────────────────────────────────────
$ErrorActionPreference = "Stop"
$repo    = Split-Path $PSScriptRoot -Parent
$compose = Join-Path $repo "infra\docker-compose.yml"

Write-Host "==> Deteniendo el Whisper worker (host)..." -ForegroundColor Cyan
& (Join-Path $PSScriptRoot "worker-down.ps1")

Write-Host "==> Bajando Docker (sin borrar volúmenes)..." -ForegroundColor Cyan
docker compose -f $compose down

Write-Host "Todo detenido. Los datos (Postgres/MinIO/Kafka) se conservan." -ForegroundColor Green
