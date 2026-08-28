# ─────────────────────────────────────────────────────────────
# Levanta TODO con un solo comando:
#   1) Docker (infra + Dagster + tablero + runner de streaming)
#   2) Whisper worker nativo en el host (GPU)
#
#   .\scripts\up.ps1                # arranque normal
#   .\scripts\up.ps1 -Build        # reconstruye imágenes (1ª vez o tras cambiar deps)
#   .\scripts\up.ps1 -Device CPU   # worker en CPU
# ─────────────────────────────────────────────────────────────
param(
    [switch]$Build,
    [ValidateSet("GPU", "CPU")]
    [string]$Device = "GPU"
)
$ErrorActionPreference = "Stop"
$repo    = Split-Path $PSScriptRoot -Parent
$compose = Join-Path $repo "infra\docker-compose.yml"

Write-Host "==> Levantando Docker (infra + Dagster + tablero + runner)..." -ForegroundColor Cyan
if ($Build) {
    docker compose -f $compose up --build -d
} else {
    docker compose -f $compose up -d
}
if ($LASTEXITCODE -ne 0) { Write-Error "docker compose up falló (código $LASTEXITCODE)" }

Write-Host "==> Arrancando el Whisper worker (host, GPU)..." -ForegroundColor Cyan
& (Join-Path $PSScriptRoot "worker-up.ps1") -Device $Device

Write-Host ""
Write-Host "Listo. Dagster: http://localhost:3000  |  Tablero: http://localhost:8501" -ForegroundColor Green
