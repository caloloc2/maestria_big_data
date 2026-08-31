# ─────────────────────────────────────────────────────────────
# Detiene el backfill (ambas pasadas) de forma limpia.
# - Mata el bucle de fondo y el `dagster asset materialize` en curso.
# - El día a medias NO queda marcado 'ok' → se retoma solo la próxima vez.
# - NO apaga Docker ni el worker (usa .\scripts\down.ps1 para eso antes de apagar la PC).
#
#   .\scripts\backfill_down.ps1
# ─────────────────────────────────────────────────────────────
$ErrorActionPreference = "Continue"
$repo = Split-Path $PSScriptRoot -Parent

foreach ($pasada in @("operativa", "ventas")) {
    $pidFile = Join-Path $repo "data\backfill_$pasada.pid"
    if (Test-Path $pidFile) {
        $bpid = Get-Content $pidFile -ErrorAction SilentlyContinue
        if ($bpid -and (Get-Process -Id $bpid -ErrorAction SilentlyContinue)) {
            Write-Host "Deteniendo bucle backfill '$pasada' (PID $bpid y sus procesos hijos)..." -ForegroundColor Cyan
            taskkill /PID $bpid /T /F 2>$null | Out-Null
        }
        Remove-Item $pidFile -ErrorAction SilentlyContinue
    }
}

# Cortar el materialize en curso dentro del contenedor (si quedó alguno).
Write-Host "Cerrando cualquier 'dagster asset materialize' en curso..." -ForegroundColor Cyan
docker exec uisrael_dagster_webserver bash -lc "pkill -f 'dagster asset materialize'" 2>$null | Out-Null

Write-Host "Backfill detenido. El progreso quedó guardado; al reanudar continúa solo." -ForegroundColor Green
Write-Host "  (Docker y worker siguen arriba. Para apagar todo antes de mover la PC: .\scripts\down.ps1)"
