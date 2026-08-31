# ─────────────────────────────────────────────────────────────
# Inicia (o REANUDA) el backfill de una pasada, en segundo plano.
# El progreso vive en PostgreSQL → siempre continúa donde se quedó, sin decirle nada.
#
#   .\scripts\backfill_up.ps1 -Pasada operativa     # rápida (KPIs, sin ASR)
#   .\scripts\backfill_up.ps1 -Pasada ventas        # lenta (solo largas + Gemini)
#   .\scripts\backfill_up.ps1 -Pasada ventas -Start 2025-01-01 -End 2025-06-30
#
# Detener:  .\scripts\backfill_down.ps1
# Requisitos: Docker arriba (.\scripts\up.ps1 -NoStream). Para 'ventas', el worker vivo.
# ─────────────────────────────────────────────────────────────
param(
    [Parameter(Mandatory)][ValidateSet("operativa", "ventas")][string]$Pasada,
    [string]$Start = "2025-01-01",
    [string]$End = ""
)
$ErrorActionPreference = "Stop"
$repo = Split-Path $PSScriptRoot -Parent
$loop = Join-Path $PSScriptRoot "_backfill_loop.ps1"
$pidFile = Join-Path $repo "data\backfill_$Pasada.pid"
$log = Join-Path $repo "data\backfill_$Pasada.out.log"

# ¿Docker arriba?
$up = docker ps --filter "name=uisrael_dagster_webserver" --format "{{.Names}}" 2>$null
if (-not $up) { Write-Error "Docker no está arriba. Corre primero: .\scripts\up.ps1 -NoStream" }

# ¿Streaming apagado? (comparte el único worker; en backfill debe estar detenido)
$strm = docker ps --filter "name=uisrael_streaming_runner" --format "{{.Names}}" 2>$null
if ($strm) {
    Write-Host "streaming_runner está corriendo; deteniéndolo (comparte worker con el backfill)..." -ForegroundColor Yellow
    docker stop uisrael_streaming_runner 2>$null | Out-Null
}

# Para 'ventas' se necesita el worker vivo.
if ($Pasada -eq "ventas") {
    $wpid = if (Test-Path (Join-Path $repo "data\worker.pid")) { Get-Content (Join-Path $repo "data\worker.pid") } else { $null }
    $alive = $wpid -and (Get-Process -Id $wpid -ErrorAction SilentlyContinue)
    if (-not $alive) { Write-Error "El worker Whisper no está vivo (necesario para 'ventas'). Corre .\scripts\worker-up.ps1 o .\scripts\up.ps1 -NoStream" }
}

# ¿Ya hay un backfill de esta pasada corriendo?
if (Test-Path $pidFile) {
    $old = Get-Content $pidFile -ErrorAction SilentlyContinue
    if ($old -and (Get-Process -Id $old -ErrorAction SilentlyContinue)) {
        Write-Host "Ya hay un backfill '$Pasada' corriendo (PID $old). Usa backfill_down.ps1 para pararlo." -ForegroundColor Yellow
        exit 0
    }
}

$argList = @("-NoProfile", "-ExecutionPolicy", "Bypass", "-File", "`"$loop`"", "-Pasada", $Pasada, "-Start", $Start)
if ($End) { $argList += @("-End", $End) }

$proc = Start-Process -FilePath "powershell.exe" -ArgumentList $argList `
    -WorkingDirectory $repo -WindowStyle Hidden -PassThru `
    -RedirectStandardOutput $log -RedirectStandardError ($log -replace '\.out\.log$', '.err.log')

$proc.Id | Out-File -FilePath $pidFile -Encoding ascii
Write-Host "Backfill '$Pasada' arrancado (PID $($proc.Id)) desde $Start." -ForegroundColor Green
Write-Host "  progreso en vivo:  Get-Content .\data\backfill_$Pasada.out.log -Wait -Tail 20"
Write-Host "  detener:           .\scripts\backfill_down.ps1"
