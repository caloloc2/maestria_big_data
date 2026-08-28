# ─────────────────────────────────────────────────────────────
# Arranca el Whisper worker NATIVO en el host (GPU/OpenVINO Arc).
# Corre en segundo plano; guarda el PID en data\worker.pid y los
# logs en data\worker.out.log / data\worker.err.log.
#
#   .\scripts\worker-up.ps1            # GPU (por defecto)
#   .\scripts\worker-up.ps1 -Device CPU
#
# Parar:  .\scripts\worker-down.ps1
# ─────────────────────────────────────────────────────────────
param(
    [ValidateSet("GPU", "CPU")]
    [string]$Device = "GPU"
)
$ErrorActionPreference = "Stop"

$repo   = Split-Path $PSScriptRoot -Parent
$python = Join-Path $repo "whisper_worker\.venv\Scripts\python.exe"
$script = "whisper_worker\worker.py"
$pidFile = Join-Path $repo "data\worker.pid"
$outLog  = Join-Path $repo "data\worker.out.log"
$errLog  = Join-Path $repo "data\worker.err.log"

if (-not (Test-Path $python)) {
    Write-Error "No se encontró el venv del worker: $python"
}

# ¿Ya hay un worker vivo? (por PID guardado o por línea de comando)
if (Test-Path $pidFile) {
    $oldPid = Get-Content $pidFile -ErrorAction SilentlyContinue
    if ($oldPid -and (Get-Process -Id $oldPid -ErrorAction SilentlyContinue)) {
        Write-Host "El worker ya está corriendo (PID $oldPid). Usa worker-down.ps1 para pararlo." -ForegroundColor Yellow
        exit 0
    }
}
$yaCorre = Get-CimInstance Win32_Process -Filter "Name='python.exe'" |
    Where-Object { $_.CommandLine -like "*whisper_worker\worker.py*" -or $_.CommandLine -like "*whisper_worker/worker.py*" }
if ($yaCorre) {
    Write-Host "Ya hay un proceso del worker vivo (PID $($yaCorre.ProcessId)). No se arranca otro." -ForegroundColor Yellow
    exit 0
}

# Variables del worker (host): Kafka en localhost:29092 y MinIO en localhost:9000
# usan los defaults del código; aquí solo fijamos device, credenciales y encoding.
$env:PYTHONIOENCODING = "utf-8"
$env:PYTHONUNBUFFERED  = "1"        # log en tiempo real (stdout sin buffer al redirigir)
$env:ASR_DEVICE       = $Device
$env:MAX_MSGS         = "0"          # 0 = corre indefinidamente
$env:MINIO_ACCESS_KEY = if ($env:MINIO_ROOT_USER)     { $env:MINIO_ROOT_USER }     else { "minioadmin" }
$env:MINIO_SECRET_KEY = if ($env:MINIO_ROOT_PASSWORD) { $env:MINIO_ROOT_PASSWORD } else { "minioadmin" }

$proc = Start-Process -FilePath $python -ArgumentList $script `
    -WorkingDirectory $repo -WindowStyle Hidden -PassThru `
    -RedirectStandardOutput $outLog -RedirectStandardError $errLog

$proc.Id | Out-File -FilePath $pidFile -Encoding ascii
Write-Host "Whisper worker arrancado (device=$Device, PID $($proc.Id))." -ForegroundColor Green
Write-Host "  logs: data\worker.out.log  |  errores: data\worker.err.log"
