# ─────────────────────────────────────────────────────────────
# Detiene el Whisper worker nativo del host (arrancado con worker-up.ps1).
# Busca primero por el PID guardado y, como respaldo, por la línea de comando.
#
#   .\scripts\worker-down.ps1
# ─────────────────────────────────────────────────────────────
$ErrorActionPreference = "Stop"

$repo    = Split-Path $PSScriptRoot -Parent
$pidFile = Join-Path $repo "data\worker.pid"
$parado  = $false

# 1) Por PID guardado
if (Test-Path $pidFile) {
    $savedPid = Get-Content $pidFile -ErrorAction SilentlyContinue
    if ($savedPid -and (Get-Process -Id $savedPid -ErrorAction SilentlyContinue)) {
        Stop-Process -Id $savedPid -Force
        Write-Host "Worker detenido (PID $savedPid)." -ForegroundColor Green
        $parado = $true
    }
    Remove-Item $pidFile -ErrorAction SilentlyContinue
}

# 2) Respaldo: cualquier python.exe que ejecute whisper_worker\worker.py
$procs = Get-CimInstance Win32_Process -Filter "Name='python.exe'" |
    Where-Object { $_.CommandLine -like "*whisper_worker\worker.py*" -or $_.CommandLine -like "*whisper_worker/worker.py*" }
foreach ($p in $procs) {
    Stop-Process -Id $p.ProcessId -Force
    Write-Host "Worker detenido (PID $($p.ProcessId), por línea de comando)." -ForegroundColor Green
    $parado = $true
}

if (-not $parado) {
    Write-Host "No había ningún worker corriendo." -ForegroundColor Yellow
}
