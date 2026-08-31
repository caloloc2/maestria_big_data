# ─────────────────────────────────────────────────────────────
# Motor del backfill reanudable (NO se llama directo — lo lanza backfill_up.ps1).
# Recorre las fechas [Start..End] día a día para una PASADA:
#   operativa -> bronze_cdr, silver_calls           (KPIs operativos, sin ASR)
#   ventas    -> bronze_audio, silver_transcriptions, gold_evaluations (solo largas >600s)
# El progreso se guarda en servido.backfill_progress (PostgreSQL) → reanuda solo.
# ─────────────────────────────────────────────────────────────
param(
    [Parameter(Mandatory)][ValidateSet("operativa", "ventas")][string]$Pasada,
    [string]$Start = "2025-01-01",
    [string]$End = ""
)
$ErrorActionPreference = "Continue"
$repo = Split-Path $PSScriptRoot -Parent
if (-not $End) { $End = (Get-Date).ToString("yyyy-MM-dd") }

function PgQuery([string]$sql) {
    (docker exec uisrael_postgres psql -U dagster -d dagster -t -A -c $sql 2>$null | Out-String).Trim()
}
function PgExec([string]$sql) {
    docker exec uisrael_postgres psql -U dagster -d dagster -c $sql 2>$null | Out-Null
}

# Tabla de progreso (idempotente).
PgExec "CREATE SCHEMA IF NOT EXISTS servido; CREATE TABLE IF NOT EXISTS servido.backfill_progress (fecha date, pasada text, estado text, n integer, ts timestamp DEFAULT now(), PRIMARY KEY (fecha, pasada));"

Write-Host "[$Pasada] backfill $Start -> $End (arranca $(Get-Date -Format 'HH:mm:ss'))"

$d = [datetime]::ParseExact($Start, "yyyy-MM-dd", $null)
$endD = [datetime]::ParseExact($End, "yyyy-MM-dd", $null)

while ($d -le $endD) {
    $day = $d.ToString("yyyy-MM-dd")

    $ok = PgQuery "SELECT 1 FROM servido.backfill_progress WHERE fecha='$day' AND pasada='$Pasada' AND estado='ok' LIMIT 1;"
    if ($ok -eq "1") { $d = $d.AddDays(1); continue }

    # ventas necesita que operativa haya hecho el día (para leer servido.llamadas).
    # Si operativa aún no lo hizo: ESPERA (si su bucle sigue vivo) o hace la cadena
    # completa por su cuenta (si operativa no está corriendo). NUNCA salta el día.
    $fullChain = $false
    if ($Pasada -eq "ventas") {
        while ($true) {
            $op = PgQuery "SELECT 1 FROM servido.backfill_progress WHERE fecha='$day' AND pasada='operativa' AND estado='ok' LIMIT 1;"
            if ($op -eq "1") { break }
            $opPidFile = Join-Path $repo "data\backfill_operativa.pid"
            $opAlive = $false
            if (Test-Path $opPidFile) {
                $opPid = Get-Content $opPidFile -ErrorAction SilentlyContinue
                if ($opPid -and (Get-Process -Id $opPid -ErrorAction SilentlyContinue)) { $opAlive = $true }
            }
            if ($opAlive) {
                Write-Host "[ventas] $day esperando a que operativa lo termine... ($(Get-Date -Format 'HH:mm:ss'))"
                Start-Sleep -Seconds 20
                continue   # reintenta el MISMO día (no avanza la fecha)
            }
            Write-Host "[ventas] $day operativa no está corriendo -> proceso la cadena completa yo mismo."
            $fullChain = $true
            break
        }
    }

    Write-Host "[$Pasada] $day procesando... ($(Get-Date -Format 'HH:mm:ss'))"
    $t0 = Get-Date

    if ($Pasada -eq "operativa") {
        docker exec uisrael_dagster_webserver bash -lc "cd /opt/dagster/app && dagster asset materialize -f src/definitions.py --select bronze_cdr,silver_calls --partition $day"
    }
    elseif ($fullChain) {
        # operativa no está corriendo: ventas hace TODA la cadena (incluye bronze_cdr+silver_calls).
        docker exec -e AUDIO_MIN_SECS=600 -e ASR_MIN_SECS=600 -e ASR_LIMIT=0 -e ASR_TIMEOUT=7200 -e EVAL_MIN_SECS=600 `
            uisrael_dagster_webserver bash -lc "cd /opt/dagster/app && dagster asset materialize -f src/definitions.py --select bronze_cdr,silver_calls,bronze_audio,silver_transcriptions,gold_evaluations --partition $day"
    }
    else {
        # operativa ya dejó servido.llamadas del día: ventas solo la capa de audio/ASR/Gemini.
        docker exec -e AUDIO_MIN_SECS=600 -e ASR_MIN_SECS=600 -e ASR_LIMIT=0 -e ASR_TIMEOUT=7200 -e EVAL_MIN_SECS=600 `
            uisrael_dagster_webserver bash -lc "cd /opt/dagster/app && dagster asset materialize -f src/definitions.py --select bronze_audio,silver_transcriptions,gold_evaluations --partition $day"
    }
    $rc = $LASTEXITCODE
    $secs = [int]((Get-Date) - $t0).TotalSeconds

    if ($rc -eq 0) {
        PgExec "INSERT INTO servido.backfill_progress (fecha,pasada,estado,ts) VALUES ('$day','$Pasada','ok',now()) ON CONFLICT (fecha,pasada) DO UPDATE SET estado='ok', ts=now();"
        Write-Host "[$Pasada] $day OK (${secs}s)"
    }
    else {
        PgExec "INSERT INTO servido.backfill_progress (fecha,pasada,estado,ts) VALUES ('$day','$Pasada','error',now()) ON CONFLICT (fecha,pasada) DO UPDATE SET estado='error', ts=now();"
        Write-Host "[$Pasada] $day ERROR rc=$rc (${secs}s) -> continuo con el siguiente"
    }

    $d = $d.AddDays(1)
}

Write-Host "[$Pasada] backfill COMPLETO $Start -> $End ($(Get-Date -Format 'HH:mm:ss'))"
