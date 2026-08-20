#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────
# diagnostico_audio.sh — Caracterización del filesystem de grabaciones.
# CORRE EN EL SERVIDOR ASTERISK (CentOS 7.5), NO en el contenedor.
#
# Objetivo: construir un ÍNDICE de todos los audios (ruta, tamaño, fecha)
# sin copiar los ~100 GB y SIN impactar a Asterisk en producción
# (ionice + nice => prioridad de E/S y CPU mínimas).
#
# Uso recomendado (horario no laboral):
#   chmod +x diagnostico_audio.sh
#   nohup ./diagnostico_audio.sh >/tmp/diag/run.log 2>&1 &
#   disown
# o agendado:   at 19:35 -f diagnostico_audio.sh
#
# Salida en /tmp/diag/:
#   df.txt              -> espacio del disco
#   du_total.txt        -> tamaño total de la carpeta de grabaciones
#   du_L1.txt           -> tamaño por carpeta de primer nivel (por extensión/agente)
#   audio_index.tsv.gz  -> ÍNDICE COMPLETO: ruta \t bytes \t epoch_mtime
#   resumen.txt         -> conteos rápidos (mp3 vs wav, total de archivos)
# ─────────────────────────────────────────────────────────────
set -euo pipefail

ROOT="${1:-/home/grabacion/monitor/111111111111}"
OUT="/tmp/diag"
LP="ionice -c3 nice -n19"   # low priority: no molesta a producción

mkdir -p "$OUT"
echo "[$(date)] Iniciando diagnóstico de audio sobre: $ROOT"

# 1) Espacio en disco y tamaño total
df -h                 > "$OUT/df.txt"        2>&1 || true
$LP du -sh "$ROOT"    > "$OUT/du_total.txt"  2>&1 || true

# 2) Tamaño por carpeta de primer nivel (cada extensión suele tener su carpeta)
$LP du -sh "$ROOT"/*/ > "$OUT/du_L1.txt"     2>&1 || true

# 3) ÍNDICE COMPLETO — una línea por archivo de audio: ruta \t bytes \t epoch
#    (GNU find soporta -printf en CentOS). Solo .mp3 y .wav.
echo "[$(date)] Construyendo índice (puede tardar en 100 GB)..."
$LP find "$ROOT" -type f \( -iname '*.mp3' -o -iname '*.wav' \) \
    -printf '%p\t%s\t%T@\n' 2>>"$OUT/find_errors.txt" \
    | gzip -c > "$OUT/audio_index.tsv.gz"

# 4) Resumen rápido (derivado del índice, sin volver a recorrer el disco)
TOTAL=$(zcat "$OUT/audio_index.tsv.gz" | wc -l)
MP3=$(zcat "$OUT/audio_index.tsv.gz" | grep -ic '\.mp3' || true)
WAV=$(zcat "$OUT/audio_index.tsv.gz" | grep -ic '\.wav' || true)
{
  echo "Fecha        : $(date)"
  echo "Raíz         : $ROOT"
  echo "Archivos     : $TOTAL"
  echo "  .mp3       : $MP3"
  echo "  .wav       : $WAV  (nunca convertidos a mp3)"
} > "$OUT/resumen.txt"

echo "[$(date)] LISTO. Índice en $OUT/audio_index.tsv.gz"
cat "$OUT/resumen.txt"

# Descargar a la laptop desde el contenedor/laptop, por ejemplo:
#   scp usuario@IP_ASTERISK:/tmp/diag/audio_index.tsv.gz ./data/diag/
