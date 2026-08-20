# Entorno de diagnóstico — Fase 1 / Ruta C

Slice mínimo y dockerizado para cerrar la **Fase 1** (diagnóstico de datos) sin
depender del stack completo de la Fase 0. Levanta un JupyterLab con `pandas` +
conector MySQL para perfilar el CDR en vivo y cruzarlo con el índice de audio.

## Arranque (una línea, desde la raíz del repo)

1. Copiar la plantilla de entorno y rellenar credenciales:
   ```bash
   cp .env.example .env      # editar .env con IP/usuario/clave reales
   ```
2. Levantar el contenedor:
   ```bash
   docker compose -f infra/diagnostico/docker-compose.yml up --build
   ```
3. Abrir `http://localhost:8888/lab` → ejecutar `notebooks/00_diagnostico.py`
   (Jupyter lo abre como notebook gracias a Jupytext).

## Índice de audio (corre en el servidor Asterisk, no en el contenedor)

```bash
# en el servidor Asterisk (CentOS), horario no laboral:
scp scripts/diagnostico_audio.sh USER@IP_ASTERISK:/tmp/diag/
ssh USER@IP_ASTERISK 'chmod +x /tmp/diag/diagnostico_audio.sh && \
    nohup /tmp/diag/diagnostico_audio.sh >/tmp/diag/run.log 2>&1 & disown'

# al terminar, descargar el índice a la laptop:
scp USER@IP_ASTERISK:/tmp/diag/audio_index.tsv.gz ./data/diag/
```

El notebook detecta `data/diag/audio_index.tsv.gz` automáticamente y completa la
caracterización del audio (tamaño total, `.mp3` vs `.wav`, regados) y el cruce
CDR↔grabación.

## Salidas (gitignored, en `data/diag/`)

- `figuras/` — PNG para el capítulo (por año, contactabilidad, hora, día).
- `tablas/tablas_diagnostico.xlsx` — Tablas 1 y 2.
- `tablas/tabla1b_audio_resumen.csv` — resumen del filesystem de audio.

## Exportar el notebook a `.ipynb` (entregable del capítulo)

```bash
docker compose -f infra/diagnostico/docker-compose.yml exec diagnostico \
    jupytext --to notebook notebooks/00_diagnostico.py
```
