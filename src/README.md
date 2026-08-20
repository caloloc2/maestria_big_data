# `src/` — Código de la solución (monorepo)

Cada subcarpeta es un "departamento" del pipeline. En la **Fase 0** se crean como
esqueleto (casi vacíos); la lógica real se llena en las fases 2–7.

| Carpeta | Departamento | Zona Medallion | Se implementa en |
|---|---|---|---|
| `ingestion/` | Recepción: conector al CDR (MySQL) + productor Kafka | 🥉 Bronce | Fases 2, 5 |
| `processing/` | Preparación: limpieza, cruce CDR↔grabación, jobs Spark + activos Dagster | 🥉→🥈 Bronce/Plata | Fase 2 |
| `asr/` | Transcripción: coordina el paso audio→texto con el `whisper_worker` | 🥈 Plata | Fase 3 |
| `anonymization/` | Privacidad (frontera): Presidio + spaCy + reglas propias | 🥉→🥈 | Fase 3 |
| `analysis/` | Evaluación: cliente Gemini + rúbrica + validación `pydantic` | 🥈→🥇 | Fase 4 |
| `anomalies/` | Anomalías: IsolationForest / z-score + series temporales | 🥇 Oro | Fase 6 |
| `serving/` | Datos servidos: modelos y vistas en PostgreSQL | 🥇 Oro | Fases 2, 7 |
| `dashboard/` | Presentación: tablero Streamlit por rol | — | Fase 7 |

El **Whisper worker** corre **fuera de Docker** (GPU Arc + OpenVINO) y vive en
`whisper_worker/` (raíz del repo), no aquí. Ver `fases.md` (tracker maestro).
