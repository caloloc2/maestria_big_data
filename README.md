# Arquitectura Big Data para la evaluación de calidad, identificación de prácticas críticas y predicción de anomalías en el desempeño de agentes de call center

**Trabajo de Titulación — Maestría en Big Data y Ciencia de Datos**
Universidad Tecnológica Israel (UISRAEL) · Escuela de Posgrados “ESPOG” · Quito, Ecuador · 2026
Autor: **Carlos Enrique Miño Flores**

---

## 1. Descripción del proyecto

Este repositorio contiene el **trabajo de titulación** (documentación académica) y la
**implementación técnica** de una **arquitectura Big Data** que integra **analítica en
tiempo real (*streaming*)** y **Modelos de Lenguaje de Gran Escala (LLM)** para procesar
de forma concurrente los **metadatos de llamadas (CDR)** y las **grabaciones de audio**
de un *call center* de venta saliente, con el fin de **evaluar la calidad de la atención**,
**identificar prácticas críticas** y **predecir anomalías en el desempeño** de los agentes.

El problema parte de una limitación real: la auditoría de calidad de las llamadas es
**manual** y cubre una fracción mínima de las interacciones, lo que impide detectar a
tiempo malas prácticas, riesgos de incumplimiento normativo y caídas de desempeño.

**Escala real (verificada en el diagnóstico):** el universo histórico asciende a
**más de 24,7 millones de registros CDR** (enero 2018 – agosto 2026) y **858 GB de
grabaciones** (≈10 millones de archivos). El alcance del proyecto se acota al *call
center* de ventas (extensiones **200–299**, llamadas **salientes**): **7 061 447
grabaciones · 424,2 GB · 100 agentes**.

## 2. Pregunta de investigación e hipótesis

> **Pregunta:** ¿Cómo diseñar e implementar una arquitectura que integre LLM y
> analítica en tiempo real sobre registros CDR y grabaciones de audio para identificar
> de forma escalable prácticas de atención críticas y predecir anomalías en el
> desempeño de los agentes de un *call center*?

> **Hipótesis:** una arquitectura modular que combine analítica de *streaming* y LLM
> sobre metadatos CDR y transcripciones de audio permitirá analizar las interacciones a
> escala con una precisión estadísticamente superior en los KPIs de calidad y reducir el
> tiempo de detección de anomalías frente a la auditoría manual.

## 3. Objetivos

**General:** Implementar una **arquitectura Big Data** para el procesamiento concurrente
de metadatos CDR y grabaciones de audio no estructuradas, mediante la integración de
analítica en tiempo real (*streaming*) y LLM, con el fin de **evaluar la calidad de la
atención, identificar prácticas críticas y predecir anomalías en el desempeño** de los
agentes de un *call center*.

**Específicos:**
1. **Contextualizar** los fundamentos teóricos (analítica en tiempo real, ASR y LLM).
2. **Diagnosticar** el proceso de auditoría actual y la calidad/trazabilidad de los datos.
3. **Desarrollar** la arquitectura (ingesta *streaming*, procesamiento por lotes, transcripción con anonimización y análisis con LLM).
4. **Validar** el impacto en un entorno real de la empresa frente a la auditoría manual (línea base).

## 4. Estado del proyecto

El proyecto avanza en dos frentes:

**A) Frente académico** — el plan de titulación fue **aprobado por el tutor**; el
documento de titulación (Capítulos I y II) se mantiene en `documentos/` siguiendo la
plantilla UISRAEL y el **estilo IEEE** (citas numeradas gestionadas con Mendeley).

**B) Frente de implementación** — pipeline en construcción por fases (tracker maestro en
[`fases.md`](fases.md)):

| Fase | Contenido | Estado |
|------|-----------|--------|
| 0 | Stack base dockerizado (Dagster, Kafka, Spark, Streamlit, worker) | ✅ |
| 1 | Diagnóstico de datos (perfilado CDR + índice de audio + *record linkage*) | ✅ |
| 2 | Preparación *batch* con Spark (Bronce→Plata, cruce CDR↔grabación 100 %) | ✅ |
| 3 | ASR + diarización + anonimización (Whisper/OpenVINO + Presidio) | ✅ núcleo |
| 4 | Análisis de calidad/cumplimiento híbrido (rúbrica determinista + Gemini) | ✅ núcleo |
| 5 | *Streaming* en tiempo real (worker persistente, Kafka near-real-time) | ⏳ pendiente |
| 7 | Tablero analítico (Streamlit) | ⏳ pendiente |

**Hitos recientes:** el lago de datos **MinIO** (object store S3, `s3a://`) aloja las
zonas **Bronce/Plata**; la capa servida sigue en **PostgreSQL**. Evidencia visual del
pipeline en el Dagster UI documentada en [`dagster.md`](dagster.md). El documento de
avance para el tutor está en [`presentacion_1.md`](presentacion_1.md).

> **Registro técnico completo** (todo lo analizado, ejecutado, validado y corregido por
> fase): [`docs/bitacora_tecnica.md`](docs/bitacora_tecnica.md).

## 5. Estructura del repositorio

```
.
├── fases.md                       # ⭐ Tracker maestro de fases (manda sobre todo)
├── presentacion_1.md              # Documento de avance para el tutor
├── dagster.md                     # Evidencia visual del pipeline (Dagster UI, capturas)
├── plan_titulacion.(md/pdf/docx)  # Plan de titulación (frente académico)
├── documentos/                    # Documento de titulación (Cap. I y II) en .docx
├── src/                           # Código del pipeline (paquetes Python)
│   ├── definitions.py             #   Dagster: activos Medallion (Bronce/Plata/Oro)
│   ├── ingestion/  processing/    #   ingesta Kafka · preparación batch Spark
│   ├── asr/  anonymization/       #   integración ASR · anonimización
│   ├── analysis/  anomalies/      #   rúbrica + Gemini · detección de anomalías
│   └── serving/  dashboard/       #   capa servida (PostgreSQL) · tablero
├── whisper_worker/                # Worker ASR nativo (OpenVINO) + diarización + anonimización
├── infra/                         # Docker Compose, Dagster, Streamlit, diagnóstico
├── notebooks/                     # 00_diagnostico (Fase 1, reproducible)
├── scripts/                       # Utilidades (copia de muestra, índice de audio, smoke S3A)
├── docs/                          # bitacora_tecnica.md + figuras + evidencia Dagster
├── data/                          # Datos locales (gitignored)
├── referencias_bibliograficas/    # referencias.bib (IEEE) + seguimiento de fuentes
├── lineamientos/                  # Guías IEEE + plantilla oficial de UISRAEL
├── proyecto/                      # Insumos de la empresa (rúbricas y guías técnicas)
└── imagenes/arquitectura_completa.html   # ⭐ Diagrama integral (abrir en navegador)
```

## 6. Marco metodológico

Enfoque **cuantitativo**, técnico y experimental aplicado, sustentado en dos marcos:

- **Design Science Research (DSR)** — diseñar, implementar y evaluar una solución para
  una necesidad real de la empresa.
- **CRISP-DM** — guía del ciclo de datos y la construcción del *pipeline*, con una
  arquitectura **híbrida** (lotes + baja latencia).

**Fases (CRISP-DM):** Comprensión del negocio → Comprensión de los datos → Preparación →
Modelado y desarrollo → Evaluación → Despliegue.

**Población / muestra:** universo de interacciones desde 2018 (>24,7 M CDR y 858 GB de
audio); la muestra resulta del cruce válido CDR–grabación acotado por criterios de
duración. La **unidad de análisis** es cada llamada (CDR + grabación + transcripción
anonimizada).

## 7. Arquitectura e instrumentos

> **Diagrama completo de arquitectura:** [`imagenes/arquitectura_completa.html`](imagenes/arquitectura_completa.html) — abrir en navegador. Muestra en una sola vista: pipeline Medallion (Bronce/Plata/Oro), Dagster, Kafka, Spark, Whisper/OpenVINO, MinIO, Gemini, Gobernanza, Big Data V's, objetivos OE1–OE4.

| Etapa | Herramienta |
|-------|-------------|
| Fuente existente | Servidor **Asterisk** + base de datos **MySQL/MariaDB** (CDR) |
| Orquestación | **Dagster** (Software-Defined Assets · linaje nativo · Medallion) |
| Ingesta *near-real-time* | **Apache Kafka** · sensor Dagster detecta CDR nuevos |
| Procesamiento por lotes | **Apache Spark / PySpark** · activos Dagster particionados por fecha |
| Lago de datos | **MinIO** (object store S3 `s3a://`) · zonas Bronce/Plata en Parquet |
| Transcripción / ASR | **Whisper** + **OpenVINO** · HOST nativo (GPU Arc en dev · CPU en prod) |
| Diarización | **pyannote** (turnos ASESOR/CLIENTE · solo llamadas relevantes) |
| Anonimización | **Presidio** + **spaCy ES** + reconocedores propios Ecuador (cédula, tarjeta, teléfono, datos dictados) |
| Análisis semántico | **Gemini API** (solo texto anonimizado de zona Plata · nunca audio ni PII) |
| Reglas de negocio | Capa **determinista** (rúbrica de palabras prohibidas / cumplimiento de guion) |
| Anomalías | **scikit-learn** (Isolation Forest · z-score) + series temporales |
| Almacenamiento servido | **PostgreSQL** (llamadas · transcripciones · evaluaciones) |
| Consumo | **Streamlit** (tablero KPIs · alertas de prácticas críticas · desempeño por agente) |

## 8. Dimensiones Big Data (V's)

- **Volumen** — >24,7 M registros CDR y 858 GB de audio; 424,2 GB (7 061 447 grabaciones) dentro del alcance.
- **Velocidad** — ingesta *near-real-time* por llamada (Kafka) + reproceso *batch* del histórico (Spark).
- **Variedad** — CDR estructurados (relacional) + audio no estructurado (97,2 % MP3, 2,8 % WAV) → transcripciones y variables derivadas (multimodal).
- **Veracidad** — control de calidad (2,25 % de registros corruptos, `urlrecord` ausente en 99,87 %) mediante gobernanza, validaciones automáticas y *record linkage* con 100 % de cobertura CDR↔grabación en el alcance.
- **Visualización** — indicadores y alertas servidos en un tablero para gerencia, auditoría y agentes.
- **Valor** — pasar de auditar manualmente una fracción mínima a analizar el 100 % de forma automatizada.

## 9. Métricas de evaluación

- **Prácticas críticas:** Accuracy, Precision, Recall y F1-score.
- **KPIs de negocio:** tasa de contactabilidad, tasa de conversión de ventas, duración media por agente, horarios/días de mayor efectividad, KPI de intentos/reintentos.
- **Métricas operativas:** tiempo de procesamiento por llamada (RTF) y *throughput*.
- **Línea base:** comparación antes/después frente a la auditoría manual (tiempo de detección de anomalías y consistencia de la evaluación).
- **Herramientas:** Python (pandas, scikit-learn, SciPy, statsmodels, PySpark).

## 10. Impacto y beneficiarios

- **Beneficiarios directos:** directivos, personal de auditoría/calidad y agentes del *call center*.
- **Beneficiarios indirectos:** clientes (atención más controlada y de calidad).
- **Aporte social:** procesos de venta más transparentes y protección al consumidor.
- **Alineación con los ODS 8 y 9** (trabajo decente, crecimiento económico, innovación e infraestructura).
- **Entregable:** tablero analítico + documentación y capacitación al equipo.

## 11. Operación del stack

```bash
docker compose -f infra/docker-compose.yml up --build -d
```

- Dagster UI: `http://localhost:3000` · Tablero Streamlit: `http://localhost:8501`
- Materializar un día (ejemplo):

```bash
docker exec uisrael_dagster_webserver bash -lc "cd /opt/dagster/app && dagster asset materialize -f src/definitions.py --select bronze_cdr,silver_calls --partition 2025-05-14"
```

- El **Whisper worker** corre **nativo en el HOST** (no dockerizado) y habla con el pipeline por *topics* de Kafka.

---

> Repositorio de carácter **académico y de implementación**. Lo sensible (audio original,
> transcripciones sin anonimizar, credenciales) permanece en la LAN y **no** se versiona.
> Algunos insumos de `proyecto/` provienen del entorno empresarial donde se aplica la solución.
