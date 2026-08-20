# Bitácora técnica de implementación

> Registro detallado de lo **analizado, ejecutado, encontrado, validado y corregido**
> durante la implementación, para alimentar la documentación de la tesis. Complementa
> el tracker maestro `fases.md` (que lleva el estado por fase) con el detalle fino.

---

## Parte A — Fase 1: Diagnóstico y comprensión de datos

### A.1 Perfilado del CDR (MySQL de Asterisk, solo lectura)

Ejecutado en vivo desde un contenedor de diagnóstico (JupyterLab) conectado por VPN a
`192.168.0.40` (MariaDB **5.5.60**, base `asterisk`, tabla `cdr`, usuario `lectura`).

Hallazgos sobre el universo completo (**24 773 167** CDR, 2018 → ago 2026):

| Métrica | Valor | Implicación |
|---|---|---|
| CDR corruptos (`calldate='0000-00-00'`) | 558 021 (2,25 %) | Descartar en Bronce |
| IVR `pregunta1/2/3` llenos | < 0,001 % | Descartado como señal |
| `urlrecord` poblado | 0,13 % | Ruta determinística inviable → enlace por reconstrucción |
| Contactabilidad global | 53,16 % | Base de KPIs |
| Caída 2020→2024 | 59,25 % → 45,57 % (−14 pp) | χ² p < 0,001; efecto TrueCaller |
| `duration` media / máx (ANSWERED) | 59,9 s / ≈ 86 días | Outliers → filtro `billsec ∈ [10, 3600] s` |

Los datos en vivo **reprodujeron exactamente** el perfilado SQL previo (2026-08-18) →
consultas y muestra reproducibles.

### A.2 Caracterización del corpus de grabaciones

Índice del filesystem construido en el servidor con `scripts/diagnostico_audio.sh`
(**solo lectura**, `ionice`+`nice`, sin impacto sobre Asterisk; ~32 min):

- **10 008 203** grabaciones · **858 GB** · **0 errores** de recorrido · mp3 97,2 % / wav 2,8 %.
- Índice descargado (`audio_index.tsv.gz`, 192 MB) y analizado en la laptop.

### A.3 Resolución del enlace CDR↔grabación (el hallazgo central del OE2)

Proceso de descubrimiento (documentado porque el camino importa):

1. **Primer intento fallido:** se supuso `nombre = {ts}-{ext}-{uniqueid}` y match por
   `uniqueid` → cobertura **8,5 %**. La muestra reveló que el 3er campo eran **teléfonos**,
   no `uniqueid`.
2. **Diagnóstico dirigido** (ventana de 1 hora, audio vs CDR lado a lado): se descubrió que
   - el nombre real de ventas es `{ts}-{extensión_agente}-{teléfono_cliente}`;
   - el `src` del CDR es el **caller-ID del troncal** (`SIP/CLARO`, `SIP/telefonica`), **no** el agente;
   - la **extensión del agente vive en `channel`/`dstchannel`** (`SIP/2xx-...`);
   - el `uniqueid` **no** está embebido en las grabaciones de ventas (solo en otras carpetas
     como `PREDICTIVO`).
3. **Método validado (record linkage, ref. Christen 2012):** llave compuesta con tolerancia
   temporal — audio(`ext`, `teléfono`, `ts`) ↔ CDR(`ext ∈ channel/dstchannel`, `dst = teléfono`,
   `|calldate − ts| ≤ 180 s`, vecino más cercano).
   - **Cobertura 100 %** en mayo 2025 (153 533 grabaciones, 0 huérfanas) y **99,7–100 %** en
     6 días repartidos 2020→2026. Supera el umbral de gobernanza (≥ 95 %).
4. **Ambigüedad = señal de negocio:** 3,4–5,3 registros CDR por par `(ext, teléfono)`/día →
   son **reintentos del asesor** al mismo contacto → habilita un **KPI de intentos/persistencia**.

### A.4 Alcance definido (decisión del tesista)

Solo el **call center de ventas: extensiones 200–299, únicamente `OUT`** (salientes) =
**7 061 447 grabaciones / 424,2 GB / 100 agentes**. Se excluyen: `INPUT` (entrantes →
trabajo futuro), 300–399 (administrativos), 400–499 (contabilidad) y carpetas de
proyectos/campañas (`PREDICTIVO`, `CLINICA_DE_VENTAS`, etc.), aunque tengan extensiones
2xx en el nombre.

### A.5 Correcciones aplicadas en Fase 1

- `infra/diagnostico/docker-compose.yml`: se quitó `env_file` (no recorta comentarios en
  línea del `.env`) y se montó el `.env` para leerlo con `python-dotenv`.
- Regex/estrategia de cruce del notebook: se reemplazó el match por `src`/`uniqueid`
  (incorrecto) por la llave `ext + teléfono + ventana`.

### A.6 Entregables de Fase 1

- `notebooks/00_diagnostico.ipynb` ejecutado (reproducible) + 5 figuras + `tablas_diagnostico.xlsx`.
- Redacción académica: `documentos/proyecto_modificado_1.docx` (Resultados y Discusión
  reescritos con datos reales, 5 figuras, 2 tablas), validado contra
  `lineamientos/guia_ieee_redaccion.md`. Original intacto como referencia.

---

## Parte B — Fase 0: Fundaciones e infraestructura

Construida **incremental** (validar/corregir en cada slice). Todo local en la laptop
(Core Ultra 9 / Arc 140V); **no** se conecta al servidor.

### B.1 Slice 1 — Dagster + PostgreSQL

- `infra/docker-compose.yml`, `infra/Dockerfile.dagster`, `infra/dagster/{dagster.yaml,workspace.yaml}`,
  `src/definitions.py` (6 activos esqueleto Medallion).
- Servicios: `postgres` (16-alpine), `dagster_webserver` (UI :3000), `dagster_daemon`.
- **Validado:** los 6 activos cargan sin errores; se materializó un run completo
  Bronce→Plata→Oro (`RUN_SUCCESS`), guardado en Postgres.
- **Corrección:** el `dagster_daemon` intentaba *descargar* la imagen en vez de construirla →
  se le añadió su propio bloque `build`.

### B.2 Slice 2 — Kafka en modo KRaft

- Servicio `kafka` (apache/kafka 3.9.0), un nodo broker+controller, **sin Zookeeper**.
- Doble listener: `kafka:9092` (contenedores) y `localhost:29092` (host / Whisper worker).
- Servicio `kafka_init`: crea los 6 topics (`llamadas.finalizadas`, `asr.jobs`, `asr.results`,
  `transcripciones`, `anonimizadas`, `analisis.calidad`) y termina.
- **Validado:** produce→consume de un mensaje de prueba en `asr.jobs` (round-trip OK).
- **Correcciones:** (1) `advertised.listeners` no admite `0.0.0.0` → bind con `//:puerto`;
  (2) Git Bash convertía rutas `/opt/kafka/...` → resuelto con `MSYS_NO_PATHCONV=1`.

### B.3 Paso 0.D — Whisper worker nativo (fuera de Docker)

- Entorno virtual aislado `whisper_worker/.venv` (gitignored).
- Instalado: **openvino 2026.3.0**, **confluent-kafka 2.15.0** (versiones fijadas).
- **Validado (crítico):** OpenVINO detecta `['CPU', 'GPU', 'NPU']` →
  **GPU: Intel Arc 140V (16 GB)** + NPU Intel AI Boost. Confirma la decisión de correr
  Whisper fuera de Docker para acceder a la GPU. El worker del host alcanza Kafka por 29092.
- `whisper_worker/worker.py`: esqueleto del bucle (consume `asr.jobs` → [ASR Fase 3] →
  `asr.results`).

### B.4 Streamlit placeholder + validación global (0.E)

- Servicio `dashboard` (Streamlit) con página mínima que valida conectividad a PostgreSQL.
- 0.E: `docker compose up` levanta el stack completo con un solo comando.

### B.5 Decisiones de arquitectura (registro)

| Decisión | Motivo |
|---|---|
| **Dagster** (no Airflow) como orquestador | Software-Defined Assets = zonas Medallion; linaje nativo; menos RAM |
| **Kafka en modo KRaft** (no Zookeeper) | Un programa menos; menos RAM; estándar moderno |
| **Whisper fuera de Docker** | El compute-runtime Intel en contenedor solo expone CPU; nativo ve la GPU Arc |
| **Alcance 200–299 / OUT** | Call center de ventas, solo salientes (llamadas que hace el agente) |

---

## Parte C — Cómo fluye el pipeline: caminos batch y streaming

Arquitectura **híbrida (estilo Lambda)**: dos caminos que **comparten los mismos módulos**
(transcripción, anonimización, análisis) y escriben en la **misma capa servida**. Cambia
**cómo se dispara** el trabajo y **a qué escala**.

### C.1 Camino STREAMING (tiempo casi real, una llamada a la vez)

**Cuándo:** una llamada **nueva** termina y se quiere analizar en minutos.

**Flujo (ejemplo):** el asesor 228 llama a un cliente; la llamada termina a las 12:32.
1. **Dagster (sensor)** detecta el CDR nuevo (polling) y dispara la materialización incremental.
2. **Kafka** lleva el aviso por los carriles: `llamadas.finalizadas` → `asr.jobs`.
3. **Whisper worker (host + GPU Arc)** toma el job de `asr.jobs`, transcribe el audio en
   segundos y publica en `asr.results`.
4. **Anonimización** (Presidio) escucha `asr.results`, borra PII → `anonimizadas` (zona Plata).
5. **Análisis (Gemini)** evalúa con la rúbrica → escribe la evaluación en PostgreSQL (Oro).
6. **Tablero (Streamlit)** muestra el resultado; la gerencia lo ve minutos después de colgar.

**Tecnologías en streaming:** Dagster (disparo + linaje) · Kafka/KRaft (transporte) ·
Whisper worker (ASR en GPU) · Presidio (privacidad) · Gemini (análisis) · PostgreSQL.
**Spark NO participa** (una llamada a la vez no necesita cómputo distribuido).

### C.2 Camino BATCH (reproceso histórico por rango de fechas, millones de llamadas)

**Cuándo:** procesar, por ejemplo, **todo 2025** (1,64 M de llamadas de ventas) para KPIs
históricos y comparación contra la auditoría manual (Fase 8).

**Flujo (ejemplo):**
1. **Dagster** lanza un job **particionado por día** (habilita backfill del rango completo).
2. **Spark / PySpark** hace el trabajo pesado de **datos**: lee el CDR por rango (JDBC),
   cruza CDR↔grabación **a escala** (la llave `ext+teléfono+ventana` sobre millones de filas),
   filtra la muestra (`ANSWERED`, `billsec ∈ [10, 3600]`) y normaliza el audio. Aquí se mide
   el **speedup** (escalabilidad) como evidencia distribuida.
3. La muestra se encola en `asr.jobs`; **el mismo Whisper worker** transcribe en la GPU.
4. Anonimización → análisis (Gemini) → PostgreSQL: **los mismos módulos** que en streaming.
5. Resultado: todo el rango evaluado; KPIs y anomalías por agente/periodo (Fase 6).

**Tecnologías en batch:** Dagster (activos particionados, backfill, linaje) · **Spark**
(preparación distribuida + speedup) · Whisper worker (ASR) · Presidio · Gemini · PostgreSQL.

### C.3 Dónde encaja Whisper (la pieza que preguntaste)

El **Whisper worker es el único motor de transcripción**, compartido por los dos caminos.
No le importa si el job vino de una llamada en vivo (streaming) o de una histórica (batch):
solo consume `asr.jobs` (`call_id` + `audio_path`), transcribe en la GPU y publica
`asr.results`. Esa es la ventaja de desacoplarlo con Kafka: **un solo motor GPU, dos
alimentadores**. Como la transcripción es *compute-bound* (limitada por la GPU), el
**throughput del batch lo marca el worker, no Spark** → por eso se procesa una **muestra**
(no los 424 GB), decisión tomada en la Fase 1.

### C.4 ¿"Para streaming ya estamos"?

**Los tubos están puestos, el agua todavía no corre.** En Fase 0 quedó validado que las
piezas del camino streaming **existen y se conectan** (Kafka + topics + worker + Dagster). Pero
la **lógica** de streaming (el sensor de Dagster que detecta llamadas nuevas y los consumidores
encadenados) se implementa en la **Fase 5**. El camino batch se implementa en las **Fases 2–4**.
Fase 0 solo construye y valida el andamiaje.

### C.5 Resumen: tecnología → rol en cada camino

| Tecnología | Streaming | Batch |
|---|---|---|
| **Dagster** | Sensor que dispara por llamada nueva | Job particionado por fecha (backfill) |
| **Kafka/KRaft** | Transporta cada llamada entre etapas | Encola la muestra hacia el worker |
| **Spark** | — (no aplica) | Preparación distribuida + speedup |
| **Whisper worker** | Transcribe la llamada en vivo (GPU) | Transcribe la muestra histórica (GPU) |
| **Presidio / Gemini / PostgreSQL** | Mismos módulos | Mismos módulos |
