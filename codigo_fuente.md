# Guía del código fuente

Explicación archivo por archivo de la solución: qué hace cada uno, qué usa y cómo
funciona. El objetivo es que, leyendo este documento, se entienda toda la arquitectura
sin tener que abrir el código.

La solución procesa en paralelo el **CDR** (registro de llamadas de Asterisk/MariaDB) y
las **grabaciones MP3** de un call center de ventas para: (1) transcribir y anonimizar
las llamadas, (2) evaluar su calidad y cumplimiento con IA, y (3) pronosticar tendencias
para gerencia. Todo se orquesta con **Dagster** sobre una arquitectura **Medallion**
(Bronce → Plata → Oro) y se sirve en **PostgreSQL** + dos tableros.

- **Regla de oro de seguridad:** el acceso a la fuente es **estrictamente de solo lectura**
  (usuario MySQL con `GRANT SELECT`, SFTP de lectura por ruta exacta). Nunca se escribe ni
  se borra en Asterisk.
- **Frontera de privacidad (LOPDP):** la anonimización ocurre entre Bronce y Plata. Solo
  texto **anonimizado** sale hacia la nube (Gemini).

Índice:
1. [`src/` — pipeline y aplicación](#1-src--pipeline-y-aplicación)
2. [`whisper_worker/` — transcripción nativa](#2-whisper_worker--transcripción-nativa)
3. [`infra/` — contenedores e imágenes](#3-infra--contenedores-e-imágenes)
4. [`scripts/` — operación (Windows)](#4-scripts--operación-windows)
5. [`data/` — runners de post-proceso](#5-data--runners-de-post-proceso)
6. [`notebooks/` — diagnóstico](#6-notebooks--diagnóstico)
7. [Flujo de datos de extremo a extremo](#7-flujo-de-datos-de-extremo-a-extremo)

---

## 1. `src/` — pipeline y aplicación

Monorepo Python. Los subpaquetes `anomalies/`, `anonymization/`, `asr/`, `ingestion/`,
`serving/` contienen solo un `__init__.py` (marcadores de la estructura planificada); la
lógica real vive en `processing/`, `analysis/`, `streaming/` y `dashboard/`.

### `src/definitions.py` — el pipeline Dagster (corazón del sistema)
Define los **activos** (assets) Medallion y los registra en `Definitions`. Usa Dagster +
PySpark. Los activos batch están **particionados por día** (`DailyPartitionsDefinition`
desde 2018-01-01, sin fecha fin) para permitir reprocesar cualquier día o rango a demanda.

Activos, en orden del flujo:
- **`bronze_cdr`** (Bronce): lee el CDR del día desde MariaDB con **Spark JDBC** (lecturas
  particionadas, solo lectura) y lo escribe como Parquet en MinIO (`s3a://bronce/cdr/`).
- **`bronze_audio_index`** (Bronce): construye el índice de grabaciones del alcance
  (extensiones 200–299, salientes `OUT`) desde el `audio_index.tsv.gz` y lo guarda
  particionado por fecha en el lago.
- **`silver_calls`** (Plata): cruza CDR ↔ grabación (enlace validado), marca la muestra
  analítica (`en_muestra` = contestada y billsec ∈ [10, 3600]) y escribe Parquet + la tabla
  `servido.llamadas` en PostgreSQL.
- **`bronze_audio`** (Bronce): aterriza el MP3 **crudo** del día en el lago por SFTP (solo
  lectura, idempotente). Con `AUDIO_MIN_SECS=600` baja **solo las llamadas largas** (las
  relevantes para evaluar). Depende de `silver_calls` (necesita saber cuáles bajar).
- **`silver_transcriptions`** (Plata): encola trabajos ASR en Kafka (`asr.jobs`), espera al
  worker Whisper, y guarda la transcripción **anonimizada** en `servido.transcripciones`.
  Marca las llamadas largas para diarización diferida. Nunca diariza en línea (lento en CPU).
- **`gold_evaluations`** (Oro): evalúa la calidad con **Gemini** sobre el texto anonimizado y
  guarda en `servido.evaluaciones`. Filtro de coste: solo evalúa grabaciones > `EVAL_MIN_SECS`
  (600 s por defecto); las cortas quedan transcritas pero no se mandan al LLM.
- **`gold_diarizations`** (Plata): diarización **diferida** (turnos ASESOR/CLIENTE). Apagada
  por defecto (`DIARIZATION_ENABLED=0`); pensada para activarse con GPU NVIDIA. No es
  requisito de ningún otro paso, es una mejora.
- **`gold_kpis`** (Oro): reconstruye la serie diaria de KPIs (`servido.kpis`) combinando lo
  operativo del CDR con las ventas/calidad de la rúbrica v2.
- **`gold_pronosticos`** (Oro): entrena los modelos de pronóstico y llena
  `servido.pronosticos*`. Depende de `gold_kpis`.

### `src/processing/` — capa batch (Spark + carga)

- **`config.py`**: configuración compartida. Lee el `.env` y expone los motores de conexión:
  `cdr_engine()`/`cdr_jdbc()` (CDR de Asterisk, **solo lectura**), `pg_engine()` (PostgreSQL
  servido) y la configuración **S3A** para que Spark lea/escriba en MinIO. Aquí están las
  claves/rutas (buckets, endpoint MinIO, tabla CDR).
- **`cdr.py`**: lectura del CDR. `read_cdr_spark()` hace la lectura **particionada por
  `calldate`** (N SELECT paralelas) para el volumen; `read_cdr()` (pandas) para conjuntos
  chicos. Detalle clave documentado en el código: se usa el esquema `jdbc:mysql://…?permitMysqlScheme`
  (no `jdbc:mariadb://`) para que Spark entrecomille con backticks y no rompa el particionado.
- **`audio_index.py`**: parsea el índice de grabaciones (TSV) con Spark, filtra el alcance
  (carpeta `2xx` + subcarpeta `OUT`) y extrae del nombre de archivo la extensión del agente,
  el teléfono y la marca temporal (`{ts}-{ext}-{telefono}`).
- **`linkage.py`**: **record linkage** CDR ↔ grabación. Extrae la extensión 2xx del agente
  del canal (el `src` del CDR es el troncal, no el agente), une por (agente + teléfono) con
  tolerancia temporal de ±180 s y se queda con el vecino más cercano en tiempo (1:1).
- **`audio_landing.py`**: baja los MP3 crudos por **SFTP de solo lectura, por ruta exacta**,
  y los sube al bucket Bronce de MinIO. Idempotente (omite si ya existe con el mismo tamaño) y
  estrangulado (`sleep`) para no saturar el disco de grabaciones. Lo usan `bronze_audio` y el
  streaming.
- **`serving.py`**: la **capa servida**. Define el esquema PostgreSQL (`servido.llamadas`,
  `transcripciones`, `evaluaciones`, `kpis`, `stream_cursor`, `backfill_progress`) y las
  escrituras idempotentes por día (borra + inserta) y por `call_id` (upsert). Contiene el SQL
  que construye `servido.kpis` (`build_kpis()`) y las funciones de la diarización diferida.
- **`spark_session.py`**: fábrica de `SparkSession` en modo `local[N]`, con la configuración
  S3A inyectada. El nº de núcleos es parametrizable (se usó para medir el *speedup*).
- **`sample_select.py`**: selecciona la **muestra estratificada** de ~500 audios (2020→2025,
  repartidos por año) para el benchmark de ASR, sin tocar el servidor (usa el índice + CDR).
  Genera `manifest.tsv` y `paths.txt`.
- **`carga_streaming.py`**: script analítico que **estima** (sin tocar el servidor) la carga
  de streaming (volumen, % de llamadas largas, capacidad de un nodo) y el tiempo de reproceso
  total del corpus, calibrando bytes→segundos con la muestra local.
- **`validate_month.py`**: valida el enlace sobre un mes completo y mide el tiempo con N
  núcleos (evidencia de cobertura ~100 % y de escalabilidad).

### `src/analysis/` — evaluación de calidad y pronóstico

- **`rubrica.py`**: capa **determinista** (sin IA). Detecta por regex las **palabras
  prohibidas** del Grupo B (garantizo, aseguro, préstamo, "de parte del banco"…), con
  normalización de acentos y condicionales que se "salvan" por contexto, y detecta el descargo
  legal por frase ancla. Es la primera capa del análisis híbrido.
- **`schema.py`**: modelo **pydantic** `Evaluacion` (valida el JSON antes de guardar) y las
  **reglas duras** de negocio (`aplicar_reglas_duras`): una infracción crítica anula la venta;
  el descargo/cierre solo se exigen si hubo venta (rúbrica v1).
- **`gemini_eval.py`**: capa **LLM** (Google Gemini). Sobre el texto anonimizado, evalúa los
  criterios de contexto del Grupo A (A01–A09), el sentimiento, `es_venta` y — señal central —
  `impersona_banco` (si el asesor da a entender que llama de parte del banco). Combina su
  salida con la capa determinista. Requiere `GEMINI_API_KEY`; el modelo es configurable
  (`GEMINI_MODEL`, por defecto `gemini-3.6-flash`). Registra los tokens consumidos por llamada
  (telemetría de coste).
- **`pesos_v2.py`**: **rúbrica v2 calibrada con Auditoría** (archivo editable a mano). Cada
  peso = puntos que se restan de 100 si el ítem se incumple. La única infracción que **anula**
  la venta es B17 (hacerse pasar por el banco). Documenta por qué A03/A07 pesan pero no anulan.
- **`recalificar.py`**: recalcula los veredictos (score, venta válida, venta con riesgo) con
  los pesos v2 **a partir de los juicios crudos ya guardados por Gemini**, sin volver a llamar
  al LLM. Cambiar pesos y re-ejecutar es instantáneo y gratis. Define la "venta con riesgo"
  (venta + impersonación del banco).
- **`weak_labels.py`**: **weak supervision** — genera etiquetas aproximadas por reglas simples
  (labeling functions: duración, frases de cierre/rechazo, dictado de tarjeta, prohibidas,
  permiso de grabación) y las combina ponderadamente. Sirve de "verdad barata" y se compara con
  Gemini para medir acuerdo. Base para la validación de Auditoría a oído.
- **`forecast.py`**: **módulo de pronóstico** (Fase 7). Modela la serie **diaria** de cada KPI
  y proyecta a un horizonte con banda de confianza. Compara 5 modelos con backtest (holdout) y
  MAE/RMSE/MAPE/R²: baseline naive-estacional, Holt-Winters, SARIMA, **Prophet** (el principal)
  y **LSTM** (PyTorch CPU, comparación de Fase 8). Persiste el histórico + forecast diario, el
  rollup mensual (para gerencia) y las métricas por modelo.

### `src/streaming/` — tiempo real (Fase 5)

- **`locate.py`**: localiza la grabación de una llamada del CDR construyendo el nombre de
  archivo `{ts}-{ext}-{telefono}.mp3` y comprobándolo con `sftp.stat` (solo metadatos, no abre
  el archivo). Prueba offsets ±unos segundos por robustez.
- **`runner.py`**: el **bucle de streaming**. Cada ciclo: (1) hace *poll* del CDR desde un
  cursor guardado en nuestro PostgreSQL, (2) localiza la grabación, (3) aterriza el MP3 en
  Bronce, (4) registra la llamada (`origen='streaming'`), (5) encola el ASR, (6) drena los
  resultados del worker y persiste transcripción + evaluación Gemini (solo llamadas largas).
  Todo en solo lectura sobre Asterisk.

### `src/dashboard/` — tableros (Streamlit)

- **`app.py`**: tablero **técnico** (puerto 8501), para auditoría/tesista. 8 pestañas: Resumen,
  ⚡ Tiempo real (streaming), 👤 Por agente, ⚖️ Calidad y cumplimiento (grupo B / riesgo), 🔁
  Intentos-reintentos, 🚨 Anomalías (z-score por agente), 🔎 Detalle de llamada (transcripción
  anonimizada + desglose de la evaluación) y 📈 Predicción (comparación de modelos, R²/RMSE).
  Lee la capa servida por SQL; usa las métricas de la rúbrica v2.
- **`gerencial.py`**: tablero **gerencial** (puerto 8502), sin métricas técnicas. Selector de
  horizonte (mes/bimestre/trimestre/semestre) y 4 tarjetas con flecha ↑↓ y % de cambio:
  llamadas, contactabilidad, ventas bien realizadas y ventas con riesgo. Usa **Prophet fijo**
  (coherente, no el "mejor por R²", que podía extrapolar disparates) y recorta las tasas a
  [0, 100]. Las ventas se estiman con la tasa de conversión actual × llamadas largas proyectadas.

---

## 2. `whisper_worker/` — transcripción nativa

Proceso Python que corre **nativo en el host** (fuera de Docker) para poder usar la GPU
Intel Arc por **OpenVINO** (en contenedor Windows solo se expone CPU). Se comunica con el
pipeline por Kafka.

- **`worker.py`**: el bucle del worker. Consume `asr.jobs`, resuelve el audio, transcribe
  (con anti-alucinación), diariza si el trabajo lo pide, anonimiza y publica en `asr.results`
  la transcripción **anonimizada** + metadatos. Amplía `max.poll.interval.ms` a 30 min para no
  ser expulsado del grupo Kafka durante llamadas largas.
- **`asr.py`**: transcripción con **OpenVINO GenAI** (Whisper `small` int8) en GPU/CPU. Incluye
  el filtro **anti-alucinación** (descarta segmentos repetidos o con repetición anómala de un
  token, típico bucle de Whisper en silencios/tonos).
- **`anonimizar.py`**: la **frontera de privacidad**. Combina **Presidio + spaCy ES** con
  reconocedores propios del dominio ecuatoriano: cédula (validación módulo 10), tarjeta
  (validación Luhn), teléfonos Ecuador (móvil/fijo/+593), nombres (NER) y — caso PCI — secuencias
  de números **dictados en voz** ("cinco nueve tres…"). Cada entidad se reemplaza por una
  etiqueta (`<CEDULA>`, `<TARJETA>`, `<TELEFONO>`, `<NOMBRE>`, `<DATO_NUMERICO>`).
- **`diarizar.py`**: diarización con **pyannote** (speaker-diarization-3.1). Separa hablantes
  por voz, asigna cada segmento del ASR por solape temporal y decide el rol ASESOR/CLIENTE por
  heurística (frases-ancla del guion + quién habla más). Corre en CPU (lento) → por eso es diferida.
- **`audio_source.py`**: resuelve el `audio_path` de un trabajo: si es una clave del lago MinIO,
  la **descarga una vez** a un caché local y la reutiliza; si es una ruta local existente, la usa
  tal cual. Así el worker consume el MP3 crudo de Bronce sin copia manual.
- **`validate_env.py`**: comprueba que OpenVINO detecta los dispositivos (idealmente la GPU Arc)
  y que Kafka es alcanzable desde el host. Sin transcribir aún (Fase 0.D).
- **`asr_probe.py`** / **`bench_asr.py`**: sondas/benchmark de ASR (GPU vs CPU) — miden tiempo y
  factor tiempo-real (RTF), en un audio y en llamadas largas.
- **`bench_modelos.py`**: compara Whisper `small` vs `medium` (tiempo y exactitud) — evidencia
  de por qué se mantuvo `small`.
- **`demo_ejemplos.py`** / **`demo_diarizado.py`**: generan ejemplos legibles (transcripción
  cruda vs anonimizada, y con turnos ASESOR/CLIENTE) para inspección del dueño de los datos.
- **`diar_probe.py`**: sonda que valida que pyannote carga y corre (nº de hablantes + primeros turnos).
- **`requirements.txt`**: dependencias base de la Fase 0.D (openvino + confluent-kafka). **Nota:**
  el resto de dependencias reales del worker (openvino-genai, librosa, presidio-analyzer/anonymizer,
  spaCy `es_core_news_md`, minio, pyannote.audio, torch/torchaudio) se fueron instalando en el
  venv `whisper_worker/.venv` conforme a las Fases 3/5.

---

## 3. `infra/` — contenedores e imágenes

- **`docker-compose.yml`**: orquesta todo el stack. Servicios: `postgres` (metadatos +
  capa servida), `kafka` (modo KRaft, sin Zookeeper) + `kafka_init` (crea los topics),
  `minio` (lago S3) + `minio_init` (crea buckets bronce/plata), `dagster_webserver` y
  `dagster_daemon` (orquestador), `streaming_runner` (bucle de streaming, `restart:
  unless-stopped`), y los dos tableros `dashboard` (8501) y `dashboard_gerencial` (8502). El
  worker Whisper **no** está aquí (corre en el host). Los volúmenes `pgdata`/`kafkadata`/
  `miniodata` guardan los datos.
- **`Dockerfile.dagster`**: imagen del orquestador + batch. Base Debian 12 con **Java 17** (para
  PySpark 3.5), instala las dependencias, aplica el *fix* de **Prophet** (crea el `makefile` que
  cmdstanpy exige), instala **PyTorch CPU** (LSTM) y hornea los **JARs** de S3A (Hadoop-AWS) y el
  **driver JDBC MariaDB 2.7.12** (compatible con MariaDB 5.5.60).
- **`Dockerfile.streamlit`**: imagen ligera de los tableros (Streamlit + cliente PostgreSQL).
- **`requirements.dagster.txt`**: dependencias de la imagen Dagster (dagster, pyspark, pandas,
  pymysql, SQLAlchemy, psycopg2, confluent-kafka, google-generativeai, s3fs, paramiko,
  statsmodels, scikit-learn, prophet). PyTorch se instala aparte en el Dockerfile.
- **`requirements.streamlit.txt`**: dependencias de los tableros (streamlit, psycopg2, pandas,
  SQLAlchemy, altair).
- **`dagster/dagster.yaml`**: apunta el almacenamiento de Dagster (runs, event log, schedules) a
  PostgreSQL; desactiva la telemetría.
- **`dagster/workspace.yaml`**: le dice a Dagster que cargue los activos desde `src/definitions.py`.
- **`diagnostico/`**: scaffold **independiente** de la Fase 1 (Dockerfile + compose +
  requirements) que levanta un JupyterLab desechable para correr el notebook de diagnóstico
  contra el CDR real.

---

## 4. `scripts/` — operación (Windows)

Scripts PowerShell para levantar/operar el sistema, y utilitarios Python de conexión/prueba.

- **`up.ps1`**: levanta **todo** con un comando: Docker (compose) + el worker Whisper del host.
  Opciones: `-Build` (reconstruye imágenes), `-NoStream` (no arranca el streaming, para
  backfill), `-Device GPU|CPU`.
- **`down.ps1`**: detiene todo (worker del host + Docker) **sin** borrar volúmenes.
- **`worker-up.ps1` / `worker-down.ps1`**: arrancan/paran el worker Whisper nativo en segundo
  plano (PID en `data/worker.pid`, logs en `data/worker.*.log`).
- **`backfill_up.ps1`**: inicia (o reanuda) el backfill de una pasada — `operativa` (KPIs, rápida)
  o `ventas` (transcribe + evalúa solo largas). En segundo plano; reanuda solo por el checkpoint
  en PostgreSQL.
- **`_backfill_loop.ps1`**: motor interno del backfill (no se llama directo). Recorre las fechas
  día a día, materializa los activos correspondientes por `docker exec`, y guarda el progreso en
  `servido.backfill_progress` (los días 'ok' se saltan; 'error' se reintentan). Resuelve la
  dependencia operativa→ventas (ventas espera o hace la cadena completa; nunca salta un día).
- **`backfill_down.ps1`**: detiene el backfill de forma limpia (el día a medias no queda 'ok', se
  retoma solo).
- **`copiar_muestra.py`**: baja la muestra de audios (de `paths.txt`) por SFTP de solo lectura
  a `data/muestra/audios/` (para los benchmarks). Idempotente.
- **`verificar_conexion.py`**: verifica conectividad **sin extraer datos**: autentica al MySQL
  del CDR (versión + existencia de la tabla) y prueba el SSH al servidor de grabaciones (lista
  solo el primer nivel).
- **`smoke_s3a.py`**: prueba de humo del lago — escribe/lee un Parquet en MinIO con Spark y con
  pandas (valida los JARs S3A y las credenciales).
- **`diagnostico_audio.sh`**: se ejecuta **en el servidor Asterisk** (no en el contenedor).
  Construye el índice de todas las grabaciones (ruta, tamaño, fecha) con `ionice`/`nice` para no
  impactar producción, sin copiar los ~100 GB. Produce el `audio_index.tsv.gz`.

---

## 5. `data/` — runners de post-proceso

Lanzadores delgados que se ejecutan **dentro del contenedor de Dagster**. La lógica canónica
vive en `src/analysis/`; estos solo la invocan. (En el repositorio la carpeta `data/` está en
`.gitignore` porque guarda datos; en este paquete se incluyen únicamente estos `.py`.)

- **`recalificar_todo.py`**: recalcula en toda la base los campos de la rúbrica v2
  (`calidad_score_v2`, `venta_valida_v2`, `venta_con_riesgo`) usando `src/analysis/recalificar.py`,
  sin volver a llamar a Gemini. Se corre tras cada sesión de backfill.
- **`reevaluar_todo.py`**: re-evalúa con Gemini las transcripciones ya hechas (p. ej. tras cambiar
  el prompt para añadir `impersona_banco`), sin re-transcribir.
- **`pronostico.py`**: runner del módulo de pronóstico (`src/analysis/forecast.py`); acepta las
  variables `KPI`/`HORIZON`/`TEST`.
- **`measure_tokens.py`**: mide el coste real de tokens de Gemini sobre un conjunto de llamadas.

---

## 6. `notebooks/` — diagnóstico

- **`00_diagnostico.py`** / **`00_diagnostico.ipynb`**: notebook de la **Fase 1** (comprensión de
  datos). Se conecta en vivo al CDR (solo lectura) y al índice de grabaciones, replica el perfilado
  (volumen, contactabilidad, corruptos, distribución por año/hora, cruce CDR↔audio) y exporta las
  figuras y tablas del capítulo. El `.py` es el fuente (formato jupytext); el `.ipynb` es la versión
  **ejecutada con resultados** (con la tabla de troncales enmascarada por privacidad).

---

## 7. Flujo de datos de extremo a extremo

**Batch / reproceso por fechas:**
```
MariaDB CDR ──(Spark JDBC, solo lectura)──▶ bronze_cdr ─┐
Índice de audio ─────────────────────────▶ bronze_audio_index ─┤
                                                                ▼
                                    silver_calls (cruce, muestra) ──▶ servido.llamadas
                                                                ▼
Asterisk (SFTP solo lectura) ──▶ bronze_audio (MP3 crudo → MinIO)
                                                                ▼
                    silver_transcriptions ──(Kafka asr.jobs)──▶ Whisper worker (host, GPU)
                    (transcribe + anonimiza) ◀──(asr.results)──┘
                                                                ▼
                    gold_evaluations (Gemini, solo >600s) ──▶ servido.evaluaciones
                                                                ▼
                    gold_kpis ──▶ servido.kpis ──▶ gold_pronosticos ──▶ servido.pronosticos*
                                                                ▼
                                            Tableros: técnico (8501) + gerencial (8502)
```

**Streaming (tiempo real):** `src/streaming/runner.py` hace lo mismo llamada por llamada,
en vivo: poll del CDR → localiza → aterriza → ASR → transcripción + evaluación, marcando
`origen='streaming'`. Comparte el mismo worker y la misma capa servida que el batch.

**Post-proceso tras cada backfill** (obligatorio para que crezca la serie de pronóstico):
`data/recalificar_todo.py` + materializar `gold_kpis,gold_pronosticos`.
