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

## Parte D — Fase 2: Preparación batch (Spark / PySpark)

Primer pipeline que **procesa datos reales** por rango de fechas, llenando las zonas
Bronce y Plata. Todo local; el CDR se lee **SOLO LECTURA** por VPN.

### D.1 Entorno

- Imagen Dagster extendida: **Java 17 (OpenJDK) + PySpark 3.5.3** + `pandas`, `pyarrow`,
  `pymysql`, `SQLAlchemy`, `psycopg2`, `pandera`. Base `python:3.11-slim-bookworm`.
- Spark en **modo local[N]** (sin clúster aparte). El nº de núcleos es parametrizable
  para medir speedup.
- Montajes nuevos en los contenedores Dagster: `.env` (credenciales, ro) y `data/` (salidas).

### D.2 Código (`src/processing/` + `src/definitions.py`)

| Módulo | Rol |
|---|---|
| `config.py` | Motores SQLAlchemy: CDR (solo lectura) y capa servida (PostgreSQL); rutas |
| `cdr.py` | `read_cdr(desde, hasta)` — SELECT del CDR, descarta corruptos; **nunca escribe** |
| `audio_index.py` | `build_audio_scope(spark)` — lee el índice tsv.gz, filtra **200–299/OUT**, parsea (ext, teléfono, ts) |
| `linkage.py` | `link_calls(cdr, audio)` — llave `(ext∈channel/dstchannel + dst=teléfono + |Δt|≤180 s)`, vecino más cercano |
| `serving.py` | Esquema `servido.llamadas` + escritura idempotente por día |
| `spark_session.py` | SparkSession local[N] (Arrow habilitado) |
| `validate_month.py` | Enlace sobre un mes + medición de speedup |
| `definitions.py` | Activos `bronze_cdr`, `bronze_audio_index`, `silver_calls` (particionados por día) |

**Zonas:** Bronce = `data/bronze/cdr/date=YYYY-MM-DD/` (Parquet por día) + `data/bronze/audio_index/`
(Parquet particionado por fecha, 7,06 M filas del alcance). Plata = `data/silver/calls/date=…/`
+ tabla `servido.llamadas` en PostgreSQL (una fila = una grabación emparejada).

### D.3 Validación (mayo 2025)

- `bronze_audio_index`: **7 061 447** grabaciones en alcance (= exactamente el alcance de la Fase 1).
- **Cobertura del cruce = 100 %**: día 2025-05-14 → 6 791/6 791 (0 huérfanas); **mes completo
  2025-05 → 153 533/153 533** (0 huérfanas). Muestra analítica (`en_muestra`): 28 844.
- **Reconciliación / calidad** (24 724 filas de 4 días): 0 nulos en `call_id`/`audio_path`,
  0 agentes fuera de 200–299, `diff_seg ∈ [0, 180]` (media 0 s, máx 1 s → emparejamiento casi
  exacto), 0 inconsistencias de muestra, **24 724 audios únicos = 24 724 filas (1:1 perfecto)**.
- Disposition de lo emparejado: NO ANSWER 64 %, ANSWERED 30 %, BUSY/FAILED resto (coherente
  con marcador predictivo).
- **Speedup Spark** (mismo job, mes completo): 1 núcleo 390,7 s · 2 núcleos 105,4 s ·
  4 núcleos **56,9 s** (≈ 6,9× con 4 núcleos; el salto super-lineal 1→2 refleja menos
  *spilling*/GC con más paralelismo). Evidencia de escalabilidad para el tribunal.

### D.4 Correcciones aplicadas (para documentación)

1. El `.env` (heredado de la Fase 1) traía `AUDIO_INDEX=/work/data/...`, ruta del contenedor
   de diagnóstico → se calcula la ruta **relativa a `DATA_DIR`** e ignora ese valor.
2. `openjdk-17-jre-headless` no está en Debian trixie (base de `python:3.11-slim`) → se fija la
   base **`python:3.11-slim-bookworm`** (Debian 12), con Java 17 que soporta PySpark 3.5.
3. Spark 3.5 no lee timestamps Parquet en **nanosegundos** (pandas los escribe así) → se escribe
   `bronze_cdr` con `to_parquet(coerce_timestamps="us", allow_truncated_timestamps=True)`.

### D.5 Reglas respetadas

- **CDR: solo `SELECT`** (lectura). Nunca se modificó la tabla ni la fuente.
- **Grabaciones/Asterisk: intactas.** Para el audio solo se usó el índice `audio_index.tsv.gz`
  ya descargado en la Fase 1; no se accedió al servidor de grabaciones.

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

---

## Parte E — Verificación en vivo y decisiones de despliegue (2026-08-20)

> Sesión desde la **oficina**, en la **misma LAN del Asterisk, sin VPN**. Se reprodujo
> end-to-end lo construido en la Fase 2 contra la fuente real y se decidió el mapa de
> despliegue por máquina. Figuras asociadas en `docs/figuras/`.

### E.1 Conectividad directa por LAN (sin VPN)

- `CDR_HOST=192.168.0.40:3306` alcanzable por TCP directo (sin VPN). La lectura del CDR
  del **mes completo** (242 106 filas) tardó **3–5 s** — mucho más rápido que por VPN.
- Implicación: en la oficina, tanto el batch (lectura de CDR) como la futura copia de la
  muestra de audios (Fase 3) van sobre LAN → rápido y fiable.

### E.2 Reproducción del pipeline Fase 2 (stack completo levantado)

- `docker compose -f infra/docker-compose.yml up --build -d` → 5 contenedores `Up`
  (`postgres` y `kafka` healthy; `dagster_webserver` :3000, `dagster_daemon`,
  `dashboard` :8501). Primera build lenta por Java 17 + PySpark (esperado).
- Materialización `bronze_cdr` + `silver_calls` partición **2025-05-14** → `RUN_SUCCESS`.
- **Verificado directamente en PostgreSQL** (`servido.llamadas`, día 2025-05-14):
  **6 791 emparejadas**, **6 791 audios únicos** (1:1 perfecto), `max(diff_seg)=1 s`,
  `en_muestra=1 389`, **cobertura 100 %** — reproduce exactamente la Fase 1.

### E.3 Aclaración conceptual: qué mide el cruce y sobre qué opera

- El cruce (Fase 2) opera **solo sobre datos**: el **CDR** (metadatos de MySQL) y el
  **índice de audio** (lista de **nombres** de archivo, p. ej.
  `20250514093012-228-0991234567.mp3`). **No se abre, decodifica ni escucha ningún audio.**
  Del nombre se extrae (extensión, teléfono, fecha).
- El **contenido** de las grabaciones se procesa recién en la **Fase 3 (Whisper)**.
- Los tiempos del test miden el **bloque completo del mes** (153 533 grabaciones), no una
  llamada: equivale a **~0,7–4 ms por llamada**. Emparejar una llamada es baratísimo; lo
  que cuesta es hacerlo 153 mil veces → por eso se paraleliza. Analogía: se cruzan las
  **etiquetas** de préstamo contra la lista de **títulos**, sin "leer los libros" (eso es ASR).

### E.4 Test de escalabilidad (speedup) — dos escenarios medidos

Mismo job (cruce del mes mayo 2025, 153 533 grabaciones), variando núcleos de Spark
(`local[N]`). Correctitud **idéntica y determinista** en todas las corridas:
153 533/153 533 emparejadas (100 %), `en_muestra = 28 844`, cambie N como cambie.

| Núcleos | Escenario A — máquina descargada | Escenario B — máquina bajo carga (RAM 87 %) | Aceleración (B) |
|---|---|---|---|
| 1 | 390,7 s | **623,1 s** | 1× |
| 2 | 105,4 s | **196,6 s** | 3,2× |
| 4 | 56,9 s | **113,4 s** | 5,5× |

- **Aceleración super-lineal** 1→2 núcleos (3,2×, > 2×): con 1 solo núcleo la máquina se
  queda sin RAM y hace *spilling* a disco + GC; con más núcleos cada uno maneja menos datos
  y cabe en memoria. **Confirmado experimentalmente**: el escenario B (RAM al 87 %) es más
  lento que el A en términos absolutos, con el **mismo código y misma cobertura** → evidencia
  directa del efecto de los recursos (RAM) sobre el rendimiento. Ambos escenarios son válidos
  para documentar; B refuerza el argumento de dimensionamiento de hardware.
- Figura: `docs/figuras/speedup_spark_mayo2025.svg` (barras del escenario B).
- **Uso para la tesis:** (1) evidencia científica de escalabilidad (dimensión Velocidad/Volumen);
  (2) planificación de capacidad (estimar hardware de producción); (3) justificación de Spark
  frente a un script mono-hilo (la columna "1 núcleo" sería Python secuencial).

### E.5 Inventario de infraestructura de producción (verificado por capturas)

| Máquina | CPU | RAM | GPU | Notas |
|---|---|---|---|---|
| **Laptop dev** | Core Ultra 9 288V | 32 GB | **Arc 140V (16 GB)** | La más potente; nodo GPU + batch |
| **Servidor HP DL160 Gen9** (host ESXi 6.0) | 16 núcleos Xeon **E5-2609 v4 @ 1,70 GHz** (lentos, sin turbo/HT) | 31,75 GB (**88 % usada**) | **ninguna** | ESXi 6.0 EOL (2016); datastore VMFS5 1,81 TB (709 GB libres) |
| **VM `Ubuntu_Dockers`** (en el HP) | 8 vCPU | 16 GB | ninguna | Ubuntu 64-bit, docker-server, disco 250 GB; candidato a "cerebro" 24/7 |
| **Equipo oficina** | Core **i5-1334U** (10 núcleos/12 hilos, 1,3 GHz) | 16 GB DDR4 (1 de 2 ranuras → ampliable a 32) | Intel UHD (integrada, débil) | Alternativa; sin GPU útil para ASR |

- **Techo de RAM del host HP:** ~32 GB totales, 88 % ya en uso. Aunque se apaguen VMs, la
  `Ubuntu_Dockers` no debería pasar de ~24–26 GB sin ahogar a ESXi. Suficiente para el
  pipeline + muestra; incómodo para reprocesar años enteros ahí.
- **Núcleos del HP:** muchos (16) pero lentos (1,7 GHz, chip 2016) → bien para Spark
  (paralelo), mal para trabajo por-hilo y para Whisper en CPU.

### E.6 Decisión de despliegue por máquina

Figura: `docs/figuras/despliegue_produccion_maquinas.svg`.

| Componente | Dónde vive | Por qué |
|---|---|---|
| Kafka + Dagster + PostgreSQL (servida) + streaming 24/7 + tablero | **Servidor HP (`Ubuntu_Dockers`)** | Carga liviana y constante; máquina siempre encendida |
| **Batch pesado** (reproceso histórico, on-demand) | **Laptop dev** | Máquina más fuerte; el batch es ocasional, no un servicio; escribe resultados en el PostgreSQL del servidor por LAN |
| **Whisper (ASR)** | **Nodo con GPU** (hoy la laptop Arc) | Desacoplado por Kafka → puede vivir en cualquier máquina de la LAN |

- Regla: **"siempre encendido" → servidor; "pesado y ocasional" → laptop**.
- El batch corre bien desde la laptop porque alcanza CDR y audio por la LAN y persiste en
  el Postgres del servidor (mismo dato para tablero y streaming).

### E.7 Whisper en producción — análisis y protocolo de benchmark (Fase 3)

**Problema:** el servidor HP **no tiene GPU** (y ESXi 6.0 EOL hace inviable el passthrough).
**Solución arquitectónica (ya prevista):** como Kafka **desacopla** a Whisper, el motor de
transcripción **no tiene que estar en el servidor** — vive en el nodo que tenga mejor cómputo.

Opciones evaluadas:
- **A — Whisper en CPU en el servidor** (OpenVINO CPU, modelo `small`/`int8`): funciona sin
  GPU; para *streaming* (una llamada a la vez, volumen bajo) probablemente aguanta; para
  *batch* de miles es lento. Es el "plan B" del plan ("CPU aceptable en prod").
- **B — Nodo GPU dedicado (recomendada):** Whisper corre en una máquina con GPU (hoy la
  laptop Arc; a futuro un mini-PC/estación con GPU) conectada al Kafka del servidor por LAN.
  Respeta la regla de oro (el audio nunca sale de la LAN) y da velocidad real.
- **C — GPU en la nube:** descartada (violaría "audio sensible se queda en la LAN").

**Recomendación:** servidor = cerebro 24/7; laptop/nodo-GPU = transcripción + batch on-demand.
Documentar en la tesis **ambas modalidades (GPU vs CPU) con su throughput medido** → capítulo
de despliegue/limitaciones. Prueba que el diseño es portable y cuantifica el costo de no tener GPU.

**Protocolo de benchmark Whisper GPU vs CPU (a ejecutar en Fase 3, para el jurado):**
- *Prerrequisitos:* `whisper_worker/worker.py` completo con faster-whisper instalado + copiar
  la **muestra de audios** (~500, estratificada por agente/disposition) por SSH (solo lectura).
- *Diseño:* transcribir el **mismo** conjunto de N audios (p. ej. 50) en dos configuraciones:
  (1) OpenVINO **GPU** (Arc 140V, laptop) y (2) OpenVINO **CPU** (equivalente al servidor).
  Modelo fijo (`small`/`int8`) e idénticos parámetros.
- *Métricas por config:* tiempo total, **tiempo medio por audio**, **factor tiempo-real**
  (tiempo_proceso ÷ duración_audio; meta del plan **< 3×**), uso de memoria, y verificación
  de que la transcripción sea equivalente (no cambia el texto, solo la velocidad).
- *Salida esperada:* una tabla/figura análoga a la del speedup de Spark, pero para ASR, que
  **decide con evidencia dónde vive Whisper en producción**.

