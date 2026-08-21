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

---

## Parte F — Fase 3: ejecución (2026-08-20, EN CURSO)

### F.1 Bloque A — Muestra de audios (toca el servidor, hecho)

- **Selección** (`src/processing/sample_select.py`, local, sin tocar el audio): 12 días
  repartidos 2020→2025 (2/año; se evita 2018–2019 por CDR incompleto), cruce reusando
  `link_calls`, filtro `en_muestra`. Muestra **estratificada por año = 500 llamadas**
  (83–84/año). Peso estimado desde el índice (columna `bytes`) = **77,8 MB**.
  `en_muestra`/día observado: 2020 (369/650), 2021 (835/618), 2022 (427/438),
  2023 (319/1 737), 2024 (1 710/1 578), 2025 (1 801/1 389) — crecimiento del call center.
- **Copia** (`scripts/copiar_muestra.py`, paramiko SFTP): **SOLO LECTURA por ruta exacta**
  (sin escanear carpetas). **500/500 archivos, 0 fallos, 77,8 MB en 16,3 s** por LAN.
  Impacto en Asterisk nulo (lectura puntual con prioridad de sistema). Salidas en
  `data/muestra/` (gitignored): `manifest.tsv`, `paths.txt`, `audios/`.

### F.2 Bloque B — Motor ASR sobre OpenVINO (validado)

- **Decisión de motor:** `openvino-genai` (WhisperPipeline) en vez de faster-whisper, porque
  faster-whisper (CTranslate2) **no usa la GPU Intel Arc**; OpenVINO GenAI sí, y permite
  elegir `CPU`/`GPU`/`NPU`, lo que da un benchmark apples-to-apples. Modelo:
  `OpenVINO/whisper-small-int8-ov` (multilingüe, int8) en `data/models/`.
- **Sonda** (`whisper_worker/asr_probe.py`) sobre 1 audio real (49,3 s):

  | Dispositivo | Carga | Transcripción | RTF (t_proc / dur) |
  |---|---|---|---|
  | CPU | 1,1 s | 3,9 s | **0,08×** |
  | GPU Arc | 3,2 s | 2,1 s | **0,04×** |

- **Hallazgos:** (1) la GPU Arc transcribe y es ~2× más rápida que la CPU en el cómputo;
  (2) **ambas superan por mucho la meta del plan (< 3×)** — 12–25× más rápido que tiempo real;
  (3) **la CPU también es muy rápida (0,08×)** → indicio fuerte de que el servidor HP sin GPU
  podría hacer ASR en CPU de forma viable (a confirmar con el benchmark en CPU tipo servidor);
  (4) **alucinación por repetición** detectada en un audio con tonos/silencio (texto degenera a
  "Nuestros… Nuestros…") → **confirma la necesidad del paso anti-alucinación** del plan.

### F.2b Benchmark ASR en llamadas LARGAS (GPU vs CPU)

`whisper_worker/bench_asr.py` sobre las 3 llamadas más largas de la muestra local (30-36 min;
la máxima del corpus en los 12 días fue 36,4 min — las de 40-60 min son raras). Figura:
`docs/figuras/benchmark_asr_llamadas_largas.svg`.

| Llamada | Audio | CPU (proc / RTF) | GPU Arc (proc / RTF) |
|---|---|---|---|
| 1 | 2 210 s (36,8 min) | 199,3 s / 0,090× | 105,7 s / 0,048× |
| 2 | 1 995 s (33,3 min) | 200,7 s / 0,101× | 105,2 s / 0,053× |
| 3 | 1 860 s (31,0 min) | 182,5 s / 0,098× | 120,0 s / 0,065× |
| **Promedio** | 6 065 s | **582 s / 0,096×** | **331 s / 0,055×** |

- **RTF estable** entre audio corto (49 s: 0,04-0,08×) y largo (33 min: 0,055-0,096×) → el
  tiempo de proceso **escala linealmente con la duración**; el promedio es confiable (long-form
  chunking sin degradación). Dato central para el jurado.
- **Regla práctica:** llamada de ~35 min → **~1,8 min (GPU) / ~3,2 min (CPU)**; proyección a
  45 min → ~2,5 min (GPU) / ~4,3 min (CPU).
- **Throughput:** GPU ~18 h de llamadas por hora de cómputo (1÷0,055); CPU ~10 h/h. GPU ~1,75× CPU.
- **Despliegue:** aun en CPU (0,096×) el servidor sin GPU es viable para streaming (una llamada
  de 45 min en ~4,3 min aquí; en el Xeon del servidor, más lento, ~13-17 min, aún bajo tiempo
  real). Para batch masivo conviene la GPU. Confirma cuantitativamente la Parte E.7.

### F.2c Métricas de carga: streaming y reproceso total

`src/processing/carga_streaming.py` (calibración bytes→segundos en la muestra local =
**1000 B/s**, mp3 8 kbps; distribución de mayo 2025; totales del corpus desde el índice).
RTF usados: GPU 0,055× / CPU 0,096×. Figura: `docs/figuras/carga_streaming_y_reproceso.svg`.

**(A) Carga de streaming (mayo 2025, 153 533 grabaciones / 26 días):**
- 5 905 grabaciones/día; por hora activa media 629 / **pico 1 555**.
- **98,3 % cortas (<5 min)**, 0,9 % medias (5-10 min), **0,8 % largas (≥10 min, prom 18 min)**.
- Largas: **45/día**, media 6/hora, **pico 20/hora**.
- Capacidad 1 nodo (solo largas): GPU ~61/h, CPU ~35/h → holgura 3× (GPU) / 2× (CPU).
- Carga total (todo): hora media GPU ~37 % / CPU ~65 %; **hora pico GPU ~91 % (justo), CPU se
  sobrecarga brevemente** (la cola drena tras el pico).
- **Veredicto:** un solo nodo de transcripción sostiene el streaming en tiempo real (cómodo en
  GPU). Las llamadas largas, por ser pocas, nunca son el cuello de botella.

**(B) Reproceso total del corpus (alcance 200-299/OUT):**
- **7 061 447 grabaciones · 455,5 GB (= 424 GiB) · ~126 500 h de audio (~14 años)**.
- 1 nodo **GPU ≈ 290 días** continuos; 1 nodo CPU ≈ 506 días. Un mes: ~3,8 días (GPU) / 6,6 (CPU).
- **Implicación:** reprocesar todo en 1 nodo es inviable → justifica el enfoque de **muestra** +
  **streaming incremental**; para histórico completo, paralelizar en N nodos (≈290/N días) o
  filtrar a llamadas relevantes (las largas+medias, 1,7 % de las llamadas, son ~32 % del audio).

### F.2d Worker operativo: transcripción + anti-alucinación + anonimización (E2E)

**Módulos nuevos (host, `whisper_worker/`):**
- `asr.py`: transcripción OpenVINO GenAI (GPU/CPU, fallback a CPU) + **anti-alucinación**
  (descarta chunks repetidos consecutivos y bucles de un token / baja diversidad léxica).
  Probado en el audio que antes alucinaba: descartó los chunks de "Nuestros… Nuestros…".
- `anonimizar.py`: **frontera de privacidad** (Presidio + spaCy ES `es_core_news_md`) con
  reconocedores propios: **cédula EC (módulo 10)**, **tarjeta (Luhn)**, teléfonos EC
  (móvil/fijo/+593), nombres (NER), y **números dictados en palabras** (caso PCI:
  tarjeta/CVV leídos en voz alta → `<DATO_NUMERICO>`). Prueba sintética: `Juan Pérez`→`<NOMBRE>`,
  `1710034065`→`<CEDULA>`, `4111111111111111`→`<TARJETA>`, `0993303489`→`<TELEFONO>`,
  "cinco nueve tres…"→`<DATO_NUMERICO>`. (Pendiente pulir: espacio tras `<TARJETA>`.)
- `worker.py`: consume `asr.jobs` → `asr.transcribe` → `anonimizar.anonimizar` → publica
  `asr.results` {call_id, transcript_anon, meta}. Var `MAX_MSGS` (test acotado), `ASR_DEVICE`.

**Prueba end-to-end por Kafka (stack real):** job de una llamada de venta de 3,5 min
(ext 291) → worker en GPU: **audio 225,4 s → proceso 12,3 s (RTF 0,055)**, 17 chunks
descartados, transcript anonimizado (2 380 chars) publicado y leído de `asr.results`.
Cadena `asr.jobs → transcribir(GPU+anti-alucinación) → anonimizar → asr.results` **operativa**.

**Dependencias nuevas del venv:** presidio-analyzer, presidio-anonymizer, spacy + es_core_news_md.

### F.2e Asset `silver_transcriptions` (Dagster) + validación E2E + ejemplos

**Cierre del flujo de la Fase 3.** Se añadió `servido.transcripciones` (serving.py:
`ensure_schema_tr`/`replace_transcripciones`), `confluent-kafka` a la imagen Dagster, y se
implementó el asset `silver_transcriptions` (src/definitions.py): lee `en_muestra` de
`servido.llamadas` para la partición, filtra a audios disponibles localmente, **encola
`asr.jobs`** (ruta host-relativa), **espera y recoge `asr.results`** (grupo único, dedupe por
call_id, timeout) y **escribe la transcripción anonimizada** en `servido.transcripciones`.
El worker nativo (GPU) corre en el host; el asset (contenedor) usa `kafka:9092`.

**Materialización 2025-05-14 (ASR_LIMIT=25):** `RUN_SUCCESS`, **25/25 transcritas**.
Validación en PostgreSQL: dur media 152 s, proceso medio 11,3 s (RTF ~0,074), 79 chunks de
alucinación descartados; anonimización aplicada (20/25 con `<NOMBRE>`, 1 `<TELEFONO>`,
1 `<CEDULA>`); **0 posibles fugas** (ninguna transcripción con 7+ dígitos seguidos).

**Bug corregido:** el worker imprimía `∞`; con stdout redirigido a archivo (2º plano) Windows
usa cp1252 y crasheaba (`UnicodeEncodeError`). Se cambió a `inf` + `PYTHONIOENCODING=utf-8`.

**3 ejemplos de llamadas largas** (`whisper_worker/demo_ejemplos.py`, textos en
`data/muestra/ejemplos/`), ventas de paquetes a EE.UU.:
- Ej1 (ag. 203, 2023, 36 min): 82 `<NOMBRE>`, 8 `<TELEFONO>` (captó "Ramírez Suárez Cristian",
  "Elisa Bechalazas").
- Ej2 (ag. 204, 2021, 33 min): 28 `<NOMBRE>`, 7 `<TELEFONO>`, 2 `<DATO_NUMERICO>` ("Kevin Rodríguez").
- Ej3 (ag. 217, 2023, 30 min): 44 `<NOMBRE>`, 4 `<TELEFONO>` ("Narcisa Buzos", "Aileen Sanchez").

**Observaciones (para documentación):**
- Anonimización **agresiva del lado seguro**: falsos positivos de nombre (`Claro`,
  `Parroquia Valle`, `Le`) → mejor sobre-redactar que fugar; afinable con lista blanca.
- **Errores del modelo `small`**: "Marketing BIP/Bits" (VIP), "Banco Pincel" (Pichincha),
  "espalda legal" (respaldo). Subir a `medium`/`large-v3` mejora exactitud a más RTF (a decidir).
- PII de **cierre** (cédula/tarjeta) aparece al final; los números dictados se capturan como
  `<DATO_NUMERICO>` (caso PCI).

### F.2f Diarización agente/cliente (pyannote) integrada

**Módulo `whisper_worker/diarizar.py`** (pyannote/speaker-diarization-3.1, HF_TOKEN del .env):
diariza el audio MONO (separa hablantes por voz), asigna cada segmento del ASR al hablante
con mayor solape, y decide ROL **ASESOR/CLIENTE** por heurística (frases-ancla del guion +
quién habla más). Integrado en `worker.py` (var `DIARIZE`, con respaldo: si falla, sigue sin
diarizar). Salida por turnos: `ASESOR: ... / CLIENTE: ...`, cada turno anonimizado.

**Dependencias / compatibilidad (Windows, notas):**
- `pyannote.audio==3.3.2` requiere torch/torchaudio 2.2.x → se fijó `torch==2.2.2`,
  `torchaudio==2.2.2` (CPU) y `numpy==1.26.4` (torch 2.2 no soporta numpy 2).
- `huggingface_hub` se bajó a `0.25.2` (el nuevo quitó `use_auth_token` que pyannote usa).
- `speechbrain==0.5.16` (el 1.x tiene un import perezoso que choca con librosa→k2 en runtime);
  el pipeline 3.1 usa embeddings wespeaker ONNX (`onnxruntime`), no speechbrain. También `matplotlib`.

**Prueba E2E integrada** (llamada 190,7 s, ext 250): worker → **2 hablantes**, transcript por
turnos ASESOR/CLIENTE anonimizado. Rol correcto (asesor = pitch; cliente = respuestas cortas).

**Rendimiento (hallazgo):** diarización en **CPU a RTF ~0,79–0,85×** (torch no usa la Arc) →
**es el paso más lento**, ~15× el ASR en GPU (0,055×). Implicación: **diarizar solo las
llamadas relevantes** (contestadas + largas, ~1,7 % del volumen), NO las 98 % cortas, para que
el streaming sea sostenible. Alternativa futura: acelerar diarización (GPU/otra librería).

### F.4 Ejemplos largos CON diarización (para documentación)

`whisper_worker/demo_diarizado.py`: regenera las 2 llamadas más largas con turnos
ASESOR/CLIENTE, en versión cruda y anonimizada (`data/muestra/ejemplos/*.DIAR_*.txt`).
Corre en 2º plano (diarización CPU ~0,8×, lenta).
**Resultados:** Ej1 (ag.203, 36 min): 2 hablantes, 132 turnos, proc **1841 s** (~31 min);
Ej2 (ag.204, 33 min): 2 hablantes, 83 turnos, proc **1711 s** (~28 min). Salida por turnos
`ASESOR:/CLIENTE:` con PII redactada; rol correcto (asesor = pitch, cliente = respuestas cortas).
Confirma el hallazgo de rendimiento: en CPU la diarización de una llamada larga tarda ~30 min
(≈ RTF 0,85×) → **la GPU NVIDIA del equipo propuesto la reduciría ~15×**. Caso borde menor: algún
segmento sin solape queda como rol `?` (silencio/apertura).

### F.3 Pendiente de Bloque B (en orden)

1. Completar `worker.py`: normalización (decodificación robusta de mp3) + **anti-alucinación**
   (descartar segmentos con `no_speech_prob` alto y bucles de repetición) + metadatos.
2. **Diarización** agente/cliente — requiere **token de Hugging Face del tesista** + aceptar
   términos de `pyannote` (modelo con permiso). Bloqueante externo.
3. **Anonimización** (Presidio + spaCy ES: cédula módulo 10, tarjeta Luhn, teléfonos, nombres).
4. Asset `silver_transcriptions` (Dagster) integrado por Kafka + validación 0 fugas PII.
5. **Benchmark formal GPU vs CPU** sobre N audios (protocolo Parte E.7).

---

## Parte G — Fase 4: análisis de calidad/cumplimiento (Gemini) — EN CURSO

Análisis **híbrido** sobre el texto **anonimizado** (única entrada que sale a la nube).
Figura: `docs/figuras/fase4_analisis_hibrido.svg`. Rúbrica: `proyecto/parametros_calidad_empresa.md`
(rubrica_v1: 9 criterios de script A, 18 palabras prohibidas B, 6 omisiones C, severidades).

### G.1 Capa determinista (`src/analysis/rubrica.py`) — sin LLM, probada

Detecta lo verificable sin red: palabras prohibidas (B, con normalización de acentos y
condicionales B05/B06 "salvables" por contexto) y el descargo legal (A07/C05, por ancla
"seguro de desgravamen"). **Probada sobre las 25 transcripciones reales (2025-05-14):**
- Palabras prohibidas: **B16** (sorteo/regalo/bono) en 2, **B13**, **B07**, **B11** en 1 c/u.
- **25/25 sin descargo legal (C05)** → todas marcadas críticas por la regla base. **Hallazgo:**
  son llamadas cortas (152 s prom) que NO llegan al cierre → **la regla C05/A07 solo debe
  exigirse si `es_venta=1`**. Se corrigió en `schema.aplicar_reglas_duras` (descarta A07/C05 si
  no hubo venta). Refuerza la necesidad de clasificar `es_venta` (lo hace la capa LLM).

### G.2 Esquema y reglas duras (`src/analysis/schema.py`)

`Evaluacion` (pydantic): grupo_A, grupo_B_infracciones, grupo_C_omisiones, es_venta,
calidad_score (0-100), venta_valida, riesgo_reclamo, sentimientos, fuente_etiqueta, confianza.
`aplicar_reglas_duras`: **venta_valida=0** ante A03/A07 fallidos, B críticas (B01-B10,B17), o C05
— con A07/C05 exigidos **solo si es_venta=1**.

### G.3 Capa LLM (`src/analysis/gemini_eval.py`) — lista, pendiente de clave

Sobre texto anonimizado, Gemini evalúa A01-A09, sentimiento, es_venta, omisiones C; se combina
con la B determinista y se aplican reglas duras. `calidad_score` = % de A cumplidos (pesos finos
se calibran con gold set). Requiere **GEMINI_API_KEY en .env** + `pip install google-generativeai`.
Modelo configurable (`GEMINI_MODEL`, default gemini-2.0-flash), `temperature=0`, salida JSON.

### G.4 Ejecución E2E con Gemini (validado)

- SDK `google-generativeai==0.8.3` (en imagen Dagster). **Modelo:** `gemini-2.0-flash` dio 404
  ("no longer available") → se usa **`gemini-3.6-flash`** (ago-2026). `temperature=0`, salida JSON.
- Tabla `servido.evaluaciones` (serving.py: `ensure_schema_ev`/`replace_evaluaciones`) + activo
  `gold_evaluations` (definitions.py): lee `servido.transcripciones` del día, corre el análisis
  híbrido por llamada (pausa `EVAL_DELAY` anti rate-limit), persiste. `RUN_SUCCESS`.
- **Materialización 2025-05-14: 25/25 evaluadas.** es_venta=1: 0 · venta_valida: 0 · críticas: 25
  · calidad_score 0/15,4/33 · riesgo {bajo 20, alto 3, medio 2} · sent_asesor {neutral 20,
  positivo 3, bajo 2} · prohibidas B16×2,B13,B07,B11.
- **Caveat:** las 25 son prospección corta (152 s prom) que NO cerró → todas venta_valida=0
  (no muestran el caso discriminante). Para validar poder discriminante hay que evaluar
  **llamadas de cierre reales** (largas). Ejemplo de 3 largas: [ver G.5].

### G.5b Evaluación de 3 llamadas largas (caso discriminante, validado)

Gemini sobre las 3 transcripciones largas anonimizadas (30-36 min):
- ag. 203: B={B13,B16}, riesgo medio, cliente confundido→interesado→desconfiado, score 22.
- **ag. 204: B={B01 GARANTIZO,B08,B13,B16}, riesgo ALTO**, cliente neutral→escéptico→negativo, score 11.
- ag. 217: B={B06,B12,B13,B16}, riesgo alto, cliente neutral→dudoso→neutral, score 11.

**Hallazgo clave (valor de la tesis):** la llamada del ag. 204 contiene **"GARANTIZO" (B01,
CRÍTICA)** — palabra prohibida que puede generar multa de la Superintendencia; el sistema la
detecta automáticamente, algo que la auditoría manual (que escucha pocas llamadas) rara vez
pilla. Gemini además captura **trayectorias de sentimiento del cliente** matizadas. Ninguna
cerró venta (es_venta=0): pitches largos donde el cliente terminó desconfiando (realista).

### G.5 Pendiente Fase 4

1. Evaluar llamadas de **cierre** (largas) para ver es_venta=1 / venta_valida variable.
2. Gold set (weak supervision): muestreo estratificado + revisión de auditor; métricas P/R/F1.
3. Calibrar pesos de `calidad_score` con Auditoría; variantes/sinónimos de palabras prohibidas.

---

## Parte H — Integración de MinIO (lago de datos S3-compatible)

**Motivación.** Hasta la Parte G las zonas Medallion Bronce/Plata se guardaban como Parquet en el
**filesystem local** (`../data` montado en los contenedores Dagster). Se integró **MinIO** — un
*object store* S3-compatible on-premise — como **lago de datos**, para desacoplar el
almacenamiento del filesystem y alinear la arquitectura con un patrón estándar de data lake
(encaja con la gobernanza: self-hosted, dentro de la LAN). Es la **decisión de arquitectura H1**.

**Alcance (Opción A, la mínima y limpia).** MinIO reemplaza **solo** el Parquet de las zonas
Bronce/Plata. **No cambian:** la capa servida `servido.*` (sigue en PostgreSQL → patrón *lago +
almacén servido*), el metadato de Dagster (PostgreSQL), Kafka, la fuente CDR (MariaDB), ni el
worker ASR / audio muestreado (siguen locales en el host). Dagster sigue siendo orquestador
agnóstico al almacenamiento.

**H.1 Cambios de infraestructura.**
- `docker-compose.yml`: servicio `minio` (`minio/minio`, API S3 en 9000, consola en 9001, volumen
  `miniodata`) + `minio_init` (`minio/mc`, crea los buckets `bronce`/`plata` y termina, mismo
  patrón que `kafka_init`). Variables S3 y `depends_on: minio` en los servicios Dagster.
- `Dockerfile.dagster`: se **hornean** los JARs de conectividad S3A —
  `hadoop-aws-3.3.4.jar` + `aws-java-sdk-bundle-1.12.262.jar` (versión atada a PySpark 3.5.3 →
  cliente Hadoop 3.3.4)— en el directorio `jars` de PySpark, para no depender de internet en
  runtime (despliegue on-premise reproducible).
- `requirements.dagster.txt`: `+ s3fs` (escritura de Parquet a MinIO desde pandas).

**H.2 Cambios de código.**
- `config.py`: `MINIO_ENDPOINT/ACCESS_KEY/SECRET_KEY`, buckets, `AUDIO_INDEX_S3A`, y helpers
  `s3a_spark_configs()` (endpoint host:puerto, `path.style.access=true`, `ssl.enabled=false`,
  `SimpleAWSCredentialsProvider`) y `s3_storage_options()` (para pandas/s3fs).
- `spark_session.py`: inyecta las configs S3A en cada `SparkSession`.
- `definitions.py`: `_paths()` → `s3a://bronce/cdr`, `s3a://bronce/audio_index`,
  `s3a://plata/calls`; `bronze_cdr` escribe con pandas+s3fs (`s3://…` + `storage_options`), Spark
  lee el mismo objeto vía `s3a://`.
- Scripts utilitarios `sample_select.py`, `validate_month.py`, `carga_streaming.py`: la lectura
  del índice de audio se repunta a `AUDIO_INDEX_S3A`.

**H.3 Migración y validación (2026-08-21).**
- Prueba de humo `scripts/smoke_s3a.py`: Spark (`s3a://`) y pandas (`s3://` vía s3fs) escriben y
  leen Parquet en MinIO → **OK** (confirma JARs + credenciales + endpoint).
- El `bronze_audio_index` existente (257 MiB) se **migró** al bucket con `mc mirror` (evita
  re-materializar miles de particiones); re-materializarlo por backfill queda opcional.
- Se **re-materializaron los 4 días** (2025-05-01/14/15/28) de `bronze_cdr` + `silver_calls`
  leyendo/escribiendo en MinIO. Resultado en `servido.llamadas` **idéntico** al pre-MinIO:
  emparejadas 2369/6791/6951/8613, en_muestra 465/1389/1386/1452, **cobertura 100 %**, total
  24 724. `servido.transcripciones` y `servido.evaluaciones` **intactas** (25/25). Objetos
  verificados en `bronce/cdr` (pandas) y `plata/calls` (Spark, con `_SUCCESS`).

**H.4 Impacto en la evidencia previa.** Nulo en los resultados: activos, linaje, particiones,
metadatos (cobertura, RTF, evaluaciones) y las 7 capturas de `dagster.md` siguen válidos; solo
cambia **dónde** se almacena el Parquet (MinIO en vez del filesystem). Actualizados `dagster.md`
(§1-2 + figura `infra_minio_s3a.svg`), `presentacion_1.md` (§4.2 lago + figura, §5.1 speedup) y
`arquitectura_integral.svg` (caja MinIO conectada a Bronce/Plata).

**H.5 Re-medición del speedup desde MinIO (2026-08-21).** `validate_month.py` sobre mayo 2025
leyendo el índice desde MinIO: cobertura **100 %** (153 533/153 533) y enlace 1/2/4 núcleos =
**35,5 / 27,1 / 20,1 s** (speedup **1,8×** a 4 núcleos). Muy por debajo de los 623/197/113 s
(5,5×) medidos bajo carga (RAM 87 %). **La diferencia es el estado de RAM de la máquina** (con
memoria saturada hay *spilling*/GC y el paralelismo ayuda más), **no MinIO**, que no añade
penalización. Se rehízo la figura `speedup_spark_mayo2025.svg` a **dos paneles** (bajo carga vs
sin presión/MinIO) para presentar el hallazgo de forma honesta.

**H.6 Decisión pendiente (validar con el tutor).** Gold se mantiene **solo en PostgreSQL** (capa
servida). Si se pide "Medallion completo sobre el lago", añadir una escritura fina de Gold-Parquet
a `s3a://` además del Postgres (~5 líneas en `gold_evaluations`). No es obligatorio (Medallion
exige el *layering* de calidad, no object storage en las tres zonas); es narrativa.

