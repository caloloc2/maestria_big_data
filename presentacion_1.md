# Avance de implementación — Presentación 1

**Proyecto:** Solución Big Data para análisis de calidad y cumplimiento de llamadas del call
center de ventas (Corporación Marketing Vip S.A., aliada de Diners Club).
**Estado:** flujo de **procesamiento e inteligencia completo y validado en batch sobre datos
reales**. Pendientes: disparador de *streaming* (Fase 5) y tablero (Fase 7).

> Figura integral (todo en una imagen): `docs/figuras/arquitectura_integral.svg`.

---

## 1. Infraestructura

### 1.1 Inventario de máquinas (verificado)

| Máquina | CPU | RAM | GPU | Rol observado | Limitación |
|---|---|---|---|---|---|
| **Laptop de desarrollo** | Intel Core Ultra 9 288V | 32 GB | **Intel Arc 140V (16 GB)** | Desarrollo + batch + ASR (GPU) | No es servidor 24/7 (equipo personal) |
| **Equipo de oficina (opción 1)** | Intel Core i5-1334U (10 núcleos) | 16 GB (ampliable a 32) | Intel UHD (integrada) | — | **Sin GPU útil** para ASR/diarización; RAM justa |
| **Servidor HP DL160 Gen9** | 16 × Xeon E5-2609 v4 **@ 1,70 GHz** | 31,75 GB (**88 % en uso**) | **Ninguna** | Streaming ligero (siempre encendido) | ESXi 6.0 EOL (2016); sin GPU; núcleos lentos |

### 1.2 Análisis: ¿cuál es la mejor opción?

- **Laptop (dev):** es la máquina más potente y la única con GPU capaz — hoy corre todo el
  desarrollo, el batch bajo demanda y la transcripción (Whisper en GPU Arc). **Pero es un
  equipo personal, no un servidor**: no puede quedar 24/7 en la oficina.
- **Equipo de oficina (i5):** **no es viable** para el núcleo pesado. La transcripción y, sobre
  todo, la **diarización** requieren GPU; su gráfica integrada (UHD) no acelera IA. Serviría a lo
  sumo como terminal de consulta del tablero.
- **Servidor HP DL160 Gen9:** apto **solo** para la parte liviana y siempre-encendida (Kafka,
  PostgreSQL, orquestación, *streaming* de una llamada a la vez). **No tiene GPU** y su ESXi 6.0
  está fuera de soporte, por lo que la diarización a escala sería inviable ahí (en CPU tarda
  ~0,8× tiempo real, ~15× más que la GPU).

**Conclusión:** **ninguna** de las máquinas actuales puede sostener el proyecto completo
(stack Docker 24/7 **+ GPU para ASR/diarización + Spark batch**) en producción. La laptop lo
hace para desarrollo, pero no como servidor.

### 1.3 Propuesta: equipo nuevo dedicado para la oficina

Se propone **adquirir un equipo dedicado** que quede en la oficina, en la LAN (para que el audio
y los datos personales nunca salgan), corriendo **todo el proyecto en Docker 24/7** más el
**motor GPU** de transcripción y diarización. Una **GPU NVIDIA** resuelve de un solo golpe los
dos cuellos actuales: acelera Whisper (CUDA) **y** la diarización pyannote (que hoy corre en CPU).

**Especificación mínima y deseable:**

| Componente | Mínimo (cubre el proyecto) | Deseable (holgura + histórico) |
|---|---|---|
| **CPU** | 8 núcleos / 16 hilos (Core i7-14700 / Ryzen 7 7700X / Xeon E-24xx) | 12–16 núcleos (Core i9 / Ryzen 9 / Xeon Silver) |
| **GPU** | **NVIDIA RTX 4060 Ti 16 GB VRAM** | NVIDIA RTX 4080/4090 o RTX A4000/A5000 (16–24 GB) — o RTX 3090 24 GB (buen valor) |
| **RAM** | 32 GB DDR5 | 64–128 GB DDR5 |
| **Almacenamiento** | 1 TB NVMe SSD | 2 TB NVMe (SO+datos) + 4 TB HDD/SSD (histórico/Parquet) |
| **Red** | Gigabit Ethernet (LAN) | Gigabit + UPS (respaldo) |
| **SO** | Ubuntu Server 22.04/24.04 LTS | Ubuntu Server LTS |

> **Por qué GPU NVIDIA:** el stack de IA (faster-whisper/CUDA y pyannote/torch-CUDA) aprovecha
> NVIDIA de forma nativa; con 16 GB de VRAM cabe el modelo grande de Whisper y la diarización se
> acelera **~15×** frente a la CPU del servidor actual. Es la diferencia entre procesar el
> histórico en semanas o en días.

### 1.4 Cómo se manejará en producción

- **Desarrollo (hoy):** laptop (todo local, GPU Arc).
- **Producción (propuesta):** **equipo nuevo en la oficina**, 24/7, corre **todo el stack**
  (Dagster, Kafka/KRaft, PostgreSQL, Spark, tablero) **y** el worker de ASR/diarización en GPU.
  Al estar en la LAN, se cumple la regla de privacidad (solo texto anonimizado sale a la nube).
- **Servidor HP:** rol complementario/respaldo o para *streaming* liviano si aún no se compra el
  equipo. No apto como nodo principal por falta de GPU y por su antigüedad (EOL).
- **Equipo i5:** terminal de consulta del tablero, no procesamiento.

---

## 2. Arquitectura y orquestación

> Ver figura integral `docs/figuras/arquitectura_integral.svg`.

### 2.1 Piezas y su rol

| Tecnología | Rol | Por qué |
|---|---|---|
| **Dagster** | Orquestador (Software-Defined Assets = zonas Medallion) | Linaje nativo, particiones/backfill (batch) y sensores (streaming) |
| **Kafka (modo KRaft)** | Bus de eventos que **desacopla** el ASR | Sin Zookeeper (más liviano); el worker de GPU escucha una cola |
| **Spark (PySpark)** | Preparación batch a escala (cruce CDR↔grabación) | Paralelismo demostrado (speedup 6,9×) |
| **Whisper worker (OpenVINO)** | Transcripción en **GPU** | El audio sensible nunca sale de la LAN |
| **pyannote** | Diarización agente/cliente | Separa hablantes en audio mono |
| **Presidio + spaCy** | **Anonimización** (frontera de privacidad) | Solo texto anonimizado va a la nube |
| **Google Gemini** | Análisis de calidad/cumplimiento | Juicio contextual sobre la rúbrica |
| **PostgreSQL** | Capa servida | Alimenta el tablero y las métricas |

### 2.2 Zonas Medallion y cómo se conectan

- **Bronce (crudo):** `bronze_cdr` (CDR de MariaDB, **solo lectura** → Parquet por día) y
  `bronze_audio_index` (índice de las grabaciones del alcance: **7,06 M archivos / 424 GB**).
- **Plata (limpio):** `silver_calls` cruza CDR↔grabación con Spark; la muestra se encola en
  **Kafka** (`asr.jobs`); el **worker** transcribe en GPU, **diariza** y **anonimiza**; el
  resultado (`asr.results`) se guarda en `silver_transcriptions` (**texto anonimizado**).
- **Oro (valor):** `gold_evaluations` aplica la rúbrica (capa determinista + Gemini) → evaluación
  estructurada por llamada.
- **Servido:** PostgreSQL (`servido.llamadas`, `servido.transcripciones`, `servido.evaluaciones`)
  → tablero (Fase 7).

### 2.3 Batch y streaming: el mismo flujo, distinto disparo

Los dos caminos **comparten el 100 % de los módulos** (transcribir, diarizar, anonimizar,
evaluar) y el mismo bus Kafka. Solo cambia **cómo se dispara**:
- **Batch (implementado):** Dagster lanza el trabajo **por rango de fechas** (backfill del
  histórico) y Spark prepara a escala.
- **Streaming (Fase 5, próximo):** un **sensor** de Dagster detecta la llamada nueva y arranca el
  mismo pipeline. **No es reconstruir nada** — es agregar el disparador automático; el worker ya
  funciona como consumidor de cola.

---

## 3. Resultados con datos reales

### 3.1 Fase 2 — Preparación batch (Spark)
- **Cruce CDR↔grabación: 100 % de cobertura** (mayo 2025 completo, 153 533/153 533; 0 huérfanas).
- **Escalabilidad Spark** (mismo trabajo, mes completo):

| Núcleos | Tiempo | Aceleración |
|---|---|---|
| 1 | 390 s | 1× |
| 2 | 105 s | 3,7× |
| 4 | 57 s | 6,9× |

> Figuras: `speedup_spark_mayo2025.svg`, `carga_streaming_y_reproceso.svg`.

### 3.2 Fase 3 — Audio → texto seguro
- Transcripción **Whisper en GPU Arc**: RTF **0,05×** (18× más rápido que tiempo real);
  en CPU 0,10× (aún viable).
- **Diarización** agente/cliente y **anonimización** (cédula módulo 10, tarjeta Luhn, teléfonos,
  nombres, números dictados) → **0 fugas de PII** en la validación.

| Config | Tiempo de proceso (llamada larga ~35 min) | RTF |
|---|---|---|
| GPU Arc | ~1,8 min | 0,055× |
| CPU | ~3,2 min | 0,096× |

> Figura: `benchmark_asr_llamadas_largas.svg`.

### 3.3 Carga y escala (métricas de negocio)
- **98,3 %** de las llamadas son cortas (<5 min); solo **0,8 %** son largas (≥10 min).
- Un **solo nodo** sostiene el *streaming* en tiempo real; las largas (pico ~20/hora) no son
  cuello de botella.
- **Reproceso total del histórico:** ~126 500 h de audio → ~290 días en 1 nodo GPU → justifica
  procesar por muestra + streaming, y **paralelizar en N nodos** para el histórico completo.

### 3.4 Fase 4 — Análisis de cumplimiento (Gemini)
- Análisis **híbrido** (reglas deterministas + IA) contra la rúbrica de la empresa (rubrica_v1).
- **Caso estrella (valor de la tesis):** en una llamada real (agente 204) el sistema detectó
  automáticamente la palabra prohibida **"GARANTIZO"** (severidad CRÍTICA → riesgo de multa de la
  Superintendencia), además de la **trayectoria de sentimiento del cliente**
  (neutral → escéptico → negativo). Esto es lo que la auditoría manual, que escucha muy pocas
  llamadas, no alcanza a revisar.

| Llamada | Palabras prohibidas | Riesgo | Sentimiento cliente |
|---|---|---|---|
| ag. 203 (36 min) | B13, B16 | medio | confundido → interesado → desconfiado |
| **ag. 204 (33 min)** | **B01 (GARANTIZO)**, B08, B13, B16 | **alto** | neutral → escéptico → negativo |
| ag. 217 (30 min) | B06, B12, B13, B16 | alto | neutral → dudoso → neutral |

> Figura: `fase4_analisis_hibrido.svg`.

---

## 4. Lo que sigue

1. **Validación del tutor** de este núcleo (objetivo de esta presentación).
2. **Fase 5 — Streaming:** sensor de Dagster + encadenado en vivo (reusa todo lo anterior).
3. **Fase 7 — Tablero:** KPIs, riesgo y palabras prohibidas por agente (Streamlit).
4. **Pulido por fase:** benchmark del modelo ASR superior (medium/large-v3), afinar la
   sobre-redacción, gold set (weak supervision) + métricas P/R/F1, calibrar pesos con Auditoría.
5. **Inversión:** equipo nuevo con GPU para llevar todo a producción en la oficina (§1.3).

---

*Detalle técnico completo y reproducible en `docs/bitacora_tecnica.md` (Partes A–G). Figuras en
`docs/figuras/`.*
