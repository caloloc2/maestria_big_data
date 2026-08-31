# Pasos de operación — demo para el tutor

> Guía operativa en orden para levantar, ejecutar y validar el proyecto.
> Todos los comandos se corren desde la raíz del repo (`C:\codigos\maestria\uisrael`) en PowerShell.
> **Requisito previo:** estar en la oficina (LAN) o con VPN activa (se necesita alcanzar
> MariaDB `192.168.0.40` y el SFTP de grabaciones de Asterisk).

---

## 0. Panorama rápido

- Todo va **dockerizado** (Postgres, Kafka, MinIO, Dagster, `streaming_runner`, dashboard)
  **excepto el Whisper worker**, que corre nativo en el host para usar la GPU (OpenVINO/Arc).
- **Batch** = reproceso de un día a demanda (horas valle). **Streaming** = tiempo real en horario laboral.
- **Regla de negocio:** solo las grabaciones **> 600 s (10 min)** se envían a Gemini (ahorro de tokens + refuerza LOPDP).
- **No-intrusión probada:** usuario de BD `lectura` = solo `SELECT`; grabaciones por SFTP de solo lectura.

---

## 1. Iniciar el proyecto completo

**Primera vez del día o tras cambiar dependencias:**
```bash
.\scripts\up.ps1 -Build
```

**Arranque normal (imágenes ya construidas):**
```bash
.\scripts\up.ps1
```

Levanta en un solo comando: Docker (infra + Dagster + tablero + `streaming_runner`) **y** el Whisper worker en GPU.
Al terminar muestra: `Dagster: http://localhost:3000 | Tablero: http://localhost:8501`.

**Verificar que todo está arriba** (6 contenedores `Up` + `kafka_init`/`minio_init` en `Exited (0)`):
```bash
docker compose -f infra/docker-compose.yml ps
```

**Confirmar que el worker vive y está consumiendo:**
```bash
Get-Content .\data\worker.out.log -Tail 20
```

---

## 2. Ejecutar un BLOQUE BATCH (reproceso de un día)

**Prueba rápida (limita a 60 transcripciones, ideal para la demo):**
```bash
docker exec uisrael_dagster_webserver bash -lc "cd /opt/dagster/app && dagster asset materialize -f src/definitions.py --select bronze_cdr,bronze_audio,silver_calls,silver_transcriptions,gold_evaluations --partition 2025-05-14"
```

**Prueba MAS rápida (limita a 10 transcripciones, ideal para la demo):**
```bash
docker exec -e AUDIO_LAND_LIMIT=20 -e ASR_LIMIT=10 uisrael_dagster_webserver bash -lc "cd /opt/dagster/app && dagster asset materialize -f src/definitions.py --select bronze_cdr,bronze_audio,silver_calls,silver_transcriptions,gold_evaluations --partition 2026-07-16"
```

**Día completo (todas las llamadas):**
```bash
docker exec -e ASR_LIMIT=0 -e ASR_TIMEOUT=36000 uisrael_dagster_webserver bash -lc "cd /opt/dagster/app && dagster asset materialize -f src/definitions.py --select bronze_cdr,bronze_audio,silver_calls,silver_transcriptions,gold_evaluations --partition 2025-05-14"
```

**Qué hace, paso a paso:**
1. `bronze_cdr` — lee el CDR del día con **Spark JDBC** (solo lectura) → Parquet a `s3a://bronce/cdr/`.
2. `bronze_audio` — copia los MP3 del día de Asterisk a `s3a://bronce/audio/` por SFTP (auditoría).
3. `silver_calls` — cruza CDR↔grabación (linkage) → `servido.llamadas`.
4. `silver_transcriptions` — encola cada audio a Kafka; el worker transcribe + anonimiza → `servido.transcripciones`.
5. `gold_evaluations` — manda solo las > 600 s a Gemini → `servido.evaluaciones`.

> El batch se corre **bajo demanda en horas valle (madrugada)**; hay un solo worker (cola FIFO compartida con el streaming).

---

## 3. Ejecutar STREAMING (tiempo real)

**Ya arranca solo** con `.\scripts\up.ps1` (es el servicio `streaming_runner`). No hay que lanzar nada aparte.

**Ver el runner detectando y procesando llamadas en vivo:**
```bash
docker logs -f uisrael_streaming_runner
```

**En otra ventana, ver el worker transcribiendo:**
```bash
Get-Content .\data\worker.out.log -Wait -Tail 30
```

Ciclo: `poll CDR → localiza grabación → aterriza en Bronce → asr.jobs → worker transcribe → asr.results → servido.transcripciones (+ Gemini si > 600 s)`. Latencia extremo-a-extremo **< 2 min por llamada**.

**Reiniciar solo el runner (si hace falta):**
```bash
docker restart uisrael_streaming_runner
```

### ¿Reanuda desde donde se quedó? — Sí
El runner guarda un **cursor** (`cdr_stream`) con la última llamada procesada en **tu PostgreSQL**
(`servido.stream_cursor`). Como `down.ps1` no borra volúmenes (`-v`), el cursor se conserva y al
reencender procesa **todo lo que entró desde el cursor hacia adelante** (no se pierde nada).
La primera vez sin cursor arranca mirando los últimos 30 min (`STREAM_LOOKBACK_SECS=1800`).

---

## 4. Validar en el DASHBOARD (Streamlit) — http://localhost:8501

7 vistas: **Resumen**, **⚡ Tiempo real** (úsala en vivo), **Por agente**, **Calidad / infracciones (grupo B)**,
**Intentos / reintentos**, **Anomalías (z-score)**, **🔎 Detalle de llamada** (transcripción anonimizada
completa + desglose de Gemini).

**Prueba de consistencia (los números cuadran con SQL directo):**
```bash
docker exec uisrael_postgres psql -U dagster -d dagster -c "SELECT origen, count(*) FROM servido.llamadas GROUP BY origen;"
```

---

## 5. Validar en DAGSTER (orquestador) — http://localhost:3000

- **Assets** → grafo Medallion `bronze_cdr → bronze_audio → silver_calls → silver_transcriptions → gold_evaluations`
  agrupado por zona (Bronce/Plata/Oro); muestra el **linaje** de datos.
- Click en un asset → **Materializations** → fecha, filas y metadatos de cada corrida.
- **Runs** → historial de ejecuciones batch con logs y tiempos por paso.
- Se puede lanzar un batch desde la UI: seleccionar assets → **Materialize** → elegir partición (fecha).

---

## 6. Validar en MinIO (lago S3 local) — http://localhost:9001

Usuario `minioadmin` / clave `minioadmin`.
- Bucket **`bronce`** → `cdr/date=YYYY-MM-DD/` (Parquet del CDR) y `audio/date=YYYY-MM-DD/` (**MP3 crudos**, auditoría).
- Bucket **`plata`** → datos limpios/anonimizados.
- La zona **Oro NO está en MinIO** (decisión con el tutor): vive solo en PostgreSQL y el tablero la lee por SQL.

**Listar objetos de un día por línea de comando:**
```bash
docker exec uisrael_minio mc ls --recursive local/bronce/audio/date=2025-05-14/
```
(Si `mc` no tiene alias en ese contenedor, usa la consola web, que es lo más visual para la reunión.)

---

## 7. Cambiar el umbral de envío a Gemini (si el tutor lo pide)

### STREAMING — no hace falta bajar todo, solo recrear el runner
La variable `STREAM_EVAL_MIN_SECS` está fijada en el compose, así que hay que editar ese valor:

1. Edita `infra/docker-compose.yml` (línea del servicio `streaming_runner`):
   `STREAM_EVAL_MIN_SECS: "600"` → el valor deseado (ej. `"300"` o `"60"`).
2. Recrea **solo** ese contenedor:
```bash
docker compose -f infra/docker-compose.yml up -d streaming_runner
```
3. Confirma el nuevo umbral en el log de arranque:
```bash
docker logs --tail 5 uisrael_streaming_runner
```
Debe decir `... eval_min=300s ...` (o el valor que pusiste).

> Kafka, MinIO, Postgres, el worker y el cursor quedan intactos. El worker **no** se reinicia
> (el filtro de duración vive en el runner, no en el worker).

### BATCH — ni se edita ni se reinicia, se pasa inline
```bash
docker exec -e EVAL_MIN_SECS=300 -e ASR_LIMIT=0 uisrael_dagster_webserver bash -lc "cd /opt/dagster/app && dagster asset materialize -f src/definitions.py --select gold_evaluations --partition 2025-05-14"
```

---

## 8. Apagar todo

Conserva los datos (Postgres/MinIO/Kafka). **Nunca uses `-v`.**
```bash
.\scripts\down.ps1
```

---

## 9. Tiempos de ejecución (para explicar al tutor)

| Operación | Tiempo / métrica |
|-----------|------------------|
| Lectura CDR de un día (Spark JDBC) | segundos |
| Speedup Spark 1 / 2 / 4 núcleos | 390 / 105 / 57 s (≈ 6,9×); bajo RAM alta 623/197/113 s |
| ASR (transcripción) RTF GPU Arc / CPU | 0,04–0,10 / 0,08 (12–25× más rápido que tiempo real) |
| Llamada larga de 30–36 min → transcripción | GPU ~1,8 min / CPU ~3,2 min |
| Streaming E2E por llamada | < 2 min, RTF 0,02–0,05 |
| Batch día completo (~886 transcripciones) | ~1 h 50 min |
| Diarización (pyannote, CPU) | RTF ~0,8 → llamada de 10 min tarda ~10–12 min (por eso está diferida) |
| Evaluación Gemini | solo llamadas > 600 s (regla de costo) |
| Throughput 1 nodo GPU | ~18 h de audio por hora de cómputo → 1 nodo sostiene el streaming en hora pico |

**Dimensionamiento:** reproceso total = 7,06 M grabaciones / ~126 500 h de audio → 1 nodo GPU ≈ 290 días.
Por eso: streaming en vivo + reproceso por fechas a demanda, no todo de golpe.

---

## 10. Guion breve para el tutor

> "Construí una arquitectura Big Data híbrida —**Kafka para streaming + Spark para batch**, orquestada por
> **Dagster** con zonas Medallion— que procesa el CDR y las grabaciones **sin tocar la base de producción**
> (usuario solo lectura, probado con `SHOW GRANTS`; grabaciones por SFTP de solo lectura).
>
> En **tiempo real**, cuando un asesor cuelga, el sistema detecta la llamada en el CDR, localiza el MP3, lo
> guarda en el lago **MinIO**, lo **transcribe con Whisper en GPU**, lo **anonimiza** (Presidio) y solo entonces
> —si dura más de 10 min— lo manda a **Gemini** para evaluar calidad y cumplimiento. Todo en **menos de 2 minutos**.
>
> El **tablero** une batch y streaming y detecta automáticamente infracciones críticas que la auditoría manual
> casi nunca pilla, como decir 'garantizo'. Apliqué las dos observaciones de la reunión pasada: migré el CDR a
> **PySpark JDBC** y **guardo el MP3 crudo en Bronce** para auditoría."

**Por si pregunta por cuellos/decisiones:** un solo worker → cola compartida (por eso batch en madrugada);
diarización diferida por falta de GPU NVIDIA (no bloquea nada); sobre-marcado de "crítica" que se corrige
con el gold set (validación a oído, siguiente paso).



