# Tecnologías del proyecto — Referencia técnica

> Documento de consulta permanente. Para cada tecnología: **qué es**, **cómo
> funciona**, **cómo la usamos en este proyecto** y su **equivalente en AWS**.
> Complementa [pipeline.md](pipeline.md), [fases.md](fases.md) e
> [infraestructura.md](infraestructura.md).

**¿Funciona igual on-premise que en AWS?** Sí. El **diseño es agnóstico**: los módulos
van en contenedores y la lógica no cambia; solo se sustituye la pieza de
infraestructura por su equivalente gestionado en la nube (ver tabla al final).

---

## Tabla resumen

| # | Tecnología | Rol en el proyecto | Zona Medallion | Equivalente AWS |
|---|---|---|---|---|
| 1 | Docker / Compose | Empaquetado y orquestación local | — | ECS / EKS |
| 2 | MySQL (lectura) | Fuente CDR | 🥉 | (origen externo) |
| 3 | Apache Kafka | Cola de eventos (streaming) | 🥉 | SQS / MSK / Kinesis |
| 4 | Apache Spark / PySpark | Procesamiento batch distribuido | 🥉🥈 | EMR / Glue |
| 5 | ffmpeg | Normalización de audio | 🥉 | (igual en EC2) |
| 6 | faster-whisper | ASR (audio→texto) | 🥉 | Transcribe / EC2 GPU |
| 7 | WhisperX / pyannote | Diarización (quién habla) | 🥉 | (igual en EC2) |
| 8 | Presidio + spaCy | Anonimización (PII) | 🥉→🥈 | Comprehend (PII) |
| 9 | Gemini / Bedrock | Análisis semántico (rúbrica, sentimiento) | 🥈→🥇 | Bedrock |
| 10 | MinIO / Parquet | Data lake Medallion | 🥉🥈🥇 | S3 |
| 11 | PostgreSQL | Capa servida (KPIs) | 🥇 | RDS |
| 12 | scikit-learn / SciPy / statsmodels | Anomalías y estadística | 🥇 | SageMaker (opc.) |
| 13 | Streamlit | Tablero analítico | — | QuickSight |
| 14 | Prefect / cron | Orquestación de jobs | — | Step Functions / MWAA |
| 15 | pydantic / rapidfuzz | Validación y fuzzy-match | 🥈🥇 | (igual) |

---

## 1. Docker y Docker Compose
- **Qué es:** un **contenedor** empaqueta un programa con sus dependencias para que
  corra idéntico en cualquier máquina. **Compose** levanta varios contenedores juntos.
- **Cómo funciona:** cada servicio se define en `docker-compose.yml`; `docker compose up`
  los arranca en una red interna. Perfiles `dev` (tu PC) y `prod` (VM/EC2).
- **En el proyecto:** garantiza reproducibilidad on-prem ↔ nube. Todo el stack
  (Kafka, Postgres, Whisper, anonimización, tablero) son contenedores.
- **AWS:** ECS (contenedores gestionados) o EKS (Kubernetes).

## 2. MySQL (solo lectura del CDR)
- **Qué es:** la base relacional de Asterisk con la tabla `cdr`.
- **Cómo funciona:** consultamos por rango (batch) o por marca de agua (streaming).
  Charset `latin1` → forzar UTF-8 para acentos.
- **En el proyecto:** **única fuente de metadatos**; nunca escribimos en producción.
- **AWS:** sigue siendo el origen externo (se lee desde la EC2/agente).

## 3. Apache Kafka
- **Qué es:** plataforma de **streaming de eventos** (topics, particiones, offsets).
- **Cómo funciona:** productores publican eventos; consumidores los leen a su ritmo y
  de forma durable. Si un consumidor cae, retoma desde su offset sin perder datos.
- **En el proyecto:** transporta cada llamada por el pipeline
  (`llamadas.finalizadas` → `transcripciones` → `anonimizadas` → `analisis.calidad`).
  Desacopla la generación de la llamada del procesamiento lento (ASR+LLM) y absorbe picos.
- **AWS:** **SQS** (cola simple y baratísima, ideal con encendido por horario), **MSK**
  (Kafka gestionado) o **Kinesis**.

## 4. Apache Spark / PySpark
- **Qué es:** motor de cómputo **distribuido** para grandes volúmenes; **PySpark** es su
  API en Python. Usa DataFrames particionados, evaluación *lazy* (DAG) y recuperación por linaje.
- **Cómo funciona:** parte los datos en trozos y los procesa en paralelo entre *workers*;
  si un nodo falla, recomputa solo el trozo perdido.
- **En el proyecto:** **camino batch** — reprocesa el histórico por rango de fechas con la
  misma limpieza/validación. Permite **medir speedup** (evidencia de escalabilidad que pide
  la maestría).
- **AWS:** **EMR** (Spark gestionado) o **Glue** (Spark serverless).

## 5. ffmpeg
- **Qué es:** herramienta estándar de procesamiento de audio/video.
- **Cómo funciona:** convierte formatos; aquí a **mono, 16 kHz, PCM**, lo que Whisper espera.
- **En el proyecto:** normaliza las grabaciones (incluye los `.wav` que nunca pasaron a `.mp3`).
- **AWS:** igual, dentro de la EC2.

## 6. faster-whisper (ASR)
- **Qué es:** implementación optimizada de **Whisper** (OpenAI) con **CTranslate2** e
  **int8** → más rápido y con menos RAM en CPU.
- **Cómo funciona:** recibe el audio normalizado y devuelve texto con marcas de tiempo.
  Modelo `small`/`medium` según RAM disponible; `language="es"`.
- **En el proyecto:** transcribe **on-premise** (el audio crudo nunca sale). Para el
  histórico completo se puede acelerar con **GPU en AWS** puntualmente.
- **AWS:** Whisper en **EC2 GPU** (`g4dn`), o **Amazon Transcribe** (gestionado; pero
  envía audio crudo a AWS).

## 7. WhisperX / pyannote (diarización)
- **Qué es:** separa **quién habla** (asesor vs cliente) cuando comparten canal, y alinea
  el texto con cada hablante.
- **Cómo funciona:** detecta segmentos por voz y los agrupa por hablante; WhisperX además
  alinea palabras con tiempos precisos.
- **En el proyecto:** imprescindible para evaluar el **script del asesor** (Grupo A) y el
  **sentimiento del cliente** por separado.
- **AWS:** igual, en la EC2 (Transcribe también ofrece *speaker labels*).

## 8. Presidio + spaCy (anonimización)
- **Qué es:** **Presidio** (Microsoft) detecta y reemplaza **PII**; **spaCy** aporta el
  modelo de lenguaje español para **NER** (nombres, entidades).
- **Cómo funciona:** combina **regex** (cédula EC, tarjetas, teléfonos, montos, correos)
  con **NER** → sustituye por etiquetas (`<CEDULA>`, `<TARJETA>`, `<NOMBRE>`).
- **En el proyecto:** es la **frontera de privacidad** (Bronce→Plata). Solo el texto
  anonimizado puede ir a Gemini.
- **AWS:** **Amazon Comprehend** (detección de PII) como alternativa gestionada.

## 9. Gemini / Amazon Bedrock (LLM)
- **Qué es:** modelos de lenguaje grandes accesibles por API. **Gemini** (Google) o
  **Bedrock** (varios modelos en AWS).
- **Cómo funciona:** se les pasa el **texto anonimizado + la rúbrica** y devuelven **JSON
  estructurado** (criterios A/B/C, `calidad_score`, `venta_valida`, sentimiento).
- **En el proyecto:** hace el **análisis semántico pesado** sin consumir tu hardware ni ver
  datos personales.
- **AWS:** **Bedrock** mantiene el análisis dentro de la nube de AWS.

## 10. MinIO / Parquet (data lake Medallion)
- **Qué es:** **MinIO** es almacenamiento de objetos compatible con S3 para on-premise;
  **Parquet** es un formato columnar comprimido y eficiente.
- **Cómo funciona:** organizamos `bronze/`, `silver/`, `gold/` como prefijos; los datos
  tabulares se guardan en Parquet particionado por fecha.
- **En el proyecto:** implementa las **zonas Medallion** on-premise.
- **AWS:** **Amazon S3** (mismo esquema de prefijos), consultable con **Athena/Glue**.

## 11. PostgreSQL (capa servida)
- **Qué es:** base de datos relacional robusta.
- **Cómo funciona:** almacena el **Oro** (`llamadas`, `evaluaciones`, `kpis_agente`,
  `anomalias`); batch y streaming escriben el **mismo esquema**.
- **En el proyecto:** fuente única del tablero, consistente entre tiempo real e histórico.
- **AWS:** **RDS PostgreSQL** (gestionado) o self-host en la EC2.

## 12. scikit-learn / SciPy / statsmodels
- **Qué es:** librerías de ML y estadística en Python.
- **Cómo funciona:** `IsolationForest`/z-score para **anomalías no supervisadas**;
  pruebas estadísticas para validar mejoras.
- **En el proyecto:** detectar agentes con comportamiento anómalo y **demostrar
  significancia** frente a la auditoría manual (Fase 8).
- **AWS:** igual (opcionalmente SageMaker).

## 13. Streamlit (tablero)
- **Qué es:** framework Python ligero para dashboards web.
- **Cómo funciona:** lee el Oro de PostgreSQL y renderiza KPIs, filtros y alertas.
- **En el proyecto:** **producto final** para gerencia/auditoría/agentes; ligero para la RAM disponible.
- **AWS:** **QuickSight** (BI gestionado) o Streamlit en la EC2.

## 14. Prefect / cron (orquestación)
- **Qué es:** programación y monitoreo de *jobs*. `cron` para lo simple; **Prefect** si se
  requiere reintentos/observabilidad.
- **En el proyecto:** dispara el batch por rango de fechas y tareas periódicas (KPIs).
- **AWS:** **Step Functions** o **MWAA** (Airflow gestionado); **EventBridge Scheduler**
  para el encendido/apagado por horario de la EC2.

## 15. pydantic / rapidfuzz (calidad de datos)
- **Qué es:** **pydantic** valida esquemas (que el JSON del LLM sea correcto); **rapidfuzz**
  hace coincidencias aproximadas de texto.
- **En el proyecto:** validar la salida de Gemini y **fuzzy-match** de los descargos legales
  exactos (A07/C05) que no deben depender solo del LLM.
- **AWS:** igual.

---

## Equivalencias On-Premise ↔ AWS (resumen)

| Capa | On-Premise (este proyecto) | AWS |
|---|---|---|
| Contenedores | Docker + Compose | ECS / EKS |
| Cola de eventos | Apache Kafka | SQS / MSK / Kinesis |
| Batch | Spark / PySpark | EMR / Glue |
| ASR | faster-whisper (CPU) | EC2 GPU / Transcribe |
| Anonimización | Presidio + spaCy | Comprehend |
| LLM | Gemini API | Bedrock |
| Data lake Medallion | MinIO + Parquet | S3 (+ Athena/Glue) |
| Capa servida | PostgreSQL | RDS |
| Tablero | Streamlit | QuickSight |
| Orquestación / horario | Prefect / cron | Step Functions / EventBridge |

**Conclusión:** el proyecto se construye una sola vez y puede **desplegarse on-premise
(recomendado por costo/privacidad/latencia) o en AWS** cambiando únicamente la capa de
infraestructura, no la lógica ni los pipelines.
