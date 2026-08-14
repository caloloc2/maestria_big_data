# Observaciones del tutor — Análisis y plan de ajuste

> Documento donde recojo el comentario de mi tutor sobre el plan de titulación,
> mi análisis de cada punto, cómo lo voy a resolver, dos propuestas de nuevo título
> enfocadas a Big Data y las preguntas que le haré para cerrar el alcance.
> Lo escribo en primera persona porque es mi hoja de trabajo para responder la observación.
> Referencias: [plan_titulacion.md](plan_titulacion.md), [fases.md](fases.md),
> [pipeline.md](pipeline.md), [tecnologias.md](tecnologias.md),
> [infraestructura.md](infraestructura.md).

---

## 1. Comentario recibido del tutor

> «Se evidencia una deficiencia en la justificación del volumen de datos, no demuestra
> una escala suficiente para requerir arquitecturas distribuidas, ya que los 100.000
> registros y 100 GB de audio podrían ser procesados con infraestructuras
> convencionales; además, no se especifica una arquitectura Big Data completa que
> incluya mecanismos de ingesta distribuida, almacenamiento masivo, procesamiento
> paralelo, gobernanza y calidad de datos. La propuesta también requiere mayor
> precisión en la definición del modelo predictivo, variables analizadas, técnicas de
> detección de anomalías, métricas de evaluación y estrategia de validación
> experimental. El uso de LLM se plantea como un componente central, pero no se
> establece claramente su integración dentro del ciclo de procesamiento Big Data. Se
> recomienda fortalecer la propuesta incorporando una arquitectura basada en streaming,
> Data Lake, motores de procesamiento distribuido, modelos analíticos reproducibles,
> criterios de escalabilidad y métricas cuantificables de impacto, con el fin de
> transformar el planteamiento actual de una solución de inteligencia artificial
> aplicada en una solución de Big Data Analytics.»
>
> Comentario adicional: **«Cambiar el tema para enfocarse a la Big Data».**

---

## 2. Estado actual

Después de contrastar la observación con todo lo que ya definí en mis documentos de
implementación, mi conclusión es clara:

- **La mayor parte de lo que pide mi tutor ya la tengo resuelta a nivel de ingeniería**
  (streaming, Data Lake Medallion, procesamiento distribuido, métricas). El problema es
  que **eso no está reflejado en el `plan_titulacion.md` que él leyó**: el plan quedó
  redactado como «IA aplicada» y no muestra la arquitectura Big Data completa.
- **Acepto el punto más fuerte:** mi justificación por **volumen es débil**. Es cierto
  que 100.000 registros son pocos y que 100 GB caben en una sola máquina. La solución
  **no es inflar el volumen**, sino **reencuadrar por qué esto es Big Data** y **hacer
  explícita la arquitectura y la gobernanza**.

Es decir: no estoy lejos; me falta **subir la arquitectura al plan**, **cambiar el
argumento de justificación** y **formalizar dos vacíos reales** (gobernanza/calidad y la
definición del modelo analítico).

---

## 3. El eje del problema: por qué SÍ es Big Data (reencuadre por las «V»)

Mi error fue defender Big Data **solo por Volumen**. Big Data se sustenta en varias
dimensiones, y en mi caso las fuertes son otras:

| Dimensión | Cómo la argumento en mi proyecto |
|---|---|
| **Variedad** (la más fuerte) | Combino datos **no estructurados** (voz), **estructurados** (CDR) y **texto** derivado; es un problema **multimodal** [ref. 12]. |
| **Velocidad** | Proceso un **flujo continuo** de eventos (cada llamada al colgar) en modo **near-real-time** por streaming. |
| **Veracidad** | Debo manejar **alucinaciones** del ASR [ref. 6], **calidad y trazabilidad** de datos y **anonimización**; esto exige gobernanza. |
| **Valor** | Genero KPIs de negocio y reduzco **riesgo regulatorio** (multas de la Superintendencia). |
| **Volumen** | No es un dataset estático de 100 GB: es un **stream de producción que crece de forma indefinida**, con **datos derivados** (transcripciones, features) y un **alto costo computacional de ASR** (procesar cada segundo de audio es *compute-bound*, no solo almacenamiento). |

Una sola máquina *podría* procesar los 100 GB
de una vez, pero **no garantiza** ingesta continua sin pérdida, tolerancia a fallos,
reprocesamiento ni **escalabilidad horizontal** en un sistema que va a operar **en
producción de forma permanente**. Ese es el criterio de arquitectura, no el tamaño
puntual del dataset. (Esta defensa ya la tengo desarrollada en
[proyecto/justificacion_kafka_spark.md](proyecto/justificacion_kafka_spark.md); solo
debo llevarla al plan.)

---

## 4. Mapeo: cada observación → qué ya tengo → qué me falta

| Observación del tutor | ¿Ya lo tengo? | Qué me falta hacer |
|---|---|---|
| Justificación de volumen/escala | Parcial | Reencuadrar por las «V» + medir escalabilidad (speedup) + estimar el crecimiento en producción |
| Ingesta distribuida | ✅ Kafka | Subirlo al plan como componente formal |
| Almacenamiento masivo / Data Lake | ✅ Medallion (MinIO/S3) | Nombrarlo explícitamente «Data Lake Medallion» |
| Procesamiento paralelo | ✅ Spark/PySpark | Añadir criterios de escalabilidad (strong/weak scaling) |
| **Gobernanza y calidad de datos** | ⚠️ Parcial (anonimización, trazabilidad) | **Formalizar** una capa transversal de gobernanza + calidad |
| Modelo predictivo: variables y técnicas | Parcial | **Definirlo con precisión** (§6) |
| Métricas de evaluación | ✅ Accuracy/P/R/F1, KPIs, throughput | Formalizarlas en tabla + pruebas estadísticas |
| Estrategia de validación experimental | Parcial | Definir muestra, validación cruzada, tests de significancia, línea base |
| Integración del LLM en el ciclo Big Data | ✅ (pipeline bronce→plata→oro) | Explicitar el LLM como **etapa del pipeline**, no como pieza aislada |
| Modelos reproducibles | Parcial (contenedores) | Versionado de rúbrica/modelos/datasets + semillas |

---

## 5. Arquitectura Big Data completa que voy a explicitar

Para responder «arquitectura Big Data completa», dejo explícitos los cinco bloques que el
tutor menciona:

1. **Ingesta distribuida:** Apache **Kafka** (evento por llamada) — camino streaming.
2. **Almacenamiento masivo (Data Lake):** **Medallion** (Bronce/Plata/Oro) sobre
   **MinIO/Parquet** on-premise (equivalente **S3** en nube).
3. **Procesamiento distribuido:** **Apache Spark / PySpark** — camino batch por rango de
   fechas, con **medición de escalabilidad**.
4. **Gobernanza y calidad de datos:** capa transversal (calidad automática, catálogo,
   linaje, contratos de datos, versionado, privacidad).
5. **Analítica y consumo:** LLM (Gemini) como etapa del pipeline + modelos de anomalías +
   **capa servida** (PostgreSQL) + **tablero**.

El **LLM se integra dentro del ciclo Big Data**, no fuera: recibe el dato de la zona
**Plata** (texto anonimizado) y produce la zona **Oro** (evaluaciones estructuradas), como
una etapa más del pipeline gobernado.

---

## 6. Los dos vacíos reales que debo cerrar

### 6.1. Gobernanza y calidad de datos
Añado una capa **transversal** con:
- **Calidad:** validaciones automáticas en cada frontera de zona (**Great Expectations /
  pandera / Soda**): esquemas, rangos, nulos, unicidad de `call_id`, cobertura del cruce
  CDR↔grabación.
- **Gobernanza:** **catálogo** de datos + **linaje** (de Bronce a Oro), **contratos de
  datos** (esquemas con `pydantic`), **versionado** de rúbrica (`rubrica_v1/v2`), de
  modelos y de datasets (**DVC/lakeFS** opcional).
- **Privacidad como gobernanza:** auditoría de anonimización, control de acceso, retención.
- **Reproducibilidad:** contenedores + dependencias fijadas + **semillas** + ejecuciones
  parametrizadas.

### 6.2. Definición precisa del modelo analítico
Lo escribo de forma formal, en dos familias:

- **Familia A — Clasificación de calidad/cumplimiento por llamada.**
  - *Entrada:* transcripción anonimizada y diarizada + variables del CDR.
  - *Técnica:* LLM (Gemini) con la rúbrica + reglas deterministas + *fuzzy-match* de
    descargos legales exactos.
  - *Salida:* vector de criterios A/B/C, `calidad_score`, `venta_valida` (regla dura).
  - *Métricas:* **Accuracy, Precision, Recall, F1** contra el *gold set*.

- **Familia B — Detección de anomalías y pronóstico por agente/periodo.**
  - *Variables (explícitas):* tasa de contactabilidad, tasa de conversión, calidad media,
    `% ventas válidas`, duración media, `% sentimiento negativo`, volumen de llamadas, franja horaria.
  - *Técnicas:* **Isolation Forest**, **autoencoders** [ref. 15] y **z-score** (no
    supervisado); **series temporales** [ref. 16] para el pronóstico de KPIs.
  - *Validación:* confirmación humana de los casos marcados + `precision@k`.

- **Estrategia de validación experimental (común):** *gold set* por *weak supervision*,
  **muestreo estratificado**, **validación cruzada**, **pruebas estadísticas**
  (SciPy/statsmodels) antes/después frente a la auditoría manual y concordancia entre
  auditores (**Cohen's Kappa**).

---

## 7. Cómo aplico el ajuste (orden de trabajo)

1. **`fases.md`** (lo hago ahora): agrego la **justificación Big Data por las «V»**, la
   **capa transversal de gobernanza y calidad** y los **criterios de escalabilidad**, y
   preciso el **modelo analítico** en las fases de modelado.
2. **Nuevo documento del plan** (a partir de ahora, sin tocar el `plan_titulacion.md`
   entregado): reescribo **II. Problema** (reencuadre por «V»), **VII.D Instrumentos**
   (Data Lake Medallion + gobernanza/calidad) y **VII.E Análisis** (modelo, variables,
   técnicas, validación) y añado un **diagrama de arquitectura Big Data** end-to-end.

---

## 8. Sugerencias de nuevo título (enfocado a Big Data)

Mi tutor pidió **cambiar el tema para enfocarlo a Big Data**. Propongo estas dos
alternativas, que ponen la **arquitectura Big Data** como núcleo y dejan los LLM como
componente:

**Opción 1**
> «**Arquitectura Big Data con procesamiento distribuido y streaming para el análisis de
> metadatos y grabaciones de call center: integración de Data Lake y LLMs en la evaluación
> de calidad y la detección de anomalías en el rendimiento de los agentes.**»

**Opción 2**
> «**Plataforma de Big Data Analytics basada en arquitectura Lambda para la ingesta
> distribuida, el procesamiento paralelo y el análisis con LLMs de interacciones de call
> center, orientada a la identificación de prácticas críticas y la predicción de anomalías
> de desempeño.**»

> Ambas conservan mi dominio (call center, calidad, anomalías) pero encabezan con
> **Big Data / arquitectura distribuida / Data Lake / streaming**, que es justo el
> desplazamiento que me pide el tutor.

---

## 9. Preguntas que le haré al tutor

Para no adivinar el alcance esperado, le preguntaré:

1. **¿Escala vs. patrón?** ¿Le basta con **demostrar el patrón distribuido**
   (contenedores + **speedup medido**) dado que trabajo on-premise, o **exige un clúster
   multinodo** real (HDFS/YARN)?
2. **¿Acepta el reencuadre por las «V»?** ¿Es válido justificar Big Data por
   **Variety + Velocity + Veracity + crecimiento en producción** en lugar del volumen bruto?
3. **¿Qué profundidad de modelo predictivo espera?** ¿Un **modelo supervisado** formal de
   predicción de rendimiento, o le parece suficiente **detección no supervisada +
   forecasting**, dado que casi no tengo etiquetas?
4. **¿Qué artefactos de gobernanza** quiere ver (catálogo, linaje, reportes de calidad,
   contratos de datos)?
5. **¿Tecnologías específicas?** ¿Espera algún componente puntual del ecosistema
   (**Delta Lake/Iceberg**, HDFS, Airflow) o acepta equivalentes (Medallion en
   MinIO/Parquet, Prefect)?
6. **¿Alcance de la validación experimental?** ¿Tamaño de muestra mínimo, tests
   estadísticos requeridos y definición de la línea base?
7. **¿Debe reflejarse el nuevo enfoque también en los objetivos** (general y específicos)
   o basta con reforzar Problema, Método y Análisis?
