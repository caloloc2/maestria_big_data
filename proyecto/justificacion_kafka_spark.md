# Justificación técnica del stack: Apache Kafka + Apache Spark

> Documento de defensa para el tribunal. Explica qué son Kafka y Spark, cómo
> funcionan, por qué se eligen para este proyecto y cómo cumplen los
> lineamientos técnicos de la Maestría en Big Data (UISRAEL).

## 1. Contexto del problema y por qué se necesita este stack

El call center genera dos tipos de carga de trabajo claramente distintos:

1. **Flujo continuo de llamadas nuevas.** Cada vez que una llamada finaliza
   (evento *hangup* de Asterisk) se produce un registro CDR y una grabación.
   Estos eventos llegan de forma **incesante y desordenada en el tiempo**, y el
   negocio necesita métricas "near-real-time" (apenas termina la llamada).
2. **Reproceso masivo del histórico.** ~100 GB de audio y 100.000+ registros
   CDR desde 2017 que deben poder reprocesarse por **rango de fechas** (p. ej.
   "marzo 2026" o "el mes anterior") con la misma lógica de limpieza para
   obtener métricas comparativas.

Ningún componente único resuelve bien ambos patrones. Por eso se adopta una
**arquitectura híbrida (estilo Lambda/Kappa)**: **Kafka** para el camino de
streaming y **Spark** para el camino batch. Esta separación es exactamente la
que la rúbrica de la maestría premia ("batch + streaming en la misma
arquitectura").

## 2. Apache Kafka — qué es y cómo funciona

**Qué es:** una plataforma distribuida de *streaming* de eventos. Funciona como
un "sistema nervioso central" donde los productores publican mensajes y los
consumidores los leen, de forma desacoplada y persistente.

**Conceptos clave:**

- **Topic:** canal con nombre donde se publican los eventos (p. ej.
  `llamadas.finalizadas`, `transcripciones`, `analisis.calidad`).
- **Partición:** cada topic se divide en particiones, lo que permite
  **paralelismo** y escalado horizontal (más particiones = más consumidores en
  paralelo).
- **Productor / Consumidor:** Asterisk (o un script puente) **produce** el
  evento de fin de llamada; los servicios de transcripción y análisis lo
  **consumen**.
- **Offset y retención:** Kafka guarda los eventos en disco de forma ordenada y
  durable; si un consumidor falla, retoma desde su último *offset* sin perder
  datos.

**Por qué es la pieza correcta aquí:**

- **Desacopla** la generación de la llamada del procesamiento pesado (ASR + LLM).
  Si la transcripción se retrasa, las llamadas no se pierden: quedan en el topic.
- **Tolerancia a fallos y reprocesamiento:** se puede "rebobinar" un topic y
  volver a procesar eventos (clave para corregir errores del pipeline).
- **Buffer de picos:** absorbe ráfagas de llamadas sin tumbar el sistema.
- **Escalabilidad horizontal:** añadir particiones/consumidores aumenta el
  *throughput* casi linealmente — evidencia directa para el lineamiento de
  escalabilidad.

## 3. Apache Spark — qué es y cómo funciona

**Qué es:** un motor de cómputo distribuido para procesar grandes volúmenes de
datos en paralelo sobre un clúster (o en modo local con múltiples núcleos).

**Conceptos clave:**

- **RDD / DataFrame:** abstracción de datos distribuidos particionados entre los
  *workers*; las operaciones se ejecutan en paralelo sobre cada partición.
- **Driver y Executors:** el *driver* coordina; los *executors* hacen el trabajo
  en paralelo. Spark reparte automáticamente el cómputo.
- **Evaluación perezosa (lazy):** Spark construye un grafo de ejecución (DAG) y
  lo optimiza antes de ejecutar, minimizando lecturas y *shuffles*.
- **Tolerancia a fallos:** si un nodo cae, Spark recomputa solo las particiones
  perdidas a partir del linaje del DAG.

**Por qué es la pieza correcta aquí:**

- **Volumen:** procesar 100 GB de audio-metadatos + 100k+ CDR por rango de
  fechas en pandas (una sola máquina, en memoria) es ineficiente o inviable;
  Spark particiona y paraleliza el trabajo.
- **Mismo código, batch escalable:** la limpieza, el cruce CDR↔grabación, la
  validación de calidad y la agregación de KPIs se expresan una vez y escalan.
- **Reproceso por fechas:** un job Spark parametrizado por rango recorre el
  histórico y recalcula métricas comparables a las del streaming.
- **Escalabilidad demostrable:** se puede medir *speedup* al añadir núcleos/nodos
  (strong/weak scaling), evidencia que pide la rúbrica.

## 4. Cómo trabajan juntos en este proyecto (arquitectura)

```
                 ┌─────────────────────── CAMINO STREAMING (llamadas nuevas) ──────────────────────┐
  Asterisk  ──▶  Kafka topic            Consumidores (Python)                Kafka topic
 (hangup +      "llamadas.finalizadas" ─▶ Whisper ASR (local) ─▶ anonimizar ─▶ Gemini análisis ─▶ "analisis.calidad" ─▶ BD/Dashboard
  CDR+audio)
                 └──────────────────────────────────────────────────────────────────────────────────┘

                 ┌─────────────────────── CAMINO BATCH (reproceso por fechas) ─────────────────────┐
  Histórico  ──▶ Job Spark parametrizado por rango ─▶ misma lógica de limpieza/validación/KPIs ─▶ BD/Dashboard
  (100GB +       (lee MySQL CDR + audios, paraleliza)
   CDR MySQL)
                 └──────────────────────────────────────────────────────────────────────────────────┘
```

- **Kafka** = la "vía rápida" para lo nuevo (baja latencia, evento a evento).
- **Spark** = la "vía pesada" para lo histórico (alto volumen, en lote).
- Ambos escriben en el **mismo modelo de datos** (capa servida) para que el
  dashboard muestre métricas consistentes entre el tiempo real y el histórico.

## 5. Por qué NO basta con una solución de una sola máquina (pandas/MySQL)

| Necesidad | Pandas / MySQL en 1 máquina | Kafka + Spark |
|---|---|---|
| Ingerir llamadas continuas sin perderlas | Frágil, sin buffer durable | Topic persistente, tolerante a fallos |
| Procesar 100 GB por rango de fechas | Límite de RAM, lento | Particionado y paralelo |
| Reprocesar tras un error | Manual, propenso a fallos | Rebobinar offset / recomputar DAG |
| Escalar al crecer el volumen | Vertical (caro, con techo) | Horizontal (añadir nodos) |
| Demostrar escalabilidad (tesis) | No medible | Speedup medible |

## 6. Mapeo directo con los lineamientos de la maestría

- **Velocidad (8V):** Kafka habilita ingesta/near-real-time → lineamiento de
  velocidad.
- **Batch + streaming en la misma arquitectura:** Kafka (streaming) + Spark
  (batch) → arquitectura Lambda/Kappa.
- **Infraestructura distribuida estrictamente necesaria:** justificada por
  volumen + continuidad → lineamiento de "no alcanza con una sola máquina".
- **Escalabilidad horizontal demostrada:** particiones Kafka + executors Spark,
  con medición de *speedup*.
- **Pipeline Ingesta→Procesamiento→Análisis→Consumo:** Kafka/Spark cubren
  ingesta y procesamiento.

## 7. Alcance realista para la tesis (honestidad ante el tribunal)

Para una tesis ejecutada en **Docker local**, se levantan Kafka y Spark en modo
contenedor (un broker, Spark en modo local/standalone con varios *workers*). No
se requiere un clúster productivo de decenas de nodos: se **demuestra el patrón
arquitectónico y se mide el escalado** a la escala disponible, dejando
documentado cómo escalaría horizontalmente en producción. Esto cumple el
lineamiento sin sobre-prometer infraestructura.

---

### Resumen en una frase para la defensa

> "Uso **Kafka** porque las llamadas llegan como un flujo continuo que debo
> ingerir sin pérdida y procesar casi en tiempo real, y **Spark** porque debo
> reprocesar 100 GB de histórico por rango de fechas de forma paralela y
> escalable; juntos forman una arquitectura batch + streaming que ninguna
> herramienta de una sola máquina resuelve con la misma robustez."
