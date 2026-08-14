# Carta de Gobernanza y Calidad de Datos

> Documento fundacional del proyecto. Aquí fijo las **decisiones de gobernanza** que
> rigen todo el pipeline (claves, contratos, calidad, versionado, privacidad y
> reproducibilidad). Es el **primer entregable de la Fase 0** y la referencia del
> **Componente transversal T** de [fases.md](fases.md).
> Relacionados: [pipeline.md](pipeline.md), [tecnologias.md](tecnologias.md),
> [observaciones.md](observaciones.md).

> **Aclaración importante — dos "calidades" distintas:**
> - **Calidad de la VENTA** (negocio) → la rúbrica de
>   [proyecto/parametros_calidad_empresa.md](proyecto/parametros_calidad_empresa.md).
> - **Calidad del DATO** (técnica) → *este documento*. Aseguro que el dato es confiable,
>   trazable y reproducible. La gobernanza además **versiona** la rúbrica de negocio.

---

## 1. Registro de decisiones (decision log)

| # | Decisión | Valor acordado |
|---|---|---|
| 1 | Clave de grabación (`call_id`) | **`uniqueid`** de Asterisk |
| 1b | Clave de llamada lógica (`conversation_id`) | **`linkedid`** de Asterisk (agrupa transferencias) |
| 2 | Retención | Por sensibilidad (ver §3); PII mínima, anonimizado indefinido |
| 3 | Acceso al Bronce (dato crudo con PII) | **Solo el autor** (Carlos Miño) |
| 4 | Calidad de datos | **pandera inline** + reporte simple (Great Expectations opcional a futuro) |
| 5 | Versionado de datos/artefactos | **Carpeta/fecha** (`run=YYYYMMDD`) + `manifest.json` |
| 6 | Umbral de calidad del cruce | **≥ 95 %** de completitud + precisión de *linkage* ≥ 0.98 (ver §5) |

---

## 2. Modelo canónico y claves

- **`call_id` = `uniqueid`**: identifica **una grabación / una pata de canal**. Es la clave
  primaria de una llamada individual y su transcripción.
- **`conversation_id` = `linkedid`**: identifica la **llamada lógica completa**. En Asterisk,
  cuando hay **transferencia** (p. ej. el **Director Comercial** cierra la venta, criterio A08),
  se generan **varias filas CDR con el mismo `linkedid`** y distinto `uniqueid`. Agrupo por
  `linkedid` para **no partir la conversación**.
- **Unidad de análisis:** la **llamada lógica** (`conversation_id`), compuesta por una o más
  grabaciones (`call_id`) + su CDR + su transcripción anonimizada.
- **Convenciones de nombres:** zonas `bronze/ silver/ gold/`; topics Kafka
  `llamadas.finalizadas`, `transcripciones`, `anonimizadas`, `analisis.calidad`; tablas
  `llamadas`, `transcripciones`, `evaluaciones`, `agentes`, `kpis`.
- **Codificación:** todo el texto se normaliza a **UTF-8** (el CDR viene en `latin1`).

---

## 3. Privacidad y retención (alineado a la LOPDP Ecuador, 2021)

Aplico **minimización de datos** y **limitación de retención**: guardo el dato con PII el
menor tiempo posible y conservo indefinidamente solo lo anonimizado.

| Dato | ¿PII? | Zona | Retención |
|---|---|---|---|
| Audio original | Sí | Bronce (referencia) | La que ya mantiene la empresa (no genero copia adicional) |
| Transcripción cruda | Sí | Bronce | **12–24 meses** (solo para verificar el gold set) |
| Transcripción anonimizada | No | Plata | **Indefinida** |
| Evaluaciones / KPIs | No | Oro | **Indefinida** |

- **Acceso:** el **Bronce** (audio + transcripción cruda) es accesible **solo por el autor**.
  Los demás roles (gerencia, auditoría, agentes) solo consumen **Plata/Oro** (anonimizado)
  vía el tablero.
- **Frontera de anonimización:** ningún dato con PII cruza a Gemini; la anonimización ocurre
  en el paso **Bronce→Plata**.
- **Auditoría de anonimización:** registro qué se anonimizó y verifico **0 fugas de PII** en
  muestras (ver checklist de la Fase 3 en [fases.md](fases.md)).

---

## 4. Contratos de datos y calidad automática

**Contratos:** cada zona tiene un **esquema versionado con `pydantic`**; una etapa solo acepta
datos que cumplen el contrato de su zona de entrada.

**Calidad automática (`pandera`)** en cada frontera de zona, con reporte simple por ejecución:

| Frontera | Chequeos |
|---|---|
| → Bronce | esquema del CDR, tipos, `calldate` no nula |
| Bronce → Plata | **unicidad de `call_id`**, nulos en campos clave, rangos de `duration`/`billsec`, **cobertura del cruce** CDR↔grabación, duración dentro de la muestra |
| Plata → Oro | JSON de evaluación válido (esquema rúbrica), `venta_valida ∈ {0,1}`, `calidad_score ∈ [0,100]` |

> Empiezo con **pandera inline**. Si más adelante necesito **reportes formales**
> incorporo **Great Expectations (Data Docs)** sin tocar el pipeline (la capa de
> validación está desacoplada).

---

## 5. Umbral de calidad del cruce CDR↔grabación (justificado)

Trato la cobertura como la **dimensión de calidad "completitud"** [Batini et al., 2009] y,
además, **mido la calidad del emparejamiento** como un problema de *record linkage*
[Christen, 2012] — no basta con cuántos emparejo, sino con que estén **bien** emparejados.

- **Completitud objetivo:** **≥ 95 %** del cruce CDR↔grabación en la **muestra analítica**.
- **Calidad del linkage:** sobre un subconjunto verificado a oído (50–100 llamadas),
  **precisión ≥ 0.98** (que el audio emparejado sí corresponde al CDR).
- **Análisis de sesgo:** reviso los **no emparejados** (¿se concentran en cierta extensión o
  fecha?) para demostrar que descartarlos **no sesga** los resultados.

> Referencias añadidas a `referencias_bibliograficas/referencias.bib`:
> `batini2009` (dimensiones de calidad de datos) y `christen2012` (métricas de record linkage).

---

## 6. Versionado y linaje

- **Versionado por carpeta/fecha:**
  ```
  data/<zona>/<dataset>/run=YYYYMMDD/…
  data/<zona>/<dataset>/run=YYYYMMDD/manifest.json
  ```
- **`manifest.json`** por ejecución: rango de fechas procesado, **versión de rúbrica**,
  **modelo/versión de ASR y de LLM**, **semilla**, parámetros y **conteos** (procesados,
  descartados con motivo).
- **Linaje:** cada registro Oro puede **rastrearse hasta su Bronce** a través de
  `call_id`/`conversation_id` + el `manifest.json` de la corrida que lo produjo.
- **Artefactos versionados:** rúbrica (`rubrica_v1`, `v2`…), *gold set*, prompts de LLM.

---

## 7. Reproducibilidad

- **Contenedores** (Docker) con **dependencias fijadas** (versiones pineadas).
- **Semillas** fijas en todo lo aleatorio (muestreo, modelos).
- **Ejecuciones parametrizadas** por rango de fechas (mismo comando → mismo resultado).
- **Criterio:** una corrida con la misma semilla y parámetros **produce el mismo resultado**.

---

## 8. Cómo se aplica por fase (resumen)

| Pilar de gobernanza | Se implementa en |
|---|---|
| Claves canónicas (`uniqueid`/`linkedid`) | Fase 1 |
| Contratos + calidad de datos (pandera) | Fases 2, 3, 4 |
| Privacidad / anonimización | Fase 3 |
| Umbral y calidad de linkage | Fases 1, 2 |
| Versionado + linaje + manifest | Fases 2–8 |
| Reproducibilidad | Todas |

> Estado y checklist de cumplimiento: **Componente transversal T** en [fases.md](fases.md).
