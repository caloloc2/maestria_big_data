# Integración de analítica en tiempo real y LLMs para el análisis de grabaciones de call center

**Trabajo de Titulación — Maestría en Big Data y Ciencia de Datos**
Universidad Tecnológica Israel (UISRAEL) · Escuela de Posgrados “ESPOG” · Quito, Ecuador · 2026
Autor: **Carlos Enrique Miño Flores**

---

## 1. Descripción del proyecto

Este repositorio contiene el **plan de titulación** y los insumos académicos de un
trabajo cuyo objetivo es diseñar una **arquitectura modular de Big Data** que
integre **analítica en tiempo real (*streaming*)** y **Modelos de Lenguaje de Gran
Escala (LLM)** para procesar de forma concurrente los **metadatos de llamadas
(CDR)** y las **grabaciones de audio** de un *call center* de venta saliente, con el
fin de **identificar prácticas de atención críticas** y **predecir anomalías** en el
rendimiento de los agentes.

El problema parte de una limitación real: la auditoría de calidad de las llamadas
es **manual** y cubre una fracción mínima de las interacciones, lo que impide
detectar a tiempo malas prácticas, riesgos de incumplimiento normativo y caídas de
desempeño.

El corpus disponible supera los **100 GB de grabaciones** y los **100.000 registros
CDR** acumulados desde **2017**.

## 2. Pregunta de investigación e hipótesis

> **Pregunta:** ¿Cómo diseñar e implementar una arquitectura que integre LLM y
> analítica en tiempo real sobre registros CDR y grabaciones de audio para
> identificar de forma escalable prácticas de atención críticas y predecir
> anomalías en el rendimiento de los agentes de un *call center*?

> **Hipótesis:** una arquitectura modular que combine analítica de *streaming* y LLM
> sobre metadatos CDR y transcripciones de audio permitirá analizar las
> interacciones a escala con una precisión estadísticamente superior en los KPIs de
> calidad y reducir el tiempo de detección de anomalías frente a la auditoría
> manual.

## 3. Objetivos

**General:** Implementar una arquitectura modular de Big Data para el procesamiento
concurrente de metadatos CDR y grabaciones de audio no estructuradas, mediante la
integración de analítica en tiempo real (*streaming*) y LLM, para identificar
prácticas de atención críticas y predecir anomalías en el rendimiento de los
agentes.

**Específicos:**
1. **Contextualizar** los fundamentos teóricos (analítica en tiempo real, ASR y LLM).
2. **Diagnosticar** el proceso de auditoría actual y la calidad/trazabilidad de los datos.
3. **Desarrollar** la arquitectura (ingesta *streaming*, procesamiento por lotes, transcripción con anonimización y análisis con LLM).
4. **Validar** el impacto en un entorno real de la empresa frente a la auditoría manual (línea base).

## 4. Estructura del repositorio

```
.
├── plan_titulacion.pdf / .docx     # Plan de titulación (documento principal, estilo IEEE)
├── informacion.md                  # Notas e información general del trabajo
├── secciones_plan_titulacion/      # Redacción por secciones + guías de citas/estilo
│   ├── 01_revision_literatura.md … 06_metodologia.md
│   ├── 00_guia_citas_ieee_mendeley.md
│   ├── 00_guia_estilos_formato_word.md
│   └── presentacion.md             #   Guion de diapositivas para la defensa
├── referencias_bibliograficas/     # Fuentes de la revisión de literatura
│   ├── ejeA_analitica_tiempo_real/ #   PDFs por eje temático
│   ├── ejeB_speech_asr/
│   ├── ejeC_llm_nlp_compliance/
│   ├── ejeD_anomalias_rendimiento/
│   ├── fuentes.md                  #   Tabla de seguimiento de fuentes
│   └── referencias.bib             #   Bibliografía IEEE (importable a Mendeley)
├── lineamientos/                   # Guías y plantilla oficiales de UISRAEL (IEEE, requisitos)
├── proyecto/                       # Insumos de la parte práctica (rúbricas y guías técnicas)
├── clases/                         # Material de clases de la maestría
└── graphify-out/                   # Grafo de conocimiento del repositorio (reporte, HTML, JSON)
```

## 5. Estado del plan

El plan de titulación está **finalizado, publicado y entregado para calificación**.
Se desarrolló siguiendo la plantilla de UISRAEL y el **estilo IEEE** (citas numeradas
gestionadas con Mendeley, 18 referencias).

| Sección | Estado |
|---------|--------|
| I. Revisión de Literatura | ✅ |
| II. Problema de Investigación | ✅ |
| III–IV. Objetivos | ✅ |
| V. Justificación Práctica | ✅ |
| VI. Vinculación y beneficiarios | ✅ |
| VII. Método de Investigación | ✅ |
| VIII. Referencias | ✅ |
| IX. Información Administrativa | ✅ |

## 6. Marco metodológico

Enfoque **cuantitativo**, técnico y experimental aplicado, sustentado en dos marcos:

- **Design Science Research (DSR)** — diseñar, implementar y evaluar una solución
  para una necesidad real de la empresa.
- **CRISP-DM** — guía del ciclo de datos y la construcción del *pipeline*, con una
  arquitectura **híbrida** (lotes + baja latencia).

**Fases (CRISP-DM):** Comprensión del negocio → Comprensión de los datos →
Preparación → Modelado y desarrollo → Evaluación → Despliegue.

**Población / muestra:** universo de interacciones desde 2017 (>100.000 CDR y
>100 GB de audio); la muestra resulta del cruce válido CDR–grabación acotado por
criterios de duración. La **unidad de análisis** es cada llamada (CDR + grabación +
transcripción anonimizada).

## 7. Arquitectura e instrumentos

| Etapa | Herramienta |
|-------|-------------|
| Fuente existente | Servidor **Asterisk** + base de datos **MySQL** (CDR) |
| Ingesta *near-real-time* | **Apache Kafka** (evento de fin de llamada) |
| Procesamiento por lotes | **Apache Spark / PySpark** (histórico) |
| Transcripción / ASR | **Whisper** (diarización + anonimización, mitigación de alucinaciones) |
| Análisis semántico | **LLM local** (datos sensibles) + **LLM vía API** (tareas no sensibles y sentimiento) |
| Programación | **Python** (pandas, PySpark, ML) |
| Almacenamiento analítico | Base de datos **relacional** |
| Consumo | **Tablero analítico** (KPIs de calidad, contactabilidad y rendimiento) |

## 8. Métricas de evaluación

- **Prácticas críticas:** Accuracy, Precision, Recall y F1-score.
- **KPIs de negocio:** tasa de contactabilidad, tasa de conversión de ventas,
  duración media por agente, horarios/días de mayor efectividad.
- **Métricas operativas:** tiempo de procesamiento por llamada y *throughput*.
- **Línea base:** comparación antes/después frente a la auditoría manual (tiempo de
  detección de anomalías y consistencia de la evaluación).
- **Herramientas:** Python (pandas, scikit-learn, SciPy, statsmodels, PySpark).

## 9. Impacto y beneficiarios

- **Beneficiarios directos:** directivos, personal de auditoría/calidad y agentes
  del *call center*.
- **Beneficiarios indirectos:** clientes (atención más controlada y de calidad).
- **Aporte social:** procesos de venta más transparentes y protección al consumidor.
- **Alineación con los ODS 8 y 9** (trabajo decente, crecimiento económico,
  innovación e infraestructura).
- **Entregable:** tablero analítico + documentación y capacitación al equipo.

## 10. Hoja de ruta

- [x] Fase académica: plan de titulación completo (Secciones I–IX) **finalizado y
      publicado**.
- [ ] **Implementación técnica** (pipeline + tablero, en `proyecto/`), que se irá
      incorporando a este repositorio en fases posteriores.

---

> Repositorio de carácter **académico**. La parte de implementación técnica se
> añadirá progresivamente. Algunos insumos de `proyecto/` provienen del entorno
> empresarial donde se aplicará la solución.
