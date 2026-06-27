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

## 2. Pregunta de investigación

> ¿Cómo diseñar e implementar una arquitectura que integre LLM y analítica en
> tiempo real sobre registros CDR y grabaciones de audio para identificar de forma
> escalable prácticas de atención críticas y predecir anomalías en el rendimiento
> de los agentes de un *call center*?

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
├── referencias_bibliograficas/     # Fuentes de la revisión de literatura
│   ├── ejeA_analitica_tiempo_real/ #   PDFs por eje temático
│   ├── ejeB_speech_asr/
│   ├── ejeC_llm_nlp_compliance/
│   ├── ejeD_anomalias_rendimiento/
│   ├── fuentes.md                  #   Tabla de seguimiento de fuentes
│   └── referencias.bib             #   Bibliografía IEEE (importable a Mendeley)
├── lineamientos/                   # Guías y plantilla oficiales de UISRAEL (IEEE, requisitos)
└── proyecto/                       # Insumos de la parte práctica (rúbricas y guías técnicas)
```

## 5. Estado y enfoque

El trabajo se desarrolla **por secciones**, siguiendo la plantilla de UISRAEL y el
**estilo IEEE** (citas numeradas gestionadas con Mendeley). Avance del plan:

| Sección | Estado |
|---------|--------|
| I. Revisión de Literatura | ✅ |
| II. Problema de Investigación | ✅ |
| III–IV. Objetivos | ✅ |
| V. Justificación Práctica | ✅ |
| VI. Vinculación y beneficiarios | ✅ |
| VII. Método de Investigación | ⏳ en curso |
| VIII. Referencias | ✅ (continuo) |
| IX. Información Administrativa | ⏳ |

**Metodología prevista:** CRISP-DM como marco de trabajo, con una arquitectura
técnica basada en **Kafka** (ingesta *near-real-time*), **Spark** (procesamiento por
lotes del histórico), **Whisper** (transcripción local con diarización y
anonimización de datos sensibles) y análisis con **LLM**, entregando un **tablero**
analítico como producto final.

## 6. Hoja de ruta

- [x] Fase académica: redacción del plan de titulación (Secciones I–VI).
- [ ] Completar Método de Investigación e Información Administrativa.
- [ ] **Implementación técnica** (pipeline + tablero, en `proyecto/`), que se irá
      incorporando a este repositorio en fases posteriores.

---

> Repositorio de carácter **académico**. La parte de implementación técnica se
> añadirá progresivamente. Algunos insumos de `proyecto/` provienen del entorno
> empresarial donde se aplicará la solución.
