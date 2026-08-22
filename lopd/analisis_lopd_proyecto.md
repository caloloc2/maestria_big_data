# Análisis de aplicabilidad de la LOPDP al proyecto de tesis

> Marco normativo: **Ley Orgánica de Protección de Datos Personales** (LOPDP), Registro Oficial Suplemento 459, 26-may-2021 `[lopdp2021]`, y su **Reglamento General** (Decreto Ejecutivo 904), 6-nov-2023 `[rlopdp2023]`.
> Documentos fuente convertidos a Markdown en esta carpeta: [lopd.md](lopd.md), [decreto.md](decreto.md), [resumen.md](resumen.md), [formulario.md](formulario.md).
> Propósito: dejar identificados los artículos que aplican al proyecto (arquitectura Big Data para evaluación de calidad de llamadas de call center) — incluidos los cambios previstos (`bronze_cdr` a Spark JDBC y almacenamiento del audio crudo en Bronce) — como referencia para la documentación de la implementación.

---

## 1. Encuadre del proyecto frente a la Ley

| Concepto LOPDP | Aplicación al proyecto |
|---|---|
| **Titular** (Art. 4) | El **cliente** de la llamada y el **agente/asesor** del call center. Ambos son titulares cuyos datos se tratan. |
| **Responsable del tratamiento** | La empresa de call center (aliado de Diners Club) que decide fines y medios. |
| **Encargado del tratamiento** | Quien procesa por cuenta del responsable. El pipeline de la tesis opera como herramienta del tratamiento; según cómo se contractualice, la maestría/tesista actúa dentro del ámbito del responsable. |
| **Tratamiento** | Grabación, transcripción (ASR), anonimización, análisis de calidad con LLM, almacenamiento (lago MinIO + PostgreSQL) y elaboración de KPIs/anomalías. |
| **Tratamiento a gran escala** (Reglamento Art. 4.7) | **Sí aplica.** ~7 M grabaciones / ~424 GB / 100 agentes; el propio Reglamento cita como ejemplos el tratamiento por proveedores de telefonía y por instituciones financieras/seguros (Art. 4.7, literales d y f). |

**Consecuencia:** al ser tratamiento a gran escala y (potencialmente) de categorías especiales, se activan las obligaciones reforzadas: evaluación de impacto (EIPD), delegado de protección de datos (DPD), registro de actividades y medidas de seguridad demostrables.

## 2. Vigencia y obligatoriedad (dato clave)

La **Disposición Transitoria Primera** de la LOPDP difirió el **régimen sancionatorio a dos años** desde la publicación (mayo 2021). Por tanto, **desde mayo de 2023 el régimen de infracciones y sanciones está plenamente vigente** y todo tratamiento debía estar adecuado (Transitoria Segunda). El proyecto se diseña, entonces, en un contexto de **cumplimiento exigible, no voluntario**.

## 3. Principios (Art. 10 LOPDP) y cómo los satisface la arquitectura

| Principio (Art. 10) | Cómo lo cumple / evidencia en el proyecto |
|---|---|
| **Finalidad** (lit. d) | Fin explícito y legítimo: auditoría de calidad y cumplimiento de la venta. No se reutiliza para fines incompatibles. |
| **Pertinencia y minimización** (lit. e) | Solo texto **anonimizado** sale del perímetro; el CDR trae solo las columnas necesarias para enlace/KPIs (no PII innecesaria). |
| **Proporcionalidad** (lit. f) | Alcance acotado (ext. 200–299 salientes); muestreo estratificado; diarización solo de llamadas relevantes. |
| **Confidencialidad** (lit. g) | Anonimización como frontera; lo sensible permanece en la LAN. |
| **Calidad y exactitud** (lit. h) | Validación de esquemas (pandera), enlace CDR↔grabación validado, control de calidad de datos. |
| **Conservación** (lit. i) | Requiere **fijar plazos de retención** para las zonas (ver §7). |
| **Seguridad** (lit. j) | Anonimización/seudonimización/cifrado (ver §5). |
| **Responsabilidad proactiva y demostrada** (lit. k) | Bitácora técnica, linaje en Dagster, métricas de cumplimiento (KPIs), gobernanza documentada. |

## 4. Base de legitimación (Art. 5–9 LOPDP; Reglamento Art. 5–7)

- El tratamiento debe apoyarse en una base legítima: **consentimiento** del titular (típicamente el aviso de grabación al inicio de la llamada), **ejecución contractual** o **interés legítimo** (Art. 7 LOPDP; Reglamento Art. 7, con regla de ponderación).
- **Datos crediticios/financieros** (tarjeta, solvencia): Art. 28–29 LOPDP y Art. 18 Reglamento — tratamiento lícito con fin acotado; regulados además por la Junta de Política y Regulación Financiera y la Superintendencia de Bancos.

## 5. Seguridad del tratamiento (Art. 37–42 LOPDP; Reglamento Art. 33–36, 58–60) — núcleo técnico

Este es el bloque que **respalda directamente las decisiones de arquitectura**:

- **Art. 37 LOPDP — Seguridad:** enumera expresamente como medidas: **1) anonimización, seudonimización o cifrado**; 2) confidencialidad/integridad/disponibilidad; 3) resiliencia. → La anonimización del proyecto (Presidio+spaCy, redacción de cédula/tarjeta/teléfonos/nombres/números dictados) es exactamente la medida del Art. 37.1.
- **Art. 39 LOPDP + Reglamento Art. 59–60 — Protección desde el diseño y por defecto:** el diseño Medallion con la **anonimización como frontera** (Bronce crudo+PII → Plata anonimizado → solo texto anonimizado a la nube) **es** protección desde el diseño y por defecto.
- **Art. 40–41 LOPDP — Análisis de riesgos y determinación de medidas:** exige metodología de riesgos, amenazas y vulnerabilidades. → Documentar en la tesis el análisis de riesgos por zona (Bronce/Plata/Oro).
- **Art. 42 LOPDP + Reglamento Art. 29–32 — Evaluación de impacto (EIPD):** **obligatoria** en tratamiento a gran escala de categorías especiales y en decisiones automatizadas/perfilado. → El proyecto debe incluir una **EIPD** como entregable/subsección (la predicción de anomalías por agente puede constituir elaboración de perfiles).

## 6. Transferencia o comunicación internacional (Art. 56 LOPDP; Reglamento Art. 71–78) — punto crítico del diseño híbrido

El uso de **Google Gemini** (procesamiento en servidores fuera de Ecuador) constituye una **transferencia/comunicación internacional de datos**. La regla general (Art. 56 LOPDP) exige país con nivel adecuado o garantías adecuadas (Reglamento Art. 74).

**La mitigación ya está en el diseño:** como **solo sale texto anonimizado** (sin datos personales identificables), la salida a Gemini **no transfiere datos personales** → se reduce/elimina la obligación de transferencia internacional. Además, el Art. 21 del Reglamento admite expresamente que, tras **disociación o cifrado robusto**, la comunicación a un tercero no requiere consentimiento porque "no se pueda identificar a qué persona se refieren". **Este es uno de los argumentos jurídicos más fuertes del proyecto** y debe destacarse en la tesis.

## 7. Conservación, eliminación, bloqueo y anonimización (Art. 10.i LOPDP; Reglamento Art. 8–11) — aplica al guardado de audio en Bronce

- Los plazos de conservación **no deben exceder lo necesario** para la finalidad (Reglamento Art. 8) y el **fichero de registro debe indicar el plazo** (Art. 10).
- Cumplida la finalidad, procede **eliminación, bloqueo o anonimización** (Reglamento Art. 9) y **eliminación segura** al vencer el plazo (Art. 11).
- **Implicación directa para guardar el MP3 crudo en Bronce:** al almacenar audio con PII en reposo, hay que **definir y documentar una política de retención** del bucket Bronce (plazo, revisión periódica, borrado seguro) y **cifrado en reposo + control de acceso**. Esto convierte la observación del tutor (guardar el audio como respaldo/auditoría) en una decisión **conforme** siempre que se acompañe de la política de retención y cifrado.

## 8. Notificación de vulneraciones de seguridad (Art. 43–46 LOPDP; Reglamento Art. 24–28)

- **Plazos:** el responsable notifica a la Autoridad y a ARCOTEL en **≤ 5 días**; el encargado notifica al responsable en **≤ 2 días**; al titular en **≤ 3 días** si hay riesgo (Art. 43, 46).
- **Contenido** de la notificación: naturaleza, afectados, sistemas, causa, volumen/tipos de datos, medidas, evaluación de riesgo (Reglamento Art. 26).
- → Conviene documentar en la gobernanza un **procedimiento de respuesta a incidentes** (aunque sea de alcance académico).

## 9. Roles organizativos (Art. 47–51 LOPDP; Reglamento Art. 33–57)

- **Art. 47 — Obligaciones del responsable:** aplicar medidas, políticas, análisis de riesgos, EIPD, contratos de confidencialidad, **designar DPD**, registrar en el Registro Nacional.
- **Art. 48 LOPDP + Reglamento Art. 48–57 — Delegado de Protección de Datos (DPD):** **obligatorio** para tratamiento a gran escala de categorías especiales. Requisitos del DPD (Reglamento Art. 55): título de tercer nivel en Derecho/Sistemas/Comunicación/Tecnologías + 5 años de experiencia.
- **Reglamento Art. 38 — Registro de actividades de tratamiento:** obligatorio para responsables con **≥ 100 trabajadores** (el call center tiene ~100 agentes) → aplica.

## 10. Régimen sancionador (Art. 67–74 LOPDP; Reglamento Art. 90) — el riesgo a evitar

- **Infracciones graves (Art. 68):** no implementar medidas de seguridad, usar datos para fines distintos, ceder sin requisitos, **no realizar EIPD cuando correspondía**, no notificar vulneraciones, no designar DPD, no mantener el registro.
- **Sanciones (Art. 71–72):** para entidad privada, **0,1 %–0,7 %** del volumen de negocio (leves) y **0,7 %–1 %** (graves). → Justifica ante el jurado por qué el diseño prioriza la anonimización y la minimización.

## 11. Mapa directo a los cambios de arquitectura acordados

| Cambio previsto | Artículos LOPDP/Reglamento relevantes | Qué hay que hacer para cumplir |
|---|---|---|
| **`bronze_cdr` → Spark JDBC** (migración pandas→PySpark) | Art. 10.e (minimización), Art. 37 (seguridad) | Mantener lectura **solo de columnas necesarias** y **SOLO LECTURA**; sin PII adicional. Neutro/positivo para cumplimiento. |
| **Guardar el MP3 crudo en Bronce (MinIO)** | Art. 10.i, 37.1; Reglamento Art. 8–11, 59–60 | **Cifrado en reposo** del bucket Bronce, **control de acceso**, **política de retención** (plazo + borrado seguro), justificación de finalidad (auditoría). |
| **Anonimización antes de Gemini** (ya existe) | Art. 21, 37.1; Art. 56 y Reglamento Art. 71–78 | Documentar que la disociación evita la transferencia internacional de datos personales. |
| **Predicción de anomalías por agente** | Art. 20 (decisiones automatizadas), Art. 42 (EIPD) | Incluir EIPD; la decisión no debe ser únicamente automatizada sin garantías. |
| **Tablero (serving PostgreSQL)** | Art. 37, 47 | Control de acceso al tablero; mostrar KPIs sobre datos ya tratados/anonimizados. |

## 12. Recomendaciones para la documentación de la tesis

1. Añadir una **sección de gobernanza y cumplimiento LOPDP** que cite `[lopdp2021]` y `[rlopdp2023]` y recorra: base de legitimación, principios, medidas de seguridad (Art. 37), protección desde el diseño (Art. 39), transferencia internacional mitigada por anonimización (Art. 56/21), retención (Reglamento Art. 8–11) y roles (DPD, registro de actividades).
2. Incluir una **Evaluación de Impacto (EIPD)** como anexo (Art. 42; Reglamento Art. 29–32), aun de alcance académico.
3. Documentar la **política de retención y cifrado del audio crudo** en Bronce antes de habilitar `bronze_audio`.
4. El **formulario ARCO** ([formulario.md](formulario.md)) sirve de referencia para el procedimiento de ejercicio de derechos (acceso, rectificación, eliminación, oposición, portabilidad).

---

### Referencias

- `[lopdp2021]` Asamblea Nacional del Ecuador, *Ley Orgánica de Protección de Datos Personales*, R.O. Suplemento 459, 26-may-2021.
- `[rlopdp2023]` Presidencia de la República del Ecuador, *Reglamento General a la LOPDP*, Decreto Ejecutivo 904, 6-nov-2023.

*(Ambas entradas ya están registradas en `referencias_bibliograficas/referencias.bib` como `lopdp2021` y `rlopdp2023`.)*
