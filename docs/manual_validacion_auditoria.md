# Manual de validación con Auditoría — calidad de venta (rubrica_v1)

> Guía operativa para que el equipo de Auditoría **valide a oído** un bloque de
> llamadas, califique de forma consistente, y para que el tesista **incorpore nuevos
> parámetros** al flujo. Es el puente entre el etiquetado automático
> (`docs/weak_supervision_resultados.md`) y la **Fase 4 formal** (Precisión/Recall/F1).

---

## Parte 1 — Plan de acción para validar un bloque

### 1.1. Objetivo
Construir la **"verdad de referencia" (gold set)**: un conjunto de llamadas donde una
persona experta escuchó el audio y dejó por escrito la respuesta correcta. Contra esa
verdad se mide después si la evaluación automática (Gemini + reglas) acierta.

### 1.2. Cómo se arma la muestra (bloque)
No se escucha todo: se escucha una **muestra representativa** de ~**30 a 50 llamadas**,
elegida a propósito para que tenga variedad:

- **Ventas y no-ventas** (para probar que el sistema las distingue).
- **Con crítica y sin crítica** (según la etiqueta débil).
- **De alta y de baja confianza** (donde las reglas estuvieron muy seguras y donde no).
- **De varios agentes** (no todo del mismo asesor).

La muestra se saca de `servido.weak_labels`. Ejemplo para elegir un bloque variado de
un día:

```sql
SELECT call_id, agente, dur_audio, wl_es_venta, wl_infraccion_critica,
       wl_venta_valida, confianza, reglas
FROM servido.weak_labels
WHERE fecha = '2026-08-06'
ORDER BY wl_es_venta DESC, confianza ASC   -- prioriza ventas y casos dudosos
LIMIT 50;
```

### 1.3. Cómo se escucha y se registra
1. Para cada `call_id`, abrir en el **tablero → pestaña "🔎 Detalle de llamada"** la
   **transcripción anonimizada** (sirve de apoyo visual mientras se escucha).
2. **Escuchar el audio original** (está en el lago Bronce/MinIO,
   `bronce/audio/date=YYYY-MM-DD/…mp3`; también la copia local en `data/cache_audio/`).
3. Llenar **una fila por llamada** con el veredicto humano (formato en §1.4).
4. **Idealmente, dos auditores** escuchan la misma muestra y se compara su concordancia
   (si difieren mucho, el criterio está mal definido y hay que aclararlo).

### 1.4. Dónde se registra la verdad (formato del gold set)
Una tabla/planilla `servido.gold_labels` (o un Excel que luego se carga), una fila por
llamada:

| campo | qué es | valores |
|-------|--------|---------|
| `call_id` | id de la llamada | — |
| `es_venta` | ¿cerró venta? | 0/1 |
| `venta_valida` | ¿venta válida (sin crítica)? | 0/1 |
| `infraccion_critica` | ¿hubo crítica? | 0/1 |
| `infracciones` | cuáles (A03, A07, B01…) | lista |
| `calidad` | banda de calidad | baja/media/alta |
| `fuente` | quién y cómo | humano_confirmada / humano_corregida |
| `auditor` | iniciales | — |
| `notas` | observaciones | libre |

`fuente = humano_confirmada` si la etiqueta automática ya estaba bien;
`humano_corregida` si el auditor la cambió (esos casos son oro para calibrar).

---

## Parte 2 — Manual de cómo calificar (libro de códigos)

Se califica **una llamada** revisando tres grupos. **Regla dura:** basta **una**
infracción CRÍTICA para que `venta_valida = 0`, sin importar lo demás (evita multas de
la Superintendencia y reclamos al banco).

### 2.1. Grupo A — ¿siguió el guion obligatorio?
| id | Debe ocurrir | Si falla |
|----|--------------|----------|
| A01 | Saludo | menor |
| A02 | Se presenta con nombre + "Corporación Marketing Vip S.A." + aliada Diners/Interdin | mayor |
| **A03** | **Pide permiso de grabación y el cliente acepta** | **CRÍTICA** |
| A04 | Explica vigencia 24 meses, reserva 45 días y valor del paquete | mayor |
| A05 | Aclara que la visa la aprueba el consulado | mayor |
| A06 | Dice que MKV **no** es institución financiera | mayor |
| **A07** | **Lee el descargo legal** (diferido Diners + "seguro de desgravamen…") | **CRÍTICA** |
| A08 | El Director Comercial confirma cédula, valor, plazo, dirección, correo | mayor |
| A09 | Declaración final (grabada de inicio a fin + no somos financiera) | mayor |

### 2.2. Grupo B — ¿dijo palabras prohibidas?
Críticas (una sola anula la venta): **GARANTIZO, ASEGURO, 100%/SEGURO, SIN RIESGO,
APROBADO** (sin "sujeto a aprobación de Diners"), **SIN INTERESES** (sin "del banco"),
**CUOTAS FIJAS, CRÉDITO INMEDIATO, NO PAGA NADA, PRÉSTAMO,** "**de parte del banco**".
Mayores (restan calidad): gratis, descuento, ilimitado, "solo por hoy", sorteo/regalo,
"el mejor precio", etc.

### 2.3. Grupo C — ¿omitió algo obligatorio?
No mencionar vigencia (C01), activación 45 días (C02), valor (C03), nombre completo de
la empresa (C04); **no leer el descargo (C05, CRÍTICA, cruza con A07)**; decir "Tipo de
Crédito" (C06).

### 2.4. Cómo decidir cada campo
- **es_venta = 1** si la llamada llega al **cierre**: el Director Comercial confirma
  datos y el cliente **dicta la tarjeta**. Si el cliente rechaza o la llamada es muy
  corta → 0.
- **infraccion_critica = 1** si ocurre **cualquiera** de: A03 falla, A07/C05 falla
  (solo se exige si hubo venta), o una palabra prohibida crítica del Grupo B.
- **venta_valida = 1** solo si `es_venta = 1` **y** `infraccion_critica = 0`.
- **calidad**: alta (cumple casi todo A, sin B/C), media (fallos mayores), baja (varios
  fallos o crítica).

---

## Parte 3 — Cómo agregar más parámetros al flujo

Cuando Auditoría detecte algo nuevo que el sistema debería vigilar, así se incorpora.

### 3.1. Nueva palabra/frase prohibida (Grupo B)
1. **Documentar** en `proyecto/parametros_calidad_empresa.md`: id (Bxx), término,
   severidad (CRÍTICA/MAYOR/MENOR), reemplazo correcto.
2. **Configurar** en `src/analysis/rubrica.py` → diccionario `PROHIBIDAS`:
   `"B19": (r"\bnuevo termino\b", "CRITICA", False)`. Si es condicional (se "salva"
   con cierto contexto), añadir su patrón en `SALVA`.
3. Si es crítica, añadir el id en `src/analysis/schema.py` → `B_CRITICAS`.

### 3.2. Nueva regla de etiquetado débil (labeling function)
1. En `src/analysis/weak_labels.py`, escribir una función
   `lf_mi_regla(t, n, dur)` que devuelva `("es_venta"|"infraccion_critica", voto, peso,
   motivo)` o `None`.
2. Registrarla en la lista `LABELING_FUNCTIONS`.
3. El **peso** (1 = pista débil, 3 = señal fuerte) se ajusta con lo que diga la
   validación de Auditoría.

### 3.3. Nuevo criterio de guion (Grupo A o C, lo evalúa el LLM)
1. Documentarlo en `parametros_calidad_empresa.md` (id, qué debe ocurrir, severidad).
2. Añadirlo al **prompt** de Gemini en `src/analysis/gemini_eval.py` (que lo devuelva
   en `grupo_A`/`grupo_C`). Si es crítico, sumarlo a `A_CRITICOS` en `schema.py`.

### 3.4. Documentar y versionar
- Todo cambio de criterios sube la versión de la rúbrica (`rubrica_v1` → `v2`); las
  métricas solo se comparan **dentro de la misma versión**.
- Cada etiqueta guarda `call_id`, versión de rúbrica, modelo y fecha (trazabilidad).

### 3.5. Volver a correr el flujo (para que el cambio surta efecto)
```bash
# re-etiquetado débil con las reglas nuevas
docker exec uisrael_dagster_webserver bash -lc "cd /opt/dagster/app && python -m src.analysis.weak_labels"
# re-evaluación con Gemini (si se tocó el prompt/criterios) para un día:
docker exec uisrael_dagster_webserver bash -lc "cd /opt/dagster/app && dagster asset materialize -f src/definitions.py --select gold_evaluations --partition 2026-08-06"
```
Los cambios se ven en el tablero (pestañas Calidad y Detalle de llamada).

---

## Parte 4 — Qué se hace con el gold set (Fase 4 formal)

Con el bloque validado a oído:
1. **Medir** cuánto acierta la evaluación automática vs. la verdad humana:
   **Precisión, Recall y F1** (sobre todo en infracción crítica y venta válida).
2. **Comparar modelos Gemini** (calidad vs. costo vs. latencia) sobre ese mismo gold
   set y elegir el de mejor relación costo-beneficio.
3. **Calibrar** los pesos de las reglas y afinar el prompt con los casos que el humano
   corrigió. Repetir el ciclo (el gold set crece).
