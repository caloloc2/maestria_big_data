# Weak supervision — resultados (rubrica_v1)

> Etiquetado **automático y aproximado** de calidad de llamada, hecho con reglas
> (sin LLM), como paso previo a la validación con Auditoría. Módulo:
> `src/analysis/weak_labels.py`. Tabla de salida: `servido.weak_labels`.
> **Resultados preliminares** (se refrescan al terminar el reproceso de 2026-08-06,
> que traerá ventas reales). Ver el manual de validación en
> `docs/manual_validacion_auditoria.md`.

## 1. Qué hace, en simple

En lugar de que un auditor escuche cientos de llamadas para marcar "esto está bien /
mal", varias **reglas simples** leen la transcripción anonimizada y **votan**. Cada
regla es imperfecta, pero juntas dan una etiqueta razonable. Cada etiqueta viene con
una **confianza** (qué tan de acuerdo estuvieron las reglas) y con **el motivo**
(qué regla la disparó). Así la persona de Auditoría no parte de cero: escucha, y solo
**confirma o corrige** la etiqueta propuesta.

Se producen tres etiquetas por llamada:

| Etiqueta | Significado |
|----------|-------------|
| `wl_es_venta` | ¿la llamada llegó al cierre de venta? |
| `wl_infraccion_critica` | ¿hay una infracción que anula la venta (multa/reclamo)? |
| `wl_venta_valida` | venta cerrada **y** sin infracción crítica |

## 2. Las reglas (labeling functions) y su peso

| Regla | Qué mira | Vota | Peso |
|-------|----------|------|------|
| `lf_muy_corta` | grabación < 90 s | no es venta | 2 |
| `lf_larga` | grabación ≥ 15 min | es venta | 1 |
| `lf_frases_cierre` | "confirmo sus datos", "número de tarjeta", "director comercial"… | es venta | 3 |
| `lf_rechazo` | "no me interesa", "ya tengo", "llame después"… | no es venta | 2 |
| `lf_dicta_tarjeta` | el cliente dictó la tarjeta (token `<TARJETA>`) | es venta | 3 |
| `lf_prohibidas_criticas` | palabra prohibida crítica (GARANTIZO, ASEGURO, 100%…) | crítica | 3 |
| `lf_sin_prohibidas` | ninguna palabra prohibida | no crítica | 1 |
| `lf_sin_permiso_grabacion` | no se menciona el permiso de grabación (posible A03) | crítica | 1 |

El peso refleja qué tan confiable es cada señal (una palabra prohibida pesa más que una
pista débil). **Estos pesos son el primer punto a calibrar con Auditoría.**

## 3. Resultados preliminares (534 llamadas ya transcritas)

| Etiqueta | Distribución |
|----------|--------------|
| `wl_es_venta` | 454 no · 13 sí · 67 sin señal |
| `wl_infraccion_critica` | **527 crítica** · 7 no |
| `wl_venta_valida` | 533 no · 1 sí |
| confianza media | 0,48 |

Acuerdo con la evaluación de Gemini (en las 40 llamadas que ya tienen evaluación):
**infracción crítica 98 % · venta válida 98 %**.

## 4. Lectura de los resultados (el hallazgo importante)

- **Casi todo sale marcado como "crítico" (527 de 534), y Gemini hace lo mismo.**
  No es que las reglas y Gemini "se equivoquen igual por casualidad": ambos disparan
  la crítica sobre todo por **dos criterios legales** que son difíciles de ver en una
  transcripción parcial o corta: el **permiso de grabación (A03)** y el **descargo
  legal obligatorio (A07/C05, "seguro de desgravamen")**. En llamadas de **prospección
  corta** —que son la mayoría de lo procesado hasta ahora— esas frases simplemente no
  aparecen, y la regla las cuenta como falta.
- Por eso el **98 % de acuerdo con Gemini es engañoso**: como casi todo es "crítica",
  coincidir es fácil (desbalance de clases). El acuerdo será informativo cuando haya
  una mezcla real de llamadas buenas y malas.
- Hay **muy pocas ventas** (13 posibles, 1 válida) porque lo procesado es prospección
  que no cerró. Por eso corrimos el **día completo 2026-08-06**: al traer todas las
  llamadas del día deberían aparecer **ventas cerradas reales** y el etiquetado se
  vuelve representativo.

**Conclusión operativa:** la señal más urgente a validar a oído es si esas "críticas"
son **verdaderas** (el asesor de verdad no pidió permiso de grabación / no leyó el
descargo) o son **falsos positivos** del detector sobre transcripciones parciales.
De eso depende calibrar el peso de `lf_sin_permiso_grabacion` y del descargo.

## 5. Qué sigue

1. **Refrescar** con el día 2026-08-06 completo (ventas reales).
2. **Auditoría valida un bloque a oído** con el procedimiento y el manual de
   `docs/manual_validacion_auditoria.md`.
3. Con esa verdad confirmada, **calibrar pesos** y recién ahí correr la **Fase 4
   formal** (Precisión/Recall/F1 y comparación de modelos Gemini).
