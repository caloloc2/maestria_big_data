# Guía y esquema de etiquetado — Calidad de venta y anomalías de agentes

> Cómo convertir los parámetros de calidad de la empresa en **etiquetas
> estructuradas** utilizables por modelos de ML y por el LLM, y cómo construir
> un conjunto confiable (gold set) con pocas auditorías previas.

## 1. Idea central: de "parámetros" a "etiquetas"

Los parámetros de la empresa (lo que un asesor debe/no debe hacer) hoy viven en
la cabeza de los auditores. Para que sirvan a un modelo hay que volverlos:

1. **Atómicos:** un criterio = una cosa verificable (no "atendió bien", sino
   "se identificó con nombre y empresa en los primeros 30 s").
2. **Tipados:** binario, escala o categórico (ver §3).
3. **Detectables:** definir CÓMO se detecta cada uno (palabra clave, métrica del
   CDR, o juicio del LLM).
4. **Ponderados:** no todos pesan igual (un criterio legal pesa más que uno de
   cortesía).

## 2. Dos familias de etiquetas

**A. Etiquetas por llamada (calidad/cumplimiento)** — describen UNA llamada.
Sirven para el modelo de calidad de venta y para la auditoría automática.

**B. Etiquetas por agente / periodo (rendimiento y anomalías)** — agregadas por
asesor y ventana de tiempo (día/semana). Sirven para el modelo de **detección de
anomalías** de rendimiento.

## 3. Esquema de criterios (plantilla a llenar con TUS parámetros)

Llena esta tabla en `proyecto/parametros_calidad_empresa.md` (o un CSV/Excel):

| id | criterio | categoría | tipo | cómo se detecta | peso | obligatorio (legal) |
|----|----------|-----------|------|-----------------|------|---------------------|
| C01 | Se identifica con nombre y empresa en ≤30 s | Apertura | binario (0/1) | LLM + keyword | 2 | No |
| C02 | Declara que la llamada es grabada / con fines de venta | Legal | binario | LLM | 5 | **Sí** |
| C03 | Menciona el precio total real sin ocultar costos | Transparencia | binario | LLM | 5 | **Sí** |
| C04 | Confirma datos del cliente antes de cerrar | Cierre | binario | LLM | 3 | No |
| C05 | No promete beneficios inexistentes | Veracidad | binario | LLM | 5 | **Sí** |
| C06 | Tono/sentimiento del asesor (respeto, sin presión indebida) | Trato | escala 1-5 | modelo sentimiento + LLM | 2 | No |
| C07 | Maneja objeciones correctamente | Técnica | escala 1-5 | LLM | 2 | No |
| C08 | Lee/confirma condiciones de cobro con el banco | Legal | binario | LLM | 5 | **Sí** |

> Tipos sugeridos: **binario** (cumple/no), **escala** (1-5 calidad),
> **categórico** (p. ej. motivo_no_conformidad ∈ {ocultó_costo, presionó,
> dato_falso, …}).

## 4. Etiqueta resultante por llamada (lo que consume el modelo)

Para cada llamada, el pipeline produce un registro como este (JSON):

```json
{
  "call_id": "20260315093210-1021-0991234567",
  "agente_ext": "1021",
  "fecha": "2026-03-15T09:32:10",
  "duracion_seg": 2740,
  "es_venta": 1,
  "criterios": { "C01":1,"C02":1,"C03":0,"C04":1,"C05":1,"C06":4,"C07":3,"C08":0 },
  "calidad_score": 62,
  "venta_valida": 0,
  "motivos_no_conformidad": ["ocultó_costo","sin_confirmacion_banco"],
  "sentimiento_asesor": "neutral",
  "sentimiento_cliente_trayectoria": ["neutral","positivo","negativo"],
  "riesgo_reclamo": "alto",
  "fuente_etiqueta": "llm_propuesta",      // llm_propuesta | humano_confirmada | humano_corregida
  "confianza_llm": 0.78
}
```

- **calidad_score** = suma ponderada de criterios normalizada a 0-100.
- **venta_valida** = regla dura: 0 si falla CUALQUIER criterio obligatorio
  (legal), independientemente del score → evita reclamos del cliente/banco.
- **fuente_etiqueta** es clave para el gold set (§6).

## 5. Etiquetas de anomalía por agente (familia B)

Agregar por asesor y ventana (ej. semanal):

```json
{
  "agente_ext": "1021",
  "semana": "2026-W11",
  "n_llamadas": 140,
  "tasa_contactabilidad": 0.31,
  "tasa_conversion": 0.08,
  "calidad_promedio": 71,
  "pct_ventas_validas": 0.84,
  "duracion_media_seg": 520,
  "sentimiento_cliente_neg_pct": 0.22,
  "anomalia": null            // se rellena con detección no supervisada
}
```

La **anomalía** no se etiqueta a mano al inicio: se detecta con métodos no
supervisados (Isolation Forest / z-score sobre el histórico del propio agente y
del grupo) y se confirma con revisión humana de los casos marcados.

## 6. Estrategia con POCAS etiquetas previas: weak supervision

Como casi no hay auditorías marcadas, NO se entrena un supervisado desde cero.
Plan en 4 pasos:

1. **LLM propone** (`fuente_etiqueta = "llm_propuesta"`) las etiquetas de cada
   llamada según la rúbrica del §3.
2. **Muestreo para humano:** un auditor revisa una **muestra estratificada**
   (incluir casos de alta y baja confianza del LLM, ventas y no-ventas). Marca
   `humano_confirmada` o `humano_corregida`.
3. **Gold set:** el subconjunto revisado por humanos es el conjunto de verdad
   (ground truth) para **medir** el acierto del LLM/modelo (Accuracy, Recall,
   F1) → cumple "comparación contra línea base" de la rúbrica.
4. **Iterar:** las correcciones humanas afinan el prompt/reglas; el gold set
   crece con cada ciclo.

## 7. Buenas prácticas de etiquetado (calidad del dataset)

- **Acuerdo entre anotadores:** que ≥2 auditores etiqueten una misma muestra y
  medir concordancia (Cohen's Kappa). Si es baja, el criterio está mal definido.
- **Libro de códigos (codebook):** documentar para cada criterio una definición,
  un ejemplo positivo y uno negativo (reduce ambigüedad).
- **Balancear clases:** asegurar suficientes ejemplos de "venta NO válida"
  (suelen ser minoría) para que el modelo aprenda lo crítico.
- **Versionar:** guardar versión de la rúbrica usada (`rubrica_v1`, `v2`) porque
  los criterios evolucionan; las métricas deben compararse dentro de la misma
  versión.
- **Trazabilidad:** cada etiqueta guarda `call_id`, versión de rúbrica, modelo
  LLM y fecha → reproducibilidad (lineamiento de gobierno de datos).

## 8. Qué necesito de ti

1. Sube tus parámetros reales a `proyecto/parametros_calidad_empresa.md`.
2. Marca cuáles son **obligatorios/legales** (peso máximo, regla dura).
3. Indícame si ya existe ALGUNA auditoría previa registrada (aunque sean pocas)
   para arrancar el gold set.

Con eso convierto esta plantilla en tu rúbrica definitiva y el esquema JSON final
del pipeline.
