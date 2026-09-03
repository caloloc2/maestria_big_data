# Limitaciones y trabajo futuro

> Documento de apoyo para redacción. Contiene el texto y los datos de las
> limitaciones identificadas y las líneas de trabajo futuro, listos para
> integrarse a la sección correspondiente del documento final (p. ej.
> "Discusión → Limitaciones" y "Conclusiones → Trabajo futuro"). No implica
> cambios en el código ni en las cifras ya validadas por el tutor.

## 1. Limitaciones

### 1.1. El índice de calidad se ve dominado por incumplimientos sistémicos

El índice de calidad (`calidad_score_v2 = 100 − Σ penalizaciones`) presenta un
valor medio bajo y poco disperso (calidad media ≈ 7,6 sobre el conjunto
evaluado). El análisis por ítem muestra que la causa no es un error de cómputo,
sino un comportamiento **sistémico** del guion de atención: los ítems A03
(solicitud de permiso de grabación) y A07 (lectura del descargo legal) están
ausentes en prácticamente todas las llamadas. Al penalizar por igual a la
totalidad de los casos, estos ítems reducen el poder discriminante del índice
para diferenciar el desempeño entre asesores.

Esta observación es, en sí misma, un **hallazgo de auditoría**: revela una falla
generalizada del guion (relevante para el cumplimiento de la LOPDP en el caso de
A03) más que una deficiencia individual. Se documenta como limitación del
indicador y, a la vez, como insumo para la mejora del guion.

### 1.2. Baja pronosticabilidad de los indicadores expresados como tasas

Los modelos de pronóstico rinden bien sobre los indicadores de **volumen**
(p. ej. número de llamadas, con Prophet R² ≈ 0,70; llamadas contestadas
R² ≈ 0,75), pero su desempeño cae a valores de R² cercanos a cero o negativos
sobre los indicadores expresados como **tasas** (contactabilidad y duración
media), que exhiben poca estructura temporal aprovechable. El tablero ya
advierte automáticamente esta condición cuando R² < 0,2.

Se documenta como limitación honesta del alcance predictivo: los volúmenes son
pronosticables con la señal disponible, mientras que las tasas requieren
predictores adicionales (variables exógenas) para alcanzar utilidad predictiva.

### 1.3. Detección de infracciones del Grupo B basada en reglas léxicas

La detección de la mayoría de las infracciones del Grupo B se realiza mediante
reglas léxicas (coincidencia de expresiones como "garantizo", "aseguro",
"100 %", "inmediato", etc.). Estas expresiones son **dependientes del
contexto**: su sola aparición no siempre constituye una infracción, lo que
introduce ambigüedad y posibles falsos positivos.

Como prueba de concepto de una alternativa superior, el ítem de suplantación de
identidad del banco (`impersona_banco`) se migró de reglas léxicas a evaluación
por modelo de lenguaje con contexto. El cambio elevó la detección de ~3 casos
(por regla léxica) a **504 de 952 llamadas (~53 %)**, cuantificando un riesgo de
reclamo antes invisible. El resto de las infracciones del Grupo B permanece bajo
el esquema de reglas, lo que constituye la limitación actual.

### 1.4. Rúbrica de auditoría con criterios ambiguos o contradictorios

Durante la calibración de la rúbrica (v2) se detectaron criterios de
calificación **ambiguos o mutuamente contradictorios**. El caso representativo
es el ítem C06 (tipo de crédito), que se retiró del cómputo porque contradice al
ítem A08 (el cual sí admite mencionar el tipo de crédito en el cierre): un mismo
enunciado del asesor podía puntuar simultáneamente como cumplimiento (A08) e
infracción (C06). Situaciones análogas de solapamiento se evitaron fijando en
cero el Grupo C, cuyos ítems ya se penalizan dentro del Grupo A (para no
contabilizar dos veces el mismo hecho).

Esta limitación no es del modelo sino de la **definición de la rúbrica de
negocio**. Se documenta para que el área de Auditoría reestructure sus criterios
de calificación, eliminando ambigüedades y contradicciones, de modo que las
métricas puedan ajustarse de forma consistente al proyecto y al modelo.

### 1.5. La selección del modelo por R² no penaliza la extrapolación implausible

La selección automática del modelo de pronóstico se realiza por el coeficiente
de determinación (R²) obtenido en la ventana de prueba (backtest). Al ampliarse
la serie operativa con el histórico procesado, el criterio pasó a favorecer, en
los indicadores de volumen y contactabilidad, a un modelo (suavizado exponencial
de Holt-Winters) que **ajusta bien la ventana de prueba pero extrapola de forma
implausible fuera de ella**: proyectaba el volumen de llamadas por debajo de un
tercio de su nivel histórico y una contactabilidad superior al 100 %, que es
imposible por definición. El R² del backtest no captura esta degradación porque
mide el ajuste dentro del horizonte de prueba, no la coherencia de la
extrapolación a futuro.

Se mitigó en la vista gerencial fijando el modelo Prophet —el modelo elegido del
proyecto, que extrapola tendencia y estacionalidad de forma acotada— y recortando
los indicadores porcentuales al rango [0, 100] % por definición. El panel técnico
conserva la comparación de todos los modelos como evidencia. Queda pendiente
formalizar un criterio de selección que incorpore la plausibilidad de la
extrapolación, no solo el ajuste en la ventana de prueba.

## 2. Trabajo futuro

1. **Reponderación del índice de calidad.** Ajustar los pesos de los ítems con
   incumplimiento sistémico (A03, A07) para preservar el valor del índice como
   señal de cumplimiento sin anular su capacidad de discriminar entre asesores;
   evaluar reportar dos indicadores separados: cumplimiento normativo (guion) y
   desempeño individual.

2. **Predictores exógenos para las tasas.** Incorporar variables externas
   (campañas, dotación de asesores, estacionalidad comercial, indicadores del
   entorno) como regresores para mejorar el pronóstico de contactabilidad y
   duración media, hoy con baja pronosticabilidad.

3. **Migración de las infracciones del Grupo B a evaluación contextual.**
   Extender el enfoque validado con `impersona_banco` al resto de infracciones
   del Grupo B, sustituyendo las reglas léxicas por evaluación con modelo de
   lenguaje y contexto, y midiendo su impacto con el conjunto de referencia
   (gold set).

4. **Reestructuración de la rúbrica de auditoría.** Revisar con el área de
   Auditoría los criterios ambiguos o contradictorios (C06 frente a A08, y los
   solapamientos del Grupo C), formalizar definiciones no ambiguas y recalibrar
   los pesos en función del proyecto y del modelo.

5. **Diarización con GPU dedicada.** Activar la diarización de hablantes
   (hoy diferida por el costo en CPU) para habilitar métricas atribuidas por
   hablante (asesor/cliente) y una lectura más rica de las transcripciones.

6. **Reproceso histórico completo.** Extender la transcripción y evaluación al
   histórico completo (hoy acotado a llamadas largas ≥ 600 s por eficiencia),
   como recomendación de producción para robustecer el pronóstico de
   ventas-con-riesgo.

7. **Selección de modelo robusta a la extrapolación.** Complementar el criterio
   de R² con una validación de plausibilidad de la proyección (rangos admisibles
   por indicador, penalización del colapso o la divergencia fuera del histórico)
   y acotar por diseño los indicadores porcentuales al rango [0, 100] %, de modo
   que la elección automática no premie modelos que ajustan la ventana de prueba
   pero extrapolan de forma incoherente.

## 3. Resultados adicionales con el histórico ampliado

Durante el procesamiento por bloques del histórico de ventas se completaron los
meses de enero y febrero de 2026 y una parte de marzo de 2026, además de enero de
2025 como referencia interanual. El conjunto evaluado creció de forma
significativa (del orden de 3 200 llamadas largas evaluadas sobre unos 85 días con
datos), lo que permitió dos observaciones que antes no eran posibles.

Primero, la **comparación interanual del mes de enero** muestra que la proporción
de ventas con riesgo de reclamo por llamada larga evaluada **disminuye**: de
≈ 8,1 % en enero de 2025 a ≈ 6,4 % en enero de 2026, y a ≈ 5,0 % en febrero de
2026, mientras la proporción de ventas válidas se mantiene alrededor del 5 %. Es
una señal favorable de negocio y solo pudo cuantificarse al disponer de meses
comparables entre dos años.

Segundo, el **hallazgo central de suplantación de identidad del banco** se
sostiene y se refuerza con la muestra ampliada: sobre el conjunto evaluado
(≈ 3 200 llamadas) la proporción de llamadas que insinúan pertenecer al banco se
mantiene en torno al 56 %, coherente con el ≈ 53 % reportado sobre la muestra
inicial de 952 llamadas. La estabilidad del indicador al multiplicarse el tamaño
de la muestra respalda la robustez del hallazgo.

> Estas cifras provienen del histórico procesado hasta la fecha (backfill por
> bloques) y se afinan automáticamente conforme avanza el procesamiento; se
> reportan como resultados de apoyo, sin sustituir las cifras ya validadas.
