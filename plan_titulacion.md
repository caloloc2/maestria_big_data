# Plan para el Trabajo de Titulación

**Universidad Tecnológica Israel — Escuela de Posgrados “ESPOG”**
**Maestría en Big Data y Data Science**

**Tema:** Integración de analítica en tiempo real y LLMs en la ingesta de metadatos y grabaciones telefónicas, para identificar prácticas de atención críticas y predecir anomalías en el rendimiento de los agentes de call center.

**Autor:** Miño Flores Carlos Enrique
**Quito – Ecuador · 2026**

> Transcripción fiel del documento `plan_titulacion.pdf` / `plan_titulacion.docx`
> a formato Markdown, para lectura y trabajo del equipo de implementación.

---

## I. Revisión de Literatura

Los call centers generan de forma continua dos tipos principales de información. Por un lado, se registran datos estructurados de cada llamada, como la fecha, hora, duración y otros detalles operativos, que son almacenados en bases de datos relacionales mediante registros (CDR). Por otro lado, se guardan las grabaciones de las conversaciones para procesos de revisión, control de calidad o auditoría, siendo estas las que contienen la mayor cantidad de información relevante sobre la interacción entre el cliente y el agente. Actualmente, gran parte de la evaluación de estas llamadas se realiza manualmente, lo que demanda mucho tiempo y esfuerzo, además de limitar la cantidad de interacciones que pueden ser revisadas y analizadas. Debido al alto volumen de información generado y a la diversidad de formatos, este escenario se relaciona directamente con el Big Data. De igual forma, las grabaciones pueden incluir información personal o financiera, por lo que es necesario considerar mecanismos de anonimización que protejan la privacidad de los usuarios. En este contexto, los avances en analítica en tiempo real, reconocimiento automático de voz (ASR) y modelos de lenguaje de gran escala (LLM) ofrecen nuevas oportunidades para automatizar la evaluación de la calidad de atención, identificar prácticas inadecuadas y detectar posibles problemas de desempeño de los agentes. Con esto, la presente revisión de literatura se estructura en cuatro ejes: arquitecturas de analítica en tiempo real, reconocimiento de voz y análisis de audio, aplicación de LLM y procesamiento de lenguaje natural (PLN) en conversaciones, y técnicas de detección de anomalías y predicción de rendimiento.

### A. Analítica en tiempo real y arquitecturas de datos masivos

El manejo de grandes cantidades de datos que requieren respuestas rápidas ha dado lugar a diferentes modelos de arquitectura para su procesamiento. Entre los más conocidos se encuentra la arquitectura Lambda, que combina el procesamiento de datos histórico por lotes con el procesamiento de información en tiempo real, permitiendo obtener resultados precisos sin perder velocidad en el análisis de nuevos datos [1]. Sin embargo, la necesidad de mantener dos procesos con lógicas similares impulsó el desarrollo de otra arquitectura denominada Kappa, la cual centraliza todo el procesamiento en un único flujo de eventos, facilitando la administración y mantenimiento [2]. Estos enfoques continúan siendo relevantes en la actualidad, por ejemplo, Khattach et al. proponen una arquitectura integral que combina procesamiento en streaming y modelos de aprendizaje automático para analizar información en tiempo real y apoyar tareas de mantenimiento predictivo en entornos del Internet de las Cosas (IoT) [3]. Esto muestra que la integración entre plataformas de mensajería de eventos y sistemas de procesamiento se ha convertido en una solución para el tratamiento de grandes volúmenes de datos. Desde otra perspectiva, Pincay y Marcillo destacan que la combinación de Big Data y aprendizaje automático ayudan a mejorar la automatización y la calidad de las decisiones gerenciales [4]. Con estas aportaciones, para el presente trabajo resulta pertinente utilizar una arquitectura híbrida capaz de analizar cada llamada casi inmediatamente después de finalizada, mientras que, de forma paralela, permita reprocesar grandes cantidades de información histórica aplicando los mismos criterios de validación y limpieza de datos.

### B. Reconocimiento automático del habla y analítica de voz

El análisis de las grabaciones requiere transformar la información de audio en texto e identificar correctamente quién está hablando en cada momento de la conversación. Este proceso es importante cuando tanto el agente como el cliente comparten el mismo canal de audio, ya que resulta necesario diferenciar cada intervención. En este sentido, Wang et al. proponen DiarizationLM, una solución que utiliza LLM para mejorar los resultados de los sistemas de diarización y reducir errores en la identificación de los que intervienen en la llamada [5]. Sin embargo, la transcripción presenta otros desafíos. Koenecke et al. señalan que herramientas de ASR como Whisper pueden generar información que realmente no está presente en el audio, esto se lo denomina alucinaciones, lo que puede ocasionar problemas cuando se trabaja con información sensible [6]. Por esta razón, se recomienda incorporar mecanismos que permitan validar la calidad de las transcripciones. Igualmente, Harris et al. identifican la existencia de sesgos en cuanto al género y variantes de dialecto en los ASR [7], siendo relevante en nuestro contexto con el dialecto ecuatoriano. Otros elementos presentes también son la emoción transmitida durante la conversación, esto aporta información valiosa. En esta línea, Barhoumi y BenAyed desarrollaron un sistema capaz de reconocer emociones en tiempo real con técnicas de aprendizaje profundo [8], lo que permite evaluar de mejor manera la interacción entre el cliente y el agente, y comprender cómo evoluciona el sentimiento a lo largo de la llamada.

### C. LLM y PLN para análisis de conversaciones, sentimiento y cumplimiento

Los LLM han generado avances importantes en el campo del PLN, ampliando las posibilidades de análisis y comprensión de información textual. Diversos estudios, como los realizados por Minaee et al. y Raiaan et al., analizan las principales características, capacidades y limitaciones, dando una base teórica para la aplicación de distintos escenarios [9], [10]. En el ámbito del análisis de sentimientos, Zhang et al. destacan que, aunque los LLM ofrecen buenos resultados y una capacidad de adaptación sin necesidad de ajustes complejos, todavía existen diferencias de rendimiento en comparación con modelos desarrollados para tareas más específicas, especialmente cuando se requiere analizar opiniones sobre aspectos concretos [11]. A pesar de las limitaciones, los LLM permiten automatizar actividades como la evaluación de la calidad de las ventas, verificación de cumplimiento de guiones de atención, identificación de mensajes engañosos y la revisión de requisitos normativos a partir de las transcripciones. Además, debido a que este estudio integra información proveniente de varias fuentes, resulta relevante considerar los aportes de Quintero Bernal et al. sobre la fusión de datos multimodales [12], ya que ofrecen lineamientos para combinar de manera adecuada distintos tipos de información y obtener un análisis más completo y valioso de las conversaciones.

### D. Detección de anomalías y predicción del rendimiento

La identificación temprana de la disminución del desempeño y de comportamientos inusuales de los agentes requiere la aplicación de técnicas de predicción y detección de anomalías. Su et al. analizan, mediante la revisión sistemática, cómo los LLM están siendo usados con mayor frecuencia para tareas de diagnóstico y detección de anomalías, complementando los métodos tradicionales [13]. Igualmente, Edozie et al. destacan los avances de la inteligencia artificial en la detección de comportamientos anómalos en las redes de telecomunicaciones [14]. En casos donde las auditorías son manuales, no se dispone de suficientes etiquetados para entrenar los modelos. Es por eso que los enfoques no supervisados se convierten en una alternativa adecuada. Berahmand et al. presentan un estudio sobre autocodificadores y sus aplicaciones en la reducción de patrones atípicos en los datos [15]. Por su parte, Kong et al. revisan diferentes técnicas de aprendizaje profundo orientadas al pronóstico de series temporales [16], las cuales pueden utilizarse para analizar y predecir indicadores relevantes, tales como tasa de contactabilidad, porcentaje de conversión a ventas o duración promedio de llamadas por cada agente.

### Síntesis y vacío de investigación

En términos generales, la literatura revisada muestra que existen avances importantes y un nivel de desarrollo alto en cada tecnología involucrada, como ASR, análisis en tiempo real, diarización, LLM y análisis de sentimiento junto con PLN para el cumplimiento normativo y detección de anomalías. Sin embargo, son limitadas las propuestas que integren todos estos componentes dentro de un único proceso. Actualmente, no se encontraron soluciones que combinen, en un mismo flujo y de manera casi inmediata, la captura de metadatos y grabaciones, la transcripción con protección de datos sensibles y análisis con LLM para predicción de posibles anomalías en el desempeño de los agentes. La mayor parte de las investigaciones se centran en resolver problemas específicos de manera independiente y pocas consideran la privacidad y trazabilidad como algo necesario cuando se manejan datos personales o financieros. Por esto, existe la posibilidad de desarrollar una solución integral que permita reemplazar auditorías manuales, por un sistema automatizado capaz de generar indicadores de calidad y alertas de riesgo de manera eficiente y consistente. Esta necesidad constituye la base del problema de investigación del presente trabajo.

---

## II. Problema de Investigación

Los call center generan cada día grandes cantidades de información, principalmente en grabaciones de audio que no están estructuradas y que, en muchos casos, no se analizan completamente. En este trabajo, se dispone de más de 100 GB de grabaciones y más de 100.000 registros de llamada (CDR) acumulados desde el 2017. Actualmente, la evaluación del desempeño de los agentes se basa principalmente en auditorías manuales, las cuales solo permiten ver una pequeña parte de las interacciones. Esto provoca demoras en la obtención de resultados y dificulta la detección de anomalías en el desempeño, prácticas inadecuadas y posible incumplimiento de las normativas establecidas por la empresa.

Aunque los CDR se almacenan en bases relacionales con analítica descriptiva, las herramientas tradicionales no procesan el contenido semántico del audio y desaprovechan la mayor parte de la información. Los LLM han demostrado eficacia en el análisis de sentimiento y la extracción de entidades [11], y existen arquitecturas maduras de analítica en tiempo real [3]; sin embargo, persiste un vacío: la ausencia de arquitecturas de referencia que integren de forma nativa los LLM con la analítica de streaming para procesar concurrentemente metadatos relacionales y flujos de voz a escala, incluyendo la predicción de anomalías [13].

A partir de este vacío se formula la siguiente pregunta de investigación:

> ¿Cómo diseñar e implementar una arquitectura que integre Modelos de Lenguaje de Gran Escala (LLM) y analítica en tiempo real sobre registros CDR y grabaciones de audio para identificar de forma escalable prácticas de atención críticas y predecir anomalías en el rendimiento de los agentes de un call center?

**Hipótesis:** una arquitectura modular que combine analítica de streaming y LLM sobre metadatos CDR y transcripciones de audio permitirá analizar las interacciones a escala con una precisión estadísticamente superior en los KPIs de calidad y reducir el tiempo de detección de anomalías frente a la auditoría manual.

---

## III. Objetivo General

- Implementar una solución de Big Data para el procesamiento concurrente de metadatos CDR y grabaciones de audio no estructuradas, mediante la integración de analítica en tiempo real (streaming) y Modelos de Lenguaje de Gran Escala (LLM), con el fin de identificar prácticas de atención críticas y predecir anomalías en el rendimiento de los agentes de un call center.

---

## IV. Objetivos Específicos

- **Contextualizar** los fundamentos teóricos sobre analítica en tiempo real, reconocimiento automático del habla (ASR) y Modelos de Lenguaje de Gran Escala aplicados al análisis de interacciones de call center, mediante una revisión de la literatura científica actualizada.
- **Diagnosticar** el proceso actual de auditoría y la calidad, trazabilidad y requerimientos de los datos, registros CDR y grabaciones, mediante el análisis de un conjunto representativo de llamadas y de la infraestructura existente (Asterisk/MySQL).
- **Desarrollar** una arquitectura modular que integre ingesta por streaming, procesamiento distribuido por lotes, transcripción con anonimización de datos sensibles y análisis mediante LLM, para identificar prácticas de atención críticas y construir indicadores de calidad y de rendimiento de los agentes.
- **Validar** el impacto de la arquitectura propuesta en un entorno real de la empresa, comparando indicadores antes y después de su implementación, cobertura de auditoría, precisión en la identificación de prácticas críticas y tiempo de detección de anomalías, frente a la auditoría manual como línea base.

---

## V. Justificación Práctica

El trabajo plantea el desarrollo de una solución basada en tecnologías Big Data, que combina analítica en tiempo real y LLM para procesar simultáneamente tanto los CDR como las grabaciones de llamadas. Esta propuesta incorpora funcionalidades como la transcripción del audio, protección de datos sensibles mediante procesos de anonimización y evaluación de la calidad de atención por los agentes. De esta manera, se busca reemplazar el modelo tradicional de la auditoría manual, que actualmente solo permite revisar una pequeña parte de las interacciones, por un sistema capaz de analizar de forma continua y casi en tiempo real la totalidad de las llamadas generadas.

Desde lo técnico, la propuesta permite integrar el procesamiento de datos en tiempo real y por lotes, incorporando mecanismos de privacidad, aprovechando infraestructura disponible como Asterisk y base de datos. En lo académico, el estudio aporta una solución que puede ser replicada en otros escenarios, al combinar varias tecnologías para identificar prácticas críticas de atención y posibles anomalías. Además, desde lo social, la solución brinda protección a los consumidores al facilitar la identificación de prácticas inadecuadas y reducir el riesgo de incumplimiento normativo. Al mismo tiempo, proporciona a la empresa indicadores que apoyan la mejora de procesos hacia los agentes, favoreciendo la toma de decisiones informada por parte de la gerencia.

---

## VI. Vinculación con la Sociedad y Beneficiarios Directos

El principal aporte de este proyecto consiste en brindar capacitación y acompañamiento a la empresa donde se pondrá en marcha la solución propuesta. Para ello, se capacitará al equipo directivo, personal de auditoría y calidad y gerentes para el uso adecuado del tablero analítico, comprensión de indicadores de desempeño y calidad y gestión de las alertas relacionadas con prácticas inadecuadas o comportamientos anómalos. La capacitación se la realizará junto con la implementación real de la solución ya que ellos serán los que administren como parte de sus actividades diarias.

Como aporte a la sociedad, el proyecto busca fomentar procesos de venta más claros y transparentes, contribuyendo a una mejor protección de los consumidores. Esto mediante la detección temprana de prácticas inadecuadas y el apoyo al cumplimiento a las normativas de la empresa. Así mismo, a los agentes se les proporciona información acerca de su desempeño con retroalimentación más precisa y contribuyendo a su desarrollo y desempeño profesional.

Como resultado de este trabajo, se entregará un tablero analítico para el seguimiento operativo y gerencial junto con la documentación pertinente. Los beneficiarios directos son los directivos de la empresa, personal de auditoría y los agentes del call center. De forma indirecta, también se beneficiarán los clientes, al recibir atención más controlada y orientada a la calidad. Adicionalmente, el proyecto se alinea con los ODS 8 y 9, relacionados con el trabajo decente, crecimiento económico e impulso a la innovación y la infraestructura tecnológica.

---

## VII. Método de Investigación

El diseño metodológico de la presente investigación se organiza en cinco componentes aplicados al problema: el enfoque investigativo, el procedimiento, los participantes, los instrumentos empleados y las técnicas de análisis de los datos obtenidos. Dado que el objetivo central consiste en diseñar, construir y evaluar una solución tecnológica que resuelva un problema real identificado, el estudio adopta un carácter esencialmente técnico y experimental aplicado, sustentado en la medición cuantitativa del desempeño de esta.

### A. Enfoque de la Investigación

El trabajo adopta un enfoque cuantitativo, ya que busca evaluar de manera objetiva el desempeño de la solución a través de indicadores medibles relacionados con la calidad y rendimiento. Para el desarrollo se utiliza el marco metodológico Design Science Research (DSR) [17], debido a que el objetivo principal es diseñar, implementar y evaluar una solución que responda a una necesidad real identificada en un entorno empresarial. Este enfoque proporciona una metodología estructurada que permite combinar el conocimiento con la aplicación práctica, garantizando una solución que tenga sustento teórico como utilidad en un contexto real.

Como metodología del ciclo de datos aplicaremos CRISP-DM (Cross-Industry Standard Process for Data Mining) [18], cuyas fases guían la construcción del pipeline. El uso de esta metodología consiste en enfoques de arquitectura híbrida los cuales combinan la ventaja del procesamiento por lotes con la capacidad de analizar nuevos datos con baja latencia [1], [2], [3]. Gracias a esto, la solución puede procesar cada llamada poco después de finalizada y que, al mismo tiempo, podamos procesar grandes cantidades de información histórica, manteniendo criterios uniformes de análisis en los datos.

La validación de la propuesta se llevará a cabo con la implementación en el entorno real de la empresa. Para esto, utilizarán un tablero analítico que presentará indicadores de calidad y rendimiento definidos por la empresa, permitiendo monitorear de forma continua los resultados. Esta implementación facilitará la comparación entre el desempeño automatizado contra el proceso manual, con el fin de evaluar los beneficios de la solución y proporcionar información oportuna que apoye la toma de decisiones de la gerencia.

### B. Procedimiento

El desarrollo se estructura en etapas secuenciales e iterativas, alineadas con las fases de CRISP-DM y con los objetivos específicos del trabajo:

- **Comprensión del negocio y diseño metodológico:** Comprensión de los fundamentos teóricos, caracterización del proceso actual de auditoría y definición de los indicadores de calidad, contactabilidad y rendimiento a partir de los parámetros establecidos por la empresa.
- **Comprensión de los datos:** Exploración de los registros (CDR) almacenados en MySQL y de las grabaciones (WAV/MP3) generadas por Asterisk, con evaluación de su calidad, trazabilidad, incluyendo el vínculo entre cada CDR y su grabación.
- **Preparación de los datos:** Limpieza, normalización y cruce CDR–grabación; conversión y estandarización de formatos de audio; filtrado de la muestra por criterios de duración; y transcripción con diarización y anonimización de datos sensibles previo a cualquier análisis.
- **Modelado y desarrollo:** Implementación de la ingesta por streaming, del procesamiento distribuido por lotes del histórico, del módulo de transcripción y del análisis mediante LLM para identificar prácticas de atención críticas y construir los indicadores de calidad y rendimiento.
- **Evaluación:** Medición del desempeño de la solución con las métricas definidas en la sección E y comparación de los indicadores frente a la auditoría manual.
- **Despliegue:** Puesta en producción del pipeline y del tablero analítico en el entorno real de la empresa, con visualización de las métricas para la gerencia, auditores y retroalimentación a los agentes.

### C. Participantes

Dado el carácter técnico del estudio y el marco DSR, la unidad de análisis principal no corresponde a personas, sino a los datos procesados por la solución y a las configuraciones evaluadas.

- **Población:** El universo de interacciones del call center registradas desde 2017, correspondiente a más de 100.000 registros CDR y más de 100 GB de grabaciones de audio.
- **Muestra:** El conjunto de llamadas resultante tras la depuración de los datos crudos y el cruce válido entre cada CDR y su grabación, acotado además mediante criterios de duración mínima y máxima de la llamada, con el fin de descartar interacciones no representativas como llamadas fallidas, sin contacto efectivo o de duración anómala y delimitar los casos con valor analítico.
- **Unidad de análisis:** Cada llamada individual, entendida como la vinculación entre el registro CDR con su grabación y su correspondiente transcripción anonimizada.

### D. Instrumentos

Los instrumentos empleados para la recolección, el procesamiento y el análisis de los datos son coherentes con la naturaleza técnica del estudio:

- **Infraestructura fuente existente:** Servidor Asterisk que gestiona llamadas y generación de grabaciones, así como la base de datos MySQL con los registros CDR, que constituyen el origen de los datos del caso de estudio.
- **Ingesta:** En tiempo real Apache Kafka, para la captura del evento de finalización de cada llamada y la incorporación incremental de sus metadatos y grabación al pipeline.
- **Procesamiento distribuido por lotes:** Apache Spark / PySpark, para el reprocesamiento masivo del histórico por rangos de fechas bajo la misma lógica de limpieza y validación.
- **Transcripción y analítica de voz:** Whisper, para el reconocimiento automático del habla (ASR) con diarización y anonimización de datos personales y financieros previa al análisis; se incorporan mecanismos de verificación de las salidas para mitigar los riesgos de alucinación.
- **Análisis semántico con LLM:** un LLM local para el procesamiento de contenido sensible, preservando la privacidad por diseño, y LLM vía API para tareas no sensibles que no comprometen datos personales y análisis del sentimiento.
- **Entorno de programación:** Python con pandas, PySpark y librerías de aprendizaje automático para la preparación de datos, el cálculo de indicadores y la evaluación de los modelos.
- **Almacenamiento analítico:** Base de datos relacional para consolidar los indicadores procesados que alimentan el consumo.
- **Consumo:** Tablero analítico como producto final, que presenta los indicadores de calidad, contactabilidad y rendimiento a la gerencia y a los equipos pertinentes.

### E. Análisis de Datos

El análisis de los datos combina la evaluación del desempeño de los modelos con la medición de los indicadores de negocio, contrastados frente al proceso de auditoría manual:

- **Identificación de prácticas de atención críticas:** Accuracy, Precision, Recall y F1-score, calculadas sobre el conjunto de datos, para cuantificar la exactitud del sistema en la detección de malas prácticas y del cumplimiento de los parámetros de calidad definidos por la empresa.
- **Indicadores clave de negocio (KPIs):** Tasa de contactabilidad, tasa de conversión de ventas y duración media de las llamadas por agente, junto con la identificación de los horarios y días de mayor efectividad, como base para las decisiones gerenciales.
- **Métricas operativas del pipeline:** Tiempo de procesamiento por llamada y capacidad de procesamiento (throughput), como evidencia del carácter en tiempo real de la arquitectura y de su escalabilidad frente al volumen histórico.
- **Comparación con la línea base:** Se evaluará el proceso de auditoría, del tiempo de detección de anomalías y de la consistencia de la evaluación, antes y después de la implementación.
- **Herramientas de análisis:** Python (pandas, scikit-learn, SciPy, statsmodels y PySpark) para el cálculo de métricas, las pruebas estadísticas y la sistematización de los resultados que se visualizan en el tablero analítico.

---

## VIII. Referencias

1. M. Kiran, P. Murphy, I. Monga, J. Dugan, and S. S. Baveja, “Lambda architecture for cost-effective batch and speed big data processing,” in *2015 IEEE International Conference on Big Data (Big Data)*, 2015. doi: 10.1109/bigdata.2015.7364082.
2. J. Kreps, “Questioning the Lambda Architecture,” 2014. [Online]. Available: https://www.oreilly.com/radar/questioning-the-lambda-architecture/
3. O. Khattach, O. Moussaoui, and M. Hassine, “End-to-End Architecture for Real-Time IoT Analytics and Predictive Maintenance Using Stream Processing and ML Pipelines,” *Sensors*, vol. 25, no. 9, p. 2945, 2025, doi: 10.3390/s25092945.
4. Y. A. Pincay Mendoza, V. C. Marcillo Salazar, V. V. García Constante, A. G. Espinoza Cesme, A. M. Morán Holguín, and G. A. Veloz Santa Cruz, “Decisiones gerenciales automatizadas: integrando Big Data y Machine Learning,” *Cienc. Desarro.*, vol. 28, no. 1, p. 357, 2025, doi: 10.21503/cyd.v28i1.2830.
5. Q. Wang, Y. Huang, G. Zhao, E. Clark, W. Xia, and H. Liao, “DiarizationLM: Speaker Diarization Post-Processing with Large Language Models,” in *Interspeech 2024*, 2024. doi: 10.21437/interspeech.2024-209.
6. A. Koenecke, A. S. G. Choi, K. X. Mei, H. Schellmann, and M. Sloane, “Careless Whisper: Speech-to-Text Hallucination Harms,” in *The 2024 ACM Conference on Fairness, Accountability, and Transparency*, 2024. doi: 10.1145/3630106.3658996.
7. C. Harris, C. Mgbahurike, N. Kumar, and D. Yang, “Modeling Gender and Dialect Bias in Automatic Speech Recognition,” in *Findings of the Association for Computational Linguistics: EMNLP 2024*, 2024. doi: 10.18653/v1/2024.findings-emnlp.890.
8. C. Barhoumi and Y. BenAyed, “Real-time speech emotion recognition using deep learning and data augmentation,” *Artif. Intell. Rev.*, vol. 58, no. 2, 2024, doi: 10.1007/s10462-024-11065-x.
9. S. Minaee et al., “Large Language Models: A Survey,” 2024. doi: 10.48550/arxiv.2402.06196.
10. M. A. K. Raiaan et al., “A Review on Large Language Models: Architectures, Applications, Taxonomies, Open Issues and Challenges,” *IEEE Access*, vol. 12, pp. 26839–26874, 2024, doi: 10.1109/access.2024.3365742.
11. W. Zhang, Y. Deng, B. Liu, S. Pan, and L. Bing, “Sentiment Analysis in the Era of Large Language Models: A Reality Check,” in *Findings of the Association for Computational Linguistics: NAACL 2024*, 2024. doi: 10.18653/v1/2024.findings-naacl.246.
12. D. F. Quintero Bernal, H. Kaschel, and J. Kern, “Una revisión de fusión de datos multimodal: aplicaciones y condiciones adversas,” *Inge CuC*, vol. 21, no. 1, pp. 95–114, 2025, doi: 10.17981/ingecuc.21.1.2025.08.
13. J. Su et al., “Large Language Models for Forecasting and Anomaly Detection: A Systematic Literature Review,” 2024. doi: 10.48550/arxiv.2402.10350.
14. E. Edozie, A. N. Shuaibu, B. O. Sadiq, and U. K. John, “Artificial intelligence advances in anomaly detection for telecom networks,” *Artif. Intell. Rev.*, vol. 58, no. 4, 2025, doi: 10.1007/s10462-025-11108-x.
15. K. Berahmand, F. Daneshfar, E. S. Salehi, Y. Li, and Y. Xu, “Autoencoders and their applications in machine learning: a survey,” *Artif. Intell. Rev.*, vol. 57, no. 2, 2024, doi: 10.1007/s10462-023-10662-6.
16. X. Kong et al., “Deep learning for time series forecasting: a survey,” *International Journal of Machine Learning and Cybernetics*, vol. 16, no. 7–8, pp. 5079–5112, 2025, doi: 10.1007/s13042-025-02560-w.
17. A. R. Hevner, S. T. March, J. Park, and S. Ram, “Design Science in Information Systems Research,” *MIS Quarterly*, vol. 28, no. 1, pp. 75–105, 2004, doi: 10.2307/25148625.
18. R. Wirth and J. Hipp, “CRISP-DM: Towards a Standard Process Model for Data Mining,” in *Proceedings of the 4th International Conference on the Practical Applications of Knowledge Discovery and Data Mining (PADD)*, 2000, pp. 29–39.

---

## IX. Información Administrativa

- **Tipo de trabajo de titulación:** ☑ Proyecto ☐ Artículo científico
- **Aprobado por:** _(pendiente)_
