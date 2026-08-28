"""Pipeline de streaming near-real-time (Fase 5).

Detecta llamadas recién finalizadas por el CDR (SOLO LECTURA), localiza su grabación
en el servidor Asterisk (SOLO LECTURA, por ruta construida), aterriza el MP3 crudo en
la zona Bronce del lago, encola el trabajo de ASR y persiste transcripción + evaluación
en la capa servida. NUNCA modifica Asterisk ni su base de datos.
"""
