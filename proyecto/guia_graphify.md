# Guía de uso de Graphify en la tesis

> Cómo funciona, cómo se ejecuta y cómo se actualiza el grafo de conocimiento de
> la documentación de la tesis. Instalado: graphify **0.8.50** (paquete
> `graphifyy`), registrado como skill de Claude Code (`/graphify`).

## 1. Qué es y cómo funciona

Graphify lee tus archivos (PDF, Word, Markdown, código, imágenes) y construye un
**grafo de conocimiento**: extrae conceptos (nodos) y relaciones (aristas) y los
guarda en `graph.json`, generando además un `graph.html` interactivo y un
`GRAPH_REPORT.md`. No usa base de datos externa (trabaja con NetworkX local) y
cachea por hash SHA256 para no reprocesar lo que no cambió.

**Decisión de arquitectura del grafo:** UN solo grafo **unificado en la raíz**
del repo, no grafos separados por carpeta. Razón: graphify genera un `graph.json`
independiente por ejecución y **no enlaza grafos distintos** entre sí; separarlos
perdería las conexiones que queremos (una clase ligada a un lineamiento ligado a
una referencia). El grafo único conecta toda la documentación.

## 2. Cómo se ejecuta

Desde Claude Code (recomendado), escribe el comando de skill:

```
/graphify .                    # construye/reconstruye el grafo de TODO el repo
/graphify . --update           # re-indexa SOLO los archivos modificados (uso diario)
/graphify ./referencias_bibliograficas --mode deep   # vista profunda de un corpus
/graphify . --no-viz           # solo JSON + reporte (sin HTML)
```

Consultas sobre el grafo ya construido:

```
/graphify query "¿qué criterios de calidad de venta debe cumplir un asesor?"
graphify explain "descargo legal"
graphify path "palabras prohibidas" "ente regulador"
graphify add https://arxiv.org/abs/XXXX      # añade un paper a ./raw y actualiza
```

> La salida se genera en `graphify-out/` (graph.json, graph.html, GRAPH_REPORT.md,
> cache/). Abrir `graph.html` en el navegador para explorar visualmente.

## 3. Cómo se actualiza (flujo recomendado)

- **Tras añadir o editar documentos** (nuevas referencias, secciones del plan,
  clases): `/graphify . --update`. Solo procesa lo nuevo/cambiado (rápido).
- **Al cerrar cada sesión de trabajo:** correr `--update` para mantener el grafo
  al día con lo redactado.
- **Automatizable (opcional):** si convertimos el repo en git (`git init`),
  `graphify hook install` reindexa en cada commit; `graphify watch .` observa
  cambios y actualiza en caliente.

## 4. Qué se excluye (.graphifyignore)

El grafo es para **documentación**, no para datos pesados. El archivo
`.graphifyignore` (raíz del repo, sintaxis tipo `.gitignore`) excluye:

```
proyecto/            # código, contenedores y datos del pipeline
graphify-out/        # salida del propio grafo
*.wav
*.mp3
*.zip
cache/
```

> Nota: si más adelante quieres un grafo SOLO del código del pipeline, se puede
> correr `/graphify ./proyecto` aparte, pero por ahora el foco es la documentación.

## 5. Resumen operativo

| Acción | Comando |
|--------|---------|
| Construir grafo completo | `/graphify .` |
| Actualizar (diario) | `/graphify . --update` |
| Vista profunda de una carpeta | `/graphify ./carpeta --mode deep` |
| Preguntar al grafo | `/graphify query "…"` |
| Explorar visual | abrir `graphify-out/graph.html` |
