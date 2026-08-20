"""Definiciones de Dagster — esqueleto de la arquitectura Medallion (Fase 0).

Cada activo (`@asset`) representa una zona/producto de datos. Por ahora son
ESQUELETOS: solo registran un log y devuelven un marcador; la lógica real se
implementa en las fases indicadas. Las dependencias entre activos (parámetros de
las funciones) hacen que Dagster dibuje el grafo Bronce → Plata → Oro en la UI.

Levantar el stack:  docker compose -f infra/docker-compose.yml up --build -d
UI:                 http://localhost:3000
"""
from dagster import Definitions, asset, AssetExecutionContext

BRONCE = "bronce"
PLATA = "plata"
ORO = "oro"


# ───────────────────────────── Zona Bronce (crudo) ─────────────────────────────
@asset(group_name=BRONCE, description="CDR crudo desde MySQL (solo lectura) → Parquet. Se implementa en la Fase 2.")
def bronze_cdr(context: AssetExecutionContext) -> dict:
    context.log.info("bronze_cdr: esqueleto (Fase 0) — sin lógica todavía; se implementa en la Fase 2.")
    return {"estado": "esqueleto", "fase": 2}


@asset(group_name=BRONCE, description="Índice del filesystem de grabaciones (ext. 200–299 / OUT). Base del enlace. Fase 2.")
def bronze_audio_index(context: AssetExecutionContext) -> dict:
    context.log.info("bronze_audio_index: esqueleto (Fase 0); el diagnóstico de la Fase 1 ya validó el enlace al 100 %.")
    return {"estado": "esqueleto", "fase": 2}


# ───────────────────────────── Zona Plata (limpio) ─────────────────────────────
@asset(group_name=PLATA, description="Llamadas normalizadas + cruce CDR↔grabación (ext + teléfono + ventana). Fase 2.")
def silver_calls(context: AssetExecutionContext, bronze_cdr: dict, bronze_audio_index: dict) -> dict:
    context.log.info("silver_calls: esqueleto (Fase 0) — reutilizará el resolvedor de enlace validado en la Fase 1.")
    return {"estado": "esqueleto", "fase": 2}


@asset(group_name=PLATA, description="Transcripciones anonimizadas (Whisper worker + Presidio). Fase 3.")
def silver_transcriptions(context: AssetExecutionContext, silver_calls: dict) -> dict:
    context.log.info("silver_transcriptions: esqueleto (Fase 0) — ASR + anonimización; solo esto sale a Gemini.")
    return {"estado": "esqueleto", "fase": 3}


# ────────────────────────────── Zona Oro (negocio) ─────────────────────────────
@asset(group_name=ORO, description="Evaluaciones de calidad/cumplimiento con Gemini + rúbrica. Fase 4.")
def gold_evaluations(context: AssetExecutionContext, silver_transcriptions: dict) -> dict:
    context.log.info("gold_evaluations: esqueleto (Fase 0) — clasificación por rúbrica sobre texto anonimizado.")
    return {"estado": "esqueleto", "fase": 4}


@asset(group_name=ORO, description="KPIs y anomalías por agente/periodo + intentos por contacto. Fase 6.")
def gold_kpis(context: AssetExecutionContext, gold_evaluations: dict) -> dict:
    context.log.info("gold_kpis: esqueleto (Fase 0) — agregados, anomalías y KPI de intentos/reintentos.")
    return {"estado": "esqueleto", "fase": 6}


defs = Definitions(
    assets=[
        bronze_cdr,
        bronze_audio_index,
        silver_calls,
        silver_transcriptions,
        gold_evaluations,
        gold_kpis,
    ]
)
