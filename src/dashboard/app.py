"""Tablero analítico (Streamlit) — placeholder de Fase 0.

Página mínima que valida que la pieza de presentación arranca y alcanza la capa
servida (PostgreSQL). Los KPIs reales se implementan en la Fase 7.
"""
import os

import streamlit as st

st.set_page_config(page_title="Call Center Ventas — Tablero", page_icon="📊", layout="wide")

st.title("📊 Tablero analítico — Call Center de Ventas")
st.caption("Placeholder de Fase 0. Los indicadores de calidad, contactabilidad, "
           "conversión y anomalías se conectan en la Fase 7.")

st.subheader("Estado de la infraestructura")

# Comprobación de conectividad a la capa servida (PostgreSQL).
host = os.getenv("POSTGRES_HOST", "postgres")
port = os.getenv("POSTGRES_PORT", "5432")
db = os.getenv("POSTGRES_DB", "dagster")
user = os.getenv("POSTGRES_USER", "dagster")
pwd = os.getenv("POSTGRES_PASSWORD", "dagster")

try:
    import psycopg2
    conn = psycopg2.connect(host=host, port=port, dbname=db, user=user, password=pwd, connect_timeout=5)
    with conn.cursor() as cur:
        cur.execute("SELECT version();")
        ver = cur.fetchone()[0]
    conn.close()
    st.success(f"PostgreSQL alcanzable en {host}:{port} — {ver.split(',')[0]}")
except Exception as e:  # noqa: BLE001
    st.warning(f"PostgreSQL aún no disponible ({type(e).__name__}). Se reintenta al recargar.")

st.divider()
st.markdown(
    "**Arquitectura (Fase 0):** Dagster (orquestador) · Kafka/KRaft (bus de eventos) · "
    "PostgreSQL (capa servida) · Whisper worker nativo (OpenVINO + GPU Arc). "
    "Ver `fases.md` para el plan por fases."
)
