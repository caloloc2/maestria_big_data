"""Tablero analítico (Streamlit) — Fase 7.

Lee la capa servida (PostgreSQL: servido.llamadas / transcripciones / evaluaciones)
por SQL y expone indicadores para gerencia, auditoría y retroalimentación al agente:
resumen, tiempo real (streaming), desempeño por agente, calidad y cumplimiento
(palabras prohibidas / infracciones críticas), KPI de intentos-reintentos y anomalías.

Se alimenta indistintamente del pipeline batch (reproceso por fechas) y del streaming
near-real-time (columna `origen`). No accede a Asterisk: solo a nuestro PostgreSQL.
"""
import json
import os

import altair as alt
import pandas as pd
import streamlit as st
from sqlalchemy import create_engine, text

st.set_page_config(page_title="Call Center Ventas — Tablero", page_icon="📊", layout="wide")


@st.cache_resource
def get_engine():
    h = os.getenv("POSTGRES_HOST", "postgres")
    p = os.getenv("POSTGRES_PORT", "5432")
    db = os.getenv("POSTGRES_DB", "dagster")
    u = os.getenv("POSTGRES_USER", "dagster")
    pw = os.getenv("POSTGRES_PASSWORD", "dagster")
    return create_engine(f"postgresql+psycopg2://{u}:{pw}@{h}:{p}/{db}", pool_pre_ping=True)


def tabla_existe(nombre: str) -> bool:
    try:
        with get_engine().connect() as c:
            return bool(c.execute(text(
                "SELECT to_regclass(:n)"), {"n": f"servido.{nombre}"}).scalar())
    except Exception:
        return False


@st.cache_data(ttl=15)
def q(sql: str, params: dict | None = None) -> pd.DataFrame:
    with get_engine().connect() as c:
        return pd.read_sql(text(sql), c, params=params or {})


# ─────────────────────────── Encabezado + filtros ───────────────────────────
st.title("📊 Tablero analítico — Call Center de Ventas")
st.caption("Capa servida PostgreSQL · pipeline batch + streaming near-real-time · "
           "calidad, cumplimiento y desempeño por agente.")

if not tabla_existe("llamadas"):
    st.warning("Aún no hay datos en la capa servida. Materializa activos o ejecuta el streaming.")
    st.stop()

rango = q("SELECT min(fecha) f0, max(fecha) f1 FROM servido.llamadas")
f0, f1 = rango.iloc[0]["f0"], rango.iloc[0]["f1"]
agentes = q("SELECT DISTINCT agente FROM servido.llamadas WHERE agente IS NOT NULL ORDER BY 1")

with st.sidebar:
    st.header("Filtros")
    d = st.date_input("Rango de fechas", value=(f0, f1), min_value=f0, max_value=f1)
    if isinstance(d, tuple) and len(d) == 2:
        di, dfin = d
    else:
        di = dfin = d
    ag_sel = st.multiselect("Agentes", agentes["agente"].tolist(), default=[])
    origen_sel = st.radio("Origen", ["todos", "batch", "streaming"], horizontal=True)
    st.divider()
    if st.button("🔄 Actualizar (tiempo real)"):
        st.cache_data.clear()
    st.caption("Los datos se refrescan cada 15 s; pulsa el botón para forzar.")

# WHERE dinámico
cond = ["fecha BETWEEN :di AND :dfin"]
par = {"di": di, "dfin": dfin}
if ag_sel:
    cond.append("agente = ANY(:ags)")
    par["ags"] = ag_sel
if origen_sel != "todos":
    cond.append("origen = :org")
    par["org"] = origen_sel
W = " AND ".join(cond)

tabs = st.tabs(["Resumen", "⚡ Tiempo real", "👤 Por agente",
                "⚖️ Calidad y cumplimiento", "🔁 Intentos", "🚨 Anomalías",
                "🔎 Detalle de llamada"])

# ──────────────────────────────── Resumen ────────────────────────────────
with tabs[0]:
    k = q(f"""
        SELECT count(*) llamadas,
               count(*) FILTER (WHERE disposition='ANSWERED') answered,
               count(DISTINCT agente) agentes,
               count(*) FILTER (WHERE origen='streaming') streaming
        FROM servido.llamadas WHERE {W}""", par)
    tr = q(f"SELECT count(*) n FROM servido.transcripciones WHERE {W}", par) \
        if tabla_existe("transcripciones") else pd.DataFrame({"n": [0]})
    ev = q(f"""SELECT count(*) n,
                      avg(calidad_score)::numeric(10,1) calidad,
                      count(*) FILTER (WHERE venta_valida=1) ventas,
                      count(*) FILTER (WHERE infraccion_critica) criticas
               FROM servido.evaluaciones WHERE {W}""", par) \
        if tabla_existe("evaluaciones") else pd.DataFrame({"n": [0], "calidad": [None], "ventas": [0], "criticas": [0]})

    c = st.columns(4)
    c[0].metric("Llamadas", f"{int(k.iloc[0]['llamadas']):,}")
    c[1].metric("Contactadas (answered)", f"{int(k.iloc[0]['answered']):,}")
    c[2].metric("Transcritas", f"{int(tr.iloc[0]['n']):,}")
    c[3].metric("Evaluadas", f"{int(ev.iloc[0]['n']):,}")
    c = st.columns(4)
    c[0].metric("Agentes activos", int(k.iloc[0]["agentes"]))
    c[1].metric("Calidad media", f"{ev.iloc[0]['calidad'] or 0}")
    c[2].metric("Ventas válidas", int(ev.iloc[0]["ventas"] or 0))
    c[3].metric("Infracciones críticas", int(ev.iloc[0]["criticas"] or 0),
                delta_color="inverse")

    serie = q(f"""SELECT fecha, count(*) llamadas,
                        count(*) FILTER (WHERE origen='streaming') streaming
                 FROM servido.llamadas WHERE {W} GROUP BY fecha ORDER BY fecha""", par)
    if len(serie):
        st.subheader("Volumen de llamadas por día")
        base = serie.melt("fecha", ["llamadas", "streaming"], "serie", "valor")
        st.altair_chart(
            alt.Chart(base).mark_line(point=True).encode(
                x="fecha:T", y="valor:Q", color="serie:N").properties(height=260),
            use_container_width=True)

# ─────────────────────────────── Tiempo real ───────────────────────────────
with tabs[1]:
    st.subheader("⚡ Últimas llamadas procesadas por streaming")
    if tabla_existe("transcripciones"):
        rt = q("""
            SELECT l.calldate, l.agente, l.telefono, l.billsec,
                   t.dur_audio, t.proc_seg, t.device, t.chars, t.run_ts,
                   e.calidad_score, e.riesgo_reclamo, e.infraccion_critica
            FROM servido.llamadas l
            JOIN servido.transcripciones t ON t.call_id=l.call_id
            LEFT JOIN servido.evaluaciones e ON e.call_id=l.call_id
            WHERE l.origen='streaming'
            ORDER BY t.run_ts DESC LIMIT 30""")
        if len(rt):
            cc = st.columns(4)
            cc[0].metric("Procesadas (streaming)", len(rt))
            cc[1].metric("RTF medio (proc/audio)",
                         f"{(rt['proc_seg'].sum()/max(rt['dur_audio'].sum(),1)):.3f}×")
            cc[2].metric("Audio medio", f"{rt['dur_audio'].mean():.0f} s")
            cc[3].metric("Dispositivo", rt["device"].mode().iat[0] if len(rt) else "—")
            st.dataframe(rt, use_container_width=True, height=380)
        else:
            st.info("Sin llamadas de streaming todavía. Ejecuta `python -m src.streaming.runner`.")
    else:
        st.info("Aún no hay transcripciones.")

# ──────────────────────────────── Por agente ────────────────────────────────
with tabs[2]:
    st.subheader("Desempeño por agente")
    if tabla_existe("evaluaciones"):
        por = q(f"""
            SELECT agente,
                   count(*) evaluadas,
                   avg(calidad_score)::numeric(10,1) calidad_media,
                   count(*) FILTER (WHERE venta_valida=1) ventas_validas,
                   count(*) FILTER (WHERE infraccion_critica) criticas,
                   round(100.0*count(*) FILTER (WHERE infraccion_critica)/count(*),1) pct_criticas
            FROM servido.evaluaciones WHERE {W}
            GROUP BY agente ORDER BY criticas DESC, calidad_media ASC""", par)
        if len(por):
            st.dataframe(por, use_container_width=True, height=340)
            st.altair_chart(
                alt.Chart(por).mark_bar().encode(
                    x=alt.X("agente:N", sort="-y"), y="calidad_media:Q",
                    color=alt.Color("criticas:Q", scale=alt.Scale(scheme="reds")),
                    tooltip=list(por.columns)).properties(height=280),
                use_container_width=True)
        else:
            st.info("Sin evaluaciones en el filtro actual.")
    else:
        st.info("Aún no hay evaluaciones.")

# ──────────────────────── Calidad y cumplimiento ────────────────────────
with tabs[3]:
    st.subheader("⚖️ Palabras prohibidas e infracciones críticas (grupo B)")
    if tabla_existe("evaluaciones"):
        binf = q(f"""SELECT grupo_b FROM servido.evaluaciones
                    WHERE {W} AND grupo_b IS NOT NULL AND grupo_b <> ''""", par)
        if len(binf):
            expl = (binf["grupo_b"].str.split(",").explode().str.strip())
            expl = expl[expl != ""]
            top = expl.value_counts().reset_index()
            top.columns = ["infraccion", "conteo"]
            c1, c2 = st.columns([2, 1])
            with c1:
                st.altair_chart(
                    alt.Chart(top).mark_bar().encode(
                        x="conteo:Q", y=alt.Y("infraccion:N", sort="-x"),
                        tooltip=["infraccion", "conteo"]).properties(height=300),
                    use_container_width=True)
            with c2:
                st.dataframe(top, use_container_width=True, height=300)
        else:
            st.success("Sin infracciones del grupo B en el filtro actual.")

        rk = q(f"""SELECT riesgo_reclamo, count(*) n FROM servido.evaluaciones
                  WHERE {W} GROUP BY riesgo_reclamo ORDER BY n DESC""", par)
        if len(rk):
            st.subheader("Riesgo de reclamo")
            st.altair_chart(
                alt.Chart(rk).mark_arc(innerRadius=50).encode(
                    theta="n:Q", color="riesgo_reclamo:N",
                    tooltip=["riesgo_reclamo", "n"]).properties(height=240),
                use_container_width=True)
    else:
        st.info("Aún no hay evaluaciones.")

# ─────────────────────────── Intentos / reintentos ───────────────────────────
with tabs[4]:
    st.subheader("🔁 KPI de intentos y reintentos por contacto")
    st.caption("El CDR es un marcador predictivo: cada contacto puede tener varios intentos. "
               "Reintentos = llamadas del mismo agente al mismo teléfono el mismo día.")
    intent = q(f"""
        WITH c AS (
            SELECT agente, telefono, fecha, count(*) intentos
            FROM servido.llamadas WHERE {W}
            GROUP BY agente, telefono, fecha)
        SELECT count(*) contactos,
               sum(intentos) llamadas_totales,
               round(avg(intentos),2) intentos_medios,
               count(*) FILTER (WHERE intentos>1) contactos_reintentados,
               max(intentos) max_intentos
        FROM c""", par)
    if len(intent) and intent.iloc[0]["contactos"]:
        r = intent.iloc[0]
        cc = st.columns(5)
        cc[0].metric("Contactos únicos", f"{int(r['contactos']):,}")
        cc[1].metric("Llamadas totales", f"{int(r['llamadas_totales']):,}")
        cc[2].metric("Intentos medios", f"{r['intentos_medios']}")
        cc[3].metric("Con reintento", f"{int(r['contactos_reintentados']):,}")
        cc[4].metric("Máx. intentos", int(r["max_intentos"]))

        dist = q(f"""
            WITH c AS (SELECT agente, telefono, fecha, count(*) intentos
                       FROM servido.llamadas WHERE {W}
                       GROUP BY agente, telefono, fecha)
            SELECT intentos, count(*) contactos FROM c GROUP BY intentos ORDER BY intentos""", par)
        st.altair_chart(
            alt.Chart(dist).mark_bar().encode(
                x="intentos:O", y="contactos:Q", tooltip=["intentos", "contactos"]
            ).properties(height=260, title="Distribución de intentos por contacto"),
            use_container_width=True)
    else:
        st.info("Sin datos en el filtro actual.")

# ──────────────────────────────── Anomalías ────────────────────────────────
with tabs[5]:
    st.subheader("🚨 Anomalías de desempeño por agente (z-score, no supervisado)")
    st.caption("Marca agentes cuyo indicador se aleja >2σ del grupo (Fase 6, línea base).")
    if tabla_existe("evaluaciones"):
        base = q(f"""
            SELECT agente, count(*) n,
                   avg(calidad_score)::float calidad,
                   avg(CASE WHEN infraccion_critica THEN 1 ELSE 0 END)::float tasa_critica
            FROM servido.evaluaciones WHERE {W} GROUP BY agente HAVING count(*)>=1""", par)
        if len(base) >= 3:
            for col in ("calidad", "tasa_critica"):
                mu, sd = base[col].mean(), base[col].std(ddof=0) or 1.0
                base[f"z_{col}"] = (base[col] - mu) / sd
            base["anomalia"] = (base["z_calidad"].abs() > 2) | (base["z_tasa_critica"].abs() > 2)
            st.dataframe(
                base.sort_values("z_tasa_critica", ascending=False),
                use_container_width=True, height=340)
            n_an = int(base["anomalia"].sum())
            (st.error if n_an else st.success)(
                f"{n_an} agente(s) marcados como anómalos." if n_an
                else "Sin anomalías por encima de 2σ en el filtro actual.")
        else:
            st.info("Se necesitan ≥3 agentes con evaluaciones para el contraste de grupo.")
    else:
        st.info("Aún no hay evaluaciones.")

# ─────────────────────────── Detalle de llamada ───────────────────────────
with tabs[6]:
    st.subheader("🔎 Detalle de llamada — transcripción y evaluación")
    st.caption("Explora TODAS las llamadas transcritas del filtro. Si la llamada tiene "
               "evaluación de Gemini (>10 min), se muestran sus métricas y el porqué de la "
               "calificación; si no, solo la transcripción anonimizada.")
    if not tabla_existe("transcripciones"):
        st.info("Aún no hay transcripciones.")
    else:
        tiene_ev = tabla_existe("evaluaciones")
        # WHERE cualificado (join con llamadas → columnas ambiguas hay que prefijarlas).
        cond_d = ["l.fecha BETWEEN :di AND :dfin"]
        if ag_sel:
            cond_d.append("l.agente = ANY(:ags)")
        if origen_sel != "todos":
            cond_d.append("l.origen = :org")
        Wd = " AND ".join(cond_d)

        sel_cols = ("t.call_id, l.fecha, l.agente, l.telefono, l.origen, "
                    "round(t.dur_audio) dur_s")
        join_ev = ""
        if tiene_ev:
            sel_cols += (", e.calidad_score, e.es_venta, e.venta_valida, "
                         "e.infraccion_critica, e.riesgo_reclamo")
            join_ev = "LEFT JOIN servido.evaluaciones e ON e.call_id = t.call_id"
        lst = q(f"""SELECT DISTINCT ON (t.call_id) {sel_cols}
                    FROM servido.transcripciones t
                    JOIN servido.llamadas l ON l.call_id = t.call_id
                    {join_ev}
                    WHERE {Wd} ORDER BY t.call_id""", par)

        if not len(lst):
            st.info("Sin transcripciones en el filtro actual.")
        else:
            df = lst.copy()
            fc = st.columns(3)
            solo_ev = fc[0].checkbox("Solo con evaluación", value=False)
            solo_riesgo = fc[1].checkbox("Solo riesgo alto", value=False)
            solo_crit = fc[2].checkbox("Solo infracción crítica", value=False)
            if tiene_ev:
                if solo_ev:
                    df = df[df["calidad_score"].notna()]
                if solo_riesgo:
                    df = df[df["riesgo_reclamo"] == "alto"]
                if solo_crit:
                    df = df[df["infraccion_critica"] == True]  # noqa: E712
                # ordenar: críticas y alto riesgo primero
                df["_ord"] = (df["infraccion_critica"].fillna(False).astype(int) * 2
                              + (df["riesgo_reclamo"] == "alto").astype(int))
                df = df.sort_values(["_ord", "fecha"], ascending=[False, False]) \
                       .drop(columns="_ord")
            df = df.head(500)

            if not len(df):
                st.info("Ninguna llamada cumple los filtros marcados.")
            else:
                st.caption(f"{len(df)} llamada(s) — ordenadas por criticidad.")

                def _etq(r):
                    tag = ""
                    if tiene_ev and bool(r.get("infraccion_critica")):
                        tag = " ⛔ CRÍTICA"
                    elif tiene_ev and r.get("riesgo_reclamo") == "alto":
                        tag = " ⚠️ alto"
                    return f"{r['call_id']} · ag {r['agente']} · {int(r['dur_s'] or 0)}s{tag}"

                opciones = {_etq(r): r["call_id"] for _, r in df.iterrows()}
                sel = st.selectbox("Elige una llamada", list(opciones.keys()))
                cid = opciones[sel]

                tr = q("SELECT transcript_anon, dur_audio, device, chars, "
                       "requiere_diarizacion, diarizado, n_hablantes "
                       "FROM servido.transcripciones WHERE call_id=:c LIMIT 1", {"c": cid})
                ev = q("SELECT * FROM servido.evaluaciones WHERE call_id=:c LIMIT 1",
                       {"c": cid}) if tiene_ev else pd.DataFrame()

                if len(ev):
                    e0 = ev.iloc[0]
                    if bool(e0["infraccion_critica"]):
                        st.error(f"⛔ Infracción crítica · riesgo de reclamo: "
                                 f"{e0['riesgo_reclamo'] or '—'}")
                    elif e0["riesgo_reclamo"] == "alto":
                        st.warning("⚠️ Riesgo de reclamo ALTO")
                    m = st.columns(4)
                    m[0].metric("Calidad", e0["calidad_score"])
                    m[1].metric("¿Es venta?", "Sí" if e0["es_venta"] else "No")
                    m[2].metric("Venta válida", "Sí" if e0["venta_valida"] else "No")
                    m[3].metric("Riesgo reclamo", e0["riesgo_reclamo"] or "—")
                    if e0.get("grupo_b"):
                        st.markdown(f"**Infracciones (grupo B):** {e0['grupo_b']}")
                    if e0.get("grupo_c"):
                        st.markdown(f"**Omisiones (grupo C):** {e0['grupo_c']}")
                    st.markdown(f"**Sentimiento asesor:** {e0.get('sentimiento_asesor') or '—'}  ·  "
                                f"**Cliente (trayectoria):** {e0.get('sentimiento_cliente') or '—'}")
                    try:
                        ga = json.loads(e0["grupo_a"]) if e0.get("grupo_a") else {}
                    except Exception:  # noqa: BLE001
                        ga = {}
                    if ga:
                        with st.expander("Ítems evaluados (grupo A)"):
                            st.json(ga)
                    st.caption(f"Modelo: {e0.get('modelo') or '—'} · "
                               f"confianza LLM: {e0.get('confianza_llm') or '—'}")
                else:
                    st.info("Sin evaluación de Gemini para esta llamada (≤10 min o no "
                            "evaluada). Se muestra solo la transcripción anonimizada.")

                if len(tr):
                    t0 = tr.iloc[0]
                    meta = (f"dur {int(t0['dur_audio'] or 0)}s · {t0['device'] or '—'} · "
                            f"{int(t0['chars'] or 0)} caracteres")
                    if bool(t0.get("diarizado")):
                        meta += f" · diarizada ({t0.get('n_hablantes')} hablantes)"
                    elif bool(t0.get("requiere_diarizacion")):
                        meta += " · pendiente de diarizar (GPU)"
                    st.caption(meta)
                    st.text_area("Transcripción anonimizada", t0["transcript_anon"] or "",
                                 height=420)

st.divider()
st.caption("Fase 7 · UISRAEL · datos servidos desde PostgreSQL (batch + streaming). "
           "Origen del audio: lago MinIO (Bronce). Cumplimiento LOPDP: solo texto anonimizado.")
