"""Tablero GERENCIAL — proyección del call center (Fase 7b).

Vista para gerencia, SIN métricas técnicas (nada de R²/RMSE/nombre de modelo). Responde
en lenguaje llano: para el próximo mes / bimestre / trimestre, ¿qué va a pasar con las
llamadas, la contactabilidad y las ventas? ¿sube o baja? Corre en el puerto 8502, aparte
del tablero técnico (8501).

Lee servido.pronosticos_mensual (pronóstico ya calculado por gold_pronosticos) y
servido.kpis (histórico + tasas de conversión). Las ventas se ESTIMAN con la tasa de
conversión actual × las llamadas largas proyectadas; el estimado se afina solo conforme
avanza el backfill de ventas.
"""
import os

import altair as alt
import pandas as pd
import streamlit as st
from sqlalchemy import create_engine, text

st.set_page_config(page_title="Proyección Gerencial — Call Center", page_icon="📊",
                   layout="wide")


@st.cache_resource
def get_engine():
    h = os.environ.get("POSTGRES_HOST", "localhost")
    p = os.environ.get("POSTGRES_PORT", "5432")
    return create_engine(f"postgresql+psycopg2://dagster:dagster@{h}:{p}/dagster")


@st.cache_data(ttl=60)
def q(sql: str, params: dict | None = None) -> pd.DataFrame:
    with get_engine().connect() as c:
        return pd.read_sql(text(sql), c, params=params or {})


def tabla_existe(nombre: str) -> bool:
    r = q("SELECT to_regclass(:n) AS t", {"n": f"servido.{nombre}"})
    return r.iloc[0]["t"] is not None


st.title("📊 Proyección del Call Center")
st.caption("Panel gerencial · qué se espera para el próximo periodo en llamadas, "
           "contactabilidad y ventas. Basado en el histórico del último año y medio.")

if not tabla_existe("pronosticos_mensual"):
    st.warning("Aún no hay proyecciones calculadas. Pídele al equipo técnico materializar "
               "el pronóstico (activo `gold_pronosticos`).")
    st.stop()

# ─────────────── selector de horizonte ───────────────
HOR = {"Próximo mes": 1, "Próximo bimestre": 2, "Próximo trimestre": 3}
c0, c1 = st.columns([2, 3])
with c0:
    horizonte = st.radio("¿Qué periodo quieres proyectar?", list(HOR.keys()), horizontal=True)
n = HOR[horizonte]


def mejor_modelo(kpi: str) -> str | None:
    r = q("SELECT modelo FROM servido.pronostico_metricas WHERE kpi=:k AND es_mejor LIMIT 1",
          {"k": kpi})
    return r.iloc[0]["modelo"] if len(r) else None


def serie_mensual(kpi: str):
    """Devuelve (historico, forecast) mensual del mejor modelo, sin el mes parcial de empalme."""
    hist = q("SELECT mes, y_real val FROM servido.pronosticos_mensual "
             "WHERE kpi=:k AND tipo='historico' ORDER BY mes", {"k": kpi})
    m = mejor_modelo(kpi)
    fc = q("SELECT mes, y_pred val, lo, hi FROM servido.pronosticos_mensual "
           "WHERE kpi=:k AND tipo='forecast' AND modelo=:m ORDER BY mes",
           {"k": kpi, "m": m}) if m else pd.DataFrame()
    # El mes que aparece en ambos (empalme) es parcial → se excluye de los dos lados.
    if len(hist) and len(fc):
        empalme = set(hist["mes"]) & set(fc["mes"])
        hist = hist[~hist["mes"].isin(empalme)]
        fc = fc[~fc["mes"].isin(empalme)]
    return hist.reset_index(drop=True), fc.reset_index(drop=True)


def agrega(df, col, how):
    vals = df[col].head(n) if how == "sum" else df[col].head(n)
    if not len(vals):
        return None
    return float(vals.sum()) if how == "sum" else float(vals.mean())


def periodo_reciente(hist, col, how):
    vals = hist[col].tail(n)
    if not len(vals):
        return None
    return float(vals.sum()) if how == "sum" else float(vals.mean())


def flecha(actual, proy, es_tasa=False):
    """(texto_cambio, emoji, color) — variación proyección vs periodo reciente."""
    if actual is None or proy is None or actual == 0:
        return "—", "", "off"
    if es_tasa:
        d = proy - actual
        txt = f"{d:+.1f} pts"
        pct = d
    else:
        pct = 100.0 * (proy - actual) / actual
        txt = f"{pct:+.0f}%"
    emoji = "▲" if pct > 3 else ("▼" if pct < -3 else "▬")
    return txt, emoji, ("normal" if pct > 3 else ("inverse" if pct < -3 else "off"))


# ─────────────── datos base ───────────────
h_ll, f_ll = serie_mensual("n_llamadas")
h_ct, f_ct = serie_mensual("contactabilidad")
h_lg, f_lg = serie_mensual("n_largas")

# Tasas de conversión actuales (ventas por llamada larga evaluada). Se afinan con el backfill.
conv = q("""SELECT sum(n_evaluadas) ev, sum(n_ventas_validas) val, sum(n_ventas_riesgo) rie,
                   count(*) FILTER (WHERE n_evaluadas>0) dias
            FROM servido.kpis""")
ev = float(conv.iloc[0]["ev"] or 0)
tasa_val = (conv.iloc[0]["val"] or 0) / ev if ev else 0
tasa_rie = (conv.iloc[0]["rie"] or 0) / ev if ev else 0
dias_datos = int(conv.iloc[0]["dias"] or 0)

# Proyección de llamadas largas para el horizonte → ventas estimadas.
largas_proy = agrega(f_lg, "val", "sum") if len(f_lg) else None
largas_rec = periodo_reciente(h_lg, "val", "sum") if len(h_lg) else None
ventas_val_proy = tasa_val * largas_proy if largas_proy else None
ventas_val_rec = tasa_val * largas_rec if largas_rec else None
ventas_rie_proy = tasa_rie * largas_proy if largas_proy else None
ventas_rie_rec = tasa_rie * largas_rec if largas_rec else None

# ─────────────── tarjetas ───────────────
st.subheader(f"Lo que se proyecta para el {horizonte.lower().replace('próximo ', '')}")
cols = st.columns(4)

# 1) Llamadas
ll_proy, ll_rec = agrega(f_ll, "val", "sum"), periodo_reciente(h_ll, "val", "sum")
txt, em, cl = flecha(ll_rec, ll_proy)
with cols[0]:
    st.metric(f"📞 Llamadas {em}", f"{ll_proy:,.0f}" if ll_proy else "—", txt, delta_color=cl)
    if ll_proy and ll_rec:
        st.caption("Sube el volumen esperado." if ll_proy > ll_rec * 1.03
                   else ("Baja el volumen (temporada más baja)." if ll_proy < ll_rec * 0.97
                         else "Volumen estable."))

# 2) Contactabilidad
ct_proy, ct_rec = agrega(f_ct, "val", "mean"), periodo_reciente(h_ct, "val", "mean")
txt, em, cl = flecha(ct_rec, ct_proy, es_tasa=True)
with cols[1]:
    st.metric(f"✅ Contactabilidad {em}", f"{ct_proy:.0f}%" if ct_proy else "—", txt, delta_color=cl)
    st.caption("Se mantiene estable." if ct_proy and abs((ct_proy or 0) - (ct_rec or 0)) < 2
               else "Cambio esperado en el nivel de contacto.")

# 3) Ventas bien realizadas
txt, em, cl = flecha(ventas_val_rec, ventas_val_proy)
with cols[2]:
    st.metric(f"💰 Ventas bien realizadas {em}",
              f"{ventas_val_proy:,.0f}" if ventas_val_proy else "—", txt, delta_color=cl)
    st.caption("Estimado según ventas por llamada del periodo con datos.")

# 4) Ventas con riesgo de reclamo
txt, em, cl = flecha(ventas_rie_rec, ventas_rie_proy)
with cols[3]:
    st.metric(f"⚠️ Ventas con riesgo {em}",
              f"{ventas_rie_proy:,.0f}" if ventas_rie_proy else "—", txt, delta_color="inverse")
    st.caption("Ventas que podrían generar reclamo (a vigilar).")

st.info(f"💡 Las cifras de **ventas** son una estimación preliminar basada en "
        f"**{int(ev):,} llamadas ya evaluadas** ({dias_datos} días con datos). "
        f"El estimado se vuelve más preciso automáticamente conforme avanza el procesamiento "
        f"del histórico de ventas.")

# ─────────────── gráfico de tendencia ───────────────
st.divider()
etiq = {"n_llamadas": "Llamadas", "contactabilidad": "Contactabilidad (%)",
        "n_largas": "Llamadas largas (oportunidades de venta)"}
kpi_g = st.selectbox("Ver la tendencia de:", list(etiq.keys()),
                     format_func=lambda k: etiq[k])
hg, fg = serie_mensual(kpi_g)
if len(hg):
    hg = hg.rename(columns={"val": "valor"}); hg["serie"] = "Real"
    capas = [alt.Chart(hg).mark_line(point=True, color="#2E86DE").encode(
        x=alt.X("mes:T", title="Mes"), y=alt.Y("valor:Q", title=etiq[kpi_g]))]
    if len(fg):
        fg = fg.rename(columns={"val": "valor"})
        banda = alt.Chart(fg).mark_area(opacity=0.18, color="#E67E22").encode(
            x="mes:T", y=alt.Y("lo:Q", title=""), y2="hi:Q")
        linea = alt.Chart(fg).mark_line(point=True, strokeDash=[6, 3], color="#E67E22").encode(
            x="mes:T", y="valor:Q")
        capas = [banda] + capas + [linea]
    st.altair_chart(alt.layer(*capas).properties(height=340), use_container_width=True)
    st.caption("Línea azul = lo ocurrido · línea naranja = proyección · "
               "franja = escenario optimista / pesimista.")

# ─────────────── tabla resumen ───────────────
st.divider()
st.subheader("Resumen del periodo proyectado")
resumen = pd.DataFrame({
    "Indicador": ["📞 Llamadas", "✅ Contactabilidad", "💰 Ventas bien realizadas",
                  "⚠️ Ventas con riesgo"],
    "Periodo actual": [f"{ll_rec:,.0f}" if ll_rec else "—",
                       f"{ct_rec:.0f}%" if ct_rec else "—",
                       f"{ventas_val_rec:,.0f}" if ventas_val_rec else "—",
                       f"{ventas_rie_rec:,.0f}" if ventas_rie_rec else "—"],
    "Proyección": [f"{ll_proy:,.0f}" if ll_proy else "—",
                   f"{ct_proy:.0f}%" if ct_proy else "—",
                   f"{ventas_val_proy:,.0f}" if ventas_val_proy else "—",
                   f"{ventas_rie_proy:,.0f}" if ventas_rie_proy else "—"],
})
st.dataframe(resumen, use_container_width=True, hide_index=True)

st.caption("Fase 7 · UISRAEL · proyección con base en el histórico del call center. "
           "Volúmenes: modelo de series de tiempo. Ventas: tasa de conversión actual × "
           "llamadas largas proyectadas.")
