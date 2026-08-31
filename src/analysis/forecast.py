"""Pronóstico de KPIs operativos (Fase 7) sobre servido.kpis.

Modela la serie DIARIA de cada KPI y proyecta a un horizonte con banda de confianza.
Trabaja en diario (no semanal) porque ahí Prophet rinde mejor: capta estacionalidad
SEMANAL (día laboral vs fin de semana) y ANUAL, y detecta los quiebres de tendencia
(la caída nov-2025→abr-2026 y la recuperación posterior) con sus changepoints.
Para gerencia se agrega el resultado a MENSUAL (proyección del próximo mes/bimestre).

Compara 4 modelos con backtest (holdout final) y mide MAE/RMSE/MAPE/R²:
  - baseline      : naive estacional (media por día de semana de las últimas 4 sem).
  - holt_winters  : ETS con tendencia + estacionalidad semanal (s=7).
  - sarima        : SARIMAX(1,1,1)(1,0,1,7) — tendencia + estacionalidad semanal.
  - prophet       : tendencia lineal a trozos + estacionalidad semanal/anual + bandas.
Prophet es el modelo PRINCIPAL (interpretable, extrapola tendencia, bandas nativas);
los otros son la línea base de comparación (Fase 8). LSTM = modelo 5, trabajo futuro.

Persiste en:
  - servido.pronosticos          (diario:  kpi/modelo/tipo[historico|forecast]/fecha/y_real/y_pred/lo/hi)
  - servido.pronosticos_mensual  (mensual: kpi/modelo/tipo/mes/y_real/y_pred/lo/hi)   ← para el tablero
  - servido.pronostico_metricas  (backtest por modelo: mae/rmse/mape/r2/es_mejor)

No depende del backfill de ventas: la serie operativa (CDR) ya está completa.

Uso (en el contenedor dagster):
  python data/pronostico.py                       # todos los KPIs operativos
  KPI=contactabilidad python data/pronostico.py   # uno solo
o vía el asset Dagster `gold_pronosticos`.
"""
from __future__ import annotations

import os
import warnings

import numpy as np
import pandas as pd
from sqlalchemy import text

from src.processing.config import pg_engine

warnings.filterwarnings("ignore")

# KPIs operativos (columna en servido.kpis). "sum" = flujo (rellena 0 los huecos);
# "mean" = tasa/promedio (interpola). El rollup mensual usa la misma regla.
KPIS = {
    "n_llamadas": "sum",
    "n_contestadas": "sum",
    "n_largas": "sum",          # llamadas largas (>600s) = base para estimar ventas
    "contactabilidad": "mean",
    "dur_media": "mean",
}
HORIZON = int(os.environ.get("HORIZON", "120"))   # días a proyectar (~4 meses)
TEST = int(os.environ.get("TEST", "84"))           # días de holdout para backtest (12 sem)
SEASONAL = 7                                        # estacionalidad semanal


# ─────────────────────────── datos ───────────────────────────
def serie_diaria(kpi: str, how: str) -> pd.Series:
    """Serie diaria continua (reindexada al rango completo de fechas)."""
    eng = pg_engine()
    df = pd.read_sql(text(f"SELECT fecha, {kpi} AS y FROM servido.kpis ORDER BY fecha"), eng)
    eng.dispose()
    if df.empty:
        return pd.Series(dtype="float64")
    df["fecha"] = pd.to_datetime(df["fecha"])
    s = df.set_index("fecha")["y"].astype("float64")
    s = s.reindex(pd.date_range(s.index.min(), s.index.max(), freq="D"))
    s = s.fillna(0.0) if how == "sum" else s.interpolate("time").ffill().bfill()
    s.index.name = "fecha"
    return s


def _clip(a):
    return np.clip(np.asarray(a, float), 0, None)


def _metrics(real, pred) -> dict:
    real, pred = np.asarray(real, float), np.asarray(pred, float)
    err = real - pred
    mae = float(np.mean(np.abs(err)))
    rmse = float(np.sqrt(np.mean(err ** 2)))
    nz = real != 0
    mape = float(np.mean(np.abs(err[nz] / real[nz])) * 100) if nz.any() else float("nan")
    ss_res = float(np.sum(err ** 2))
    ss_tot = float(np.sum((real - np.mean(real)) ** 2)) or 1e-9
    return {"mae": mae, "rmse": rmse, "mape": mape, "r2": 1 - ss_res / ss_tot}


# ─────────────────────────── modelos ───────────────────────────
# Cada modelo: (fit sobre `train`) -> predicción de `n` pasos; y para el forecast final
# devuelve (yhat, lo, hi). Se implementan como funciones que reciben la serie completa.
def _baseline(train: pd.Series, n: int, index: pd.DatetimeIndex):
    """Naive estacional: media por día de semana de las últimas 4 semanas."""
    dow = train.iloc[-28:].groupby(train.iloc[-28:].index.dayofweek).mean()
    pred = np.array([dow.get(d.dayofweek, float(train.iloc[-28:].mean())) for d in index])
    resid = float(train.iloc[-28:].std())
    return _clip(pred), _clip(pred - 1.28 * resid), _clip(pred + 1.28 * resid)


def _holt_winters(train: pd.Series, n: int, index: pd.DatetimeIndex):
    from statsmodels.tsa.holtwinters import ExponentialSmoothing
    fit = ExponentialSmoothing(train, trend="add", seasonal="add",
                               seasonal_periods=SEASONAL).fit()
    pred = fit.forecast(n).values
    resid = float(np.std(fit.resid))
    return _clip(pred), _clip(pred - 1.28 * resid), _clip(pred + 1.28 * resid)


def _sarima(train: pd.Series, n: int, index: pd.DatetimeIndex):
    from statsmodels.tsa.statespace.sarimax import SARIMAX
    fit = SARIMAX(train, order=(1, 1, 1), seasonal_order=(1, 0, 1, SEASONAL),
                  enforce_stationarity=False, enforce_invertibility=False).fit(disp=False)
    fc = fit.get_forecast(n)
    ci = fc.conf_int(alpha=0.2).values
    return _clip(fc.predicted_mean.values), _clip(ci[:, 0]), _clip(ci[:, 1])


def _prophet(train: pd.Series, n: int, index: pd.DatetimeIndex):
    from prophet import Prophet
    dfp = pd.DataFrame({"ds": train.index, "y": train.values})
    m = Prophet(weekly_seasonality=True, yearly_seasonality=True, daily_seasonality=False,
                changepoint_prior_scale=0.1, interval_width=0.8)
    m.fit(dfp)
    fut = pd.DataFrame({"ds": index})
    fc = m.predict(fut)
    return _clip(fc["yhat"].values), _clip(fc["yhat_lower"].values), _clip(fc["yhat_upper"].values)


def modelos_disponibles() -> dict:
    """Modelos a comparar. Prophet solo si está instalado (post-rebuild de la imagen)."""
    mods = {"baseline": _baseline, "holt_winters": _holt_winters, "sarima": _sarima}
    try:
        import prophet  # noqa: F401
        mods["prophet"] = _prophet
    except Exception:  # noqa: BLE001
        print("  (prophet no instalado: se omite; rebuild la imagen para habilitarlo)")
    return mods


# ─────────────────────────── rollup mensual ───────────────────────────
def _mensual(fechas, valores, how: str) -> pd.Series:
    """Agrega una serie diaria a mensual (suma si flujo, media si tasa)."""
    s = pd.Series(np.asarray(valores, float), index=pd.to_datetime(fechas))
    return s.resample("MS").agg("sum" if how == "sum" else "mean")


# ─────────────────────────── DDL ───────────────────────────
DDL = [
    "CREATE SCHEMA IF NOT EXISTS servido",
    """CREATE TABLE IF NOT EXISTS servido.pronosticos (
        kpi text, modelo text, tipo text, fecha date,
        y_real real, y_pred real, lo real, hi real, run_ts timestamp DEFAULT now())""",
    """CREATE TABLE IF NOT EXISTS servido.pronosticos_mensual (
        kpi text, modelo text, tipo text, mes date,
        y_real real, y_pred real, lo real, hi real, run_ts timestamp DEFAULT now())""",
    """CREATE TABLE IF NOT EXISTS servido.pronostico_metricas (
        kpi text, modelo text, mae real, rmse real, mape real, r2 real,
        n_train integer, n_test integer, es_mejor boolean, run_ts timestamp DEFAULT now(),
        PRIMARY KEY (kpi, modelo))""",
]


def pronosticar_kpi(kpi: str, how: str, horizon: int = HORIZON, test: int = TEST) -> dict:
    s = serie_diaria(kpi, how)
    if s.empty or len(s) < test + SEASONAL * 4:
        return {"kpi": kpi, "estado": f"serie corta ({len(s)} días)"}

    mods = modelos_disponibles()
    train, testv = s.iloc[:-test], s.iloc[-test:]

    # 1) Backtest: cada modelo predice el tramo de prueba.
    met, mejor, mejor_rmse = [], None, 1e18
    bt_pred = {}
    for nombre, fn in mods.items():
        try:
            yhat, _, _ = fn(train, len(testv), testv.index)
        except Exception as e:  # noqa: BLE001
            print(f"  {kpi}/{nombre} backtest falló: {e}")
            continue
        bt_pred[nombre] = yhat
        mm = _metrics(testv.values, yhat)
        met.append({"kpi": kpi, "modelo": nombre, "n_train": int(len(train)),
                    "n_test": int(len(testv)), **mm})
        if mm["rmse"] < mejor_rmse:
            mejor, mejor_rmse = nombre, mm["rmse"]

    # 2) Forecast final: reentrena con TODA la serie y proyecta (mejor modelo + prophet).
    idx_fut = pd.date_range(s.index.max() + pd.Timedelta(days=1), periods=horizon, freq="D")
    a_guardar = {mejor}
    if "prophet" in mods:
        a_guardar.add("prophet")   # el principal se guarda siempre para el tablero

    fc_final = {}
    for nombre in a_guardar:
        try:
            fc_final[nombre] = mods[nombre](s, horizon, idx_fut)
        except Exception as e:  # noqa: BLE001
            print(f"  {kpi}/{nombre} forecast falló: {e}")

    # 3) Persistir.
    eng = pg_engine()
    with eng.begin() as c:
        for stmt in DDL:
            c.execute(text(stmt))
        c.execute(text("DELETE FROM servido.pronosticos WHERE kpi=:k"), {"k": kpi})
        c.execute(text("DELETE FROM servido.pronosticos_mensual WHERE kpi=:k"), {"k": kpi})
        c.execute(text("DELETE FROM servido.pronostico_metricas WHERE kpi=:k"), {"k": kpi})

        # 3a) Histórico diario observado (y_real) — una sola vez, modelo 'observado'.
        _ins_diario(c, kpi, "observado", "historico", s.index, s.values, None, None)
        m_hist = _mensual(s.index, s.values, how)
        _ins_mensual(c, kpi, "observado", "historico", m_hist.index, m_hist.values, None, None)

        # 3b) Forecast diario + rollup mensual de cada modelo guardado.
        for nombre, (yhat, lo, hi) in fc_final.items():
            _ins_diario(c, kpi, nombre, "forecast", idx_fut, yhat, lo, hi)
            my = _mensual(idx_fut, yhat, how)
            ml = _mensual(idx_fut, lo, how)
            mh = _mensual(idx_fut, hi, how)
            _ins_mensual(c, kpi, nombre, "forecast", my.index, my.values, ml.values, mh.values)

        # 3c) Métricas por modelo.
        for m in met:
            m["es_mejor"] = (m["modelo"] == mejor)
            c.execute(text("INSERT INTO servido.pronostico_metricas "
                           "(kpi,modelo,mae,rmse,mape,r2,n_train,n_test,es_mejor) VALUES "
                           "(:kpi,:modelo,:mae,:rmse,:mape,:r2,:n_train,:n_test,:es_mejor)"),
                      {k: (None if isinstance(v, float) and np.isnan(v) else v)
                       for k, v in m.items()})
    eng.dispose()

    return {"kpi": kpi, "dias": int(len(s)), "mejor": mejor,
            "metricas": {m["modelo"]: {"MAE": round(m["mae"], 1), "RMSE": round(m["rmse"], 1),
                                       "MAPE%": round(m["mape"], 1), "R2": round(m["r2"], 3)}
                         for m in met}}


def _ins_diario(c, kpi, modelo, tipo, fechas, y, lo, hi):
    lo = [None] * len(fechas) if lo is None else lo
    hi = [None] * len(fechas) if hi is None else hi
    ycol = "y_real" if tipo == "historico" else "y_pred"
    rows = [{"k": kpi, "m": modelo, "t": tipo, "f": pd.Timestamp(f).date(),
             "y": _n(v), "lo": _n(l), "hi": _n(h)} for f, v, l, h in zip(fechas, y, lo, hi)]
    c.execute(text(f"INSERT INTO servido.pronosticos (kpi,modelo,tipo,fecha,{ycol},lo,hi) "
                   "VALUES (:k,:m,:t,:f,:y,:lo,:hi)"), rows)


def _ins_mensual(c, kpi, modelo, tipo, meses, y, lo=None, hi=None):
    n = len(meses)
    lo = [None] * n if lo is None else (lo.values if hasattr(lo, "values") else lo)
    hi = [None] * n if hi is None else (hi.values if hasattr(hi, "values") else hi)
    y = y.values if hasattr(y, "values") else y
    ycol = "y_real" if tipo == "historico" else "y_pred"
    rows = [{"k": kpi, "m": modelo, "t": tipo, "mes": pd.Timestamp(mm).date(),
             "y": _n(v), "lo": _n(l), "hi": _n(h)} for mm, v, l, h in zip(meses, y, lo, hi)]
    c.execute(text(f"INSERT INTO servido.pronosticos_mensual (kpi,modelo,tipo,mes,{ycol},lo,hi) "
                   "VALUES (:k,:m,:t,:mes,:y,:lo,:hi)"), rows)


def _n(v):
    if v is None:
        return None
    v = float(v)
    return None if np.isnan(v) else v


def run(kpis: dict | None = None, horizon: int = HORIZON, test: int = TEST) -> dict:
    kpis = kpis or ({os.environ["KPI"]: KPIS.get(os.environ["KPI"], "sum")}
                    if os.environ.get("KPI") else KPIS)
    return {k: pronosticar_kpi(k, how, horizon, test) for k, how in kpis.items()}


if __name__ == "__main__":
    import json
    print(json.dumps(run(), indent=2, ensure_ascii=False))
