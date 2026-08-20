# ---
# jupyter:
#   jupytext:
#     formats: py:percent
#     text_representation:
#       extension: .py
#       format_name: percent
#   kernelspec:
#     display_name: Python 3
#     language: python
#     name: python3
# ---

# %% [markdown]
# # 00 — Diagnóstico y comprensión de datos (Fase 1 / Ruta C)
#
# **Objetivo:** conectarse en vivo al MySQL CDR de Asterisk y al índice del
# filesystem de grabaciones para replicar las consultas E1–E6, generar figuras y
# exportar las **Tablas 1 y 2** del capítulo académico, cerrando la Fase 1 con
# evidencia reproducible.
#
# > Todo corre dentro del contenedor `uisrael_diagnostico`. Las credenciales se
# > leen de `.env` (nunca embebidas en el notebook). Acceso al CDR **solo lectura**.
#
# Para exportar a `.ipynb` (entregable del capítulo):
# ```bash
# jupytext --to notebook notebooks/00_diagnostico.py
# ```

# %%
import os
import gzip
import warnings
from pathlib import Path

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

warnings.filterwarnings("ignore")
sns.set_theme(style="whitegrid")
pd.set_option("display.float_format", lambda v: f"{v:,.2f}")

load_dotenv()  # lee /work/.env montado (o variables del env_file de compose)

OUT_DIR = Path(os.getenv("OUT_DIR", "/work/data/diag"))
FIG_DIR = OUT_DIR / "figuras"
TAB_DIR = OUT_DIR / "tablas"
for d in (OUT_DIR, FIG_DIR, TAB_DIR):
    d.mkdir(parents=True, exist_ok=True)

CDR_TABLE = os.getenv("CDR_TABLE", "cdr")
print("Salidas ->", OUT_DIR)


# %%
def get_engine():
    """Motor SQLAlchemy de SOLO LECTURA al CDR de Asterisk (latin1 -> UTF-8)."""
    user = os.environ["CDR_USER"]
    pwd = os.environ["CDR_PASSWORD"]
    host = os.environ["CDR_HOST"]
    port = os.getenv("CDR_PORT", "3306")
    db = os.environ["CDR_DB"]
    charset = os.getenv("CDR_CHARSET", "latin1")
    url = f"mysql+pymysql://{user}:{pwd}@{host}:{port}/{db}?charset={charset}"
    return create_engine(url, pool_pre_ping=True)


engine = get_engine()

# Sanidad: ¿conecta y cuántas columnas tiene el CDR?
with engine.connect() as c:
    cols = pd.read_sql(text(f"SHOW COLUMNS FROM {CDR_TABLE}"), c)
print(f"Conexión OK. Columnas en `{CDR_TABLE}`:")
cols

# %% [markdown]
# ## E1 — Volúmenes por año y `disposition`
# Confirma el universo real (~24,76 M CDR) y la distribución de resultados.

# %%
q_e1 = text(f"""
    SELECT YEAR(calldate) AS anio, disposition, COUNT(*) AS n
    FROM {CDR_TABLE}
    WHERE calldate <> '0000-00-00 00:00:00'
    GROUP BY YEAR(calldate), disposition
    ORDER BY anio, disposition
""")
e1 = pd.read_sql(q_e1, engine)
piv_e1 = e1.pivot_table(index="anio", columns="disposition", values="n",
                        aggfunc="sum", fill_value=0)
piv_e1["TOTAL"] = piv_e1.sum(axis=1)
piv_e1

# %%
ax = piv_e1.drop(columns="TOTAL").plot(kind="bar", stacked=True, figsize=(11, 5))
ax.set_title("CDR por año y disposition")
ax.set_xlabel("Año"); ax.set_ylabel("N.º de CDR")
plt.tight_layout(); plt.savefig(FIG_DIR / "e1_cdr_por_anio.png", dpi=150); plt.show()

# %% [markdown]
# ## E2 — Registros corruptos (`calldate = '0000-00-00'`)
# Esperado ≈ 2,25 %, coincidentes con `src`/`disposition` vacíos → descartar en Bronce.

# %%
q_e2 = text(f"""
    SELECT
      COUNT(*) AS total,
      SUM(calldate = '0000-00-00 00:00:00') AS corruptos,
      SUM(calldate = '0000-00-00 00:00:00' AND (src = '' OR src IS NULL)) AS corr_src_vacio,
      SUM(calldate = '0000-00-00 00:00:00' AND (disposition = '' OR disposition IS NULL)) AS corr_disp_vacio
    FROM {CDR_TABLE}
""")
e2 = pd.read_sql(q_e2, engine)
e2["pct_corruptos"] = 100 * e2["corruptos"] / e2["total"]
e2

# %% [markdown]
# ## E3 — Tasa de llenado del IVR (`pregunta1/2/3`)
# Esperado < 0,001 % → **descartado** como señal analítica. (Se salta si las columnas no existen.)

# %%
col_names = set(cols["Field"].str.lower())
if {"pregunta1", "pregunta2", "pregunta3"} & col_names:
    q_e3 = text(f"""
        SELECT
          COUNT(*) AS total,
          SUM(pregunta1 IS NOT NULL AND pregunta1 <> '') AS p1,
          SUM(pregunta2 IS NOT NULL AND pregunta2 <> '') AS p2,
          SUM(pregunta3 IS NOT NULL AND pregunta3 <> '') AS p3
        FROM {CDR_TABLE}
    """)
    e3 = pd.read_sql(q_e3, engine)
    display(e3)
else:
    print("Sin columnas pregunta1/2/3 en este esquema — nada que evaluar.")

# %% [markdown]
# ## E4 — Utilización de `urlrecord`
# Esperado ≈ 0,13 % → el enlace por reconstrucción del filesystem es el camino PRINCIPAL.

# %%
if "urlrecord" in col_names:
    q_e4 = text(f"""
        SELECT COUNT(*) AS total,
               SUM(urlrecord IS NOT NULL AND urlrecord <> '') AS con_url
        FROM {CDR_TABLE}
    """)
    e4 = pd.read_sql(q_e4, engine)
    e4["pct_con_url"] = 100 * e4["con_url"] / e4["total"]
    display(e4)
else:
    print("Sin columna urlrecord en este esquema.")

# %% [markdown]
# ## E5 — Contactabilidad, duración/billsec e interanual
# Contactabilidad global ≈ 53 %; caída interanual 2020→2024 (efecto TrueCaller);
# outliers de `duration` por `hangup` fallido (máx ≈ 86 días).

# %%
q_e5_global = text(f"""
    SELECT
      COUNT(*) AS validos,
      SUM(disposition = 'ANSWERED') AS answered,
      AVG(CASE WHEN disposition='ANSWERED' THEN duration END) AS dur_media,
      MAX(CASE WHEN disposition='ANSWERED' THEN duration END) AS dur_max,
      AVG(CASE WHEN disposition='ANSWERED' THEN billsec  END) AS billsec_media
    FROM {CDR_TABLE}
    WHERE calldate <> '0000-00-00 00:00:00'
""")
e5g = pd.read_sql(q_e5_global, engine)
e5g["contactabilidad_pct"] = 100 * e5g["answered"] / e5g["validos"]
e5g

# %%
q_e5_anual = text(f"""
    SELECT YEAR(calldate) AS anio,
           COUNT(*) AS validos,
           SUM(disposition='ANSWERED') AS answered
    FROM {CDR_TABLE}
    WHERE calldate <> '0000-00-00 00:00:00'
    GROUP BY YEAR(calldate) ORDER BY anio
""")
e5a = pd.read_sql(q_e5_anual, engine)
e5a["contactabilidad_pct"] = 100 * e5a["answered"] / e5a["validos"]

ax = sns.lineplot(data=e5a, x="anio", y="contactabilidad_pct", marker="o")
ax.set_title("Evolución interanual de la contactabilidad")
ax.set_xlabel("Año"); ax.set_ylabel("Contactabilidad (%)")
plt.tight_layout(); plt.savefig(FIG_DIR / "e5_contactabilidad_anual.png", dpi=150); plt.show()
e5a

# %% [markdown]
# ## E6 — Top 20 agentes (`src`), distribución por hora y por día de la semana

# %%
q_top = text(f"""
    SELECT src, COUNT(*) AS n
    FROM {CDR_TABLE}
    WHERE calldate <> '0000-00-00 00:00:00'
    GROUP BY src ORDER BY n DESC LIMIT 20
""")
top_src = pd.read_sql(q_top, engine)
top_src

# %%
q_hora = text(f"""
    SELECT HOUR(calldate) AS hora,
           COUNT(*) AS n,
           SUM(disposition='ANSWERED') AS answered
    FROM {CDR_TABLE}
    WHERE calldate <> '0000-00-00 00:00:00'
    GROUP BY HOUR(calldate) ORDER BY hora
""")
por_hora = pd.read_sql(q_hora, engine)

fig, ax = plt.subplots(figsize=(11, 4))
sns.barplot(data=por_hora, x="hora", y="n", ax=ax, color="#4C78A8")
ax.set_title("Volumen de llamadas por hora del día")
ax.set_xlabel("Hora"); ax.set_ylabel("N.º de CDR")
plt.tight_layout(); plt.savefig(FIG_DIR / "e6_por_hora.png", dpi=150); plt.show()

# %%
q_dow = text(f"""
    SELECT DAYOFWEEK(calldate) AS dow,
           COUNT(*) AS n,
           SUM(disposition='ANSWERED') AS answered
    FROM {CDR_TABLE}
    WHERE calldate <> '0000-00-00 00:00:00'
    GROUP BY DAYOFWEEK(calldate) ORDER BY dow
""")
por_dia = pd.read_sql(q_dow, engine)
dias = {1: "Dom", 2: "Lun", 3: "Mar", 4: "Mié", 5: "Jue", 6: "Vie", 7: "Sáb"}
por_dia["dia"] = por_dia["dow"].map(dias)

fig, ax = plt.subplots(figsize=(9, 4))
sns.barplot(data=por_dia, x="dia", y="n", ax=ax, color="#54A24B")
ax.set_title("Volumen de llamadas por día de la semana")
ax.set_xlabel(""); ax.set_ylabel("N.º de CDR")
plt.tight_layout(); plt.savefig(FIG_DIR / "e6_por_dia.png", dpi=150); plt.show()
por_dia

# %% [markdown]
# ## Audio — Caracterización del filesystem de grabaciones
#
# Requiere `data/diag/audio_index.tsv.gz` (generado por `scripts/diagnostico_audio.sh`
# en el servidor Asterisk y descargado con `scp`). Aquí lo parseamos: tamaño total,
# `.mp3` vs `.wav`, archivos "regados" (fuera de `{ext}/OUT/`) y años.

# %%
AUDIO_INDEX = Path(os.getenv("AUDIO_INDEX", "/work/data/diag/audio_index.tsv.gz"))

if AUDIO_INDEX.exists():
    audio = pd.read_csv(AUDIO_INDEX, sep="\t", names=["path", "bytes", "mtime"],
                        header=None)
    audio["ext"] = audio["path"].str.extract(r"\.(mp3|wav)$", expand=False).str.lower()
    audio["mtime"] = pd.to_datetime(audio["mtime"], unit="s", errors="coerce")
    audio["fname"] = audio["path"].str.rsplit("/", n=1).str[-1]
    # Patrón esperado: {YYYYMMDDHHmmss}-{ext}-{seq}
    parsed = audio["fname"].str.extract(r"(?P<ts>\d{14})-(?P<agente>\d+)-(?P<dst>\w+)")
    audio = pd.concat([audio, parsed], axis=1)
    audio["ts_parse"] = pd.to_datetime(audio["ts"], format="%Y%m%d%H%M%S", errors="coerce")
    audio["anio"] = audio["ts_parse"].dt.year.fillna(audio["mtime"].dt.year)
    audio["regado"] = ~audio["path"].str.contains("/OUT/", case=False, regex=False)

    resumen_audio = pd.DataFrame({
        "métrica": [
            "Archivos totales", "  .mp3", "  .wav (sin convertir)",
            "Tamaño total (GB)", "Regados (fuera de {ext}/OUT/)",
            "Nombre no parseable",
        ],
        "valor": [
            len(audio),
            int((audio["ext"] == "mp3").sum()),
            int((audio["ext"] == "wav").sum()),
            round(audio["bytes"].sum() / 1024**3, 2),
            int(audio["regado"].sum()),
            int(audio["ts_parse"].isna().sum()),
        ],
    })
    display(resumen_audio)

    por_anio_audio = audio.groupby("anio").size().rename("archivos").reset_index()
    display(por_anio_audio)
    resumen_audio.to_csv(TAB_DIR / "tabla1b_audio_resumen.csv", index=False)
else:
    print(f"Falta {AUDIO_INDEX}. Corre scripts/diagnostico_audio.sh en el servidor")
    print("y descárgalo:  scp USER@IP_ASTERISK:/tmp/diag/audio_index.tsv.gz ./data/diag/")
    audio = None

# %% [markdown]
# ## Cruce CDR ↔ grabación — tasa de emparejamiento
#
# Umbral de gobernanza: **cobertura ≥ 95 %** en la muestra analítica y
# **precisión de linkage ≥ 0,98** (verificación a oído aparte).
# Parametrizado por rango de fechas para mantenerlo tratable en la laptop.

# %%
FECHA_DESDE = "2024-01-01"
FECHA_HASTA = "2024-12-31"

if audio is not None:
    q_muestra = text(f"""
        SELECT uniqueid, calldate, src, dst, duration, billsec, disposition
        FROM {CDR_TABLE}
        WHERE calldate BETWEEN :d AND :h
          AND disposition = 'ANSWERED'
          AND billsec BETWEEN 10 AND 3600
    """)
    cdr = pd.read_sql(q_muestra, engine, params={"d": FECHA_DESDE, "h": FECHA_HASTA})
    cdr["key"] = (pd.to_datetime(cdr["calldate"]).dt.strftime("%Y%m%d%H%M%S")
                  + "-" + cdr["src"].astype(str))

    aud = audio.dropna(subset=["ts", "agente"]).copy()
    aud["key"] = aud["ts"] + "-" + aud["agente"].astype(str)

    matched = cdr["key"].isin(set(aud["key"]))
    cobertura = 100 * matched.mean() if len(cdr) else float("nan")
    print(f"Muestra CDR {FECHA_DESDE}..{FECHA_HASTA} (ANSWERED, billsec 10–3600): {len(cdr):,}")
    print(f"Emparejados por (timestamp exacto + agente): {matched.sum():,}  "
          f"=> cobertura {cobertura:.2f} %")
    print("Nota: emparejar por segundo exacto es estricto; si la cobertura es baja,")
    print("probar tolerancia ±1–2 s o join por minuto — se ajusta aquí.")
else:
    print("Sin índice de audio: no se puede calcular el cruce todavía.")

# %% [markdown]
# ## Exportación de Tablas 1 y 2 (capítulo académico)

# %%
with pd.ExcelWriter(TAB_DIR / "tablas_diagnostico.xlsx") as xl:
    piv_e1.to_excel(xl, sheet_name="E1_por_anio")
    e2.to_excel(xl, sheet_name="E2_corruptos", index=False)
    e5g.to_excel(xl, sheet_name="E5_global", index=False)
    e5a.to_excel(xl, sheet_name="E5_anual", index=False)
    top_src.to_excel(xl, sheet_name="E6_top_agentes", index=False)
    por_hora.to_excel(xl, sheet_name="E6_por_hora", index=False)
    por_dia.to_excel(xl, sheet_name="E6_por_dia", index=False)
print("Tablas exportadas a", TAB_DIR / "tablas_diagnostico.xlsx")
