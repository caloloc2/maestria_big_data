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
# ## Audio — Caracterización del alcance (call center de ventas: ext. 200–299, OUT)
#
# Requiere `data/diag/audio_index.tsv.gz` (generado por `scripts/diagnostico_audio.sh`
# en el servidor Asterisk, SOLO LECTURA, y descargado con `scp`).
#
# **Alcance del proyecto (decisión del tesista):** solo las **llamadas salientes** de los
# agentes de ventas → carpetas `2xx/OUT` con `2xx` = extensión **200–299**. Se excluyen
# obligatoriamente la subcarpeta `INPUT` (entrantes → trabajo futuro), otros departamentos
# (300–399, 400–499) y carpetas de proyectos/pruebas/campañas (`PREDICTIVO`,
# `CLINICA_DE_VENTAS`, etc.), aunque contengan extensiones 2xx en el nombre.
# Nombre en ventas: `{YYYYMMDDHHmmss}-{extensión}-{teléfono_cliente}` (100 % del periodo).

# %%
import re

AUDIO_INDEX = Path(os.getenv("AUDIO_INDEX", "/work/data/diag/audio_index.tsv.gz"))
AUDIO_ROOT_REL = os.getenv("AUDIO_ROOT", "/home/grabacion/monitor/111111111111").rstrip("/") + "/"
RX_EXT = re.compile(r"^2\d\d$")      # carpeta L1 = extensión 200–299 EXACTA
RX_TS = re.compile(r"^(\d{14})")     # timestamp YYYYMMDDHHmmss al inicio del nombre


def scope_row(rel):
    """Devuelve (ext, telefono, ts, bytes_ok) si `rel` está en el alcance 2xx/OUT, si no None."""
    segs = rel.split("/")
    if len(segs) < 3 or not RX_EXT.match(segs[0]) or segs[1] != "OUT":
        return None
    base = re.sub(r"\.(mp3|wav)$", "", segs[-1], flags=re.I)
    if not RX_TS.match(base):
        return None
    flds = base.split("-")
    tel = "-".join(flds[2:]) if len(flds) >= 3 else ""
    return (segs[0], tel, base[:14])


if AUDIO_INDEX.exists():
    n_files = 0
    tot_bytes = 0
    ext_mp3 = 0
    ext_wav = 0
    by_year = {}
    by_year_bytes = {}
    by_ext = {}
    with gzip.open(AUDIO_INDEX, "rt", encoding="utf-8", errors="replace") as fh:
        for ln in fh:
            cols_ln = ln.rstrip("\n").split("\t")
            path = cols_ln[0]
            try:
                size = int(cols_ln[1])
            except (IndexError, ValueError):
                size = 0
            rel = path[len(AUDIO_ROOT_REL):] if path.startswith(AUDIO_ROOT_REL) else path
            r = scope_row(rel)
            if r is None:
                continue
            ext_dir, _tel, ts = r
            n_files += 1
            tot_bytes += size
            yr = ts[:4]
            by_year[yr] = by_year.get(yr, 0) + 1
            by_year_bytes[yr] = by_year_bytes.get(yr, 0) + size
            by_ext[ext_dir] = by_ext.get(ext_dir, 0) + 1
            if path.lower().endswith(".wav"):
                ext_wav += 1
            else:
                ext_mp3 += 1

    resumen_audio = pd.DataFrame({
        "métrica": [
            "Grabaciones (alcance 200–299 / OUT)", "  .mp3", "  .wav (sin convertir)",
            "Tamaño total (GB)", "Agentes (extensiones 2xx)",
        ],
        "valor": [
            n_files, ext_mp3, ext_wav,
            round(tot_bytes / 1024**3, 1), len(by_ext),
        ],
    })
    print(f"ALCANCE 200–299 / OUT: {n_files:,} grabaciones | "
          f"{tot_bytes/1024**3:.1f} GB | {len(by_ext)} agentes")
    display(resumen_audio)

    audio_anual = pd.DataFrame(sorted(by_year.items()), columns=["anio", "archivos"])
    audio_anual["GB"] = [round(by_year_bytes[y] / 1024**3, 1) for y in audio_anual["anio"]]
    display(audio_anual)

    fig, ax = plt.subplots(figsize=(10, 4))
    ax.bar(audio_anual["anio"], audio_anual["archivos"], color="#B279A2")
    ax.set_title("Grabaciones de ventas (200–299 / OUT) por año")
    ax.set_xlabel("Año"); ax.set_ylabel("N.º de grabaciones")
    plt.tight_layout(); plt.savefig(FIG_DIR / "audio_ventas_por_anio.png", dpi=150); plt.show()

    audio_top_ext = (pd.DataFrame(sorted(by_ext.items(), key=lambda kv: -kv[1]),
                                  columns=["extension", "grabaciones"]).head(20))
    display(audio_top_ext)
    resumen_audio.to_csv(TAB_DIR / "tabla_audio_resumen.csv", index=False)
    audio_disponible = True
else:
    print(f"Falta {AUDIO_INDEX}. Corre scripts/diagnostico_audio.sh en el servidor")
    print("y descárgalo:  scp USER@IP_ASTERISK:/tmp/diag/audio_index.tsv.gz ./data/diag/")
    audio_disponible = False

# %% [markdown]
# ## Cruce CDR ↔ grabación (ventas OUT) — tasa de emparejamiento
#
# **Método (record linkage, ref. Christen 2012):** llave compuesta con tolerancia temporal
#
# > audio(`ext`, `teléfono`, `ts`) ↔ CDR(`ext`∈`channel`/`dstchannel`, `dst`=`teléfono`,
# > `|calldate − ts| ≤ TOL`, vecino más cercano)
#
# Claves del hallazgo: el `src` del CDR es el caller-ID del **troncal** (no el agente);
# la extensión del agente vive en `channel`/`dstchannel` (`SIP/2xx-...`). El `uniqueid`
# **no** está embebido en las grabaciones de ventas. Umbral de gobernanza: cobertura ≥ 95 %.
# Parametrizado por rango (un mes representativo) para que sea tratable en la laptop.

# %%
FECHA_DESDE = "2025-05-01"
FECHA_HASTA = "2025-05-31"
TOL_SEG = 180


def cdr_ext(s):
    """Extrae la extensión 2xx del agente desde channel/dstchannel (p. ej. SIP/228-...)."""
    m = re.search(r"(?<!\d)(2\d\d)(?!\d)", str(s))
    return m.group(1) if m else None


if audio_disponible:
    d0, d1 = FECHA_DESDE.replace("-", ""), FECHA_HASTA.replace("-", "")
    filas = []
    with gzip.open(AUDIO_INDEX, "rt", encoding="utf-8", errors="replace") as fh:
        for ln in fh:
            path = ln.split("\t", 1)[0]
            rel = path[len(AUDIO_ROOT_REL):] if path.startswith(AUDIO_ROOT_REL) else path
            r = scope_row(rel)
            if r is None:
                continue
            ext_dir, tel, ts = r
            if d0 <= ts[:8] <= d1 and tel:
                filas.append((ts, ext_dir, tel))
    aud = pd.DataFrame(filas, columns=["ts", "ext", "phone"])
    aud["dt"] = pd.to_datetime(aud["ts"], format="%Y%m%d%H%M%S", errors="coerce")
    print(f"Grabaciones ventas OUT {FECHA_DESDE}..{FECHA_HASTA}: {len(aud):,}  "
          f"(agentes: {aud['ext'].nunique()}, contactos: {aud['phone'].nunique():,})")

    q_cdr = text(f"""
        SELECT calldate, dst, channel, dstchannel
        FROM {CDR_TABLE}
        WHERE calldate >= :d AND calldate < :h
    """)
    cdr = pd.read_sql(q_cdr, engine,
                      params={"d": FECHA_DESDE + " 00:00:00", "h": FECHA_HASTA + " 23:59:59"})
    cdr["ext_any"] = cdr["channel"].map(cdr_ext).fillna(cdr["dstchannel"].map(cdr_ext))
    cdr["dt"] = pd.to_datetime(cdr["calldate"], errors="coerce")

    # índice (ext, teléfono) -> lista de tiempos de llamada
    idx = {}
    par = cdr.dropna(subset=["ext_any"])
    for e, dd, t in zip(par["ext_any"], par["dst"].astype(str), par["dt"]):
        idx.setdefault((e, dd), []).append(t)

    def match_key(row):
        return (row["ext"], row["phone"]) in idx

    def match_win(row, tol=TOL_SEG):
        lst = idx.get((row["ext"], row["phone"]))
        return bool(lst) and (not pd.isna(row["dt"])) and \
            any(abs((row["dt"] - t).total_seconds()) <= tol for t in lst)

    m_key = aud.apply(match_key, axis=1)
    m_win = aud.apply(match_win, axis=1)
    cob_key = 100 * m_key.mean() if len(aud) else float("nan")
    cob_win = 100 * m_win.mean() if len(aud) else float("nan")

    cruce_resumen = pd.DataFrame({
        "métrica": [
            "Grabaciones en el rango",
            "Cobertura (ext + teléfono)",
            f"Cobertura (ext + teléfono + |t|≤{TOL_SEG}s)",
            "Huérfanas (sin CDR emparejado)",
        ],
        "valor": [
            f"{len(aud):,}",
            f"{cob_key:.2f} %",
            f"{cob_win:.2f} %",
            f"{(~m_win).sum():,}",
        ],
    })
    display(cruce_resumen)
    print(f"Umbral de gobernanza (cobertura ≥ 95 %): "
          f"{'CUMPLE' if cob_win >= 95 else 'NO CUMPLE'}")

    # KPI de intentos/reintentos por contacto (cada grabación = un intento del asesor)
    intentos = aud.groupby(["ext", "phone"]).size().rename("intentos").reset_index()
    dist_intentos = (intentos["intentos"].value_counts().sort_index()
                     .rename_axis("intentos_por_contacto").reset_index(name="n_contactos"))
    print(f"\nContactos únicos (ext, teléfono): {len(intentos):,} | "
          f"intentos medios/contacto: {intentos['intentos'].mean():.2f} | "
          f"máx: {intentos['intentos'].max()}")
    display(dist_intentos.head(12))
else:
    aud = None
    cruce_resumen = None
    print("Sin índice de audio: no se puede calcular el cruce todavía.")

# %% [markdown]
# ## Exportación de Tablas (capítulo académico)

# %%
with pd.ExcelWriter(TAB_DIR / "tablas_diagnostico.xlsx") as xl:
    piv_e1.to_excel(xl, sheet_name="E1_por_anio")
    e2.to_excel(xl, sheet_name="E2_corruptos", index=False)
    e5g.to_excel(xl, sheet_name="E5_global", index=False)
    e5a.to_excel(xl, sheet_name="E5_anual", index=False)
    top_src.to_excel(xl, sheet_name="E6_top_agentes", index=False)
    por_hora.to_excel(xl, sheet_name="E6_por_hora", index=False)
    por_dia.to_excel(xl, sheet_name="E6_por_dia", index=False)
    if audio_disponible:
        resumen_audio.to_excel(xl, sheet_name="Audio_resumen", index=False)
        audio_anual.to_excel(xl, sheet_name="Audio_por_anio", index=False)
        audio_top_ext.to_excel(xl, sheet_name="Audio_top_agentes", index=False)
    if cruce_resumen is not None:
        cruce_resumen.to_excel(xl, sheet_name="Cruce_cobertura", index=False)
print("Tablas exportadas a", TAB_DIR / "tablas_diagnostico.xlsx")
