"""Localización de la grabación de una llamada del CDR — ESTRICTO SOLO LECTURA.

Dada una fila del CDR (ext del agente, teléfono del cliente = `dst`, `calldate`), se
construye el nombre de archivo `{YYYYMMDDHHmmss}-{ext}-{telefono}.{mp3|wav}` y se comprueba
su existencia con `sftp.stat` (una operación de metadatos, sin abrir ni escribir nada).

Se validó en vivo (2026-08-27) sobre 20 llamadas reales: 19/20 localizadas, todas con
desfase 0 s entre el `ts` del nombre y `calldate`. Se mantiene una ventana ±`tol` por
robustez. La única que falla suele ser una llamada de ~1 s sin grabación (o aún en .wav).
"""
import re
from datetime import timedelta

# Extensión 2xx del agente aislada en el string del canal (channel/dstchannel).
PAT_EXT = re.compile(r"(?<![0-9])(2[0-9][0-9])(?![0-9])")


def ext_agente(channel: str | None, dstchannel: str | None) -> str | None:
    """Extensión 2xx del agente (el `src` del CDR es el troncal, no el agente)."""
    for s in (channel or "", dstchannel or ""):
        m = PAT_EXT.search(s)
        if m:
            return m.group(1)
    return None


def _candidatos(root: str, ext: str, telefono: str, calldate, tol: int):
    root = root.rstrip("/")
    # offsets: 0, +1, -1, +2, -2, ... hasta ±tol (el 0 acierta ~95 %)
    offs = [0]
    for k in range(1, tol + 1):
        offs += [k, -k]
    for dz in offs:
        ts = (calldate + timedelta(seconds=dz)).strftime("%Y%m%d%H%M%S")
        for ext_file in ("mp3", "wav"):  # mp3 preferente; wav = conversión pendiente
            yield f"{root}/{ext}/OUT/{ts}-{ext}-{telefono}.{ext_file}", dz, ext_file


def localizar(sftp, root: str, ext: str, telefono: str, calldate, tol: int = 8):
    """Devuelve (ruta_remota, desfase_seg, formato, bytes) o None si no existe.

    Solo emite `sftp.stat` (lectura de metadatos). No abre ni transfiere el archivo.
    """
    for path, dz, fmt in _candidatos(root, ext, telefono, calldate, tol):
        try:
            st = sftp.stat(path)
            return path, dz, fmt, st.st_size
        except Exception:  # noqa: BLE001
            continue
    return None
