"""Copia (SOLO LECTURA) la muestra de audios desde el servidor Asterisk (Fase 3).

Lee data/muestra/paths.txt (rutas exactas, generadas por sample_select) y baja
cada archivo por SFTP a data/muestra/audios/. NUNCA escribe ni borra en el
servidor; va por ruta EXACTA (sin escanear carpetas), así el impacto en Asterisk
es mínimo. Es idempotente: si el archivo ya está local, lo salta.

Uso (host, con paramiko + python-dotenv en el venv):
  whisper_worker/.venv/Scripts/python scripts/copiar_muestra.py
"""
import os
import sys
import time

from dotenv import load_dotenv

load_dotenv(".env")
HOST = os.environ["AST_SSH_HOST"]
USER = os.environ["AST_SSH_USER"]
PW = os.environ["AST_SSH_PASSWORD"]
PORT = int(os.getenv("AST_SSH_PORT", "22"))

PATHS = os.path.join("data", "muestra", "paths.txt")
DEST = os.path.join("data", "muestra", "audios")


def main() -> int:
    import paramiko

    os.makedirs(DEST, exist_ok=True)
    with open(PATHS, encoding="utf-8") as fh:
        rutas = [ln.strip() for ln in fh if ln.strip()]
    print(f"[copia] {len(rutas)} archivos a bajar desde {USER}@{HOST} (SOLO LECTURA)")

    cli = paramiko.SSHClient()
    cli.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    cli.connect(HOST, port=PORT, username=USER, password=PW, timeout=20)
    sftp = cli.open_sftp()

    ok = fail = skip = 0
    total = 0
    t0 = time.time()
    for i, r in enumerate(rutas, 1):
        dst = os.path.join(DEST, os.path.basename(r))
        if os.path.exists(dst):  # idempotente
            skip += 1
            continue
        try:
            sftp.get(r, dst)  # lectura remota, escritura LOCAL
            total += os.path.getsize(dst)
            ok += 1
        except Exception as e:  # noqa: BLE001
            fail += 1
            print(f"  [FALLO] {r} -> {e}")
        if i % 50 == 0:
            print(f"  {i}/{len(rutas)}  ok={ok} skip={skip} fail={fail}  {total/1e6:.1f} MB")

    sftp.close()
    cli.close()
    print(f"\n[copia] LISTO  ok={ok} skip={skip} fail={fail}  {total/1e6:.1f} MB "
          f"en {time.time()-t0:.1f}s")
    print(f"[copia] destino: {DEST}")
    return 0 if fail == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
