"""Verificación de conectividad (Fase 1 / Ruta C) — NO extrae datos.

Comprueba:
  1) MySQL CDR: autentica, versión del servidor y que la tabla exista.
  2) SSH al servidor de grabaciones: login + que AUDIO_ROOT sea legible
     (lista solo el primer nivel; NO recorre los ~100 GB).

Uso (contenedor desechable):
  docker run --rm \
    -v "$PWD/.env":/work/.env:ro -v "$PWD/scripts":/work/scripts:ro \
    -w /work python:3.11-slim \
    sh -c "pip install -q pymysql cryptography paramiko python-dotenv && \
           python scripts/verificar_conexion.py"
"""
from dotenv import dotenv_values

cfg = dotenv_values("/work/.env")  # parsea el archivo y recorta comentarios en línea

OK = "\033[92mOK\033[0m"
FAIL = "\033[91mFALLO\033[0m"


def check_mysql():
    import pymysql
    host = cfg["CDR_HOST"].strip()
    port = int(cfg.get("CDR_PORT", "3306"))
    db = cfg["CDR_DB"].strip()
    table = cfg["CDR_TABLE"].strip()
    user = cfg["CDR_USER"].strip()
    print(f"\n[MySQL] {user}@{host}:{port}/{db}  tabla={table}")
    try:
        conn = pymysql.connect(
            host=host, port=port, user=user,
            password=cfg["CDR_PASSWORD"], database=db,
            charset=cfg.get("CDR_CHARSET", "latin1"), connect_timeout=10,
        )
        with conn.cursor() as cur:
            cur.execute("SELECT VERSION()")
            ver = cur.fetchone()[0]
            cur.execute("SHOW TABLES LIKE %s", (table,))
            existe = cur.fetchone() is not None
        conn.close()
        print(f"  {OK}  Autenticación correcta. MySQL {ver}")
        print(f"  {'OK ' if existe else 'FALLO'}  Tabla `{table}` "
              f"{'encontrada' if existe else 'NO encontrada'}")
        return existe
    except Exception as e:
        print(f"  {FAIL}  {type(e).__name__}: {e}")
        return False


def check_ssh():
    import paramiko
    host = cfg["AST_SSH_HOST"].strip()
    user = cfg["AST_SSH_USER"].strip()
    root = cfg["AUDIO_ROOT"].strip()
    print(f"\n[SSH]  {user}@{host}  raíz de audio={root}")
    try:
        cli = paramiko.SSHClient()
        cli.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        cli.connect(host, username=user, password=cfg["AST_SSH_PASSWORD"],
                    timeout=10, look_for_keys=False, allow_agent=False)
        # Solo primer nivel: existencia, muestra de subcarpetas y conteo inmediato.
        cmd = (f"test -d '{root}' && echo EXISTS || echo MISSING; "
               f"ls -1 '{root}' 2>/dev/null | head -8; "
               f"echo '---'; ls -1 '{root}' 2>/dev/null | wc -l")
        _, out, _ = cli.exec_command(cmd, timeout=20)
        lines = out.read().decode("utf-8", "replace").splitlines()
        cli.close()
        estado = lines[0] if lines else "MISSING"
        print(f"  {OK}  Login SSH correcto.")
        if estado == "EXISTS":
            muestra = [l for l in lines[1:] if l != "---"][:-1]
            total = lines[-1]
            print(f"  {OK}  '{root}' existe y es legible. "
                  f"Subcarpetas/nivel-1: {total}")
            print(f"       Muestra: {muestra}")
            return True
        print(f"  {FAIL}  '{root}' no existe o no es legible.")
        return False
    except Exception as e:
        print(f"  {FAIL}  {type(e).__name__}: {e}")
        return False


if __name__ == "__main__":
    print("=" * 60)
    print("VERIFICACIÓN DE CONECTIVIDAD — no se extrae ni procesa data")
    print("=" * 60)
    m = check_mysql()
    s = check_ssh()
    print("\n" + "=" * 60)
    print(f"RESULTADO:  MySQL={'OK' if m else 'FALLO'}   "
          f"SSH/grabaciones={'OK' if s else 'FALLO'}")
    print("=" * 60)
