# Imagen ligera de diagnóstico: JupyterLab + pandas + conector MySQL.
# Aislada del stack completo de Fase 0 (Kafka/Dagster/etc.) para cerrar la Fase 1
# sin depender de toda la infraestructura.
FROM python:3.11-slim

# Herramientas mínimas: build para ruedas nativas + openssh-client (scp del índice de audio)
RUN apt-get update \
    && apt-get install -y --no-install-recommends build-essential openssh-client \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

WORKDIR /work
EXPOSE 8888

# Token vacío: el contenedor solo escucha en la LAN local del desarrollador.
CMD ["jupyter", "lab", "--ip=0.0.0.0", "--port=8888", "--no-browser", \
     "--allow-root", "--ServerApp.token=", "--ServerApp.password=", \
     "--ServerApp.root_dir=/work"]
