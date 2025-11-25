FROM python:3.11-slim

# Evitar prompts
ENV DEBIAN_FRONTEND=noninteractive

# Carpeta de trabajo
WORKDIR /central

# Copiar app y requirements (ya dentro de central/)
COPY app ./app
COPY requirements.txt ./

# Instalar dependencias
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && pip install --no-cache-dir -r requirements.txt \
    && apt-get remove -y build-essential \
    && apt-get autoremove -y \
    && rm -rf /var/lib/apt/lists/*

# Variables de entorno
ENV PYTHONUNBUFFERED=1
ENV KAFKA_HOST=kafka
ENV KAFKA_PORT=9092
ENV CENTRAL_CP_PORT=5001
ENV CENTRAL_API_PORT=8000

# Ejecutar CENTRAL como módulo para que los imports relativos funcionen
CMD ["python", "-m", "app.central"]
