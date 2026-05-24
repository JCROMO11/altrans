FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /app

# Dependencias de sistema mínimas (httpx + supabase no requieren psycopg2 nativo)
RUN apt-get update && apt-get install -y --no-install-recommends \
        curl ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install -r requirements.txt

# Copiamos todo el módulo
COPY . .

# Usuario no-root
RUN useradd -m -u 1000 altrans && chown -R altrans:altrans /app
USER altrans

ENV PORT=8080
EXPOSE 8080

# Healthcheck simple (Railway lo usa para deploy)
HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
    CMD curl -fsS http://localhost:${PORT}/health || exit 1

# uvicorn con 2 workers (Railway Hobby = 0.5 vCPU; subir si se actualiza el plan)
# Cada worker corre el agent independientemente — sesiones se sincronizan vía Supabase.
CMD ["sh", "-c", "uvicorn main:app --host 0.0.0.0 --port ${PORT} --workers 2"]
