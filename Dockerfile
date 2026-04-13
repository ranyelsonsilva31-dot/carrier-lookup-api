# ── Build stage ───────────────────────────────────────────────────────────────
FROM python:3.12-slim AS builder

WORKDIR /build
COPY requirements.txt .

RUN pip install --no-cache-dir --prefix=/install -r requirements.txt


# ── Runtime stage ──────────────────────────────────────────────────────────────
FROM python:3.12-slim

LABEL maintainer="carrier-lookup"
LABEL description="API de consulta de operadora em massa via ABR Telecom"

# Cria usuário não-root por segurança
RUN addgroup --system appgroup && adduser --system --ingroup appgroup appuser

WORKDIR /app

# Copia dependências já instaladas
COPY --from=builder /install /usr/local

# Copia código da aplicação
COPY app/ ./

# Diretórios de dados (mapeados via volume no docker-compose)
RUN mkdir -p /tmp/carrier_checkpoints /tmp/carrier_outputs \
    && chown -R appuser:appgroup /tmp/carrier_checkpoints /tmp/carrier_outputs /app

USER appuser

# Variáveis de ambiente com defaults razoáveis
ENV MAX_CONCURRENT=50 \
    BATCH_SIZE=500 \
    CHECKPOINT_DIR=/tmp/carrier_checkpoints \
    OUTPUT_DIR=/tmp/carrier_outputs \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

EXPOSE 8000

# Healthcheck nativo do Docker
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:8000/health')"

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000", "--workers", "2", "--loop", "uvloop"]
