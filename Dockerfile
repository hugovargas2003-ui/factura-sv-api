# ─────────────────────────────────────────────────────────────
# FACTURA-SV API — Production Dockerfile
# ─────────────────────────────────────────────────────────────
# Build:  docker build -t factura-sv-api .
# Run:    docker run -p 8000:8000 -e MH_ENVIRONMENT=test factura-sv-api
# ─────────────────────────────────────────────────────────────

FROM python:3.11-slim

LABEL maintainer="Efficient AI Algorithms LLC <info@algoritmos.io>"
LABEL description="FACTURA-SV — DTE Electronic Invoicing API for El Salvador"

WORKDIR /app

# cryptography tiene wheels precompilados para python:3.11-slim,
# no se necesita gcc ni libffi-dev (ahorra ~150MB en imagen final).
COPY requirements.txt .
RUN pip install --no-cache-dir --only-binary :all: -r requirements.txt

# Copiar solo el código de la aplicación (no tests, no .env, no docs)
COPY ./app ./app

# Puerto por defecto — Railway inyecta PORT dinámicamente
ENV PORT=8000

EXPOSE ${PORT}

# Health check nativo de Docker (útil para Railway, ECS, K8s)
HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
  CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:${PORT}/health')" || exit 1

# IMPORTANTE: Usar shell form para que $PORT se expanda en runtime.
# Railway asigna PORT dinámicamente; no se puede hardcodear.
# --workers 1: obligatorio mientras las sesiones estén en memoria (no compartidas).
# --timeout-keep-alive 65: evita que Railway cierre conexiones antes que el proxy.
CMD sh -c "uvicorn app.main:app --host 0.0.0.0 --port ${PORT} --workers 1 --timeout-keep-alive 65"
# Sprint 1 - Fri Feb 20 19:17:24 CST 2026
