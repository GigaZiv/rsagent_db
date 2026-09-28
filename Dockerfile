FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

WORKDIR /rsagent_db

COPY requirements.txt .

RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    python3-dev \
    libc6-dev \
    # libpq5 может понадобиться, если с psycopg-binary на чистый psycopg
    && pip install --no-cache-dir --upgrade pip setuptools wheel \
    && pip install --no-cache-dir -r requirements.txt \
    && apt-get purge -y --auto-remove gcc python3-dev libc6-dev \
    && rm -rf /var/lib/apt/lists/*

RUN useradd -m -r agentuser && \
    mkdir -p /rsagent_db/logs && \
    chown -R agentuser:agentuser /rsagent_db

COPY --chown=agentuser:agentuser utils/ /rsagent_db/utils/
COPY --chown=agentuser:agentuser convertors/ /rsagent_db/convertors/
COPY --chown=agentuser:agentuser rsagent_db.py .
COPY --chown=agentuser:agentuser health_check.py .

USER agentuser

#HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
#    CMD python health_check.py

CMD ["python", "rsagent_db.py"]