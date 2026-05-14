FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

WORKDIR /rsagent_db

RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    python3-dev \
    libc6-dev \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .

RUN pip install --no-cache-dir --root-user-action=ignore --upgrade pip setuptools wheel && \
    pip install --no-cache-dir --root-user-action=ignore -r requirements.txt

COPY utils/ /rsagent_db/utils/
COPY convertors/ /rsagent_db/convertors/
COPY rsagent_db.py .
COPY health_check.py .

HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD python health_check.py

CMD ["python", "rsagent_db.py"]