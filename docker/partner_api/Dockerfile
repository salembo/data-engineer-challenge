FROM python:3.11.18-slim-trixie

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /app

COPY docker/partner_api/requirements.txt .
RUN pip install -r requirements.txt \
    && useradd --system --uid 10001 --no-create-home --shell /usr/sbin/nologin api

# The legacy gateway is used as provided, without modification.
COPY legacy_sources/partner_api/main.py .

USER api

EXPOSE 8000

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000", "--workers", "1"]
