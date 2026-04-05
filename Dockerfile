FROM python:3.12-slim

RUN groupadd -g 1000 appuser && useradd -u 1000 -g 1000 -r -d /app -s /sbin/nologin appuser
WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app/ ./app/
COPY openwebui/ ./openwebui/
COPY entrypoint.py .

RUN mkdir -p /app/cache && chown -R appuser:appuser /app/cache
USER appuser

EXPOSE 8093 8088

HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
    CMD python -c "import httpx; httpx.get('http://localhost:8093/healthz').raise_for_status()"

CMD ["python", "entrypoint.py"]
