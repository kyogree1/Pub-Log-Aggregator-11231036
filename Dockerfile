FROM python:3.11-slim

RUN adduser --disabled-password --gecos '' appuser \
    && mkdir -p /app /data \
    && chown -R appuser:appuser /app /data

USER appuser
WORKDIR /app

COPY --chown=appuser:appuser requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY --chown=appuser:appuser src/ ./src/

ENV AGG_DB_PATH=/data/agg.db
EXPOSE 8080
CMD ["python", "-m", "src.main"]
