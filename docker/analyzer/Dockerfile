FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends ffmpeg libsndfile1 \
    && rm -rf /var/lib/apt/lists/*

COPY docker/analyzer/requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

COPY scripts/analyze-track-features.py /app/scripts/analyze-track-features.py
COPY scripts/analyzer-sidecar.py /app/scripts/analyzer-sidecar.py

EXPOSE 8765

CMD ["python3", "/app/scripts/analyzer-sidecar.py"]
