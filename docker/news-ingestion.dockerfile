FROM python:3.10-slim

WORKDIR /app

COPY /workers/news_ingestion_worker/requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

# Copy once to build image (good for dependencies)
COPY /workers/news_ingestion_worker /app/news_ingestion_worker
