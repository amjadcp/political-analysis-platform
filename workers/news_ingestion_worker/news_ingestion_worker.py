#!/usr/bin/env python3
"""
Deduplication worker:
- Consumes messages from Kafka topic `raw-articles`.
- Recomputes canonical content hash (sha256).
- Fast-check in Redis. If exists => skip.
- Authoritative insert into Postgres (unique constraint on content_hash).
  - If insert succeeds (new): persist JSON to MinIO (curated path) and set Redis key with TTL.
  - If insert conflicts (already present): set Redis key (so future fast checks succeed) and skip.
"""

import os
import json
import time
import logging
import hashlib
import re
import unicodedata
from datetime import datetime
from urllib.parse import urlparse

from kafka import KafkaConsumer
import redis
import psycopg2
from psycopg2.extras import Json
from minio import Minio
from minio.error import S3Error
from backoff import on_exception, expo
from typing import Optional
from io import BytesIO

# ---------------------------
# Logging
# ---------------------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("dedupe_worker")

# ---------------------------
# Config via env vars
# ---------------------------
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "raw-articles")

REDIS_URL = os.getenv("REDIS_URL", "redis://redis:6379/0")
REDIS_TTL_SECONDS = int(os.getenv("REDIS_TTL_SECONDS", 60 * 60 * 24 * 90))  # 90 days

PG_HOST = os.getenv("PG_HOST", "airflow_postgres")
PG_PORT = int(os.getenv("PG_PORT", 5432))
PG_DB = os.getenv("PG_DB", "articles")
PG_USER = os.getenv("PG_USER", "airflow")
PG_PASSWORD = os.getenv("PG_PASSWORD", "airflow")

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
MINIO_SECURE = os.getenv("MINIO_SECURE", "false").lower() in ("1", "true", "yes")
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "kerala-news")
MINIO_CURATED_PREFIX = os.getenv("MINIO_CURATED_PREFIX", "curated")

# Optional: set a prefix for keys to isolate environments
ENV_PREFIX = os.getenv("ENV_PREFIX", "")

# ---------------------------
# Helpers: canonicalize & hash
# ---------------------------
def canonicalize_text(text: str) -> str:
    """
    Basic canonicalization:
    - Ensure NFC unicode
    - Remove HTML tags (naive)
    - Collapse whitespace
    - Lowercase
    """
    if not text:
        return ""
    t = unicodedata.normalize("NFC", text)
    # Remove simple HTML tags if present (spiders usually already strip)
    t = re.sub(r"<[^>]+>", " ", t)
    # Replace multiple whitespace with single space
    t = re.sub(r"\s+", " ", t)
    t = t.strip()
    return t.lower()

def sha256_hash(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()

def safe_get(d: dict, *keys, default=""):
    for k in keys:
        if k in d and d[k]:
            return d[k]
    return default

def make_minio_path(source: str, published_at_str: Optional[str], content_hash: str) -> str:
    # Try parse published_at to date; fallback to today.
    try:
        if published_at_str:
            dt = datetime.fromisoformat(published_at_str.replace("Z", "+00:00"))
        else:
            dt = datetime.utcnow()
    except Exception:
        dt = datetime.utcnow()
    yyyy = dt.strftime("%Y")
    mm = dt.strftime("%m")
    dd = dt.strftime("%d")
    src = source.replace(" ", "_").lower()
    # path: curated/<source>/<YYYY>/<MM>/<DD>/<content_hash>.json
    prefix = f"{MINIO_CURATED_PREFIX}/{src}/{yyyy}/{mm}/{dd}"
    return f"{prefix}/{content_hash}.json"

def migrate_schema(pg_conn):
    """
    Ensure required tables and indexes exist.
    Safe to run on each worker startup.
    """
    logger.info("Running DB migration check...")

    create_table_sql = """
    CREATE TABLE IF NOT EXISTS article_fingerprints (
        id BIGSERIAL PRIMARY KEY,
        content_hash VARCHAR(128) NOT NULL UNIQUE,
        title_hash VARCHAR(128),
        url TEXT,
        source VARCHAR(255),
        first_seen TIMESTAMP WITH TIME ZONE DEFAULT now(),
        last_seen TIMESTAMP WITH TIME ZONE DEFAULT now(),
        meta JSONB
    );
    """

    create_index_sql = """
    CREATE INDEX IF NOT EXISTS idx_article_fingerprints_last_seen
    ON article_fingerprints (last_seen);
    """

    with pg_conn:
        with pg_conn.cursor() as cur:
            cur.execute(create_table_sql)
            cur.execute(create_index_sql)

    logger.info("DB migration complete.")

# ---------------------------
# Initialize clients
# ---------------------------
logger.info("Connecting to Redis...")
redis_client = redis.from_url(REDIS_URL, decode_responses=True)

logger.info("Connecting to Postgres...")
pg_conn = psycopg2.connect(
    host=PG_HOST, port=PG_PORT, dbname=PG_DB, user=PG_USER, password=PG_PASSWORD
)
pg_conn.autocommit = False  # transactions used

logger.info("Connecting to MinIO...")
minio_client = Minio(
    MINIO_ENDPOINT,
    access_key=MINIO_ACCESS_KEY,
    secret_key=MINIO_SECRET_KEY,
    secure=MINIO_SECURE,
)

# Ensure bucket exists
if not minio_client.bucket_exists(MINIO_BUCKET):
    logger.info("Creating MinIO bucket: %s", MINIO_BUCKET)
    minio_client.make_bucket(MINIO_BUCKET)

# ---------------------------
# Backoff helper for MinIO & Postgres
# ---------------------------

@on_exception(expo, (S3Error, Exception), max_tries=5)
def minio_put_bytes(object_name: str, data_bytes: bytes, content_type: str = "application/json"):
    data_stream = BytesIO(data_bytes)
    minio_client.put_object(
        MINIO_BUCKET,
        object_name,
        data=data_stream,
        length=len(data_bytes),
        content_type=content_type,
    )

@on_exception(expo, (psycopg2.DatabaseError, Exception), max_tries=5)
def pg_insert_fingerprint(tx_conn, content_hash: str, title_hash: str, url: str, source: str, meta_json: dict):
    """
    Try to insert into article_fingerprints.
    Uses INSERT ... ON CONFLICT DO NOTHING RETURNING id;
    If return row -> new insert; else conflict -> None
    """
    with tx_conn.cursor() as cur:
        sql = """
        INSERT INTO article_fingerprints (content_hash, title_hash, url, source, first_seen, last_seen, meta)
        VALUES (%s, %s, %s, %s, NOW(), NOW(), %s)
        ON CONFLICT (content_hash) DO UPDATE
          SET last_seen = NOW()
        RETURNING id;
        """
        cur.execute(sql, (content_hash, title_hash, url, source, Json(meta_json)))
        res = cur.fetchone()
        if res:
            return res[0]
        return None

# ---------------------------
# Kafka consumer setup
# ---------------------------
logger.info("Setting up Kafka consumer (%s)...", KAFKA_BOOTSTRAP)
consumer = KafkaConsumer(
    KAFKA_TOPIC,
    bootstrap_servers=[KAFKA_BOOTSTRAP],
    auto_offset_reset="latest",
    enable_auto_commit=True,
    value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    consumer_timeout_ms=1000,  # loop timeout for graceful checks
)

# ---------------------------
# Main processing loop
# ---------------------------
def process_message(msg_value: dict):
    """
    msg_value is the JSON scraped by Scrapy.
    Expected fields (best-effort): title, body, source, source_url, published_at, language, etc.
    """
    # Defensive: ensure we have a dict
    if not isinstance(msg_value, dict):
        logger.warning("Skipping non-dict message: %s", type(msg_value))
        return

    # Prefer body, fall back to summary or title
    raw_body = safe_get(msg_value, "body", "summary", "title", default="")
    canonical = canonicalize_text(raw_body)
    if not canonical:
        # Nothing to dedupe on; use title fallback
        canonical = canonicalize_text(safe_get(msg_value, "title", default=""))

    content_hash = sha256_hash(canonical)

    # Optionally compute title hash as well
    title_hash = sha256_hash(canonicalize_text(safe_get(msg_value, "title", default="")))

    redis_key = f"article:hash:{ENV_PREFIX}{content_hash}"

    # Fast check: Redis
    try:
        if redis_client.exists(redis_key):
            logger.debug("Duplicate found in Redis (fast path): %s", content_hash)
            return  # skip duplicate
    except Exception as e:
        logger.exception("Redis check failed: %s", e)
        # don't fail hard; continue to DB check

    # Re-attach/override content_hash into JSON for downstream systems
    msg_value["content_hash"] = content_hash
    msg_value["title_hash"] = title_hash
    msg_value["canonical_body"] = canonical  # optional store for traceability

    # Authoritative dedupe & insert into Postgres inside transaction
    try:
        new_id = None
        # Use a transaction block
        with pg_conn:
            new_id = pg_insert_fingerprint(pg_conn, content_hash, title_hash, msg_value.get("source", "unknown"), msg_value.get("source_url", None), {
                "maybe_title": msg_value.get("title"),
                "maybe_published_at": msg_value.get("published_at"),
            })
        if new_id:
            # This is a new article (or updated first_seen). Persist to MinIO curated layer.
            object_name = make_minio_path(msg_value.get("source", "unknown"), msg_value.get("published_at", None), content_hash)
            json_bytes = json.dumps(msg_value, ensure_ascii=False).encode("utf-8")
            minio_put_bytes(object_name, json_bytes)
            # Set Redis key to accelerate future checks (with TTL)
            try:
                redis_client.set(redis_key, "1", ex=REDIS_TTL_SECONDS)
            except Exception as e:
                logger.exception("Failed set Redis after successful DB insert: %s", e)
            logger.info("Persisted new article: %s (minio: %s)", content_hash, object_name)
        else:
            # ON CONFLICT path: already present in DB
            # Ensure Redis has key so fast path works next time
            try:
                redis_client.set(redis_key, "1", ex=REDIS_TTL_SECONDS)
            except Exception:
                logger.exception("Failed set Redis in conflict path")
            logger.info("Article already exists (dedup): %s", content_hash)
    except Exception as e:
        logger.exception("Failed to process message; will re-raise to allow reprocessing: %s", e)
        # Re-raise or return depending on desired behavior. We will raise to surface failure.
        raise

def main_loop():
    logger.info("Starting main loop; listening for messages on topic '%s'", KAFKA_TOPIC)
    try:
        while True:
            for message in consumer:
                try:
                    # message.value is already deserialized to dict
                    process_message(message.value)
                except Exception as e:
                    # If a message processing fails, log and continue (offset commit policy controls reprocess)
                    logger.exception("Error processing message: %s", e)
                    # Depending on semantics you may want to commit or not commit offset here.
                    # With enable_auto_commit=True and short consumer_timeout, failed message will be skipped.
                    # For stronger guarantees consider manual commit after success.
            # Sleep briefly between poll cycles to avoid tight loop when no messages
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info("Shutdown requested by KeyboardInterrupt")
    finally:
        consumer.close()
        pg_conn.close()

if __name__ == "__main__":
    migrate_schema(pg_conn)
    main_loop()
