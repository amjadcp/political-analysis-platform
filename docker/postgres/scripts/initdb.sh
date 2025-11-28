#!/bin/bash
set -e

echo "Starting PostgreSQL in the background..."
docker-entrypoint.sh postgres &

# Wait for PostgreSQL to accept connections
until pg_isready -h localhost -p 5432 -U "$POSTGRES_USER"; do
  echo "Waiting for PostgreSQL..."
  sleep 2
done

echo "Running custom SQL scripts..."
psql -U "$POSTGRES_USER" -d postgres -f /scripts/initdb.sql

echo "Custom SQL completed."

# Bring Postgres to foreground
wait
