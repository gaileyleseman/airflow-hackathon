#!/bin/bash
set -e

echo "=== Waiting for PostgreSQL ==="
while ! nc -z postgres 5432; do
    echo "Waiting for postgres..."
    sleep 2
done

echo "=== Creating superset database if not exists ==="
PGPASSWORD=$POSTGRES_PASSWORD psql -h postgres -U $POSTGRES_USER -d postgres -tc \
    "SELECT 1 FROM pg_database WHERE datname = 'superset'" | grep -q 1 || \
    PGPASSWORD=$POSTGRES_PASSWORD psql -h postgres -U $POSTGRES_USER -d postgres -c \
    "CREATE DATABASE superset OWNER $POSTGRES_USER"

echo "=== Running Superset DB upgrade ==="
superset db upgrade

echo "=== Creating admin user ==="
superset fab create-admin \
    --username admin \
    --firstname Admin \
    --lastname User \
    --email admin@superset.local \
    --password admin || true

echo "=== Initializing Superset ==="
superset init

echo "=== Adding hackathon database connection ==="
superset set-database-uri -d hackathon -u "postgresql://${POSTGRES_USER}:${POSTGRES_PASSWORD}@${POSTGRES_HOST}:${POSTGRES_PORT}/${POSTGRES_DB}" || true

echo "=== Starting Superset ==="
superset run -h 0.0.0.0 -p 8088 --with-threads --reload
