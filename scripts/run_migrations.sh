#!/bin/bash

# Load database credentials securely
set -euo pipefail

# Load secrets safely
export POSTGRES_HOST="postgres"
export POSTGRES_PORT="5432"
export $(cat /run/secrets/postgres_config | xargs)

# Wait for PostgreSQL with credentials
echo "Waiting for PostgreSQL at ${POSTGRES_HOST}:${POSTGRES_PORT}..."
until pg_isready -h "$POSTGRES_HOST" -p "$POSTGRES_PORT" -U "$POSTGRES_USER"
do
    echo "PostgreSQL not ready. Retrying in 5 seconds..."
    sleep 5
done
echo "PostgreSQL connection established."

# Run migrations with explicit credentials
echo "Running database migrations..."
for migration in $(find /migrations -name "*.sql" | sort -V); do
    echo "Applying migration: ${migration}"
    psql -v ON_ERROR_STOP=1 \
         -U "$POSTGRES_USER" \
         -d "$POSTGRES_DB" \
         -h postgres \
         -p 5432 \
         -f "${migration}"
    if [ $? -ne 0 ]; then
        echo "Migration ${migration} failed!"
        exit 1
    fi
done