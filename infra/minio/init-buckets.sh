#!/bin/sh
# =============================================================================
# MinIO bucket initialization script
# Creates the required buckets for the data pipeline
#
# Bucket structure uses path-based separation:
#   raw/citizens/YYYY-MM-DD.json  - Daily citizen data files
#   raw/logins/YYYY-MM-DD.json    - Daily login event files
# =============================================================================

# Configure mc alias for the local MinIO instance
mc alias set local http://minio:9000 minioadmin minioadmin

# Wait until MinIO is reachable
echo "Waiting for MinIO to be ready..."
until mc admin info local > /dev/null 2>&1; do
    echo "MinIO not ready yet, retrying in 2 seconds..."
    sleep 2
done
echo "MinIO is ready!"

# Create the raw bucket
echo "Creating bucket: raw"
mc mb local/raw --ignore-existing

echo "Bucket initialization complete!"
echo "  - raw: stores raw JSON files with path-based separation (citizens/, logins/)"

exit 0
