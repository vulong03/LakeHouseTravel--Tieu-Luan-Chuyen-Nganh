#!/bin/bash
set -e

echo "Starting Superset initialization..."

# Wait for PostgreSQL to be ready
echo "Waiting for PostgreSQL..."
while ! nc -z postgres 5432; do
  echo "PostgreSQL is unavailable - sleeping"
  sleep 1
done

echo "PostgreSQL is up!"

# Initialize Superset database
echo "Upgrading Superset database..."
superset db upgrade

# Create default admin user if not exists
echo "Creating admin user..."
superset fab create-admin \
  --username admin \
  --firstname Admin \
  --lastname User \
  --email admin@lakehouse.local \
  --password admin123 2>&1 || echo "Admin user already exists"

# Load examples
echo "Loading example data..."
superset load_examples 2>&1 || true

# Initialize app
echo "Initializing Superset..."
superset init 2>&1 || true

echo "✓ Superset initialization complete!"
echo "Starting Superset server..."

# Start Superset
exec gunicorn \
  --bind 0.0.0.0:8088 \
  --workers 4 \
  --worker-class gthread \
  --threads 2 \
  --timeout 60 \
  --access-logfile - \
  --error-logfile - \
  "superset.app:create_app()"
