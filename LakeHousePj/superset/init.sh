#!/bin/bash
# Superset initialization script

echo "Waiting for PostgreSQL to be ready..."
while ! nc -z postgres 5432; do
  sleep 1
done

echo "PostgreSQL is ready!"
echo "Initializing Superset database..."

# Initialize Superset database
superset db upgrade

# Create default admin user
echo "Creating admin user..."
superset fab create-admin \
  --username admin \
  --firstname Admin \
  --lastname User \
  --email admin@lakehouse.local \
  --password admin || echo "Admin user might already exist"

# Load example data (optional)
echo "Loading examples..."
superset load_examples || true

# Initialize permissions
echo "Initializing permissions..."
superset init || true

echo "Superset initialization complete!"
