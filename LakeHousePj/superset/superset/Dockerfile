FROM apache/superset:4.0.2

USER root

# Update and install required packages
RUN apt-get update && apt-get install -y --no-install-recommends \
    netcat-traditional \
    curl && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

USER superset

# Install database drivers and utilities
RUN pip install --no-cache-dir \
    pyhive==0.6.5 \
    thrift==0.16.0 \
    sasl==0.3.1 \
    psycopg2-binary==2.9.9 \
    minio==7.2.0 \
    s3fs==2023.12.0 \
    sqlalchemy==1.4.53

# Copy configuration files
COPY superset_config.py /app/superset_config.py
COPY entrypoint.sh /app/entrypoint.sh

# Make entrypoint executable as root
USER root
RUN chmod +x /app/entrypoint.sh
USER superset

# Expose Superset port
EXPOSE 8088

# Health check
HEALTHCHECK --interval=30s --timeout=10s --retries=5 --start-period=60s \
    CMD curl -f http://localhost:8088/health || exit 1

# Run Superset with custom entrypoint
ENTRYPOINT ["/app/entrypoint.sh"]
