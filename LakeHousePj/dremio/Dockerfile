FROM dremio/dremio-oss:latest

# Switch to root for installations
USER root

# Install necessary tools (Dremio base image uses CentOS/RHEL with yum)
# Note: curl and netcat may already be included in base image
RUN if command -v yum > /dev/null; then \
        yum update -y && \
        yum install -y curl net-tools nc openssl && \
        yum clean all && \
        rm -rf /var/cache/yum; \
    elif command -v apt-get > /dev/null; then \
        apt-get update && \
        apt-get install -y curl netcat-openbsd openssl && \
        apt-get clean && \
        rm -rf /var/lib/apt/lists/*; \
    fi

# Create necessary directories
RUN mkdir -p /opt/dremio/data && \
    mkdir -p /opt/dremio/conf && \
    mkdir -p /opt/dremio/logs && \
    chown -R dremio:dremio /opt/dremio/data && \
    chown -R dremio:dremio /opt/dremio/conf && \
    chown -R dremio:dremio /opt/dremio/logs

# Copy custom Dremio configuration (if exists)
COPY conf/dremio.conf /opt/dremio/conf/dremio.conf

# Switch back to dremio user
USER dremio

# Expose ports
# 9047: Web UI
# 31010: Client port (ODBC/JDBC)
# 45678: Coordinator port
EXPOSE 9047
EXPOSE 31010
EXPOSE 45678

# Healthcheck
HEALTHCHECK --interval=30s --timeout=10s --retries=5 --start-period=60s \
    CMD curl -f http://localhost:9047/apiv2/server_status || exit 1

# Default entrypoint is inherited from base image
# Dremio will start automatically when container starts

