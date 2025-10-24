# ============================================
# Airflow Custom Image
# Base: Apache Airflow 2.10.2 with Python 3.11
# ============================================

FROM apache/airflow:2.10.2-python3.11

# Switch to root to install system packages
USER root

# Install system dependencies
RUN apt-get update && apt-get install -y \
    # Docker CLI để chạy docker exec commands
    apt-transport-https \
    ca-certificates \
    curl \
    gnupg \
    lsb-release \
    && curl -fsSL https://download.docker.com/linux/debian/gpg | gpg --dearmor -o /usr/share/keyrings/docker-archive-keyring.gpg \
    && echo "deb [arch=amd64 signed-by=/usr/share/keyrings/docker-archive-keyring.gpg] https://download.docker.com/linux/debian $(lsb_release -cs) stable" | tee /etc/apt/sources.list.d/docker.list > /dev/null \
    && apt-get update \
    && apt-get install -y docker-ce-cli \
    # PostgreSQL client để query database
    postgresql-client \
    # Network tools
    netcat-traditional \
    telnet \
    # Cleanup
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Switch back to airflow user
USER airflow

# Copy requirements file
COPY requirements.txt /tmp/requirements.txt

# Install Python packages
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r /tmp/requirements.txt

# Set working directory
WORKDIR /opt/airflow

# Environment variables
ENV PYTHONPATH="/opt/airflow:${PYTHONPATH}"
ENV AIRFLOW_HOME=/opt/airflow

# Health check
HEALTHCHECK --interval=30s --timeout=10s --retries=3 \
    CMD curl --fail http://localhost:8080/health || exit 1

# Entrypoint and command will be specified in docker-compose.yml
