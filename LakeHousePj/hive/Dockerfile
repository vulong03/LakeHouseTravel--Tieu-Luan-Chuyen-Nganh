FROM apache/hive:4.0.0

# Switch to root for installations
USER root

# Install necessary tools and PostgreSQL JDBC driver
RUN apt-get update && \
    apt-get install -y wget netcat-openbsd curl && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Download PostgreSQL JDBC driver
RUN wget -P /opt/hive/lib/ \
    https://jdbc.postgresql.org/download/postgresql-42.7.1.jar

# Download AWS SDK for S3/MinIO support
RUN wget -P /opt/hive/lib/ \
    https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar && \
    wget -P /opt/hive/lib/ \
    https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.367/aws-java-sdk-bundle-1.12.367.jar

# Copy custom Hive configuration
COPY init/hive-site.xml /opt/hive/conf/hive-site.xml

# Create necessary directories
RUN mkdir -p /opt/hive/data && \
    chown -R hive:hive /opt/hive/data

# Switch back to hive user
USER hive

# Expose Metastore port
EXPOSE 9083
EXPOSE 10000

# Healthcheck - check both Metastore (9083) and HiveServer2 (10000) ports
HEALTHCHECK --interval=30s --timeout=10s --retries=5 \
    CMD nc -z localhost 9083 && nc -z localhost 10000 || exit 1
# Default entrypoint is inherited from base image
