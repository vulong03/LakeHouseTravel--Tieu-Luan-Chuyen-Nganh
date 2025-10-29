FROM apache/spark:3.5.0-python3

USER root

# Install dependencies
RUN apt-get update && \
    apt-get install -y curl wget netcat-traditional vim && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Iceberg and AWS SDK versions
ENV ICEBERG_VERSION=1.4.3
ENV SPARK_VERSION=3.5
ENV AWS_SDK_VERSION=1.12.367
ENV HADOOP_AWS_VERSION=3.3.4
ENV POSTGRESQL_JDBC_VERSION=42.7.2

# Download Iceberg Spark Runtime JAR
RUN curl -L https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-${SPARK_VERSION}_2.12/${ICEBERG_VERSION}/iceberg-spark-runtime-${SPARK_VERSION}_2.12-${ICEBERG_VERSION}.jar \
    -o /opt/spark/jars/iceberg-spark-runtime-${SPARK_VERSION}_2.12-${ICEBERG_VERSION}.jar

# Download AWS SDK Bundle for S3 support (v1 - required by Hadoop)
RUN curl -L https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/${AWS_SDK_VERSION}/aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar \
    -o /opt/spark/jars/aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar

# Download Hadoop AWS for S3A filesystem
RUN curl -L https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/${HADOOP_AWS_VERSION}/hadoop-aws-${HADOOP_AWS_VERSION}.jar \
    -o /opt/spark/jars/hadoop-aws-${HADOOP_AWS_VERSION}.jar

# Download PostgreSQL JDBC Driver
RUN curl -L https://repo1.maven.org/maven2/org/postgresql/postgresql/${POSTGRESQL_JDBC_VERSION}/postgresql-${POSTGRESQL_JDBC_VERSION}.jar \
    -o /opt/spark/jars/postgresql-${POSTGRESQL_JDBC_VERSION}.jar

# Install Python dependencies (psycopg2 for direct PostgreSQL access)
RUN pip install --no-cache-dir psycopg2-binary

# Install MinIO client for UTF-8 file uploads
RUN wget -q https://dl.min.io/client/mc/release/linux-amd64/mc -O /usr/local/bin/mc && \
    chmod +x /usr/local/bin/mc

# Create directories
RUN mkdir -p /opt/spark/jobs /opt/spark/logs /opt/spark/work-dir && \
    chown -R spark:spark /opt/spark/jobs /opt/spark/logs /opt/spark/work-dir

# Copy Spark configuration
COPY spark-defaults.conf /opt/spark/conf/spark-defaults.conf

# Copy job scripts
COPY jobs/ /opt/spark/jobs/

# Set permissions
RUN chown -R spark:spark /opt/spark/conf /opt/spark/jobs

# Switch to spark user
USER spark

WORKDIR /opt/spark/work-dir

# Expose Spark UI and other ports
EXPOSE 4040 7077 8080 8081

# Default command
CMD ["/opt/spark/bin/spark-submit", "--help"]
