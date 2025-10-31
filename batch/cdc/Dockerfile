# Apache Spark 3.5.5, compilado com Scala 2.12.18 e rodando no OpenJDK 17.0.14.
FROM bitnami/spark:3.5.5

# Set user and working directory
USER root
WORKDIR /app

# Install dependencies
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    wget \
    tree \
    bash \
    curl \
    jq \
    python3 \
    python3-pip \
    unzip \
    ca-certificates \
    && pip install py4j==0.10.9.7 google-cloud-storage==3.1.0 delta-spark==3.3.0

# JARs obrigatórios
RUN wget -P /opt/bitnami/spark/jars/ https://storage.googleapis.com/hadoop-lib/gcs/gcs-connector-hadoop3-latest.jar
RUN wget -P /opt/bitnami/spark/jars/ https://repo1.maven.org/maven2/io/delta/delta-spark_2.12/3.1.0/delta-spark_2.12-3.1.0.jar
RUN wget -P /opt/bitnami/spark/jars/ https://repo1.maven.org/maven2/io/delta/delta-storage/3.3.1/delta-storage-3.3.1.jar

# Set environment variables for CLI tools
ENV SPARK_HOME=/opt/bitnami/spark
ENV PATH=$SPARK_HOME/bin:$PATH
ENV PYTHONPATH="${PYTHONPATH}:/app"

# Copy necessary files
COPY main.py main.py
COPY schemas.py schemas.py

# Create secrets directory and copy the service account key
# RUN mkdir -p .secrets
# COPY sa-data-stack-v3.json .secrets/sa-data-stack-v3.json
# ENV GOOGLE_APPLICATION_CREDENTIALS=".secrets/sa-data-stack-v3.json"

ENV CDC_TOPIC_PREFIX="onfly-app-prod.main_booking"

# RUN tree /

# Default command
CMD ["python3", "main.py"]
