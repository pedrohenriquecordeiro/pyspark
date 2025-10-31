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
    && pip install py4j==0.10.9.7 google-cloud-storage==3.1.0 delta-spark==3.3.1

# Install AWS CLI v2
RUN curl -O "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" \
    && unzip awscli-exe-linux-x86_64.zip \
    && ./aws/install \
    && rm -rf awscli-exe-linux-x86_64.zip aws

# Install Google Cloud SDK and initialize it in the current shell
# Install Google Cloud SDK properly
RUN apt-get update && apt-get install -y curl gnupg apt-transport-https ca-certificates && \
    echo "deb [signed-by=/usr/share/keyrings/cloud.google.gpg] http://packages.cloud.google.com/apt cloud-sdk main" \
      | tee -a /etc/apt/sources.list.d/google-cloud-sdk.list && \
    curl https://packages.cloud.google.com/apt/doc/apt-key.gpg \
      | apt-key --keyring /usr/share/keyrings/cloud.google.gpg add - && \
    apt-get update && apt-get install -y google-cloud-sdk


# JARs obrigatórios
RUN wget -P /opt/bitnami/spark/jars/ https://storage.googleapis.com/hadoop-lib/gcs/gcs-connector-hadoop3-latest.jar
RUN wget -P /opt/bitnami/spark/jars/ https://repo1.maven.org/maven2/io/delta/delta-spark_2.12/3.1.0/delta-spark_2.12-3.1.0.jar
RUN wget -P /opt/bitnami/spark/jars/ https://repo1.maven.org/maven2/io/delta/delta-storage/3.3.1/delta-storage-3.3.1.jar

# Set environment variables for CLI tools
ENV SPARK_HOME=/opt/bitnami/spark
ENV PATH=$SPARK_HOME/bin:$PATH
ENV PYTHONPATH="${PYTHONPATH}:/app"

# Copy necessary files
COPY main.py app/main.py
COPY .env app/.env
COPY entrypoint.sh app/entrypoint.sh

# Create secrets directory and copy the service account key
RUN mkdir -p /app/.secrets
COPY sa-data-stack-v3.json /app/.secrets/sa-data-stack-v3.json
ENV GOOGLE_APPLICATION_CREDENTIALS="/app/.secrets/sa-data-stack-v3.json"

# Set environment variables for CLI tools
RUN gcloud auth activate-service-account --key-file="/app/.secrets/sa-data-stack-v3.json" --project="dw-corp-dev"

# Set permissions for scripts
RUN chmod +x ./app/entrypoint.sh ./app/main.py

# Define entrypoint
ENTRYPOINT ["./app/entrypoint.sh"]

# Default command
CMD ["python3", "./app/main.py"]
