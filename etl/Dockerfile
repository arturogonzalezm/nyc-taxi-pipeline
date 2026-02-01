FROM python:3.12-slim

# Install Java (required for PySpark)
RUN apt-get update && apt-get install -y --no-install-recommends \
    openjdk-21-jre-headless \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Set Java environment
ENV JAVA_HOME=/usr/lib/jvm/java-21-openjdk-arm64
ENV PATH="${JAVA_HOME}/bin:${PATH}"

# Set working directory
WORKDIR /app

# Install Python dependencies first (cached layer)
COPY pyproject.toml .
RUN pip install --no-cache-dir pyspark==4.1.1 delta-spark==4.0.1 psycopg2-binary>=2.9.9 requests minio python-dateutil

# Copy project files
COPY etl/ ./etl/
COPY .hadoop-conf/ ./.hadoop-conf/

# Install package in editable mode (fast, deps already installed)
RUN pip install --no-cache-dir --no-deps -e .

# Set Python path
ENV PYTHONPATH=/app

# Keep container running for development
CMD ["tail", "-f", "/dev/null"]
