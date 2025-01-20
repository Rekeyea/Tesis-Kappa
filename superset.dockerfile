FROM apache/superset:latest

USER root

# Install system dependencies
RUN apt-get update && \
    apt-get install -y \
    pkg-config \
    default-libmysqlclient-dev \
    build-essential \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Install Python packages
RUN pip install --no-cache-dir \
    psycopg2-binary \
    mysqlclient \
    pymysql \
    cryptography \
    apache-superset[mysql] \
    apache-superset[postgres]

USER superset