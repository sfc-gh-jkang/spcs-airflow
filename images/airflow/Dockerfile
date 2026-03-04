FROM apache/airflow:3.1.7-python3.12

USER root

# Install system dependencies for psycopg2 and celery
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    libpq-dev \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow

# Install uv for fast, deterministic dependency resolution
RUN pip install --no-cache-dir uv

# Install additional providers and celery (pinned to Airflow 3.1.7 constraints)
COPY pyproject.toml /tmp/pyproject.toml
RUN cd /tmp && uv pip install --system --no-cache .

# Copy entrypoint script and make executable
COPY entrypoint.sh /entrypoint.sh
USER root
RUN chmod +x /entrypoint.sh
USER airflow

ENTRYPOINT ["/entrypoint.sh"]
