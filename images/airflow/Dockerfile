FROM apache/airflow:3.1.7-python3.12

USER root

# Install system dependencies for psycopg2 and celery
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    libpq-dev \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Install uv via standalone installer (Airflow 3.x blocks root pip)
ADD https://astral.sh/uv/install.sh /tmp/uv-install.sh
RUN sh /tmp/uv-install.sh && rm /tmp/uv-install.sh
ENV PATH="/root/.local/bin:${PATH}"

# Install additional providers and celery (pinned to Airflow 3.1.7 constraints)
COPY pyproject.toml /tmp/pyproject.toml
RUN cd /tmp && uv pip install --system --no-cache .

# Copy entrypoint script and make executable
COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

USER airflow

ENTRYPOINT ["/entrypoint.sh"]
