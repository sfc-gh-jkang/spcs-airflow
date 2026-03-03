FROM redis:7.4-alpine

# Redis 7.4 - Broker for Airflow CeleryExecutor
# Alpine variant for minimal image size on SPCS

# Default redis config is sufficient for Airflow broker use.
# For production tuning, mount a custom redis.conf via SPCS volumes.

EXPOSE 6379

CMD ["redis-server", "--protected-mode", "no"]
