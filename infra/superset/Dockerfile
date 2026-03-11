FROM apache/superset:3.1.0

USER root

# Install PostgreSQL client for init script
RUN apt-get update && \
    apt-get install -y --no-install-recommends netcat-openbsd postgresql-client && \
    rm -rf /var/lib/apt/lists/*

# Install additional database drivers
RUN pip install psycopg2-binary

# Copy configuration
COPY --chmod=644 superset_config.py /app/superset_config.py
COPY --chmod=755 init-superset.sh /app/init-superset.sh

ENV SUPERSET_CONFIG_PATH=/app/superset_config.py

USER superset

ENTRYPOINT ["/bin/bash", "/app/init-superset.sh"]
