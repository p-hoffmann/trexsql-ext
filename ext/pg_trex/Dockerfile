FROM postgres:17

ARG TREXSQL_VERSION=v1.4.4-trex

# Download libtrexsql from GitHub release
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates wget unzip \
    && wget -O /tmp/libtrexsql.zip \
      https://github.com/p-hoffmann/trexsql-rs/releases/download/${TREXSQL_VERSION}/libtrexsql-linux-amd64.zip \
    && unzip /tmp/libtrexsql.zip -d /tmp/trexsql \
    && cp /tmp/trexsql/libtrexsql.so /usr/lib/postgresql/17/lib/ \
    && ln -sf /usr/lib/postgresql/17/lib/libtrexsql.so /usr/lib/postgresql/17/lib/libtrexsql.so.1.4 \
    && ln -sf /usr/lib/postgresql/17/lib/libtrexsql.so /usr/lib/postgresql/17/lib/libduckdb.so \
    && echo "/usr/lib/postgresql/17/lib" >> /etc/ld.so.conf.d/pg_trex.conf \
    && ldconfig \
    && rm -rf /tmp/libtrexsql.zip /tmp/trexsql \
    && apt-get purge -y wget unzip && apt-get autoremove -y && rm -rf /var/lib/apt/lists/*

# Install pg_trex extension
COPY target/debug/libpg_trex.so /usr/lib/postgresql/17/lib/pg_trex.so
COPY pg_trex.control /usr/share/postgresql/17/extension/
COPY sql/pg_trex--0.1.0.sql /usr/share/postgresql/17/extension/

# Copy trexsql extensions (db) for distributed query support
COPY extensions/ /usr/lib/trexsql/extensions/

# Pre-initialize the database to avoid init-then-restart cycle.
# The entrypoint detects an existing PGDATA and skips initdb+init scripts.
ENV POSTGRES_PASSWORD=test
ENV POSTGRES_HOST_AUTH_METHOD=trust
RUN mkdir -p /var/lib/postgresql/data \
    && chown postgres:postgres /var/lib/postgresql/data \
    && su postgres -c "initdb -D /var/lib/postgresql/data --auth=trust --no-sync" \
    && su postgres -c "pg_ctl -D /var/lib/postgresql/data -o '-c shared_preload_libraries=pg_trex -c pg_trex.swarm_extension_path=/usr/lib/trexsql/extensions/db.trex' -w start" \
    && su postgres -c "psql -c \"CREATE EXTENSION pg_trex;\"" \
    && echo "host all all 0.0.0.0/0 trust" >> /var/lib/postgresql/data/pg_hba.conf \
    && su postgres -c "pg_ctl -D /var/lib/postgresql/data -m immediate stop"

ENV PGDATA=/var/lib/postgresql/data
