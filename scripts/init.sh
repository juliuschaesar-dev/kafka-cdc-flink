#!/usr/bin/env bash
# Fills ${VAR} in each mounted config and applies it. Idempotent, safe to re-run.
set -euo pipefail

CLICKHOUSE="http://clickhouse:${CLICKHOUSE_HTTP_PORT}"
CONNECT="http://connect:${CONNECT_PORT}"
FLINK="http://flink-jobmanager:${FLINK_UI_PORT}"

wait_for() {
  echo "waiting for $1..."
  until curl -sf -o /dev/null "$2"; do sleep 2; done
}

# --- ClickHouse tables (DDL is IF NOT EXISTS) ---------------------------------
wait_for clickhouse "${CLICKHOUSE}/ping"

# Statements below target ?database=${CLICKHOUSE_DB}, which requires it to
# already exist. Create it first, against the root endpoint.
curl -sf -u "${CLICKHOUSE_USER}:${CLICKHOUSE_PASSWORD}" \
  "${CLICKHOUSE}/" --data-binary "CREATE DATABASE IF NOT EXISTS ${CLICKHOUSE_DB}" > /dev/null

# ClickHouse HTTP takes one statement per request, so split the file on ';'.
# Comments are stripped first so a ';' in prose can't split one apart.
statements=$(mktemp -d)
envsubst < /opt/init/clickhouse.sql | sed 's/^[[:space:]]*--.*$//' | awk -v d="$statements" '
  BEGIN { RS = ";" }
  { gsub(/^[[:space:]]+|[[:space:]]+$/, ""); if ($0 != "") printf "%s", $0 > sprintf("%s/%02d.sql", d, ++n) }'

for f in "$statements"/*.sql; do
  curl -sf -u "${CLICKHOUSE_USER}:${CLICKHOUSE_PASSWORD}" \
    "${CLICKHOUSE}/?database=${CLICKHOUSE_DB}" --data-binary "@${f}" > /dev/null
done
echo "clickhouse: tables ready in ${CLICKHOUSE_DB}"

# --- Debezium connector -------------------------------------------------------
wait_for connect "${CONNECT}/connectors"

# PUT creates the connector if missing, updates it in place if not.
envsubst < /opt/init/connector.json > /tmp/connector.json
curl -sf -X PUT -H 'Content-Type: application/json' --data @/tmp/connector.json \
  "${CONNECT}/connectors/${POSTGRES_DB}-connector/config" > /dev/null
echo "connect: ${POSTGRES_DB}-connector registered"

# --- Flink job ----------------------------------------------------------------
wait_for flink-jobmanager "${FLINK}/overview"

if curl -sf "${FLINK}/jobs/overview" | tr '{' '\n' | grep cdc-analytics | grep -q RUNNING; then
  echo "flink: job already running, nothing to submit"
else
  envsubst < /opt/init/pipeline.sql > /tmp/pipeline.sql
  "${FLINK_HOME}/bin/sql-client.sh" \
    -Drest.address=flink-jobmanager -Drest.port="${FLINK_UI_PORT}" \
    -f /tmp/pipeline.sql
fi

echo "pipeline ready."
