# Kafka CDC Flink

Streaming CDC pipeline: PostgreSQL (dvdrental) → Debezium → Kafka → Flink → ClickHouse.
All Kafka topics use Avro, with schemas managed by Confluent Schema Registry.

```
Postgres (dvdrental) --WAL--> Debezium --CDC, Avro--> Kafka --> Flink --> Kafka --> ClickHouse
                                                                  ▲
                                                                  │ JDBC lookup joins
                                                                  │
                                                                  Postgres (dimensions)
```

Debezium captures `customer`, `payment`, and `rental` — the tables that drive a
result. Dimension tables (`inventory`, `film`, `film_category`, `category`) are
read from Postgres via JDBC lookup joins instead, keeping them out of Flink
state. One Flink job (`cdc-analytics`) turns it all into six result streams,
written back to Kafka as Avro. ClickHouse consumes those topics directly via
its native Kafka table engine.

On first run Debezium snapshots the existing rows once, then tails the WAL for
ongoing inserts/updates/deletes. Since the dvdrental dump is static, a
`generator` service can keep writing to `payment` so there's always something
to stream (insert every `GENERATOR_INTERVAL_SECONDS`, periodic update/delete).
It only touches rows it created. It's not started by `docker compose up` —
start it manually (see [Run it](#run-it)) and stop it with
`docker compose stop generator`.

Captured tables use `REPLICA IDENTITY FULL` (see `postgres/init/`) — without
it, updates/deletes carry no "before" value and Flink can't build the
retraction its aggregates need.

## Analytics

| ClickHouse table      | What it answers                          | How Flink computes it            |
|------------------------|------------------------------------------|----------------------------------|
| `enriched_payments`    | Revenue per customer, 1-minute windows   | Window TVF + changelog join      |
| `revenue_monthly`      | Revenue per calendar month               | Regular aggregate                |
| `revenue_by_category`  | Revenue per film category                | 4 chained lookup joins           |
| `top_customers`        | Top 20 spenders, continuously re-ranked  | Lookup join + unbounded Top-N    |
| `store_rental_stats`   | Avg rental length, late-return ratio per store | 2 lookup joins + aggregate |
| `revenue_trend`        | Monthly revenue with a running total     | Daily windows + OVER             |

The Kafka-consuming plumbing (Kafka-engine tables + materialized views) lives
in a separate `<db>_ingest` database, since a Kafka-engine table can only be
read once — by its own view.

All six tables are `ReplacingMergeTree(updated_at)`, so newest write wins
deterministically. Since old versions linger until a background merge, the
server sets `final = 1` by default so plain `SELECT`s already dedup:

```sql
SELECT * FROM revenue_by_category ORDER BY revenue DESC;
```

To see version history instead, add `SETTINGS final = 0`.

All tables match Postgres exactly, except `revenue_trend`: it uses
**event-time** windows with a 30-day watermark, so only its current month
lags. Use `revenue_monthly` for numbers that are always complete right now.
(The wide watermark exists because the Debezium snapshot replays payments out
of event-time order — a tighter watermark silently dropped ~19% of revenue.)

## Prerequisites

- Docker + Docker Compose (v2.23+, for the inline `configs:` block)
- `data/dvdrental.tar` (already included)

Everything runs through compose — no shell-specific requirements.

## Configuration

All credentials, names, and ports live in `.env` (gitignored):

```bash
cp .env.example .env
```

- **Compose interpolates most of it** — `docker-compose.yml` and the small inlined configs (AKHQ, ClickHouse ports).
- **The `init` service handles the rest** — three configs get `envsubst`'d straight into the target service, never written back to disk:

| File                               | Applied to        |
|-------------------------------------|-------------------|
| `clickhouse/init.sql`               | ClickHouse HTTP   |
| `debezium/postgres-connector.json`  | Connect REST API  |
| `flink/sql/pipeline.sql`            | Flink SQL client  |

Editing `.env` takes effect on the next `docker compose up`.

Each service's compose-network port and published port are the same value.
Schema Registry runs on 8085 (not the usual 8081) since the Flink dashboard
already uses 8081.

## Run it

```bash
cp .env.example .env      # first time only
docker compose up -d --build
```

The `init` service creates ClickHouse tables, registers the Debezium
connector, and submits the Flink job, then exits. ClickHouse then consumes the
result topics on its own.

Watch progress:

```bash
docker compose logs -f init
```

To feed the pipeline with continuous inserts/updates/deletes, start the
`generator` service manually — it's gated behind the `load` profile so it
doesn't start with a plain `docker compose up`:

```bash
docker compose --profile load up -d generator
```

Stop it with:

```bash
docker compose stop generator
```

Re-running `docker compose up -d` is safe — DDL is idempotent, the connector
is re-registered, and the Flink job is skipped if already running. To force
resubmission, cancel the job from the dashboard first.

## Services

| Service            | URL                     | Purpose                          |
|---------------------|-------------------------|-----------------------------------|
| Postgres             | localhost:5432          | Source database (dvdrental)       |
| Schema Registry      | http://localhost:8085   | Avro schemas for the CDC and enriched topics |
| Kafka Connect        | http://localhost:8083   | Debezium connector REST API       |
| Flink dashboard      | http://localhost:8081   | Job status, metrics               |
| ClickHouse HTTP       | http://localhost:8123   | SQL over HTTP                     |
| ClickHouse native     | localhost:9000          | `clickhouse-client` / drivers      |
| AKHQ                  | http://localhost:8080   | Browse Kafka topics, consumer groups, schemas, and Connect status |

Ports shown are `.env.example` defaults — actual ports follow your `.env`.

`init` and `generator` have no ports of their own.

## Querying the results

```bash
set -a; source .env; set +a

docker compose exec clickhouse clickhouse-client \
  --user "$CLICKHOUSE_USER" --password "$CLICKHOUSE_PASSWORD" --database "$CLICKHOUSE_DB" \
  -q "SELECT * FROM enriched_payments ORDER BY window_start DESC LIMIT 10"
```

Or over HTTP:

```bash
curl -u "$CLICKHOUSE_USER:$CLICKHOUSE_PASSWORD" \
  "http://localhost:${CLICKHOUSE_HTTP_PORT}/?database=${CLICKHOUSE_DB}" \
  --data-binary "SELECT * FROM revenue_monthly ORDER BY revenue_month"
```

With the generator running, re-running these a few seconds apart shows the
current month's numbers moving.

## Repository layout

```
.
├── .env.example                      # copy to .env; ports, credentials, db names
├── docker-compose.yml                # all services
├── postgres/init/                    # seed data, replica identity, publication, heartbeat
├── debezium/postgres-connector.json  # Debezium Postgres source connector config
├── connect/Dockerfile                # Kafka Connect image: Avro converter + Debezium plugin
├── flink/                            # custom Flink image + SQL pipeline
│   ├── Dockerfile                    # Kafka, Avro/Schema-Registry, JDBC connectors + driver
│   └── sql/pipeline.sql              # sources, lookups, sinks, views
├── clickhouse/init.sql               # result tables + Kafka consumers/views
├── scripts/
│   ├── init.sh                       # table, connector, and pipeline setup
│   └── generate-load.sh              # continuous insert/update/delete load
└── data/dvdrental.tar                # sample dvdrental dump
```

## Resetting

```bash
docker compose --profile "*" down -v
```

Drops all containers and volumes, including Postgres data, Kafka topics, and
ClickHouse data. The `--profile "*"` flag matters if `generator` was ever
started — plain `docker compose down` doesn't know about services in
unactivated profiles and leaves their containers running.
