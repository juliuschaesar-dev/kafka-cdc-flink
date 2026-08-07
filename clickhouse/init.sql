-- Applied by scripts/init.sh.
--
-- Two databases: ${CLICKHOUSE_DB} holds the six result tables, and
-- ${CLICKHOUSE_DB}_ingest holds the Kafka plumbing feeding them -- kept
-- separate since a Kafka-engine table can only be read once, by its view.
--
-- Storage tables are ReplacingMergeTree, versioned by updated_at so the
-- newest row wins. FINAL is on by default (see docker-compose.yml), so plain
-- SELECTs already dedup; add SETTINGS final = 0 to see version history.

CREATE DATABASE IF NOT EXISTS ${CLICKHOUSE_DB}_ingest;

-- --------------------------------------------- windowed revenue per customer
CREATE TABLE IF NOT EXISTS ${CLICKHOUSE_DB}.enriched_payments
(
    window_start  DateTime64(3),
    window_end    DateTime64(3),
    customer_id   Int32,
    customer_name String,
    payment_count Int64,
    total_amount  Decimal(10, 2),
    updated_at    DateTime64(3)
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (window_start, window_end, customer_id);

CREATE TABLE IF NOT EXISTS ${CLICKHOUSE_DB}_ingest.enriched_payments
AS ${CLICKHOUSE_DB}.enriched_payments
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka:${KAFKA_PORT}',
    kafka_topic_list = 'enriched-payments',
    kafka_group_name = 'ch-enriched-payments',
    kafka_format = 'AvroConfluent',
    format_avro_schema_registry_url = 'http://schema-registry:${SCHEMA_REGISTRY_PORT}';

CREATE MATERIALIZED VIEW IF NOT EXISTS ${CLICKHOUSE_DB}_ingest.enriched_payments_mv
TO ${CLICKHOUSE_DB}.enriched_payments AS
SELECT * FROM ${CLICKHOUSE_DB}_ingest.enriched_payments;

-- ------------------------------------------------------ revenue per month ---
CREATE TABLE IF NOT EXISTS ${CLICKHOUSE_DB}.revenue_monthly
(
    revenue_month String,
    payment_count Int64,
    revenue       Decimal(12, 2),
    updated_at    DateTime64(3)
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY revenue_month;

CREATE TABLE IF NOT EXISTS ${CLICKHOUSE_DB}_ingest.revenue_monthly
AS ${CLICKHOUSE_DB}.revenue_monthly
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka:${KAFKA_PORT}',
    kafka_topic_list = 'revenue-monthly',
    kafka_group_name = 'ch-revenue-monthly',
    kafka_format = 'AvroConfluent',
    format_avro_schema_registry_url = 'http://schema-registry:${SCHEMA_REGISTRY_PORT}';

CREATE MATERIALIZED VIEW IF NOT EXISTS ${CLICKHOUSE_DB}_ingest.revenue_monthly_mv
TO ${CLICKHOUSE_DB}.revenue_monthly AS
SELECT * FROM ${CLICKHOUSE_DB}_ingest.revenue_monthly;

-- --------------------------------------------------- revenue per category ---
CREATE TABLE IF NOT EXISTS ${CLICKHOUSE_DB}.revenue_by_category
(
    category      String,
    payment_count Int64,
    revenue       Decimal(12, 2),
    updated_at    DateTime64(3)
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY category;

CREATE TABLE IF NOT EXISTS ${CLICKHOUSE_DB}_ingest.revenue_by_category
AS ${CLICKHOUSE_DB}.revenue_by_category
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka:${KAFKA_PORT}',
    kafka_topic_list = 'revenue-by-category',
    kafka_group_name = 'ch-revenue-by-category',
    kafka_format = 'AvroConfluent',
    format_avro_schema_registry_url = 'http://schema-registry:${SCHEMA_REGISTRY_PORT}';

CREATE MATERIALIZED VIEW IF NOT EXISTS ${CLICKHOUSE_DB}_ingest.revenue_by_category_mv
TO ${CLICKHOUSE_DB}.revenue_by_category AS
SELECT * FROM ${CLICKHOUSE_DB}_ingest.revenue_by_category;

-- ------------------------------------------------------------ top spenders --
CREATE TABLE IF NOT EXISTS ${CLICKHOUSE_DB}.top_customers
(
    rn            Int64,
    customer_id   Int32,
    customer_name String,
    payment_count Int64,
    total_spent   Decimal(12, 2),
    updated_at    DateTime64(3)
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY rn;

CREATE TABLE IF NOT EXISTS ${CLICKHOUSE_DB}_ingest.top_customers
AS ${CLICKHOUSE_DB}.top_customers
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka:${KAFKA_PORT}',
    kafka_topic_list = 'top-customers',
    kafka_group_name = 'ch-top-customers',
    kafka_format = 'AvroConfluent',
    format_avro_schema_registry_url = 'http://schema-registry:${SCHEMA_REGISTRY_PORT}';

CREATE MATERIALIZED VIEW IF NOT EXISTS ${CLICKHOUSE_DB}_ingest.top_customers_mv
TO ${CLICKHOUSE_DB}.top_customers AS
SELECT * FROM ${CLICKHOUSE_DB}_ingest.top_customers;

-- ------------------------------------------ rental duration / late ratio ----
CREATE TABLE IF NOT EXISTS ${CLICKHOUSE_DB}.store_rental_stats
(
    store_id        Int32,
    rentals         Int64,
    returned        Int64,
    avg_rental_days Float64,
    late_count      Int64,
    late_ratio      Float64,
    updated_at      DateTime64(3)
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY store_id;

CREATE TABLE IF NOT EXISTS ${CLICKHOUSE_DB}_ingest.store_rental_stats
AS ${CLICKHOUSE_DB}.store_rental_stats
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka:${KAFKA_PORT}',
    kafka_topic_list = 'store-rental-stats',
    kafka_group_name = 'ch-store-rental-stats',
    kafka_format = 'AvroConfluent',
    format_avro_schema_registry_url = 'http://schema-registry:${SCHEMA_REGISTRY_PORT}';

CREATE MATERIALIZED VIEW IF NOT EXISTS ${CLICKHOUSE_DB}_ingest.store_rental_stats_mv
TO ${CLICKHOUSE_DB}.store_rental_stats AS
SELECT * FROM ${CLICKHOUSE_DB}_ingest.store_rental_stats;

-- ------------------------------- monthly revenue trend with running total ---
CREATE TABLE IF NOT EXISTS ${CLICKHOUSE_DB}.revenue_trend
(
    revenue_month String,
    revenue       Decimal(12, 2),
    running_total Decimal(12, 2),
    updated_at    DateTime64(3)
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY revenue_month;

CREATE TABLE IF NOT EXISTS ${CLICKHOUSE_DB}_ingest.revenue_trend
AS ${CLICKHOUSE_DB}.revenue_trend
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka:${KAFKA_PORT}',
    kafka_topic_list = 'revenue-trend',
    kafka_group_name = 'ch-revenue-trend',
    kafka_format = 'AvroConfluent',
    format_avro_schema_registry_url = 'http://schema-registry:${SCHEMA_REGISTRY_PORT}';

CREATE MATERIALIZED VIEW IF NOT EXISTS ${CLICKHOUSE_DB}_ingest.revenue_trend_mv
TO ${CLICKHOUSE_DB}.revenue_trend AS
SELECT * FROM ${CLICKHOUSE_DB}_ingest.revenue_trend;
