CREATE DATABASE IF NOT EXISTS ${CLICKHOUSE_DB}_ingest;

-- Each result table keeps only the current row per key; there is no history.
--
-- Flink sends a changelog (see the sinks in flink/sql/pipeline.sql): 'c' carries
-- the new row in `after`, 'd' carries the removed row in `before`. The Kafka
-- engine table reads those raw messages, the materialized view unpacks them and
-- inserts them with is_deleted = 1 for 'd'. ReplacingMergeTree(updated_at,
-- is_deleted) then keeps the newest version per key, and a key whose newest
-- version is a delete disappears.
--
-- Merge settings: min_age_to_force_merge_seconds makes a merge happen soon after
-- a part is written, and the cleanup settings let that merge physically drop
-- deleted rows, so nothing old lingers on disk. Until a merge runs, the server
-- default final = 1 already hides old versions and deleted keys from queries.

-- windowed revenue per customer ---------------------------------------------
CREATE TABLE IF NOT EXISTS ${CLICKHOUSE_DB}.enriched_payments
(
    window_start  DateTime64(3),
    window_end    DateTime64(3),
    customer_id   Int32,
    customer_name String,
    payment_count Int64,
    total_amount  Decimal(10, 2),
    updated_at    DateTime64(3),
    is_deleted    UInt8
)
ENGINE = ReplacingMergeTree(updated_at, is_deleted)
ORDER BY (window_start, window_end, customer_id)
SETTINGS min_age_to_force_merge_seconds = 30,
         min_age_to_force_merge_on_partition_only = 1,
         allow_experimental_replacing_merge_with_cleanup = 1,
         enable_replacing_merge_with_cleanup_for_min_age_to_force_merge = 1;

CREATE TABLE IF NOT EXISTS ${CLICKHOUSE_DB}_ingest.enriched_payments
(
    op     String,
    before String,
    after  String
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka:${KAFKA_PORT}',
    kafka_topic_list = 'enriched-payments',
    kafka_group_name = 'ch-enriched-payments',
    kafka_format = 'JSONEachRow';

CREATE MATERIALIZED VIEW IF NOT EXISTS ${CLICKHOUSE_DB}_ingest.enriched_payments_mv
TO ${CLICKHOUSE_DB}.enriched_payments AS
SELECT
    JSONExtract(if(op = 'd', before, after), 'window_start', 'DateTime64(3)') AS window_start,
    JSONExtract(if(op = 'd', before, after), 'window_end', 'DateTime64(3)') AS window_end,
    JSONExtract(if(op = 'd', before, after), 'customer_id', 'Int32') AS customer_id,
    JSONExtract(if(op = 'd', before, after), 'customer_name', 'String') AS customer_name,
    JSONExtract(if(op = 'd', before, after), 'payment_count', 'Int64') AS payment_count,
    JSONExtract(if(op = 'd', before, after), 'total_amount', 'Decimal(10, 2)') AS total_amount,
    JSONExtract(if(op = 'd', before, after), 'updated_at', 'DateTime64(3)') AS updated_at,
    toUInt8(op = 'd') AS is_deleted
FROM ${CLICKHOUSE_DB}_ingest.enriched_payments;

-- revenue per month ---------------------------------------------------------
CREATE TABLE IF NOT EXISTS ${CLICKHOUSE_DB}.revenue_monthly
(
    revenue_month String,
    payment_count Int64,
    revenue       Decimal(12, 2),
    updated_at    DateTime64(3),
    is_deleted    UInt8
)
ENGINE = ReplacingMergeTree(updated_at, is_deleted)
ORDER BY revenue_month
SETTINGS min_age_to_force_merge_seconds = 30,
         min_age_to_force_merge_on_partition_only = 1,
         allow_experimental_replacing_merge_with_cleanup = 1,
         enable_replacing_merge_with_cleanup_for_min_age_to_force_merge = 1;

CREATE TABLE IF NOT EXISTS ${CLICKHOUSE_DB}_ingest.revenue_monthly
(
    op     String,
    before String,
    after  String
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka:${KAFKA_PORT}',
    kafka_topic_list = 'revenue-monthly',
    kafka_group_name = 'ch-revenue-monthly',
    kafka_format = 'JSONEachRow';

CREATE MATERIALIZED VIEW IF NOT EXISTS ${CLICKHOUSE_DB}_ingest.revenue_monthly_mv
TO ${CLICKHOUSE_DB}.revenue_monthly AS
SELECT
    JSONExtract(if(op = 'd', before, after), 'revenue_month', 'String') AS revenue_month,
    JSONExtract(if(op = 'd', before, after), 'payment_count', 'Int64') AS payment_count,
    JSONExtract(if(op = 'd', before, after), 'revenue', 'Decimal(12, 2)') AS revenue,
    JSONExtract(if(op = 'd', before, after), 'updated_at', 'DateTime64(3)') AS updated_at,
    toUInt8(op = 'd') AS is_deleted
FROM ${CLICKHOUSE_DB}_ingest.revenue_monthly;

-- revenue per category ------------------------------------------------------
CREATE TABLE IF NOT EXISTS ${CLICKHOUSE_DB}.revenue_by_category
(
    category_id   Int32,
    category      String,
    payment_count Int64,
    revenue       Decimal(12, 2),
    updated_at    DateTime64(3),
    is_deleted    UInt8
)
ENGINE = ReplacingMergeTree(updated_at, is_deleted)
ORDER BY category_id
SETTINGS min_age_to_force_merge_seconds = 30,
         min_age_to_force_merge_on_partition_only = 1,
         allow_experimental_replacing_merge_with_cleanup = 1,
         enable_replacing_merge_with_cleanup_for_min_age_to_force_merge = 1;

CREATE TABLE IF NOT EXISTS ${CLICKHOUSE_DB}_ingest.revenue_by_category
(
    op     String,
    before String,
    after  String
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka:${KAFKA_PORT}',
    kafka_topic_list = 'revenue-by-category',
    kafka_group_name = 'ch-revenue-by-category',
    kafka_format = 'JSONEachRow';

CREATE MATERIALIZED VIEW IF NOT EXISTS ${CLICKHOUSE_DB}_ingest.revenue_by_category_mv
TO ${CLICKHOUSE_DB}.revenue_by_category AS
SELECT
    JSONExtract(if(op = 'd', before, after), 'category_id', 'Int32') AS category_id,
    JSONExtract(if(op = 'd', before, after), 'category', 'String') AS category,
    JSONExtract(if(op = 'd', before, after), 'payment_count', 'Int64') AS payment_count,
    JSONExtract(if(op = 'd', before, after), 'revenue', 'Decimal(12, 2)') AS revenue,
    JSONExtract(if(op = 'd', before, after), 'updated_at', 'DateTime64(3)') AS updated_at,
    toUInt8(op = 'd') AS is_deleted
FROM ${CLICKHOUSE_DB}_ingest.revenue_by_category;

-- top spenders --------------------------------------------------------------
CREATE TABLE IF NOT EXISTS ${CLICKHOUSE_DB}.top_customers
(
    rn            Int64,
    customer_id   Int32,
    customer_name String,
    payment_count Int64,
    total_spent   Decimal(12, 2),
    updated_at    DateTime64(3),
    is_deleted    UInt8
)
ENGINE = ReplacingMergeTree(updated_at, is_deleted)
ORDER BY rn
SETTINGS min_age_to_force_merge_seconds = 30,
         min_age_to_force_merge_on_partition_only = 1,
         allow_experimental_replacing_merge_with_cleanup = 1,
         enable_replacing_merge_with_cleanup_for_min_age_to_force_merge = 1;

CREATE TABLE IF NOT EXISTS ${CLICKHOUSE_DB}_ingest.top_customers
(
    op     String,
    before String,
    after  String
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka:${KAFKA_PORT}',
    kafka_topic_list = 'top-customers',
    kafka_group_name = 'ch-top-customers',
    kafka_format = 'JSONEachRow';

CREATE MATERIALIZED VIEW IF NOT EXISTS ${CLICKHOUSE_DB}_ingest.top_customers_mv
TO ${CLICKHOUSE_DB}.top_customers AS
SELECT
    JSONExtract(if(op = 'd', before, after), 'rn', 'Int64') AS rn,
    JSONExtract(if(op = 'd', before, after), 'customer_id', 'Int32') AS customer_id,
    JSONExtract(if(op = 'd', before, after), 'customer_name', 'String') AS customer_name,
    JSONExtract(if(op = 'd', before, after), 'payment_count', 'Int64') AS payment_count,
    JSONExtract(if(op = 'd', before, after), 'total_spent', 'Decimal(12, 2)') AS total_spent,
    JSONExtract(if(op = 'd', before, after), 'updated_at', 'DateTime64(3)') AS updated_at,
    toUInt8(op = 'd') AS is_deleted
FROM ${CLICKHOUSE_DB}_ingest.top_customers;

-- rental duration / late ratio ----------------------------------------------
CREATE TABLE IF NOT EXISTS ${CLICKHOUSE_DB}.store_rental_stats
(
    store_id        Int32,
    rentals         Int64,
    returned        Int64,
    avg_rental_days Float64,
    late_count      Int64,
    late_ratio      Float64,
    updated_at      DateTime64(3),
    is_deleted      UInt8
)
ENGINE = ReplacingMergeTree(updated_at, is_deleted)
ORDER BY store_id
SETTINGS min_age_to_force_merge_seconds = 30,
         min_age_to_force_merge_on_partition_only = 1,
         allow_experimental_replacing_merge_with_cleanup = 1,
         enable_replacing_merge_with_cleanup_for_min_age_to_force_merge = 1;

CREATE TABLE IF NOT EXISTS ${CLICKHOUSE_DB}_ingest.store_rental_stats
(
    op     String,
    before String,
    after  String
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka:${KAFKA_PORT}',
    kafka_topic_list = 'store-rental-stats',
    kafka_group_name = 'ch-store-rental-stats',
    kafka_format = 'JSONEachRow';

CREATE MATERIALIZED VIEW IF NOT EXISTS ${CLICKHOUSE_DB}_ingest.store_rental_stats_mv
TO ${CLICKHOUSE_DB}.store_rental_stats AS
SELECT
    JSONExtract(if(op = 'd', before, after), 'store_id', 'Int32') AS store_id,
    JSONExtract(if(op = 'd', before, after), 'rentals', 'Int64') AS rentals,
    JSONExtract(if(op = 'd', before, after), 'returned', 'Int64') AS returned,
    JSONExtract(if(op = 'd', before, after), 'avg_rental_days', 'Float64') AS avg_rental_days,
    JSONExtract(if(op = 'd', before, after), 'late_count', 'Int64') AS late_count,
    JSONExtract(if(op = 'd', before, after), 'late_ratio', 'Float64') AS late_ratio,
    JSONExtract(if(op = 'd', before, after), 'updated_at', 'DateTime64(3)') AS updated_at,
    toUInt8(op = 'd') AS is_deleted
FROM ${CLICKHOUSE_DB}_ingest.store_rental_stats;
