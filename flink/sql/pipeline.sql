-- Submitted by scripts/init.sh. Six result streams, run as one job via the
-- STATEMENT SET below so sources are read once.

SET 'execution.checkpointing.interval' = '10s';
SET 'pipeline.name' = 'cdc-analytics';

-- --------------------------------------------------------- cdc sources ------
-- Only tables whose changes drive a result; everything else is a lookup join.

CREATE TABLE customer (
  customer_id INT,
  first_name  STRING,
  last_name   STRING,
  PRIMARY KEY (customer_id) NOT ENFORCED
) WITH (
  'connector' = 'kafka',
  'topic' = '${POSTGRES_DB}.public.customer',
  'properties.bootstrap.servers' = 'kafka:${KAFKA_PORT}',
  'properties.group.id' = 'flink-cdc-customer',
  'scan.startup.mode' = 'earliest-offset',
  'format' = 'debezium-avro-confluent',
  'debezium-avro-confluent.schema-registry.url' = 'http://schema-registry:${SCHEMA_REGISTRY_PORT}'
);

CREATE TABLE payment (
  customer_id  INT,
  rental_id    INT,
  amount       DECIMAL(5, 2),
  payment_date TIMESTAMP(3),
  proc_time AS PROCTIME(),
  -- Wide on purpose: a tight watermark drops rows from the out-of-order snapshot.
  WATERMARK FOR payment_date AS payment_date - INTERVAL '30' DAY
) WITH (
  'connector' = 'kafka',
  'topic' = '${POSTGRES_DB}.public.payment',
  'properties.bootstrap.servers' = 'kafka:${KAFKA_PORT}',
  'properties.group.id' = 'flink-cdc-payment',
  'scan.startup.mode' = 'earliest-offset',
  'format' = 'debezium-avro-confluent',
  'debezium-avro-confluent.schema-registry.url' = 'http://schema-registry:${SCHEMA_REGISTRY_PORT}'
);

CREATE TABLE rental (
  rental_id    INT,
  rental_date  TIMESTAMP(3),
  inventory_id INT,
  return_date  TIMESTAMP(3),
  proc_time AS PROCTIME(),
  PRIMARY KEY (rental_id) NOT ENFORCED
) WITH (
  'connector' = 'kafka',
  'topic' = '${POSTGRES_DB}.public.rental',
  'properties.bootstrap.servers' = 'kafka:${KAFKA_PORT}',
  'properties.group.id' = 'flink-cdc-rental',
  'scan.startup.mode' = 'earliest-offset',
  'format' = 'debezium-avro-confluent',
  'debezium-avro-confluent.schema-registry.url' = 'http://schema-registry:${SCHEMA_REGISTRY_PORT}'
);

-- ------------------------------------------------------ lookup tables -------
-- Read from Postgres per event via FOR SYSTEM_TIME AS OF; keeps no state.

CREATE TABLE dim_rental (
  rental_id    INT,
  inventory_id INT,
  PRIMARY KEY (rental_id) NOT ENFORCED
) WITH (
  'connector' = 'jdbc',
  'url' = 'jdbc:postgresql://postgres:${POSTGRES_PORT}/${POSTGRES_DB}',
  'table-name' = 'rental',
  'username' = '${POSTGRES_USER}',
  'password' = '${POSTGRES_PASSWORD}',
  'lookup.cache' = 'PARTIAL',
  'lookup.partial-cache.max-rows' = '20000',
  'lookup.partial-cache.expire-after-write' = '10min'
);

CREATE TABLE dim_inventory (
  inventory_id INT,
  film_id      INT,
  store_id     INT,
  PRIMARY KEY (inventory_id) NOT ENFORCED
) WITH (
  'connector' = 'jdbc',
  'url' = 'jdbc:postgresql://postgres:${POSTGRES_PORT}/${POSTGRES_DB}',
  'table-name' = 'inventory',
  'username' = '${POSTGRES_USER}',
  'password' = '${POSTGRES_PASSWORD}',
  'lookup.cache' = 'PARTIAL',
  'lookup.partial-cache.max-rows' = '10000',
  'lookup.partial-cache.expire-after-write' = '10min'
);

CREATE TABLE dim_film (
  film_id         INT,
  rental_duration INT,
  PRIMARY KEY (film_id) NOT ENFORCED
) WITH (
  'connector' = 'jdbc',
  'url' = 'jdbc:postgresql://postgres:${POSTGRES_PORT}/${POSTGRES_DB}',
  'table-name' = 'film',
  'username' = '${POSTGRES_USER}',
  'password' = '${POSTGRES_PASSWORD}',
  'lookup.cache' = 'PARTIAL',
  'lookup.partial-cache.max-rows' = '5000',
  'lookup.partial-cache.expire-after-write' = '10min'
);

CREATE TABLE dim_film_category (
  film_id     INT,
  category_id INT,
  PRIMARY KEY (film_id) NOT ENFORCED
) WITH (
  'connector' = 'jdbc',
  'url' = 'jdbc:postgresql://postgres:${POSTGRES_PORT}/${POSTGRES_DB}',
  'table-name' = 'film_category',
  'username' = '${POSTGRES_USER}',
  'password' = '${POSTGRES_PASSWORD}',
  'lookup.cache' = 'PARTIAL',
  'lookup.partial-cache.max-rows' = '5000',
  'lookup.partial-cache.expire-after-write' = '10min'
);

CREATE TABLE dim_category (
  category_id INT,
  name        STRING,
  PRIMARY KEY (category_id) NOT ENFORCED
) WITH (
  'connector' = 'jdbc',
  'url' = 'jdbc:postgresql://postgres:${POSTGRES_PORT}/${POSTGRES_DB}',
  'table-name' = 'category',
  'username' = '${POSTGRES_USER}',
  'password' = '${POSTGRES_PASSWORD}',
  'lookup.cache' = 'PARTIAL',
  'lookup.partial-cache.max-rows' = '1000',
  'lookup.partial-cache.expire-after-write' = '10min'
);

CREATE TABLE dim_customer (
  customer_id INT,
  first_name  STRING,
  last_name   STRING,
  PRIMARY KEY (customer_id) NOT ENFORCED
) WITH (
  'connector' = 'jdbc',
  'url' = 'jdbc:postgresql://postgres:${POSTGRES_PORT}/${POSTGRES_DB}',
  'table-name' = 'customer',
  'username' = '${POSTGRES_USER}',
  'password' = '${POSTGRES_PASSWORD}',
  'lookup.cache' = 'PARTIAL',
  'lookup.partial-cache.max-rows' = '10000',
  'lookup.partial-cache.expire-after-write' = '10min'
);

-- ------------------------------------------------------------------ sinks ---
-- upsert-kafka since results update in place. updated_at is the version
-- ClickHouse's ReplacingMergeTree collapses on.

CREATE TABLE enriched_payments (
  window_start   TIMESTAMP(3),
  window_end     TIMESTAMP(3),
  customer_id    INT,
  customer_name  STRING,
  payment_count  BIGINT,
  total_amount   DECIMAL(10, 2),
  updated_at     TIMESTAMP(3),
  PRIMARY KEY (window_start, window_end, customer_id) NOT ENFORCED
) WITH (
  'connector' = 'upsert-kafka',
  'topic' = 'enriched-payments',
  'properties.bootstrap.servers' = 'kafka:${KAFKA_PORT}',
  'key.format' = 'json',
  'value.format' = 'avro-confluent',
  'value.avro-confluent.schema-registry.url' = 'http://schema-registry:${SCHEMA_REGISTRY_PORT}'
);

CREATE TABLE revenue_monthly (
  revenue_month STRING,
  payment_count BIGINT,
  revenue       DECIMAL(12, 2),
  updated_at    TIMESTAMP(3),
  PRIMARY KEY (revenue_month) NOT ENFORCED
) WITH (
  'connector' = 'upsert-kafka',
  'topic' = 'revenue-monthly',
  'properties.bootstrap.servers' = 'kafka:${KAFKA_PORT}',
  'key.format' = 'json',
  'value.format' = 'avro-confluent',
  'value.avro-confluent.schema-registry.url' = 'http://schema-registry:${SCHEMA_REGISTRY_PORT}'
);

CREATE TABLE revenue_by_category (
  category      STRING,
  payment_count BIGINT,
  revenue       DECIMAL(12, 2),
  updated_at    TIMESTAMP(3),
  PRIMARY KEY (category) NOT ENFORCED
) WITH (
  'connector' = 'upsert-kafka',
  'topic' = 'revenue-by-category',
  'properties.bootstrap.servers' = 'kafka:${KAFKA_PORT}',
  'key.format' = 'json',
  'value.format' = 'avro-confluent',
  'value.avro-confluent.schema-registry.url' = 'http://schema-registry:${SCHEMA_REGISTRY_PORT}'
);

CREATE TABLE top_customers (
  rn            BIGINT,
  customer_id   INT,
  customer_name STRING,
  payment_count BIGINT,
  total_spent   DECIMAL(12, 2),
  updated_at    TIMESTAMP(3),
  PRIMARY KEY (rn) NOT ENFORCED
) WITH (
  'connector' = 'upsert-kafka',
  'topic' = 'top-customers',
  'properties.bootstrap.servers' = 'kafka:${KAFKA_PORT}',
  'key.format' = 'json',
  'value.format' = 'avro-confluent',
  'value.avro-confluent.schema-registry.url' = 'http://schema-registry:${SCHEMA_REGISTRY_PORT}'
);

CREATE TABLE store_rental_stats (
  store_id        INT,
  rentals         BIGINT,
  returned        BIGINT,
  avg_rental_days DOUBLE,
  late_count      BIGINT,
  late_ratio      DOUBLE,
  updated_at      TIMESTAMP(3),
  PRIMARY KEY (store_id) NOT ENFORCED
) WITH (
  'connector' = 'upsert-kafka',
  'topic' = 'store-rental-stats',
  'properties.bootstrap.servers' = 'kafka:${KAFKA_PORT}',
  'key.format' = 'json',
  'value.format' = 'avro-confluent',
  'value.avro-confluent.schema-registry.url' = 'http://schema-registry:${SCHEMA_REGISTRY_PORT}'
);

CREATE TABLE revenue_trend (
  revenue_month STRING,
  revenue       DECIMAL(12, 2),
  running_total DECIMAL(12, 2),
  updated_at    TIMESTAMP(3),
  PRIMARY KEY (revenue_month) NOT ENFORCED
) WITH (
  'connector' = 'upsert-kafka',
  'topic' = 'revenue-trend',
  'properties.bootstrap.servers' = 'kafka:${KAFKA_PORT}',
  'key.format' = 'json',
  'value.format' = 'avro-confluent',
  'value.avro-confluent.schema-registry.url' = 'http://schema-registry:${SCHEMA_REGISTRY_PORT}'
);

-- ------------------------------------------------------------------ views ---

-- Window TVF turns the updating Debezium stream into append-only rows OVER can use.
CREATE VIEW daily_revenue AS
SELECT
  window_start,
  PROCTIME() AS emitted_at,
  CAST(SUM(amount) AS DECIMAL(12, 2)) AS revenue
FROM TABLE(
  TUMBLE(TABLE payment, DESCRIPTOR(payment_date), INTERVAL '1' DAY)
)
GROUP BY window_start, window_end;

-- Ordered by processing time, not rowtime -- a rowtime OVER would drop every
-- row as late, since windows emit already behind their own watermark.
CREATE VIEW daily_running AS
SELECT
  window_start,
  revenue,
  SUM(revenue) OVER (
    ORDER BY emitted_at
    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
  ) AS running_total
FROM daily_revenue;

-- rental_days is fractional -- TIMESTAMPDIFF(DAY, ...) would truncate it.
CREATE VIEW rental_detail AS
SELECT
  i.store_id        AS store_id,
  f.rental_duration AS rental_duration,
  CAST(TIMESTAMPDIFF(SECOND, r.rental_date, r.return_date) AS DOUBLE) / 86400 AS rental_days
FROM rental r
JOIN dim_inventory FOR SYSTEM_TIME AS OF r.proc_time AS i ON r.inventory_id = i.inventory_id
JOIN dim_film      FOR SYSTEM_TIME AS OF r.proc_time AS f ON i.film_id = f.film_id;

-- ------------------------------------------------------------------- jobs ---

EXECUTE STATEMENT SET
BEGIN

-- Revenue per customer, 1-minute tumbling windows.
INSERT INTO enriched_payments
SELECT
  window_start,
  window_end,
  c.customer_id,
  c.first_name || ' ' || c.last_name AS customer_name,
  COUNT(*)      AS payment_count,
  SUM(p.amount) AS total_amount,
  CURRENT_TIMESTAMP AS updated_at
FROM TABLE(
  TUMBLE(TABLE payment, DESCRIPTOR(payment_date), INTERVAL '1' MINUTE)
) AS p
JOIN customer AS c ON p.customer_id = c.customer_id
GROUP BY window_start, window_end, c.customer_id, c.first_name, c.last_name;

-- Revenue per calendar month.
INSERT INTO revenue_monthly
SELECT
  DATE_FORMAT(payment_date, 'yyyy-MM')      AS revenue_month,
  COUNT(*)                                  AS payment_count,
  CAST(SUM(amount) AS DECIMAL(12, 2))       AS revenue,
  CURRENT_TIMESTAMP                         AS updated_at
FROM payment
GROUP BY DATE_FORMAT(payment_date, 'yyyy-MM');

-- Revenue per film category: payment -> rental -> inventory -> film_category.
INSERT INTO revenue_by_category
SELECT
  c.name                                AS category,
  COUNT(*)                              AS payment_count,
  CAST(SUM(p.amount) AS DECIMAL(12, 2)) AS revenue,
  CURRENT_TIMESTAMP                     AS updated_at
FROM payment p
JOIN dim_rental        FOR SYSTEM_TIME AS OF p.proc_time AS r  ON p.rental_id = r.rental_id
JOIN dim_inventory     FOR SYSTEM_TIME AS OF p.proc_time AS i  ON r.inventory_id = i.inventory_id
JOIN dim_film_category FOR SYSTEM_TIME AS OF p.proc_time AS fc ON i.film_id = fc.film_id
JOIN dim_category      FOR SYSTEM_TIME AS OF p.proc_time AS c  ON fc.category_id = c.category_id
GROUP BY c.name;

-- Top 20 spenders, re-ranked as payments arrive.
INSERT INTO top_customers
SELECT rn, customer_id, customer_name, payment_count, total_spent, CURRENT_TIMESTAMP
FROM (
  SELECT
    customer_id,
    customer_name,
    payment_count,
    total_spent,
    ROW_NUMBER() OVER (ORDER BY total_spent DESC) AS rn
  FROM (
    SELECT
      c.customer_id                         AS customer_id,
      c.first_name || ' ' || c.last_name    AS customer_name,
      COUNT(*)                              AS payment_count,
      CAST(SUM(p.amount) AS DECIMAL(12, 2)) AS total_spent
    FROM payment p
    JOIN dim_customer FOR SYSTEM_TIME AS OF p.proc_time AS c ON p.customer_id = c.customer_id
    GROUP BY c.customer_id, c.first_name, c.last_name
  )
)
WHERE rn <= 20;

-- Late when returned more days after rental_date than the film allows.
INSERT INTO store_rental_stats
SELECT
  store_id,
  rentals,
  returned,
  avg_rental_days,
  late_count,
  CASE WHEN returned > 0 THEN CAST(late_count AS DOUBLE) / returned ELSE 0.0 END AS late_ratio,
  CURRENT_TIMESTAMP AS updated_at
FROM (
  SELECT
    store_id,
    COUNT(*)            AS rentals,
    COUNT(rental_days)  AS returned,
    AVG(rental_days)    AS avg_rental_days,
    CAST(SUM(CASE WHEN rental_days > rental_duration THEN 1 ELSE 0 END) AS BIGINT) AS late_count
  FROM rental_detail
  GROUP BY store_id
);

-- MAX(running_total) per month = that month's closing cumulative revenue.
-- Event-time windows mean the newest month trails until the watermark passes it.
INSERT INTO revenue_trend
SELECT
  DATE_FORMAT(window_start, 'yyyy-MM')       AS revenue_month,
  CAST(SUM(revenue) AS DECIMAL(12, 2))       AS revenue,
  CAST(MAX(running_total) AS DECIMAL(12, 2)) AS running_total,
  CURRENT_TIMESTAMP                          AS updated_at
FROM daily_running
GROUP BY DATE_FORMAT(window_start, 'yyyy-MM');

END;
