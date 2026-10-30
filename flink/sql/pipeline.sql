SET 'execution.checkpointing.interval' = '10s';
SET 'pipeline.name' = 'cdc-analytics';

-- --------------------------------------------------------- cdc sources ------
-- Every table is read from its Debezium topic; Flink never queries Postgres.

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

CREATE TABLE inventory (
  inventory_id INT,
  film_id      INT,
  store_id     INT,
  PRIMARY KEY (inventory_id) NOT ENFORCED
) WITH (
  'connector' = 'kafka',
  'topic' = '${POSTGRES_DB}.public.inventory',
  'properties.bootstrap.servers' = 'kafka:${KAFKA_PORT}',
  'properties.group.id' = 'flink-cdc-inventory',
  'scan.startup.mode' = 'earliest-offset',
  'format' = 'debezium-avro-confluent',
  'debezium-avro-confluent.schema-registry.url' = 'http://schema-registry:${SCHEMA_REGISTRY_PORT}'
);

CREATE TABLE film (
  film_id         INT,
  rental_duration INT,
  PRIMARY KEY (film_id) NOT ENFORCED
) WITH (
  'connector' = 'kafka',
  'topic' = '${POSTGRES_DB}.public.film',
  'properties.bootstrap.servers' = 'kafka:${KAFKA_PORT}',
  'properties.group.id' = 'flink-cdc-film',
  'scan.startup.mode' = 'earliest-offset',
  'format' = 'debezium-avro-confluent',
  'debezium-avro-confluent.schema-registry.url' = 'http://schema-registry:${SCHEMA_REGISTRY_PORT}'
);

CREATE TABLE film_category (
  film_id     INT,
  category_id INT,
  PRIMARY KEY (film_id, category_id) NOT ENFORCED
) WITH (
  'connector' = 'kafka',
  'topic' = '${POSTGRES_DB}.public.film_category',
  'properties.bootstrap.servers' = 'kafka:${KAFKA_PORT}',
  'properties.group.id' = 'flink-cdc-film-category',
  'scan.startup.mode' = 'earliest-offset',
  'format' = 'debezium-avro-confluent',
  'debezium-avro-confluent.schema-registry.url' = 'http://schema-registry:${SCHEMA_REGISTRY_PORT}'
);

CREATE TABLE category (
  category_id INT,
  name        STRING,
  PRIMARY KEY (category_id) NOT ENFORCED
) WITH (
  'connector' = 'kafka',
  'topic' = '${POSTGRES_DB}.public.category',
  'properties.bootstrap.servers' = 'kafka:${KAFKA_PORT}',
  'properties.group.id' = 'flink-cdc-category',
  'scan.startup.mode' = 'earliest-offset',
  'format' = 'debezium-avro-confluent',
  'debezium-avro-confluent.schema-registry.url' = 'http://schema-registry:${SCHEMA_REGISTRY_PORT}'
);

-- ------------------------------------------------------------------ sinks ---
-- Results are written as a Debezium-style changelog (debezium-json), not as
-- upsert-kafka: a deleted row must reach ClickHouse as an explicit 'd' event,
-- because upsert-kafka signals a delete with a tombstone that ClickHouse's Kafka
-- engine ignores. An update is a 'd' of the old row followed by a 'c' of the new
-- one. The message key is the row key, so all changes to one row stay in order
-- on one partition. updated_at is the version ClickHouse's ReplacingMergeTree
-- keeps the newest row by.

CREATE TABLE enriched_payments (
  window_start   TIMESTAMP(3),
  window_end     TIMESTAMP(3),
  customer_id    INT,
  customer_name  STRING,
  payment_count  BIGINT,
  total_amount   DECIMAL(10, 2),
  updated_at     TIMESTAMP(3)
) WITH (
  'connector' = 'kafka',
  'topic' = 'enriched-payments',
  'properties.bootstrap.servers' = 'kafka:${KAFKA_PORT}',
  'key.format' = 'json',
  'key.fields' = 'window_start;window_end;customer_id',
  'value.format' = 'debezium-json'
);

CREATE TABLE revenue_monthly (
  revenue_month  STRING,
  payment_count  BIGINT,
  revenue        DECIMAL(12, 2),
  updated_at     TIMESTAMP(3)
) WITH (
  'connector' = 'kafka',
  'topic' = 'revenue-monthly',
  'properties.bootstrap.servers' = 'kafka:${KAFKA_PORT}',
  'key.format' = 'json',
  'key.fields' = 'revenue_month',
  'value.format' = 'debezium-json'
);

CREATE TABLE revenue_by_category (
  category_id    INT,
  category       STRING,
  payment_count  BIGINT,
  revenue        DECIMAL(12, 2),
  updated_at     TIMESTAMP(3)
) WITH (
  'connector' = 'kafka',
  'topic' = 'revenue-by-category',
  'properties.bootstrap.servers' = 'kafka:${KAFKA_PORT}',
  'key.format' = 'json',
  'key.fields' = 'category_id',
  'value.format' = 'debezium-json'
);

CREATE TABLE top_customers (
  rn             BIGINT,
  customer_id    INT,
  customer_name  STRING,
  payment_count  BIGINT,
  total_spent    DECIMAL(12, 2),
  updated_at     TIMESTAMP(3)
) WITH (
  'connector' = 'kafka',
  'topic' = 'top-customers',
  'properties.bootstrap.servers' = 'kafka:${KAFKA_PORT}',
  'key.format' = 'json',
  'key.fields' = 'rn',
  'value.format' = 'debezium-json'
);

CREATE TABLE store_rental_stats (
  store_id       INT,
  rentals        BIGINT,
  returned       BIGINT,
  avg_rental_days DOUBLE,
  late_count     BIGINT,
  late_ratio     DOUBLE,
  updated_at     TIMESTAMP(3)
) WITH (
  'connector' = 'kafka',
  'topic' = 'store-rental-stats',
  'properties.bootstrap.servers' = 'kafka:${KAFKA_PORT}',
  'key.format' = 'json',
  'key.fields' = 'store_id',
  'value.format' = 'debezium-json'
);

-- ------------------------------------------------------------------ views ---

-- rental_days is fractional -- TIMESTAMPDIFF(DAY, ...) would truncate it.
CREATE VIEW rental_detail AS
SELECT
  i.store_id        AS store_id,
  f.rental_duration AS rental_duration,
  CAST(TIMESTAMPDIFF(SECOND, r.rental_date, r.return_date) AS DOUBLE) / 86400 AS rental_days
FROM rental r
JOIN inventory i ON r.inventory_id = i.inventory_id
JOIN film      f ON i.film_id = f.film_id;

-- ------------------------------------------------------------------- jobs ---

EXECUTE STATEMENT SET
BEGIN

-- Revenue per customer, 1-minute tumbling windows. Grouped by customer_id only,
-- with the name as MAX(): a rename then updates the row instead of moving it to
-- a different group, whose delete/insert order the sink could not guarantee.
INSERT INTO enriched_payments
SELECT
  window_start,
  window_end,
  c.customer_id,
  MAX(c.first_name || ' ' || c.last_name) AS customer_name,
  COUNT(*)      AS payment_count,
  SUM(p.amount) AS total_amount,
  CURRENT_TIMESTAMP AS updated_at
FROM TABLE(
  TUMBLE(TABLE payment, DESCRIPTOR(payment_date), INTERVAL '1' MINUTE)
) AS p
JOIN customer AS c ON p.customer_id = c.customer_id
GROUP BY window_start, window_end, c.customer_id;

-- Revenue per calendar month.
INSERT INTO revenue_monthly
SELECT
  DATE_FORMAT(payment_date, 'yyyy-MM')      AS revenue_month,
  COUNT(*)                                  AS payment_count,
  CAST(SUM(amount) AS DECIMAL(12, 2))       AS revenue,
  CURRENT_TIMESTAMP                         AS updated_at
FROM payment
GROUP BY DATE_FORMAT(payment_date, 'yyyy-MM');

-- Revenue per film category: payment -> rental -> inventory -> film_category -> category.
-- Grouped by category_id, with the name as MAX(): a rename updates the row instead of
-- moving it to a new group.
INSERT INTO revenue_by_category
SELECT
  c.category_id                         AS category_id,
  MAX(c.name)                           AS category,
  COUNT(*)                              AS payment_count,
  CAST(SUM(p.amount) AS DECIMAL(12, 2)) AS revenue,
  CURRENT_TIMESTAMP                     AS updated_at
FROM payment p
JOIN rental        r  ON p.rental_id = r.rental_id
JOIN inventory     i  ON r.inventory_id = i.inventory_id
JOIN film_category fc ON i.film_id = fc.film_id
JOIN category      c  ON fc.category_id = c.category_id
GROUP BY c.category_id;

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
      MAX(c.first_name || ' ' || c.last_name) AS customer_name,
      COUNT(*)                              AS payment_count,
      CAST(SUM(p.amount) AS DECIMAL(12, 2)) AS total_spent
    FROM payment p
    JOIN customer c ON p.customer_id = c.customer_id
    GROUP BY c.customer_id
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

END;
