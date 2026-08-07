#!/bin/bash
set -euo pipefail

# pg_restore exits non-zero on harmless warnings, so verify the result instead.
pg_restore -U "$POSTGRES_USER" -d "$POSTGRES_DB" --no-owner --role="$POSTGRES_USER" /dvdrental.tar || true

# Check the tables the pipeline actually reads, so a partial restore fails here
# instead of showing up later as quietly wrong analytics.
missing=$(psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -t -A -c "
  SELECT COALESCE(string_agg(t, ', '), '')
  FROM unnest(ARRAY['customer','payment','rental','inventory','film','film_category','category']) AS t
  WHERE to_regclass('public.' || t) IS NULL")
if [ -n "$(echo "$missing" | tr -d '[:space:]')" ]; then
  echo "dvdrental restore failed: missing tables: ${missing}" >&2
  exit 1
fi

payment_rows=$(psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -t -A -c 'SELECT count(*) FROM payment')
if [ "$(echo "$payment_rows" | tr -d '[:space:]')" -lt 1 ]; then
  echo "dvdrental restore failed: payment table is empty" >&2
  exit 1
fi
echo "dvdrental restored: ${payment_rows} payments"

# dbz_heartbeat gives the connector something to write on a timer, so the
# replication slot keeps advancing during quiet periods instead of piling up WAL.
psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -v ON_ERROR_STOP=1 <<-SQL
  CREATE TABLE public.dbz_heartbeat (id int PRIMARY KEY, ts timestamptz NOT NULL);
  INSERT INTO public.dbz_heartbeat (id, ts) VALUES (1, now());

  -- Published but not captured: its WAL records advance the slot without
  -- becoming CDC events.
  CREATE PUBLICATION dbz_publication FOR TABLE
    public.customer, public.payment, public.rental, public.dbz_heartbeat;

  -- FULL logs the whole old row, not just its key -- required for Flink to
  -- build retractions on UPDATE/DELETE.
  ALTER TABLE public.customer REPLICA IDENTITY FULL;
  ALTER TABLE public.payment  REPLICA IDENTITY FULL;
  ALTER TABLE public.rental   REPLICA IDENTITY FULL;
SQL
