#!/usr/bin/env bash
# Keeps dvdrental changing: inserts a payment every GENERATOR_INTERVAL_SECONDS,
# updates one every 5th round, deletes one every 10th.
set -euo pipefail

INTERVAL="${GENERATOR_INTERVAL_SECONDS:-5}"
export PGPASSWORD="$POSTGRES_PASSWORD"
q() { psql -h postgres -p "$POSTGRES_PORT" -U "$POSTGRES_USER" -d "$POSTGRES_DB" -t -A -c "$1"; }

# Wait on the publication, not the table -- it only exists once the restore
# (and its 14596-row copy) has fully finished.
until [ "$(q "SELECT count(*) FROM pg_publication WHERE pubname = 'dbz_publication'" 2>/dev/null || echo 0)" = "1" ]; do
  sleep 2
done

baseline=$(q 'SELECT COALESCE(max(payment_id), 0) FROM payment')
echo "generator: baseline payment_id = ${baseline}, interval = ${INTERVAL}s"

# Second guard: dump rows are dated 2007, generated rows are dated now().
readonly GENERATED="payment_id > ${baseline} AND payment_date > TIMESTAMP '2020-01-01'"

# Queries may fail (e.g. Postgres restart) without killing the loop.
i=0
while true; do
  i=$((i + 1))

  q "INSERT INTO payment (customer_id, staff_id, rental_id, amount, payment_date)
     VALUES (
       (SELECT customer_id FROM customer ORDER BY random() LIMIT 1),
       1,
       (SELECT rental_id FROM rental ORDER BY random() LIMIT 1),
       round((random() * 9 + 0.99)::numeric, 2),
       now()
     )" > /dev/null || echo "generator: insert failed, continuing"

  if [ $((i % 5)) -eq 0 ]; then
    q "UPDATE payment SET amount = round((random() * 9 + 0.99)::numeric, 2)
       WHERE payment_id = (
         SELECT payment_id FROM payment WHERE ${GENERATED} ORDER BY random() LIMIT 1
       )" > /dev/null || echo "generator: update failed, continuing"
  fi

  if [ $((i % 10)) -eq 0 ]; then
    q "DELETE FROM payment
       WHERE payment_id = (
         SELECT payment_id FROM payment WHERE ${GENERATED} ORDER BY random() LIMIT 1
       )" > /dev/null || echo "generator: delete failed, continuing"
    echo "generator: ${i} rounds (inserts + updates + deletes)"
  fi

  sleep "$INTERVAL"
done
