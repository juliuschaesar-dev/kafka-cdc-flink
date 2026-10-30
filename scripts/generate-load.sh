#!/usr/bin/env bash
# Keeps dvdrental changing in a varied way, so every dashboard panel moves:
#   - sales: usually one new rental + payment per round, sometimes a burst
#   - hot customers: 30% of sales go to a current top-25 spender, so the ranking reshuffles
#   - prices: mostly the usual 0.99-9.99 tiers, now and then a large one
#   - returns: a generated rental is returned 1-10 days after it was rented (late or not)
#   - re-pricing and refunds: a generated payment is changed or deleted now and then
# A round runs about every GENERATOR_INTERVAL_SECONDS (jittered 50%-150%).
# Only rows created by this script are updated or deleted.
set -euo pipefail

INTERVAL="$GENERATOR_INTERVAL_SECONDS"
export PGPASSWORD="$POSTGRES_PASSWORD"
q() { psql -h postgres -p "$POSTGRES_PORT" -U "$POSTGRES_USER" -d "$POSTGRES_DB" -t -A -c "$1"; }

# Wait on the publication, not the table -- it only exists once the restore
# (and its 14596-row copy) has fully finished.
until [ "$(q "SELECT count(*) FROM pg_publication WHERE pubname = 'dbz_publication'" 2>/dev/null || echo 0)" = "1" ]; do
  sleep 2
done

payment_baseline=$(q 'SELECT COALESCE(max(payment_id), 0) FROM payment')
rental_baseline=$(q 'SELECT COALESCE(max(rental_id), 0) FROM rental')
echo "generator: baseline payment_id = ${payment_baseline}, rental_id = ${rental_baseline}, interval = ~${INTERVAL}s"

# Second guard: dump rows are dated 2007, generated rows are dated now().
readonly GEN_PAYMENT="payment_id > ${payment_baseline} AND payment_date > TIMESTAMP '2020-01-01'"
readonly GEN_RENTAL="rental_id > ${rental_baseline} AND rental_date > TIMESTAMP '2020-01-01'"

readonly CUSTOMER="CASE WHEN random() < 0.3
  THEN (SELECT customer_id FROM (SELECT customer_id FROM payment GROUP BY customer_id
                                 ORDER BY sum(amount) DESC LIMIT 25) hot ORDER BY random() LIMIT 1)
  ELSE (SELECT customer_id FROM customer ORDER BY random() LIMIT 1) END"
readonly AMOUNT="CASE WHEN random() < 0.05
  THEN round((15 + random() * 15)::numeric, 2)
  ELSE (ARRAY[0.99, 1.99, 2.99, 3.99, 4.99, 5.99, 6.99, 7.99, 8.99, 9.99])[1 + floor(random() * 10)::int] END"

# A new rental and its payment in one statement, so the category and store panels move.
sale_with_rental() {
  q "WITH picked AS (SELECT ${CUSTOMER} AS customer_id),
          new_rental AS (
            INSERT INTO rental (rental_date, inventory_id, customer_id, staff_id)
            SELECT now(), (SELECT inventory_id FROM inventory ORDER BY random() LIMIT 1),
                   customer_id, 1 + floor(random() * 2)::int
            FROM picked
            RETURNING rental_id, customer_id, staff_id)
     INSERT INTO payment (customer_id, staff_id, rental_id, amount, payment_date)
     SELECT customer_id, staff_id, rental_id, ${AMOUNT}, now() FROM new_rental" > /dev/null
}

# A payment on an existing rental: moves revenue and customers, not the store stats.
sale_on_existing_rental() {
  q "INSERT INTO payment (customer_id, staff_id, rental_id, amount, payment_date)
     SELECT ${CUSTOMER}, 1 + floor(random() * 2)::int,
            (SELECT rental_id FROM rental ORDER BY random() LIMIT 1), ${AMOUNT}, now()" > /dev/null
}

# The return date is simulated: rental_date + 1-10 days, which is late for films
# rented for fewer days, so the late-return ratio moves both ways.
return_rental() {
  q "UPDATE rental SET return_date = rental_date + (1 + random() * 9) * interval '1 day'
     WHERE rental_id = (SELECT rental_id FROM rental
                        WHERE ${GEN_RENTAL} AND return_date IS NULL ORDER BY random() LIMIT 1)" > /dev/null
}

reprice_payment() {
  q "UPDATE payment SET amount = ${AMOUNT}
     WHERE payment_id = (SELECT payment_id FROM payment WHERE ${GEN_PAYMENT} ORDER BY random() LIMIT 1)" > /dev/null
}

# Keeps a few generated payments around so refunds never empty the dashboard.
refund_payment() {
  q "DELETE FROM payment
     WHERE payment_id = (SELECT payment_id FROM payment WHERE ${GEN_PAYMENT} ORDER BY random() LIMIT 1)
       AND (SELECT count(*) FROM payment WHERE ${GEN_PAYMENT}) > 5" > /dev/null
}

# Queries may fail (e.g. Postgres restart) without killing the loop.
i=0
while true; do
  i=$((i + 1))
  roll=$((RANDOM % 100))
  if   [ "$roll" -lt 10 ]; then sales=$((4 + RANDOM % 3)); kind="burst"
  elif [ "$roll" -lt 35 ]; then sales=2; kind="busy"
  else sales=1; kind="normal"; fi

  done_sales=0
  for _ in $(seq "$sales"); do
    if [ $((RANDOM % 5)) -eq 0 ]; then sale_on_existing_rental; else sale_with_rental; fi \
      && done_sales=$((done_sales + 1)) || echo "generator: sale failed, continuing"
  done
  line="round ${i}: ${done_sales} sale(s) [${kind}]"

  if [ $((i % 3)) -eq 0 ]; then return_rental && line="${line}, return" || echo "generator: return failed, continuing"; fi
  if [ $((i % 4)) -eq 0 ]; then reprice_payment && line="${line}, re-price" || echo "generator: re-price failed, continuing"; fi
  if [ $((i % 7)) -eq 0 ]; then refund_payment && line="${line}, refund" || echo "generator: refund failed, continuing"; fi
  echo "generator: ${line}"

  pause=$((INTERVAL / 2 + RANDOM % (INTERVAL + 1)))
  [ "$pause" -ge 1 ] || pause=1
  sleep "$pause"
done
