"""Live dashboard over the ClickHouse datamarts that Flink produces."""
import os

import requests
from flask import Flask, jsonify, render_template

CLICKHOUSE_URL = f"http://clickhouse:{os.environ['CLICKHOUSE_HTTP_PORT']}"
DATABASE = os.environ["CLICKHOUSE_DB"]
AUTH = (os.environ["CLICKHOUSE_USER"], os.environ["CLICKHOUSE_PASSWORD"])
REFRESH_MS = 2000

DATAMARTS = [
    "enriched_payments",
    "revenue_monthly",
    "revenue_by_category",
    "top_customers",
    "store_rental_stats",
]

app = Flask(__name__)


def query(sql):
    """Run one statement and return its rows as dicts.

    The server's default final = 1 (docker-compose.yml) makes every result the
    latest version per key.
    """
    response = requests.post(
        CLICKHOUSE_URL,
        params={
            "database": DATABASE,
            "default_format": "JSON",
            "output_format_json_quote_64bit_integers": 0,
        },
        data=sql.encode(),
        auth=AUTH,
        timeout=10,
    )
    if response.status_code != 200:
        raise RuntimeError(response.text.strip().splitlines()[0])
    return response.json()["data"]


ACTIVITY_SQL = " UNION ALL ".join(
    f"SELECT '{t}' AS datamart, count() AS `rows`, "
    f"countIf(updated_at > now() - 60) AS updated_last_minute, "
    f"if(count() = 0, NULL, dateDiff('second', max(updated_at), now())) AS seconds_since_update "
    f"FROM {t}"
    for t in DATAMARTS
)

QUERIES = {
    "totals": "SELECT sum(revenue) AS revenue, sum(payment_count) AS payments FROM revenue_monthly",
    "monthly": "SELECT revenue_month, payment_count, revenue FROM revenue_monthly ORDER BY revenue_month",
    "categories": "SELECT category, payment_count, revenue FROM revenue_by_category ORDER BY revenue DESC",
    "top_customers": "SELECT rn, customer_name, payment_count, total_spent FROM top_customers ORDER BY rn",
    "stores": (
        "SELECT store_id, rentals, returned, round(avg_rental_days, 2) AS avg_rental_days, "
        "late_count, round(late_ratio * 100, 1) AS late_pct FROM store_rental_stats ORDER BY store_id"
    ),
    # Newest minutes that had a payment, oldest first so the chart reads left to right.
    "timeline": (
        "SELECT * FROM (SELECT formatDateTime(window_start, '%Y-%m-%d %H:%i') AS minute, "
        "sum(total_amount) AS revenue, sum(payment_count) AS payments "
        "FROM enriched_payments GROUP BY window_start ORDER BY window_start DESC LIMIT 30) "
        "ORDER BY minute"
    ),
    "feed": (
        "SELECT formatDateTime(window_start, '%Y-%m-%d %H:%i') AS minute, customer_name, "
        "payment_count, total_amount, formatDateTime(updated_at, '%H:%i:%S') AS updated "
        "FROM enriched_payments ORDER BY updated_at DESC, window_start DESC LIMIT 12"
    ),
    "activity": ACTIVITY_SQL,
}


@app.get("/")
def index():
    return render_template("index.html", refresh_ms=REFRESH_MS)


@app.get("/api/data")
def data():
    try:
        payload = {name: query(sql) for name, sql in QUERIES.items()}
        payload["server_time"] = query("SELECT formatDateTime(now(), '%H:%i:%S') AS t")[0]["t"]
    except Exception as error:  # ClickHouse down or tables not created yet
        return jsonify(error=str(error)), 503
    return jsonify(payload)
