from datetime import datetime, timedelta

import psycopg2
from airflow import DAG
from airflow.operators.python import PythonOperator

PG_CONN = dict(
    host="postgres",
    port=5432,
    dbname="cryptodb",
    user="cryptouser",
    password="cryptopass",
)

CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS daily_prices (
    price_date      DATE NOT NULL,
    coin_id         TEXT NOT NULL,
    symbol          TEXT,
    avg_price_usd   NUMERIC,
    min_price_usd   NUMERIC,
    max_price_usd   NUMERIC,
    close_price_usd NUMERIC,
    avg_change_24h  NUMERIC,
    avg_market_cap  NUMERIC,
    avg_volume_24h  NUMERIC,
    rows_counted    INTEGER,
    loaded_at       TIMESTAMP DEFAULT now(),
    PRIMARY KEY (price_date, coin_id)
);
"""

TRANSFORM_SQL = """
WITH day_data AS (
    SELECT *
    FROM crypto_prices
    WHERE fetched_at::date = %(ds)s
),
agg AS (
    SELECT
        coin_id,
        MAX(symbol)                 AS symbol,
        AVG(price_usd)              AS avg_price_usd,
        MIN(price_usd)              AS min_price_usd,
        MAX(price_usd)              AS max_price_usd,
        AVG(change_24h)             AS avg_change_24h,
        AVG(market_cap)             AS avg_market_cap,
        AVG(volume_24h)             AS avg_volume_24h,
        COUNT(*)                    AS rows_counted
    FROM day_data
    GROUP BY coin_id
),
last_price AS (
    SELECT DISTINCT ON (coin_id)
        coin_id, price_usd AS close_price_usd
    FROM day_data
    ORDER BY coin_id, fetched_at DESC
)
SELECT
    %(ds)s::date AS price_date,
    agg.coin_id,
    agg.symbol,
    agg.avg_price_usd,
    agg.min_price_usd,
    agg.max_price_usd,
    last_price.close_price_usd,
    agg.avg_change_24h,
    agg.avg_market_cap,
    agg.avg_volume_24h,
    agg.rows_counted
FROM agg
JOIN last_price ON last_price.coin_id = agg.coin_id;
"""

UPSERT_SQL = """
INSERT INTO daily_prices (
    price_date, coin_id, symbol, avg_price_usd, min_price_usd, max_price_usd,
    close_price_usd, avg_change_24h, avg_market_cap, avg_volume_24h, rows_counted
) VALUES (
    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
)
ON CONFLICT (price_date, coin_id) DO UPDATE SET
    symbol          = EXCLUDED.symbol,
    avg_price_usd   = EXCLUDED.avg_price_usd,
    min_price_usd   = EXCLUDED.min_price_usd,
    max_price_usd   = EXCLUDED.max_price_usd,
    close_price_usd = EXCLUDED.close_price_usd,
    avg_change_24h  = EXCLUDED.avg_change_24h,
    avg_market_cap  = EXCLUDED.avg_market_cap,
    avg_volume_24h  = EXCLUDED.avg_volume_24h,
    rows_counted    = EXCLUDED.rows_counted,
    loaded_at       = now();
"""


def transform_daily_prices(ds, **_):
    conn = psycopg2.connect(**PG_CONN)
    conn.autocommit = False
    try:
        with conn.cursor() as cur:
            cur.execute(CREATE_TABLE_SQL)
            cur.execute(TRANSFORM_SQL, {"ds": ds})
            rows = cur.fetchall()

            if not rows:
                print(f"[{ds}] crypto_prices-da bu günə aid data tapılmadı.")
            else:
                for row in rows:
                    cur.execute(UPSERT_SQL, row)
                print(f"[{ds}] {len(rows)} sikkə üçün daily_prices yeniləndi.")
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="daily_prices_transform",
    description="crypto_prices cədvəlini günlük aqreqasiya edib daily_prices-a yazır",
    default_args=default_args,
    schedule_interval="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["crypto", "postgres", "transform"],
) as dag:

    transform_task = PythonOperator(
        task_id="transform_daily_prices",
        python_callable=transform_daily_prices,
        op_kwargs={"ds": "{{ ds }}"},
    )