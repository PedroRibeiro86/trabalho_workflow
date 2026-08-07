from __future__ import annotations

import json
from datetime import timedelta

import pendulum
import requests
from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook

POSTGRES_CONN_ID = "postgres_default"
TIMEZONE = "America/Sao_Paulo"


def _hook() -> PostgresHook:
    return PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)


@dag(
    dag_id="bitcoin_medallion_pipeline",
    schedule="0 8 * * *",
    start_date=pendulum.datetime(2026, 1, 1, tz=TIMEZONE),
    catchup=False,
    tags=["final-project", "bitcoin", "medallion"],
    default_args={
        "owner": "aluno",
        "retries": 2,
        "retry_delay": timedelta(minutes=2),
    },
)
def bitcoin_medallion_pipeline():
    @task
    def create_tables() -> None:
        sql = """
        CREATE SCHEMA IF NOT EXISTS workflow;

        CREATE TABLE IF NOT EXISTS workflow.bronze_bitcoin_quotes (
            quote_date DATE PRIMARY KEY,
            fetched_at TIMESTAMPTZ NOT NULL,
            source TEXT NOT NULL,
            raw_payload JSONB NOT NULL
        );

        CREATE TABLE IF NOT EXISTS workflow.silver_bitcoin_quotes (
            quote_date DATE PRIMARY KEY,
            price_usd NUMERIC(18, 2) NOT NULL,
            market_cap_usd NUMERIC(20, 2),
            total_volume_usd NUMERIC(20, 2),
            updated_at TIMESTAMPTZ NOT NULL
        );

        CREATE TABLE IF NOT EXISTS workflow.gold_bitcoin_daily_metrics (
            quote_date DATE PRIMARY KEY,
            price_usd NUMERIC(18, 2) NOT NULL,
            price_change_pct_1d NUMERIC(10, 4),
            updated_at TIMESTAMPTZ NOT NULL
        );
        """
        _hook().run(sql)

    @task
    def extract_to_bronze(ds: str) -> None:
        response = requests.get(
            "https://api.coingecko.com/api/v3/simple/price",
            params={
                "ids": "bitcoin",
                "vs_currencies": "usd",
                "include_market_cap": "true",
                "include_24hr_vol": "true",
            },
            timeout=15,
        )
        response.raise_for_status()

        payload = response.json()
        fetched_at = pendulum.now("UTC")

        upsert = """
        INSERT INTO workflow.bronze_bitcoin_quotes (quote_date, fetched_at, source, raw_payload)
        VALUES (%s, %s, %s, %s::jsonb)
        ON CONFLICT (quote_date)
        DO UPDATE SET
            fetched_at = EXCLUDED.fetched_at,
            source = EXCLUDED.source,
            raw_payload = EXCLUDED.raw_payload;
        """

        _hook().run(
            upsert,
            parameters=(
                ds,
                fetched_at,
                "coingecko_simple_price",
                json.dumps(payload),
            ),
        )

    @task
    def transform_to_silver(ds: str) -> None:
        select_sql = """
        SELECT raw_payload
        FROM workflow.bronze_bitcoin_quotes
        WHERE quote_date = %s;
        """
        records = _hook().get_records(select_sql, parameters=(ds,))
        if not records:
            raise ValueError(f"No bronze record found for date {ds}")

        payload = records[0][0]
        if isinstance(payload, str):
            payload = json.loads(payload)

        bitcoin_data = payload["bitcoin"]
        price_usd = float(bitcoin_data["usd"])
        market_cap_usd = float(bitcoin_data.get("usd_market_cap", 0))
        total_volume_usd = float(bitcoin_data.get("usd_24h_vol", 0))

        upsert = """
        INSERT INTO workflow.silver_bitcoin_quotes
            (quote_date, price_usd, market_cap_usd, total_volume_usd, updated_at)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (quote_date)
        DO UPDATE SET
            price_usd = EXCLUDED.price_usd,
            market_cap_usd = EXCLUDED.market_cap_usd,
            total_volume_usd = EXCLUDED.total_volume_usd,
            updated_at = EXCLUDED.updated_at;
        """

        _hook().run(
            upsert,
            parameters=(ds, price_usd, market_cap_usd, total_volume_usd, pendulum.now("UTC")),
        )

    @task
    def build_gold(ds: str) -> None:
        current_sql = """
        SELECT quote_date, price_usd
        FROM workflow.silver_bitcoin_quotes
        WHERE quote_date = %s;
        """
        current_rows = _hook().get_records(current_sql, parameters=(ds,))
        if not current_rows:
            raise ValueError(f"No silver record found for date {ds}")

        quote_date, current_price = current_rows[0]
        previous_date = pendulum.parse(ds).subtract(days=1).to_date_string()

        prev_sql = """
        SELECT price_usd
        FROM workflow.silver_bitcoin_quotes
        WHERE quote_date = %s;
        """
        prev_rows = _hook().get_records(prev_sql, parameters=(previous_date,))

        price_change_pct_1d = None
        if prev_rows:
            previous_price = float(prev_rows[0][0])
            if previous_price != 0:
                price_change_pct_1d = ((float(current_price) - previous_price) / previous_price) * 100

        upsert = """
        INSERT INTO workflow.gold_bitcoin_daily_metrics
            (quote_date, price_usd, price_change_pct_1d, updated_at)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (quote_date)
        DO UPDATE SET
            price_usd = EXCLUDED.price_usd,
            price_change_pct_1d = EXCLUDED.price_change_pct_1d,
            updated_at = EXCLUDED.updated_at;
        """

        _hook().run(
            upsert,
            parameters=(quote_date, float(current_price), price_change_pct_1d, pendulum.now("UTC")),
        )

    tables = create_tables()
    bronze = extract_to_bronze()
    silver = transform_to_silver()
    gold = build_gold()

    tables >> bronze >> silver >> gold


bitcoin_medallion_pipeline()
