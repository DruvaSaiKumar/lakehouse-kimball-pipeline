"""Silver layer on DuckDB. Same rules as spark_silver.py; runs anywhere without a JVM.

It exists for two reasons: fast local development, and as an independent reference
implementation that CI compares against the Spark output row for row.
"""

from __future__ import annotations

import shutil
from pathlib import Path

import duckdb

from .rules import BOOKING_STATUSES, LOYALTY_TIERS


def _sql_list(values: tuple[str, ...]) -> str:
    return ", ".join(f"'{v}'" for v in values)


def _reset(path: Path) -> None:
    if path.exists():
        shutil.rmtree(path)
    path.mkdir(parents=True)


def _copy(con, query: str, target: Path, partition_by: str | None = None) -> None:
    """Write a directory of Parquet files (like Spark does), never a single bare file."""
    if partition_by:
        con.execute(
            f"COPY ({query}) TO '{target.as_posix()}' (FORMAT PARQUET, PARTITION_BY ({partition_by}))"
        )
    else:
        target.mkdir(parents=True)
        con.execute(f"COPY ({query}) TO '{(target / 'part-0.parquet').as_posix()}' (FORMAT PARQUET)")


def _by_reason(con, table: str) -> dict[str, int]:
    rows = con.execute(
        f"SELECT reject_reason, COUNT(*) FROM {table} WHERE reject_reason IS NOT NULL GROUP BY 1"
    ).fetchall()
    return {reason: n for reason, n in rows}


def _scalar(con, sql: str) -> int:
    return int(con.execute(sql).fetchone()[0])


def build_silver(landing: Path, silver: Path, quarantine: Path) -> dict:
    for path in (silver, quarantine):
        _reset(path)
    con = duckdb.connect()
    con.execute("SET TimeZone = 'UTC'")

    def csv(dataset: str, partitioned: bool = True) -> str:
        glob = f"{landing.as_posix()}/{dataset}/{'*/' if partitioned else ''}*.csv"
        hive = str(partitioned).lower()
        return f"read_csv('{glob}', header = true, all_varchar = true, hive_partitioning = {hive})"

    # ---- routes: static reference data
    con.execute(f"""
        CREATE TEMP TABLE routes AS
        SELECT UPPER(TRIM(route_code)) AS route_code, UPPER(TRIM(origin)) AS origin,
               UPPER(TRIM(destination)) AS destination,
               CAST(TRIM(distance_miles) AS INTEGER) AS distance_miles
        FROM {csv("routes", partitioned=False)}
    """)
    _copy(con, "SELECT * FROM routes", silver / "routes")

    # ---- bookings
    con.execute(f"""
        CREATE TEMP TABLE bookings_raw AS
        SELECT *, CAST(load_date AS VARCHAR) AS load_date_str FROM {csv("bookings")}
    """)
    con.execute(f"""
        CREATE TEMP TABLE bookings_classified AS
        WITH parsed AS (
          SELECT
            NULLIF(TRIM(booking_id), '') AS booking_id,
            NULLIF(TRIM(passenger_id), '') AS passenger_id,
            UPPER(NULLIF(TRIM(route_code), '')) AS route_code,
            TRY_CAST(NULLIF(TRIM(booking_ts), '') AS TIMESTAMP) AS booking_ts,
            TRY_CAST(NULLIF(TRIM(travel_date), '') AS DATE) AS travel_date,
            TRY_CAST(NULLIF(TRIM(fare_amount), '') AS DECIMAL(10, 2)) AS fare_amount,
            UPPER(NULLIF(TRIM(currency), '')) AS currency,
            UPPER(NULLIF(TRIM(status), '')) AS status,
            UPPER(NULLIF(TRIM(channel), '')) AS channel,
            TRY_CAST(NULLIF(TRIM(updated_ts), '') AS TIMESTAMP) AS updated_ts,
            load_date_str AS load_date,
            booking_id AS raw_booking_id, passenger_id AS raw_passenger_id,
            route_code AS raw_route_code, booking_ts AS raw_booking_ts,
            travel_date AS raw_travel_date, fare_amount AS raw_fare_amount,
            currency AS raw_currency, status AS raw_status, channel AS raw_channel,
            updated_ts AS raw_updated_ts
          FROM bookings_raw
        )
        SELECT *,
          CASE
            WHEN booking_id IS NULL OR booking_ts IS NULL OR travel_date IS NULL
                 OR fare_amount IS NULL OR updated_ts IS NULL THEN 'malformed_value'
            WHEN passenger_id IS NULL THEN 'missing_passenger_id'
            WHEN fare_amount < 0 THEN 'negative_fare'
            WHEN status IS NULL OR status NOT IN ({_sql_list(BOOKING_STATUSES)}) THEN 'invalid_status'
            WHEN route_code IS NULL OR route_code NOT IN (SELECT route_code FROM routes)
                 THEN 'unknown_route'
          END AS reject_reason
        FROM parsed
    """)
    con.execute("""
        CREATE TEMP TABLE bookings_valid AS
        SELECT booking_id, passenger_id, route_code, booking_ts, CAST(booking_ts AS DATE) AS booking_date,
               travel_date, fare_amount, currency, status, channel, updated_ts, load_date
        FROM (
          SELECT *, ROW_NUMBER() OVER (
                   PARTITION BY booking_id ORDER BY updated_ts DESC, load_date DESC) AS rn
          FROM bookings_classified WHERE reject_reason IS NULL
        ) WHERE rn = 1
    """)
    _copy(con, "SELECT * FROM bookings_valid", silver / "bookings", partition_by="booking_date")
    _copy(
        con,
        """SELECT raw_booking_id AS booking_id, raw_passenger_id AS passenger_id,
                  raw_route_code AS route_code, raw_booking_ts AS booking_ts,
                  raw_travel_date AS travel_date, raw_fare_amount AS fare_amount,
                  raw_currency AS currency, raw_status AS status, raw_channel AS channel,
                  raw_updated_ts AS updated_ts, load_date, reject_reason
           FROM bookings_classified WHERE reject_reason IS NOT NULL""",
        quarantine / "bookings",
    )

    # ---- passengers
    con.execute(f"""
        CREATE TEMP TABLE passengers_raw AS
        SELECT *, CAST(load_date AS VARCHAR) AS load_date_str FROM {csv("passengers")}
    """)
    con.execute(f"""
        CREATE TEMP TABLE passengers_classified AS
        WITH parsed AS (
          SELECT
            NULLIF(TRIM(passenger_id), '') AS passenger_id,
            TRIM(full_name) AS full_name,
            LOWER(TRIM(email)) AS email,
            UPPER(TRIM(loyalty_tier)) AS loyalty_tier,
            UPPER(TRIM(home_airport)) AS home_airport,
            TRY_CAST(NULLIF(TRIM(updated_at), '') AS DATE) AS updated_at,
            load_date_str AS load_date,
            passenger_id AS raw_passenger_id, full_name AS raw_full_name, email AS raw_email,
            loyalty_tier AS raw_loyalty_tier, home_airport AS raw_home_airport,
            updated_at AS raw_updated_at
          FROM passengers_raw
        )
        SELECT *,
          CASE
            WHEN updated_at IS NULL THEN 'malformed_value'
            WHEN passenger_id IS NULL THEN 'missing_passenger_id'
            WHEN loyalty_tier IS NULL OR loyalty_tier NOT IN ({_sql_list(LOYALTY_TIERS)})
                 THEN 'invalid_loyalty_tier'
          END AS reject_reason
        FROM parsed
    """)
    con.execute("""
        CREATE TEMP TABLE passengers_valid AS
        SELECT passenger_id, full_name, email, loyalty_tier, home_airport, updated_at, load_date
        FROM (
          SELECT *, ROW_NUMBER() OVER (
                   PARTITION BY passenger_id, updated_at ORDER BY load_date DESC) AS rn
          FROM passengers_classified WHERE reject_reason IS NULL
        ) WHERE rn = 1
    """)
    _copy(con, "SELECT * FROM passengers_valid", silver / "passengers")
    _copy(
        con,
        """SELECT raw_passenger_id AS passenger_id, raw_full_name AS full_name, raw_email AS email,
                  raw_loyalty_tier AS loyalty_tier, raw_home_airport AS home_airport,
                  raw_updated_at AS updated_at, load_date, reject_reason
           FROM passengers_classified WHERE reject_reason IS NOT NULL""",
        quarantine / "passengers",
    )

    metrics = {"routes": {"silver_rows": _scalar(con, "SELECT COUNT(*) FROM routes")}}
    for name in ("bookings", "passengers"):
        classified, valid = f"{name}_classified", f"{name}_valid"
        landing_rows = _scalar(con, f"SELECT COUNT(*) FROM {classified}")
        accepted = _scalar(con, f"SELECT COUNT(*) FROM {classified} WHERE reject_reason IS NULL")
        silver_rows = _scalar(con, f"SELECT COUNT(*) FROM {valid}")
        metrics[name] = {
            "landing_rows": landing_rows,
            "silver_rows": silver_rows,
            "rejected": landing_rows - accepted,
            "rejected_by_reason": dict(sorted(_by_reason(con, classified).items())),
            "duplicates_removed": accepted - silver_rows,
        }
    con.close()
    return metrics
