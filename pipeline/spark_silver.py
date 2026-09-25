"""Silver layer on PySpark: the production-shaped implementation.

Rules are identical to duckdb_silver.py. CI runs both on the same landing data and fails if the
silver tables or the metrics differ.

Design notes:
  * Everything is read as strings and cast explicitly. A failed cast becomes NULL (ANSI mode off),
    which is then reported as `malformed_value` instead of Spark silently mis-typing a column.
  * No Python UDFs: every rule is a native Spark expression, so it stays on the JVM and optimiser.
  * The output is fully rebuilt on each run (idempotent), which is right at this size. At scale this
    is where you would switch to incremental loads keyed on `load_date`.
"""

from __future__ import annotations

import shutil
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from .rules import BOOKING_STATUSES, LOYALTY_TIERS


def get_spark(app_name: str = "lakehouse-silver") -> SparkSession:
    return (
        SparkSession.builder.master("local[2]")
        .appName(app_name)
        .config("spark.ui.enabled", "false")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.ansi.enabled", "false")
        .config("spark.sql.parquet.outputTimestampType", "TIMESTAMP_MICROS")
        .getOrCreate()
    )


def _text(name: str):
    """Trimmed string column; empty becomes NULL."""
    trimmed = F.trim(F.col(name))
    return F.when(trimmed == "", F.lit(None)).otherwise(trimmed)


def _reset(path: Path) -> None:
    if path.exists():
        shutil.rmtree(path)


def _read_csv(spark: SparkSession, path: Path) -> DataFrame:
    df = spark.read.option("header", "true").csv(str(path))
    # Partition discovery infers load_date as a date; keep it a plain string like the reference.
    return df.withColumn("load_date", F.col("load_date").cast("string")) if "load_date" in df.columns else df


def _summarise(classified: DataFrame, silver: DataFrame) -> dict:
    landing_rows = classified.count()
    accepted = classified.filter(F.col("reject_reason").isNull()).count()
    silver_rows = silver.count()
    by_reason = {
        row["reject_reason"]: row["n"]
        for row in classified.filter(F.col("reject_reason").isNotNull())
        .groupBy("reject_reason")
        .agg(F.count("*").alias("n"))
        .collect()
    }
    return {
        "landing_rows": landing_rows,
        "silver_rows": silver_rows,
        "rejected": landing_rows - accepted,
        "rejected_by_reason": dict(sorted(by_reason.items())),
        "duplicates_removed": accepted - silver_rows,
    }


def build_silver(landing: Path, silver: Path, quarantine: Path, spark: SparkSession | None = None) -> dict:
    spark = spark or get_spark()
    for path in (silver, quarantine):
        _reset(path)

    # ---- routes
    routes = (
        _read_csv(spark, landing / "routes")
        .select(
            F.upper(F.trim("route_code")).alias("route_code"),
            F.upper(F.trim("origin")).alias("origin"),
            F.upper(F.trim("destination")).alias("destination"),
            F.trim("distance_miles").cast("int").alias("distance_miles"),
        )
    )  # fmt: skip
    routes.write.mode("overwrite").parquet(str(silver / "routes"))
    route_codes = [r["route_code"] for r in routes.select("route_code").collect()]  # ~50 codes

    # ---- bookings
    raw = _read_csv(spark, landing / "bookings")
    raw_cols = ["booking_id", "passenger_id", "route_code", "booking_ts", "travel_date",
                "fare_amount", "currency", "status", "channel", "updated_ts"]  # fmt: skip
    parsed = raw.select(
        _text("booking_id").alias("booking_id"),
        _text("passenger_id").alias("passenger_id"),
        F.upper(_text("route_code")).alias("route_code"),
        _text("booking_ts").cast("timestamp").alias("booking_ts"),
        _text("travel_date").cast("date").alias("travel_date"),
        _text("fare_amount").cast("decimal(10,2)").alias("fare_amount"),
        F.upper(_text("currency")).alias("currency"),
        F.upper(_text("status")).alias("status"),
        F.upper(_text("channel")).alias("channel"),
        _text("updated_ts").cast("timestamp").alias("updated_ts"),
        F.col("load_date"),
        *[F.col(c).alias(f"raw_{c}") for c in raw_cols],
    )
    classified = parsed.withColumn(
        "reject_reason",
        F.when(
            F.col("booking_id").isNull() | F.col("booking_ts").isNull() | F.col("travel_date").isNull()
            | F.col("fare_amount").isNull() | F.col("updated_ts").isNull(),
            "malformed_value",
        )
        .when(F.col("passenger_id").isNull(), "missing_passenger_id")
        .when(F.col("fare_amount") < 0, "negative_fare")
        .when(F.col("status").isNull() | ~F.col("status").isin(*BOOKING_STATUSES), "invalid_status")
        .when(F.col("route_code").isNull() | ~F.col("route_code").isin(*route_codes), "unknown_route"),
    ).cache()  # fmt: skip

    latest = Window.partitionBy("booking_id").orderBy(F.col("updated_ts").desc(), F.col("load_date").desc())
    bookings_valid = (
        classified.filter(F.col("reject_reason").isNull())
        .withColumn("rn", F.row_number().over(latest))
        .filter(F.col("rn") == 1)
        .select(
            "booking_id", "passenger_id", "route_code", "booking_ts",
            F.to_date("booking_ts").alias("booking_date"),
            "travel_date", "fare_amount", "currency", "status", "channel", "updated_ts", "load_date",
        )
    )  # fmt: skip
    bookings_valid.write.mode("overwrite").partitionBy("booking_date").parquet(str(silver / "bookings"))
    (
        classified.filter(F.col("reject_reason").isNotNull())
        .select(*[F.col(f"raw_{c}").alias(c) for c in raw_cols], "load_date", "reject_reason")
        .write.mode("overwrite").parquet(str(quarantine / "bookings"))
    )  # fmt: skip
    bookings_metrics = _summarise(classified, spark.read.parquet(str(silver / "bookings")))
    classified.unpersist()

    # ---- passengers
    raw_p = _read_csv(spark, landing / "passengers")
    p_cols = ["passenger_id", "full_name", "email", "loyalty_tier", "home_airport", "updated_at"]
    parsed_p = raw_p.select(
        _text("passenger_id").alias("passenger_id"),
        F.trim("full_name").alias("full_name"),
        F.lower(F.trim("email")).alias("email"),
        F.upper(F.trim("loyalty_tier")).alias("loyalty_tier"),
        F.upper(F.trim("home_airport")).alias("home_airport"),
        _text("updated_at").cast("date").alias("updated_at"),
        F.col("load_date"),
        *[F.col(c).alias(f"raw_{c}") for c in p_cols],
    )
    classified_p = parsed_p.withColumn(
        "reject_reason",
        F.when(F.col("updated_at").isNull(), "malformed_value")
        .when(F.col("passenger_id").isNull(), "missing_passenger_id")
        .when(
            F.col("loyalty_tier").isNull() | ~F.col("loyalty_tier").isin(*LOYALTY_TIERS),
            "invalid_loyalty_tier",
        ),
    ).cache()  # fmt: skip
    latest_p = Window.partitionBy("passenger_id", "updated_at").orderBy(F.col("load_date").desc())
    passengers_valid = (
        classified_p.filter(F.col("reject_reason").isNull())
        .withColumn("rn", F.row_number().over(latest_p))
        .filter(F.col("rn") == 1)
        .select(*p_cols, "load_date")
    )
    passengers_valid.write.mode("overwrite").parquet(str(silver / "passengers"))
    (
        classified_p.filter(F.col("reject_reason").isNotNull())
        .select(*[F.col(f"raw_{c}").alias(c) for c in p_cols], "load_date", "reject_reason")
        .write.mode("overwrite").parquet(str(quarantine / "passengers"))
    )  # fmt: skip
    passengers_metrics = _summarise(classified_p, spark.read.parquet(str(silver / "passengers")))
    classified_p.unpersist()

    return {
        "routes": {"silver_rows": routes.count()},
        "bookings": bookings_metrics,
        "passengers": passengers_metrics,
    }
