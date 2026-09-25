"""Compare the PySpark silver layer with the DuckDB reference, table by table.

Skipped when Java/PySpark are unavailable or on Windows without winutils. CI runs it on Linux.
"""

import os
import shutil
import sys

import duckdb
import pytest

pytest.importorskip("pyspark")
pytestmark = pytest.mark.skipif(
    shutil.which("java") is None or (sys.platform == "win32" and not os.environ.get("HADOOP_HOME")),
    reason="needs Java, and on Windows HADOOP_HOME/winutils; runs in CI on Linux",
)

from pipeline.spark_silver import build_silver, get_spark  # noqa: E402

from .conftest import run_dbt  # noqa: E402

DATASETS = {
    "silver": ["bookings/**/*.parquet", "passengers/*.parquet", "routes/*.parquet"],
    "quarantine": ["bookings/*.parquet", "passengers/*.parquet"],
}


@pytest.fixture(scope="module")
def spark_out(landing, tmp_path_factory):
    base = tmp_path_factory.mktemp("spark_engine")
    spark = get_spark("parity-test")
    try:
        metrics = build_silver(landing[0], base / "silver", base / "quarantine", spark)
    finally:
        spark.stop()
    return {"silver": base / "silver", "quarantine": base / "quarantine", "metrics": metrics}


def test_metrics_are_identical(spark_out, duckdb_silver):
    assert spark_out["metrics"] == duckdb_silver["metrics"]


@pytest.mark.parametrize(("layer", "pattern"), [(k, p) for k, ps in DATASETS.items() for p in ps])
def test_tables_are_identical(layer, pattern, spark_out, duckdb_silver):
    """Symmetric difference of the two engines' outputs must be empty, in both directions."""
    con = duckdb.connect()
    con.execute("SET TimeZone = 'UTC'")

    def relation(root):
        path = f"{root[layer].as_posix()}/{pattern}"
        cols = con.execute(
            f"DESCRIBE SELECT * FROM read_parquet('{path}', hive_partitioning = true)"
        ).fetchall()
        # Spark marks timestamps UTC-adjusted and DuckDB does not; normalise both to naive UTC.
        select = ", ".join(
            f'CAST("{name}" AS TIMESTAMP) AS "{name}"' if "TIMESTAMP" in kind else f'"{name}"'
            for name, kind, *_ in cols
        )
        return f"SELECT {select} FROM read_parquet('{path}', hive_partitioning = true)"

    spark_sql, duck_sql = relation(spark_out), relation(duckdb_silver)
    assert con.execute(f"SELECT COUNT(*) FROM ({spark_sql})").fetchone()[0] > 0
    assert con.execute(f"({spark_sql}) EXCEPT ({duck_sql})").fetchall() == []
    assert con.execute(f"({duck_sql}) EXCEPT ({spark_sql})").fetchall() == []


def test_dbt_builds_the_same_star_schema_from_spark_output(spark_out, warehouse, tmp_path):
    spark_wh = run_dbt("build", spark_out["silver"], tmp_path)
    tables = ["fact_bookings", "dim_passenger", "dim_route", "mart_daily_route_revenue"]
    a, b = duckdb.connect(str(spark_wh), read_only=True), duckdb.connect(str(warehouse), read_only=True)
    for table in tables:
        assert (
            a.execute(f"select * from {table} order by all").fetchall()
            == b.execute(f"select * from {table} order by all").fetchall()
        )
