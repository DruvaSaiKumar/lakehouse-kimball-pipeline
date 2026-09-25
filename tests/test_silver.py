import duckdb

from data_gen.generate import generate
from pipeline.duckdb_silver import build_silver


def q(sql: str):
    con = duckdb.connect()
    con.execute("SET TimeZone = 'UTC'")
    try:
        return con.execute(sql).fetchall()
    finally:
        con.close()


def test_generator_is_deterministic(tmp_path):
    generate(tmp_path / "a", seed=5)
    generate(tmp_path / "b", seed=5)
    files_a = sorted((tmp_path / "a").rglob("*.csv"))
    files_b = sorted((tmp_path / "b").rglob("*.csv"))
    assert [f.relative_to(tmp_path / "a") for f in files_a] == [
        f.relative_to(tmp_path / "b") for f in files_b
    ]
    assert all(a.read_bytes() == b.read_bytes() for a, b in zip(files_a, files_b, strict=True))


def test_metrics_match_injected_defects_exactly(landing, duckdb_silver):
    expected, m = landing[1], duckdb_silver["metrics"]
    b, p = m["bookings"], m["passengers"]
    assert m["routes"]["silver_rows"] == expected["routes"]
    assert (b["landing_rows"], b["silver_rows"], b["rejected"], b["duplicates_removed"]) == (
        expected["bookings_landing_rows"],
        expected["bookings_silver_rows"],
        expected["bookings_rejected"],
        expected["booking_duplicates_removed"],
    )
    assert b["rejected_by_reason"] == {
        "invalid_status": 8,
        "malformed_value": 10,
        "missing_passenger_id": 10,
        "negative_fare": 15,
        "unknown_route": 8,
    }
    assert (p["landing_rows"], p["silver_rows"], p["rejected"], p["duplicates_removed"]) == (
        expected["passengers_landing_rows"],
        expected["passengers_silver_rows"],
        expected["passengers_rejected"],
        expected["passenger_duplicates_removed"],
    )
    assert p["rejected_by_reason"] == {"invalid_loyalty_tier": 2, "missing_passenger_id": 3}


def test_every_landing_row_is_accounted_for(duckdb_silver):
    """landing = silver + rejected + duplicates removed, for both datasets."""
    for name in ("bookings", "passengers"):
        m = duckdb_silver["metrics"][name]
        assert m["landing_rows"] == m["silver_rows"] + m["rejected"] + m["duplicates_removed"]


def test_quarantine_keeps_raw_values_and_reason(duckdb_silver):
    glob = duckdb_silver["quarantine"].as_posix()
    rows = q(f"""SELECT reject_reason, fare_amount FROM read_parquet('{glob}/bookings/*.parquet')
                 WHERE reject_reason IN ('negative_fare', 'malformed_value')
                 ORDER BY 1, 2""")
    assert ("negative_fare", "-25.00") in rows
    assert {v for reason, v in rows if reason == "malformed_value"} >= {"abc", "12,50", "n/a"}


def test_latest_version_of_a_booking_wins(duckdb_silver):
    glob = duckdb_silver["silver"].as_posix()
    rows = q(f"""SELECT status, CAST(updated_ts AS VARCHAR)
                 FROM read_parquet('{glob}/bookings/**/*.parquet', hive_partitioning = true)
                 WHERE booking_id IN ('B0000001', 'B0000050', 'B0000100')""")
    assert len(rows) == 3
    assert all(status == "CANCELLED" and ts.startswith("2024-06-02 09:00:00") for status, ts in rows)


def test_passenger_text_is_cleaned(duckdb_silver):
    glob = duckdb_silver["silver"].as_posix()
    (bad,) = q(f"""SELECT COUNT(*) FROM read_parquet('{glob}/passengers/*.parquet')
                   WHERE email <> LOWER(TRIM(email)) OR full_name <> TRIM(full_name)""")[0:1]
    assert bad == (0,)


def test_silver_has_no_invalid_rows(duckdb_silver):
    glob = duckdb_silver["silver"].as_posix()
    (bad,) = q(f"""SELECT COUNT(*) FROM read_parquet('{glob}/bookings/**/*.parquet',
                          hive_partitioning = true)
                   WHERE fare_amount < 0 OR passenger_id IS NULL
                      OR status NOT IN ('CONFIRMED','CANCELLED','CHECKED_IN','FLOWN')""")[0:1]
    assert bad == (0,)


def test_rebuilding_is_idempotent(landing, tmp_path):
    first = build_silver(landing[0], tmp_path / "s", tmp_path / "q")
    second = build_silver(landing[0], tmp_path / "s", tmp_path / "q")
    assert first == second
    glob = (tmp_path / "s").as_posix()
    (n,) = q(f"SELECT COUNT(*) FROM read_parquet('{glob}/bookings/**/*.parquet')")[0]
    assert n == first["bookings"]["silver_rows"]
