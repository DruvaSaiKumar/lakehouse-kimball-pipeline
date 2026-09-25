"""The Kimball layer, verified by building it with dbt (which also runs all 29 dbt data tests)."""


def scalar(con, sql):
    return con.execute(sql).fetchone()[0]


def test_row_counts_match_expectations(landing, wh):
    expected = landing[1]
    assert scalar(wh, "select count(*) from fact_bookings") == expected["bookings_silver_rows"]
    # 350 real versions + the unknown member
    assert scalar(wh, "select count(*) from dim_passenger") == expected["dim_passenger_versions"] + 1
    assert scalar(wh, "select count(*) from dim_route") == expected["routes"] + 1


def test_no_op_updates_do_not_create_versions(wh):
    """P000101-P000110 were re-sent unchanged on day 2 and must stay a single version."""
    rows = wh.execute("""
        select passenger_id, count(*) from dim_passenger
        where passenger_id between 'P000101' and 'P000110' group by 1""").fetchall()
    assert len(rows) == 10 and all(n == 1 for _, n in rows)


def test_changed_passengers_have_two_contiguous_versions(wh):
    rows = wh.execute("""
        select passenger_id, count(*), min(valid_from), max(valid_to), sum(is_current::int)
        from dim_passenger where passenger_id between 'P000001' and 'P000030' group by 1""").fetchall()
    assert len(rows) == 30
    for _, versions, first_from, last_to, current in rows:
        assert (versions, str(first_from), str(last_to), current) == (2, "1900-01-01", "9999-12-31", 1)


def test_bookings_resolve_to_the_version_valid_on_the_booking_date(wh):
    """The point-in-time join: every attributed booking falls inside its version's window."""
    bad = scalar(
        wh,
        """
        select count(*) from fact_bookings f
        join stg_bookings b using (booking_id)
        join dim_passenger p on p.passenger_key = f.passenger_key
        where f.passenger_key <> '-1'
          and not (b.booking_date >= p.valid_from and b.booking_date < p.valid_to)""",
    )
    assert bad == 0


def test_bookings_get_the_tier_held_on_the_booking_date(wh):
    """Independent recomputation: the tier in force on the booking date, from the staging history."""
    mismatches = scalar(
        wh,
        """
        with expected as (
            select b.booking_id, arg_max(s.loyalty_tier, s.updated_at) as tier
            from stg_bookings b
            join stg_passengers s
              on s.passenger_id = b.passenger_id and s.updated_at <= b.booking_date
            group by b.booking_id)
        select count(*) from fact_bookings f
        join dim_passenger p using (passenger_key)
        join expected e using (booking_id)
        where p.loyalty_tier <> e.tier""",
    )
    assert mismatches == 0
    # The check is only meaningful if some bookings were made under a later (non-first) version.
    later_versions = scalar(
        wh,
        """
        select count(*) from fact_bookings f join dim_passenger p using (passenger_key)
        where p.valid_from > date '1900-01-01'""",
    )
    assert later_versions > 0


def test_late_arriving_passengers_map_to_the_unknown_member(landing, wh):
    unknown = scalar(wh, "select count(*) from fact_bookings where passenger_key = '-1'")
    assert unknown == landing[1]["bookings_unknown_passenger"]


def test_marts_reconcile_to_the_fact(wh):
    fact_bookings = scalar(wh, "select count(*) from fact_bookings")
    fact_revenue = scalar(wh, "select sum(fare_amount) from fact_bookings where not is_cancelled")
    assert scalar(wh, "select sum(bookings) from mart_daily_route_revenue") == fact_bookings
    assert scalar(wh, "select sum(gross_revenue) from mart_daily_route_revenue") == fact_revenue
    assert scalar(wh, "select sum(bookings) from mart_revenue_by_loyalty_tier") == fact_bookings
    assert scalar(wh, "select sum(gross_revenue) from mart_revenue_by_loyalty_tier") == fact_revenue


def test_cancelling_updates_flow_through_to_the_fact(wh):
    assert scalar(
        wh,
        "select bool_and(is_cancelled) from fact_bookings where booking_id in "
        "('B0000001','B0000050','B0000100')",
    )
