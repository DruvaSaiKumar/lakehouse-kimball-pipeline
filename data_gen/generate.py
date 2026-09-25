"""Synthetic airline-style bookings with a known set of injected problems (seeded).

Two daily loads are produced, as an incremental extract would deliver them:

    landing/routes/routes.csv                      static reference data
    landing/passengers/load_date=D/passengers.csv  day 1 = full extract, day 2 = changes only
    landing/bookings/load_date=D/bookings.csv      new bookings, updates to old ones, and defects

Nothing here comes from any real airline or employer.
"""

from __future__ import annotations

import argparse
import csv
import random
import shutil
from datetime import date, datetime, timedelta
from pathlib import Path

DAY1, DAY2 = date(2024, 6, 1), date(2024, 6, 2)
AIRPORTS = ["DAL", "HOU", "AUS", "SAT", "DEN", "PHX", "LAS", "MDW"]
TIERS = ["NONE", "BLUE", "SILVER", "GOLD"]
FIRST = ["Ava", "Liam", "Noah", "Mia", "Zoe", "Ethan", "Ivy", "Owen", "Ruby", "Leo", "Nina", "Omar"]
LAST = ["Reyes", "Chen", "Patel", "Nguyen", "Kim", "Garcia", "Singh", "Lopez", "Brown", "Khan"]
CHANNELS = ["WEB", "MOBILE", "AGENT"]

PASSENGER_COLS = ["passenger_id", "full_name", "email", "loyalty_tier", "home_airport", "updated_at"]
BOOKING_COLS = [
    "booking_id", "passenger_id", "route_code", "booking_ts", "travel_date",
    "fare_amount", "currency", "status", "channel", "updated_ts",
]  # fmt: skip
ROUTE_COLS = ["route_code", "origin", "destination", "distance_miles"]

# What day 2 injects. The tests and the README quote these, so change them together.
N_DAY1_PASSENGERS, N_DAY1_BOOKINGS = 300, 2000
N_NEW_PASSENGERS, N_TIER_OR_AIRPORT_CHANGES, N_NOOP_UPDATES = 20, 30, 10
N_PASSENGER_DUPLICATES, N_PASSENGER_MISSING_ID, N_PASSENGER_BAD_TIER = 5, 3, 2
N_NEW_BOOKINGS, N_STATUS_UPDATES, N_BOOKING_DUPLICATES = 1500, 100, 5
N_NEGATIVE_FARE, N_MISSING_PASSENGER, N_BAD_STATUS, N_UNKNOWN_ROUTE = 15, 10, 8, 8
N_MALFORMED, N_UNKNOWN_PASSENGER = 10, 12


def _write(path: Path, header: list[str], rows: list[list]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.writer(fh)
        writer.writerow(header)
        writer.writerows(rows)


def _routes(rng: random.Random) -> list[list]:
    rows = []
    for origin in AIRPORTS:
        for destination in AIRPORTS:
            if origin != destination:
                rows.append([f"{origin}-{destination}", origin, destination, rng.randint(150, 1900)])
    return rows


def _passenger(n: int, rng: random.Random, updated: date) -> list:
    first, last = rng.choice(FIRST), rng.choice(LAST)
    return [
        f"P{n:06d}", f"{first} {last}", f"{first}.{last}{n}@example.com".lower(),
        rng.choices(TIERS, weights=[50, 30, 15, 5])[0], rng.choice(AIRPORTS), updated.isoformat(),
    ]  # fmt: skip


def _booking(n: int, rng: random.Random, day: date, passenger_max: int, routes: list[str]) -> list:
    ts = datetime(day.year, day.month, day.day) + timedelta(seconds=rng.randrange(86_400))
    return [
        f"B{n:07d}", f"P{rng.randint(1, passenger_max):06d}", rng.choice(routes),
        ts.strftime("%Y-%m-%d %H:%M:%S"), (day + timedelta(days=rng.randint(1, 60))).isoformat(),
        f"{rng.uniform(40, 900):.2f}", "USD",
        rng.choices(["CONFIRMED", "CANCELLED", "CHECKED_IN"], weights=[80, 8, 12])[0],
        rng.choice(CHANNELS), ts.strftime("%Y-%m-%d %H:%M:%S"),
    ]  # fmt: skip


def generate(landing: str | Path, seed: int = 11) -> dict[str, int]:
    """Write both loads under `landing` (replacing it) and return exact expectations."""
    landing = Path(landing)
    if landing.exists():
        shutil.rmtree(landing)
    rng = random.Random(seed)
    routes = _routes(rng)
    route_codes = [r[0] for r in routes]
    _write(landing / "routes" / "routes.csv", ROUTE_COLS, routes)

    # ---- passengers, day 1: full extract, 20 rows with messy casing/whitespace to be cleaned
    day1_p = [_passenger(n, rng, DAY1) for n in range(1, N_DAY1_PASSENGERS + 1)]
    day1_written = [list(r) for r in day1_p]
    for row in day1_written[:20]:
        row[1], row[2] = f"  {row[1].lower()} ", f" {row[2].upper()} "
    _write(landing / "passengers" / f"load_date={DAY1}" / "passengers.csv", PASSENGER_COLS,
           day1_written)  # fmt: skip

    # ---- passengers, day 2: changes only (+ new, no-op, duplicate and invalid rows)
    day2_p: list[list] = []
    for row in day1_p[:N_TIER_OR_AIRPORT_CHANGES]:
        changed = list(row)
        changed[5] = DAY2.isoformat()
        if changed[3] != "GOLD":
            changed[3] = TIERS[TIERS.index(changed[3]) + 1]
        else:
            changed[4] = next(a for a in AIRPORTS if a != changed[4])
        day2_p.append(changed)
    for row in day1_p[100 : 100 + N_NOOP_UPDATES]:  # same attributes, newer timestamp
        day2_p.append([*row[:5], DAY2.isoformat()])
    day2_p += [_passenger(n, rng, DAY2) for n in range(301, 301 + N_NEW_PASSENGERS)]
    day2_p += [list(r) for r in day2_p[-N_PASSENGER_DUPLICATES:]]  # exact duplicates
    for i in range(N_PASSENGER_MISSING_ID):
        day2_p.append(["", f"No Id{i}", f"noid{i}@example.com", "BLUE", "DAL", DAY2.isoformat()])
    for i in range(N_PASSENGER_BAD_TIER):
        day2_p.append([f"P9{i:05d}", "Bad Tier", f"bad{i}@example.com", "PLATINUM", "DAL",
                       DAY2.isoformat()])  # fmt: skip
    rng.shuffle(day2_p)
    _write(landing / "passengers" / f"load_date={DAY2}" / "passengers.csv", PASSENGER_COLS, day2_p)

    # ---- bookings, day 1
    day1_b = [_booking(n, rng, DAY1, N_DAY1_PASSENGERS, route_codes) for n in range(1, N_DAY1_BOOKINGS + 1)]
    _write(landing / "bookings" / f"load_date={DAY1}" / "bookings.csv", BOOKING_COLS, day1_b)

    # ---- bookings, day 2
    n = N_DAY1_BOOKINGS
    valid_new = []
    for _ in range(N_NEW_BOOKINGS - N_UNKNOWN_PASSENGER):
        n += 1
        valid_new.append(_booking(n, rng, DAY2, N_DAY1_PASSENGERS + N_NEW_PASSENGERS, route_codes))
    for i in range(N_UNKNOWN_PASSENGER):  # valid, but the passenger is not in any extract yet
        n += 1
        row = _booking(n, rng, DAY2, N_DAY1_PASSENGERS, route_codes)
        row[1] = f"P8{i:05d}"
        valid_new.append(row)

    updates = []
    for row in day1_b[:N_STATUS_UPDATES]:  # a later version of an existing booking
        updated = list(row)
        updated[7] = "CANCELLED"
        updated[9] = f"{DAY2} 09:00:00"
        updates.append(updated)
    duplicates = [list(r) for r in valid_new[:N_BOOKING_DUPLICATES]]

    def bad(mutate) -> list:
        nonlocal n
        n += 1
        row = _booking(n, rng, DAY2, N_DAY1_PASSENGERS, route_codes)
        mutate(row)
        return row

    invalid = []
    invalid += [bad(lambda r: r.__setitem__(5, "-25.00")) for _ in range(N_NEGATIVE_FARE)]
    invalid += [bad(lambda r: r.__setitem__(1, "")) for _ in range(N_MISSING_PASSENGER)]
    invalid += [bad(lambda r: r.__setitem__(7, "BOGUS")) for _ in range(N_BAD_STATUS)]
    invalid += [bad(lambda r: r.__setitem__(2, "XXX-YYY")) for _ in range(N_UNKNOWN_ROUTE)]
    # Avoid values both engines accept as special dates ("yesterday", "today", "2024/06/02", ...).
    malformed = [(5, "abc"), (5, "12,50"), (5, "n/a"), (4, "2024-13-45"), (4, "soon"),
                 (4, "2024-02-31"), (3, "garbage-ts"), (3, "not-a-time"), (3, ""), (9, "??")]  # fmt: skip
    assert len(malformed) == N_MALFORMED
    invalid += [bad(lambda r, i=i, v=v: r.__setitem__(i, v)) for i, v in malformed]

    day2_b = valid_new + updates + duplicates + invalid
    rng.shuffle(day2_b)
    _write(landing / "bookings" / f"load_date={DAY2}" / "bookings.csv", BOOKING_COLS, day2_b)

    invalid_bookings = N_NEGATIVE_FARE + N_MISSING_PASSENGER + N_BAD_STATUS + N_UNKNOWN_ROUTE + N_MALFORMED
    return {
        "routes": len(routes),
        "passengers_landing_rows": len(day1_written) + len(day2_p),
        "passengers_silver_rows": N_DAY1_PASSENGERS
        + N_TIER_OR_AIRPORT_CHANGES
        + N_NOOP_UPDATES
        + N_NEW_PASSENGERS,
        "passengers_rejected": N_PASSENGER_MISSING_ID + N_PASSENGER_BAD_TIER,
        "passenger_duplicates_removed": N_PASSENGER_DUPLICATES,
        "dim_passenger_versions": N_DAY1_PASSENGERS + N_NEW_PASSENGERS + N_TIER_OR_AIRPORT_CHANGES,
        "bookings_landing_rows": len(day1_b) + len(day2_b),
        "bookings_silver_rows": N_DAY1_BOOKINGS + N_NEW_BOOKINGS,
        "bookings_rejected": invalid_bookings,
        "booking_duplicates_removed": N_STATUS_UPDATES + N_BOOKING_DUPLICATES,
        "bookings_unknown_passenger": N_UNKNOWN_PASSENGER,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--landing", default="data/landing")
    parser.add_argument("--seed", type=int, default=11)
    args = parser.parse_args()
    print(generate(args.landing, args.seed))


if __name__ == "__main__":
    main()
