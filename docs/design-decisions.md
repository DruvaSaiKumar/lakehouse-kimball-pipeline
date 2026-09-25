# Design decisions

## Two engines for the silver layer

The production-shaped implementation is PySpark (`pipeline/spark_silver.py`). A second
implementation of the same rules runs on DuckDB (`pipeline/duckdb_silver.py`). This is deliberate:

- it lets the whole pipeline run on a laptop without a JVM, which is how I develop the dbt layer;
- it is an **independent reference**. CI runs both on the same landing data and fails if any silver or
  quarantine table differs by even one row, or if the metrics differ. Two implementations written
  separately agreeing is much stronger evidence than either one passing hand-written assertions;
- it exposed real semantic traps while writing it, such as both engines accepting the strings
  `yesterday` or `2024/06/02` as valid dates, which would have silently turned bad data into good.

The cost is that every rule has to be written twice. `pipeline/rules.py` holds the shared constants
(allowed statuses and tiers, and the order in which reject reasons apply).

## Everything is read as strings, then cast

Spark's CSV reader with a schema and `PERMISSIVE` mode has subtle behaviour around column pruning and
the corrupt-record column. Reading every field as text and casting explicitly turns a failed cast into
`NULL`, which the rules then report as `malformed_value`. The behaviour is the same in both engines
and easy to reason about.

## Quarantine instead of drop or fail

Each rejected row is kept with its raw values and a single `reject_reason` (the first rule that
applies, in a fixed order). Every landing row is accounted for:
`landing = silver + rejected + duplicates_removed`, and a test asserts that identity.

## Why `dim_passenger` is not a dbt snapshot

A dbt snapshot infers history from the moments it happens to run, so `valid_from` depends on run
timing and changes between runs are lost. Here the daily extract already carries `updated_at` for each
version, so the SCD2 history is derived deterministically with window functions
(`lag` to detect change, `lead` to close the window). The model can be dropped and rebuilt from
scratch and produces identical keys. Use a snapshot when the source exposes only current state.

Details that matter:

- **No-op updates are not versions.** A re-sent record with unchanged attributes is dropped by
  comparing each row with the previous one; a test covers 10 such passengers.
- **Half-open windows** (`valid_from <= date < valid_to`) so a date is never in two versions. A dbt
  test asserts that no two windows for one passenger overlap.
- **The first version starts at 1900-01-01**, so a booking that precedes the passenger's first
  extract still resolves to their earliest known attributes. The true first-seen date is kept in
  `first_seen_date`.
- **Surrogate keys are `md5(passenger_id | valid_from)`**, stable across rebuilds, so facts keep
  pointing at the same row.

## Late-arriving dimensions and the unknown member

Twelve bookings reference passengers that appear in no extract. Rather than dropping them or failing
the load, the fact maps them to an unknown member (`-1`) so revenue is still counted. A dbt test makes
sure a booking is never mapped to the unknown member when its passenger does exist, and the count of
unknown-member bookings is asserted in the pipeline tests so it is visible, not silent.

## Full rebuild per run

Silver is rebuilt from all landing files every run. That is idempotent and simple, and right at this
size. At scale the next step is incremental processing keyed on `load_date` with a merge into silver.

## Adapter-specific SQL

Everything is standard SQL except `dim_date`, which uses DuckDB's `generate_series`. On Databricks,
Snowflake or BigQuery that one model would use the platform's date spine; the other models use
`lag`, `lead`, `row_number`, `md5` and `filter` clauses that most engines support (the aggregate
`filter` clause would become `case when` on engines without it).

## What is not verified

- `land_raw_data` regenerates synthetic files; a real deployment replaces it with ingestion.
- The Airflow DAG is import-tested and its structure is asserted, but it has not been run
  end to end in a live Airflow deployment.
- No Databricks workspace was used. The code is standard PySpark and would run on Databricks, but
  that has not been done.
