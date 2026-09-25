# Design notes

**Two engines for the silver layer.** `pipeline/spark_silver.py` is the PySpark version. `pipeline/duckdb_silver.py` implements the same rules on DuckDB. I did this for three reasons:

- the pipeline and the dbt layer can run on a laptop without a JVM;
- it's an independent reference. CI runs both on the same landing data and fails if any silver or quarantine table differs by a single row, or if the metrics differ. Two separately written versions agreeing is stronger evidence than one version passing hand-written assertions;
- it exposed real traps, such as both engines accepting strings like `yesterday` or `2024/06/02` as valid dates, which would have turned bad data into good data.

Every rule has to be written twice. `pipeline/rules.py` holds the shared constants: allowed statuses and tiers, and the order reject reasons are applied.

**Read everything as strings, then cast.** Spark's CSV reader with a schema in `PERMISSIVE` mode does subtle things with column pruning and the corrupt-record column. Reading text and casting explicitly turns a failed cast into `NULL`, which the rules report as `malformed_value`. Both engines behave the same way and it's easy to reason about.

**Quarantine, don't drop.** Each rejected row keeps its raw values and one `reject_reason`, the first rule that applies in a fixed order. Every landing row is accounted for, and a test asserts `landing = silver + rejected + duplicates_removed`.

**`dim_passenger` isn't a dbt snapshot.** A snapshot infers history from when it happens to run, so `valid_from` depends on run timing and changes between runs are lost. Here the daily extract already carries `updated_at` for each version, so the history comes from window functions (`lag` to spot a change, `lead` to close the window). The model can be dropped and rebuilt with identical keys. A snapshot is the right tool when the source only exposes current state.

- A record re-sent with unchanged attributes isn't a new version. It's dropped by comparing each row with the previous one, and a test covers 10 such passengers.
- Windows are half-open (`valid_from <= date < valid_to`), so a date is never in two versions. A dbt test checks that no two windows for a passenger overlap.
- The first version starts at 1900-01-01, so a booking made before the passenger's first extract still resolves to their earliest known attributes. The real first-seen date is kept in `first_seen_date`.
- Surrogate keys are `md5(passenger_id | valid_from)`, stable across rebuilds, so facts keep pointing at the same row.

**Unknown member for late-arriving dimensions.** Twelve bookings reference passengers who appear in no extract. Rather than drop them or fail the load, the fact maps them to an unknown member (`-1`) so revenue is still counted. A dbt test checks that a booking never maps to the unknown member when its passenger does exist, and the count of unknown-member bookings is asserted so it stays visible.

**Full rebuild each run.** Silver is rebuilt from all landing files every time. That's idempotent and simple, and fine at this size. At scale I'd load incrementally by `load_date` and merge into silver.

**SQL portability.** Almost everything is standard SQL. The exception is `dim_date`, which uses DuckDB's `generate_series`. On Databricks, Snowflake or BigQuery that model would use the platform's date spine. The other models use `lag`, `lead`, `row_number`, `md5` and `filter` clauses that most engines support. The aggregate `filter` clause would become `case when` where it isn't available.

## Not verified

- `land_raw_data` regenerates synthetic files. A real deployment would replace it with ingestion.
- The Airflow DAG is import-tested and its structure is asserted, but it hasn't run end to end in a live Airflow deployment.
- I haven't used a Databricks workspace. The code is standard PySpark and should run there, but I haven't tried it.
