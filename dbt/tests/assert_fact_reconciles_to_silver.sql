-- The star schema must neither drop nor duplicate bookings, and must carry the same total fare
-- as the silver layer. Returns a row (and fails) when either differs.
with fact as (
    select count(*) as n, sum(fare_amount) as fare from {{ ref('fact_bookings') }}
),

silver as (
    select count(*) as n, sum(fare_amount) as fare from {{ ref('stg_bookings') }}
)

select fact.n as fact_rows, silver.n as silver_rows, fact.fare as fact_fare, silver.fare as silver_fare
from fact, silver
where fact.n <> silver.n or fact.fare <> silver.fare
