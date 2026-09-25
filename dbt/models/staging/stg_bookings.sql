-- Type-normalising view over silver. Spark writes timestamps as UTC-adjusted and DuckDB as naive, so
-- both are cast to a naive UTC timestamp here and nothing downstream has to care which engine ran.
select
    booking_id,
    passenger_id,
    route_code,
    cast(booking_ts as timestamp) as booking_ts,
    cast(booking_date as date) as booking_date,
    cast(travel_date as date) as travel_date,
    cast(fare_amount as decimal(10, 2)) as fare_amount,
    currency,
    status,
    channel,
    cast(updated_ts as timestamp) as updated_ts
from {{ source('silver', 'bookings') }}
