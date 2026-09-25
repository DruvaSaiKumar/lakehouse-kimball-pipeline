-- Grain: one row per booking (its latest valid version).
-- Measures: fare_amount, days_to_travel. Passenger attributes are resolved point-in-time, so a
-- booking keeps the loyalty tier the passenger had when the booking was made.
select
    b.booking_id,
    coalesce(p.passenger_key, '-1') as passenger_key,
    coalesce(r.route_key, '-1') as route_key,
    cast(strftime(b.booking_date, '%Y%m%d') as integer) as booking_date_key,
    cast(strftime(b.travel_date, '%Y%m%d') as integer) as travel_date_key,
    b.fare_amount,
    b.currency,
    b.status,
    b.channel,
    b.status = 'CANCELLED' as is_cancelled,
    date_diff('day', b.booking_date, b.travel_date) as days_to_travel,
    b.updated_ts
from {{ ref('stg_bookings') }} as b
left join {{ ref('dim_passenger') }} as p
    on
        b.passenger_id = p.passenger_id
        and b.booking_date >= p.valid_from
        and b.booking_date < p.valid_to
left join {{ ref('dim_route') }} as r
    on b.route_code = r.route_code
