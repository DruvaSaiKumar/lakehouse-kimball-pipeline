-- Revenue by the loyalty tier the passenger held when booking (point-in-time), not their tier today.
select
    d.date_day as booking_date,
    p.loyalty_tier,
    count(*) as bookings,
    count(distinct p.passenger_id) as passengers,
    sum(f.fare_amount) filter (where not f.is_cancelled) as gross_revenue
from {{ ref('fact_bookings') }} as f
inner join {{ ref('dim_passenger') }} as p on f.passenger_key = p.passenger_key
inner join {{ ref('dim_date') }} as d on f.booking_date_key = d.date_key
group by all
