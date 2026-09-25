-- BI-facing aggregate: one row per travel date and route.
-- Revenue excludes cancelled bookings; cancellation_rate is cancelled / all bookings.
select
    d.date_day as travel_date,
    d.day_name,
    r.route_code,
    r.origin,
    r.destination,
    r.route_type,
    count(*) as bookings,
    count(*) filter (where f.is_cancelled) as cancelled_bookings,
    round(count(*) filter (where f.is_cancelled) * 1.0 / count(*), 4) as cancellation_rate,
    sum(f.fare_amount) filter (where not f.is_cancelled) as gross_revenue,
    round(avg(f.days_to_travel), 1) as avg_days_to_travel
from {{ ref('fact_bookings') }} as f
inner join {{ ref('dim_date') }} as d on f.travel_date_key = d.date_key
inner join {{ ref('dim_route') }} as r on f.route_key = r.route_key
group by all
