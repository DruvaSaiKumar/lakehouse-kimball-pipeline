-- Two versions of the same passenger must never be valid on the same day, otherwise the
-- point-in-time join in fact_bookings would duplicate bookings. Returns offending pairs.
select a.passenger_id, a.valid_from as a_from, b.valid_from as b_from
from {{ ref('dim_passenger') }} as a
inner join {{ ref('dim_passenger') }} as b
    on
        a.passenger_id = b.passenger_id
        and a.passenger_key < b.passenger_key
        and a.valid_from < b.valid_to
        and b.valid_from < a.valid_to
