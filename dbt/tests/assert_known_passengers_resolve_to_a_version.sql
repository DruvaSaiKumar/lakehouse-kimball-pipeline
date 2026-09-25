-- A booking by a passenger who exists in the dimension must resolve to a real version, not to the
-- unknown member. Bookings whose passenger is genuinely missing are expected to map to -1 and are
-- counted separately by the pipeline tests.
select b.booking_id, b.passenger_id
from {{ ref('stg_bookings') }} as b
inner join {{ ref('fact_bookings') }} as f on b.booking_id = f.booking_id
where
    f.passenger_key = '-1'
    and exists (select 1 from {{ ref('stg_passengers') }} as p where p.passenger_id = b.passenger_id)
