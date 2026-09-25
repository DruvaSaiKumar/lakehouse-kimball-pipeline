select
    passenger_id,
    full_name,
    email,
    loyalty_tier,
    home_airport,
    cast(updated_at as date) as updated_at
from {{ source('silver', 'passengers') }}
