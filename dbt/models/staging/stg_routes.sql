select
    route_code,
    origin,
    destination,
    cast(distance_miles as integer) as distance_miles
from {{ source('silver', 'routes') }}
