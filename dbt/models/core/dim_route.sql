select
    md5(route_code) as route_key,
    route_code,
    origin,
    destination,
    distance_miles,
    case
        when distance_miles < 500 then 'SHORT'
        when distance_miles < 1200 then 'MEDIUM'
        else 'LONG'
    end as route_type
from {{ ref('stg_routes') }}

union all

-- Unknown member: keeps fact rows joinable if a route ever fails to resolve.
select '-1', 'UNKNOWN', 'UNKNOWN', 'UNKNOWN', null, 'UNKNOWN'
