select passenger_id, count(*) as current_versions
from {{ ref('dim_passenger') }}
where is_current
group by passenger_id
having count(*) <> 1
