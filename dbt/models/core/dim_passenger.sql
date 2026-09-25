-- Type 2 slowly changing dimension built from the version history that the daily extract provides.
--
-- Why not a dbt snapshot? A snapshot infers history from the moments it happens to run, which makes
-- valid_from depend on run timing and lose changes between runs. Here the source already carries
-- `updated_at` per version, so history is derived deterministically and the model can be rebuilt
-- from scratch at any time with the same result. Use a snapshot when the source only exposes
-- current state.
--
-- Validity windows are half-open: valid_from <= date < valid_to. The first version of each
-- passenger starts at 1900-01-01 so a booking made before the passenger's first extract still
-- resolves to their earliest known attributes.
with ordered as (
    select
        *,
        row_number() over w as version_no,
        lag(full_name) over w as prev_full_name,
        lag(email) over w as prev_email,
        lag(loyalty_tier) over w as prev_loyalty_tier,
        lag(home_airport) over w as prev_home_airport,
        min(updated_at) over (partition by passenger_id) as first_seen_date
    from {{ ref('stg_passengers') }}
    window w as (partition by passenger_id order by updated_at)
),

-- Keep a version only if something actually changed; a re-sent identical record is not a new version.
changed as (
    select *
    from ordered
    where
        version_no = 1
        or full_name is distinct from prev_full_name
        or email is distinct from prev_email
        or loyalty_tier is distinct from prev_loyalty_tier
        or home_airport is distinct from prev_home_airport
),

windows as (
    select
        passenger_id,
        full_name,
        email,
        loyalty_tier,
        home_airport,
        first_seen_date,
        case when version_no = 1 then date '1900-01-01' else updated_at end as valid_from,
        lead(updated_at) over (partition by passenger_id order by updated_at) as next_change
    from changed
)

select
    md5(passenger_id || '|' || cast(valid_from as varchar)) as passenger_key,
    passenger_id,
    full_name,
    email,
    loyalty_tier,
    home_airport,
    first_seen_date,
    valid_from,
    coalesce(next_change, date '9999-12-31') as valid_to,
    next_change is null as is_current
from windows

union all

-- Unknown member: bookings whose passenger has not arrived in any extract yet (late-arriving dimension).
select
    '-1',
    'UNKNOWN',
    'Unknown',
    null,
    'UNKNOWN',
    'UNKNOWN',
    date '1900-01-01',
    date '1900-01-01',
    date '9999-12-31',
    true
