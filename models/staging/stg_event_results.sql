with 

source as (
  select * 
  from {{ source("zrapp", "event_results")}}
),

select_type_and_rename as (
  select
    event_id::int as event_id,
    rider_id::int as rider_id,
    rider::varchar as rider,
    weight::float as weight,
    category::varchar as category,
    club_id::int as club_id,
    club::varchar as club,
    case gender_numeric when 1 then 'm' when 0 then 'f' else null end as gender,
    position::int as position,
    category_position::int as category_position,
    time_seconds::float as time_seconds,
    gap_seconds::float as gap_seconds,
    watts_average::float as watts_average,
    watts_normalised::float as watts_normalised,
    watts_20m::float as watts_20m,
    watts_5m::float as watts_5m,
    watts_1m::float as watts_1m,
    watts_15s::float as watts_15s,
    _dlt_load_id::double as _dlt_load_id,
  from source
)

select * from select_type_and_rename