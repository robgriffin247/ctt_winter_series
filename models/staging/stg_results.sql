with 

source as (
  select * 
  from {{ source("zrapp", "results")}}
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
    gender_numeric::int as gender_numeric,
    zp_position::int as zp_position,
    category_position::int as category_position,
    time_seconds::float as time_seconds,
    gap_seconds::float as gap_seconds,
    watts_average::float as watts_average,
    watts_normalised::float as watts_normalised,
    watts_1200s::float as watts_1200s,
    watts_300s::float as watts_300s,
    watts_120s::float as watts_120s,
    watts_60s::float as watts_60s,
    watts_30s::float as watts_30s,
    watts_15s::float as watts_15s,
    watts_5s::float as watts_5s,
    _dlt_load_id::double as _dlt_load_id,
  from source
)

select * from select_type_and_rename