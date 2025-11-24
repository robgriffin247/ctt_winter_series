with 

source as (
  select * 
  from {{ source("zrapp", "results")}}
),

select_type_and_rename as (
  select
    event_id::int as event_id,
    rider_id::int as rider_id,
    trim(rider)::varchar as rider,
    weight::float as weight,
    gender_numeric::int as gender_numeric,
    zp_position::int as zp_position,
    time_seconds::float as race_seconds,
    watts_average::float as watts_average,
    watts_1200s::float as watts_1200s,
    watts_300s::float as watts_300s,
    watts_120s::float as watts_120s,
    watts_60s::float as watts_60s,
    watts_30s::float as watts_30s,
    watts_15s::float as watts_15s,
    watts_5s::float as watts_5s,
    wkg_average::float as wkg_average,
    wkg_1200s::float as wkg_1200s,
    wkg_300s::float as wkg_300s,
    wkg_120s::float as wkg_120s,
    wkg_60s::float as wkg_60s,
    wkg_30s::float as wkg_30s,
    wkg_15s::float as wkg_15s,
    wkg_5s::float as wkg_5s,
  from source
)

select * from select_type_and_rename