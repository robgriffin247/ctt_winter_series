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
  from source
)

select * from select_type_and_rename