with source as (
  select 
    round_id,
    start_datetime_utc,
    route_type,
    route,
    event_id,
    rider_id,
    rider,
    weight,
    zp_position,
    race_seconds,
    watts_average,
    segment_seconds,
    segments_rider,
    pb,
    new_pb,
    n_races,
    gender,
    watts_kg_average,
    mixed_category,
    womens_category,
  from {{ ref("int_results") }}
),

mixed_results as (
  select
    round_id,
    event_id,
    start_datetime_utc,
    rider_id,
    rider,
    gender,
    'Mixed' as gender_category,
    mixed_category as power_category,
    watts_average,
    watts_kg_average,
    race_seconds,
    new_pb,
    segment_seconds,
  from source
),

womens_results as (
  select
    round_id,
    event_id,
    start_datetime_utc,
    rider_id,
    rider,
    gender,
    'Womens' as gender_category,
    womens_category as power_category,
    watts_average,
    watts_kg_average,
    race_seconds,
    new_pb,
    segment_seconds,
  from source
  where womens_category is not null
),

unioned as (
  select * from mixed_results union all select * from womens_results
)

select * exclude(start_datetime_utc) 
from unioned 
where round_id<=2
order by start_datetime_utc, gender_category, power_category, race_seconds, segment_seconds