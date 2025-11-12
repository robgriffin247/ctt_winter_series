with 

source as (
  select
    event_id,
    rider_id,
    rider,
    weight,
    category,
    club_id,
    club,
    gender,
    position,
    category_position,
    time_seconds,
    time_formatted,
    gap_seconds,
    gap_formatted,
    watts_average,
    watts_normalised,
    watts_20m,
    watts_5m,
    watts_1m,
    watts_15s,
    load_timestamp_utc,
  from {{ ref("int_event_results") }}
  order by position
)

select * from source