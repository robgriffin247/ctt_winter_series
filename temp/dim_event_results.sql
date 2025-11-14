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
    gender_position,
    category_gender_position,
    time_seconds,
    gap,
    gap_category,
    gap_gender,
    gap_category_gender,
    watts_average,
    watts_normalised,
    watts_1200s,
    watts_300s,
    watts_120s,
    watts_60s,
    watts_30s,
    watts_15s,
    watts_5s,
    load_timestamp_utc,
  from {{ ref("int_event_results") }}
  order by position
)

select * from source