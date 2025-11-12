with 

source as (
  select
    event_id,
    rider_id,
    rider,
    category,
    club_id,
    club,
    gender,
    position,
    category_position,
    time,
    gap,
    load_timestamp_utc,
  from {{ ref("int_event_results") }}
)

select * from source