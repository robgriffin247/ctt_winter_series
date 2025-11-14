with 

source as (
  select * 
  from {{ source("google_sheets", "segment_times")}}
),

select_type_and_rename as (
  select
    event_id::int as event_id,
    position::int as position,
    rider::varchar as rider_segments,
    data::varchar as time,
  from source
)

select * from select_type_and_rename