with 

source as (
  select * 
  from {{ source("google_sheets", "segment_times")}}
),

select_type_and_rename as (
  select
    event_id::int as event_id,
    zp_position::int as zp_position,
    rider::varchar as segments_rider, -- useful for validation!
    data::varchar as segment_data,
  from source
)

select * from select_type_and_rename