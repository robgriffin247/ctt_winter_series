with 

source as (
  select * 
  from {{ source("google_sheets", "segment_times")}}
),

select_type_and_rename as (
  select
    event_id::int as event_id, -- used to merge
    zp_position::int as zp_position, -- used to merge
    rider::varchar as segments_rider, -- useful for validation!
    data::varchar as segment_data, -- parse this for time
  from source
)

select * from select_type_and_rename