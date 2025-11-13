with 

source as (
  select * 
  from {{ source("google_sheets", "fts_times")}}
),

select_type_and_rename as (
  select
    event_id::int as event_id,
    category::varchar as category,
    rider_id::int as rider_id,
    fts_minutes::int as fts_minutes,
    fts_seconds::int as fts_seconds,
  from source
)

select * from select_type_and_rename