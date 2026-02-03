with 

source as (
  select * 
  from {{ source('google_sheets', 'races') }}
),

select_type_and_rename as (
  select
    event_id::int as event_id,
    round_id::int as round_id,
    start_datetime_utc::datetime as start_datetime_utc,
  from source
)

select * from select_type_and_rename