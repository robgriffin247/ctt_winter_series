with 

source as (
  select * 
  from {{ source("google_sheets", "rounds")}}
),

select_type_and_rename as (
  select
    round_id::int as round_id,
    type::varchar as route_type,
    route::varchar as route,
  from source
)

select * from select_type_and_rename