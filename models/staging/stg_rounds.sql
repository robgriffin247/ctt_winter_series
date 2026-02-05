with 

source as (
  select * 
  from {{ source('google_sheets', 'rounds') }}
),

select_type_and_rename as (
  select
    round_id::int as round_id,
    year::int as year,
    week::int as week,
    type::varchar as route_type,
    route::varchar as route,
    route_length::decimal as route_length,
    route_elevation::int as route_elevation,
    route_link::varchar as route_link,
    segment::varchar as segment,
    segment_length::decimal as segment_length,
    segment_elevation::int segment_elevation,
    segment_link::varchar as segment_link,
    completed::boolean as round_completed,
    1 as hello
  from source
)

select * from select_type_and_rename