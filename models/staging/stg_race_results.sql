with 

source as (
  select * 
  from {{ source("zrapp", "race_results")}}
),

select_type_and_rename as (
  select
    event_id::int as event_id,
    rider_id::int as rider_id,
    rider::varchar as rider,
    category::varchar as category,
    club_id::int as club_id,
    club::varchar as club,
    case gender_numeric when 1 then 'm' when 0 then 'f' else null end as gender,
    position::int as position,
    category_position::int as category_position,
    time::float as time,
    gap::float as gap,
    _dlt_load_id::double as _dlt_load_id,
  from source
)

select * from select_type_and_rename