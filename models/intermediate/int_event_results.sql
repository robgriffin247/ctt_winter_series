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
    _dlt_load_id,
  from {{ ref("stg_event_results") }}
),

add_timestamp as (
  select 
    * exclude(_dlt_load_id),
    to_timestamp(_dlt_load_id) as load_timestamp_utc,
  from source
)

select * from add_timestamp