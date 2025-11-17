with source as (
  select 
    rank,
    rider_id, 
    rider, 
    gender, 
    gender_category, 
    power_category, 
    position_points,
    segment_bonuses,
    pb_bonuses,
    score,
  from {{ ref("int_standings") }}
  order by gender_category, power_category, score
)

select * from source