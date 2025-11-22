with source as (
  select 
    rank,
    rider_id, 
    rider, 
    gender, 
    gender_category, 
    power_category, 
    race_count,
    position_points,
    segment_bonuses,
    pb_bonuses,
    score,
  from {{ ref("int_standings") }}
  order by gender_category, power_category, race_count desc, score
)

select * 
from source
