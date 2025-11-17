with round_results as (
    select * from {{ ref("int_round_results") }}
),

rank_rider_performances as (
  select round_id, route_type, rider_id, rider, gender, power_category, gender_category, race_rank, segment_bonus, pb_bonus, score,
    row_number() over (partition by rider_id, power_category, gender_category order by score) as rider_round_ranking
  from round_results
),

best_seven as (
  select * exclude(route_type, rider_round_ranking)
  from rank_rider_performances
  where 
    (rider_round_ranking<=4 and route_type='flat')
    or (rider_round_ranking<=4 and route_type='rolling')
    or (rider_round_ranking<=1 and route_type='mountain')
),

sum_scores as (
  select rider_id, rider, gender, gender_category, power_category, 
    sum(race_rank) as position_points,
    sum(segment_bonus) as segment_bonuses,
    sum(pb_bonus) as pb_bonuses,
    sum(score) as score,
  from best_seven
  group by rider_id, rider, gender, gender_category, power_category
),

add_leaderboard_rank as (
  select *,
    rank() over (partition by gender_category, power_category order by score) as rank
    
  from sum_scores
)

select * from add_leaderboard_rank 