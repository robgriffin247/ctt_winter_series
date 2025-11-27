with 
-- use this to get pb counts
results as (
    select * 
    from {{ref("int_results")}}
),

-- use this to get position points and segment bonus per round, then best seven round
round_efforts as (
    select * 
    from {{ref("dim_round_efforts")}}
),

-- 
derive_fts_bonus as (
    select *,
        case when segment_rank <=5 then segment_rank-6 else 0 end as fts_bonus
    from round_efforts
),

get_points_per_round as (
    select *,
        race_rank + fts_bonus as score
    from derive_fts_bonus
),

-- Rank to find each riders best rounds (so ranking within rider and gender category [not power category; a rider has two gender categories and one power category])
add_rider_round_ranking as (
    select *,
        row_number() over (partition by rider_id, gender_category order by score) as rider_round_ranking
    from get_points_per_round
),

get_best_seven as (
  select * exclude(route_type, rider_round_ranking)
  from add_rider_round_ranking
  where 
    (rider_round_ranking<=4 and route_type='flat')
    or (rider_round_ranking<=4 and route_type='rolling')
    or (rider_round_ranking<=1 and route_type='mountain')
),

-- and sum the scores to one per rider/gender_category
total_scores as (
    select 
        rider_id,
        rider,
        club_id,
        club,
        gender_category,
        power_category,
        sum(1) as race_count,
        sum(race_rank) as position_points,
        sum(fts_bonus) as segment_bonuses,
        sum(score) as score    
    from get_best_seven
    group by 1,2,3,4,5,6 
),

-- then add in the pb bonuses per rider
pb_counts as (
    select rider_id, sum(new_pb) as pb_count 
    from results
    where gender_category='Mixed'
    group by rider_id
),

-- add pb bonus and get the final score
add_pb_bonus as (
    select 
        total_scores.* exclude(score),
        - pb_counts.pb_count as pb_bonuses,
        50 + score - pb_counts.pb_count as score,
    from total_scores left join pb_counts using(rider_id)
),

add_rank as (
    select *, 
        rank() over (partition by power_category, gender_category order by race_count desc, score, pb_bonuses, segment_bonuses) as rank
    from add_pb_bonus
)

select * from add_rank
order by race_count desc, power_category, gender_category, rank
