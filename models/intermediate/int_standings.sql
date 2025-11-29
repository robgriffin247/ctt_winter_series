with 

inputs as (
  select 
  'Womens' as gender_category
  ),
  
source as (
  select
    *,
    case 
      when (select gender_category from inputs) in ('Mixed', 'Mens') then mixed_category 
        else womens_category end as category
    from {{ ref("int_results") }}
    --from ctt_winter_series_test.intermediate.int_new_results
),

filter_data as (
  select * from source
  where gender in ('F','M') and category='B'
),
  
rankings_per_round as (
    select *,
        rank() over (partition by is_best_effort_in_round, round_id order by race_seconds) as race_position,
        (1 - rank() over (partition by is_best_effort_in_round, round_id order by race_seconds) / sum(1) over (partition by is_best_effort_in_round, round_id)) * 100 as race_percentile,
        rank() over (partition by is_best_effort_in_round, round_id order by segment_seconds) as fts_position,
        (1 - rank() over (partition by is_best_effort_in_round, round_id order by segment_seconds) / sum(1) over (partition by is_best_effort_in_round, round_id)) * 100 as fts_percentile,
        sum(1) over (partition by is_best_effort_in_round, round_id) as of_riders
  from filter_data
  where is_best_effort_in_round
  order by round_id, race_position
),

fts_bonuses as (
  select *,
    case when fts_position<=5 then fts_position-6 else 0 end as fts_bonus
  from rankings_per_round    
),

round_scores as (
  select *, race_position + fts_bonus as round_score from fts_bonuses
),

rank_rounds_per_rider as (
    select *,
        row_number() over (partition by rider_id, route_type order by round_score) as rider_round_ranking
    from round_scores
),

best_seven_rounds as (
  select * 
  from rank_rounds_per_rider
  where 
    (route_type='flat' and rider_round_ranking<=4) or 
    (route_type='rolling' and rider_round_ranking<=2) or 
    (route_type='mounatinous' and rider_round_ranking<=1) 
),
  
pb_counts as (
  select rider_id, -count(*) as pb_bonus from source where is_new_pb group by rider_id
),

scores_per_rider as (
  -- Also count races and race per type
  select
    best_seven_rounds.rider_id,
    rider, club, gender, age_category, category, country, 
    sum(1) as qualifying_races,
    sum(race_position) as position_points,
    sum(fts_bonus) as fts_bonus,
    coalesce(last(pb_counts.pb_bonus), 0) as pb_bonus,
    50 + sum(round_score) + coalesce(last(pb_counts.pb_bonus), 0) as score
  from best_seven_rounds
    left join pb_counts using(rider_id)
  group by best_seven_rounds.rider_id, rider, club, gender, age_category, category, country

),
  
  
filter_riders as (
  select * from scores_per_rider
  )


select rank() over (order by qualifying_races desc, score, category) as rank, rider, qualifying_races as races, score, position_points, fts_bonus, pb_bonus,
  from scores_per_rider 
  order by category, qualifying_races desc, score