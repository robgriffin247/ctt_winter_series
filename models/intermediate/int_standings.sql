/*
-- use this one later when getting pb points
new_pbs_per_round as (
  select round_id, rider_id, -sum(new_pb) as pb_bonus from new_pb group by 1,2
)
*/

with 

results as (
    select * from {{ ref("int_results") }}
),

-- create three tables then merge on round_id and rider_id; one for best positions (gender and cat best specific for each round - the best position may be from different events between different breakdowns)

-- count PBs per round
pb_bonuses_per_round as (
    select round_id, rider_id, -sum(new_pb) as pb_bonus 
    from results 
    group by 1,2
),

-- work out best time for fts per round
segment_bests_per_round as (
  select 
    rider_id, round_id, category, gender, event_id,
    min(segment_seconds) over (partition by round_id, rider_id) as fts_seconds
  from results
),

fts_ranks_per_round as (
  select rider_id, round_id,
    rank() over (partition by round_id order by fts_seconds) as fts_rank,
    rank() over (partition by round_id, category order by fts_seconds) as category_fts_rank,
    rank() over (partition by round_id, gender order by fts_seconds) as gender_fts_rank,
    rank() over (partition by round_id, category, gender order by fts_seconds) as category_gender_fts_rank,
  from segment_bests_per_round
),

fts_bonuses_per_round as (
    select rider_id, round_id,
        case when fts_rank <=5 then fts_rank - 6 else 0 end as fts_bonus,
        case when category_fts_rank <=5 then category_fts_rank - 6 else 0 end as category_fts_bonus,
        case when gender_fts_rank <=5 then gender_fts_rank - 6 else 0 end as gender_fts_bonus,
        case when category_gender_fts_rank <=5 then category_gender_fts_rank - 6 else 0 end as category_gender_fts_bonus,
    from fts_ranks_per_round
),

-- work out best time for route per round
route_bests_per_round as (
  select 
    *,
    row_number() over (partition by round_id, rider_id order by time_seconds) as best_time
  from results
),

route_ranks_per_round as (
  select *,
    rank() over (partition by round_id order by time_seconds) as route_rank,
    rank() over (partition by round_id, category order by time_seconds) as category_route_rank,
    rank() over (partition by round_id, gender order by time_seconds) as gender_route_rank,
    rank() over (partition by round_id, category, gender order by time_seconds) as category_gender_route_rank,
  from route_bests_per_round
  where best_time=1
),

merge_bonuses as (
    select route_ranks_per_round.*, 
        fts_bonuses_per_round.fts_bonus,
        fts_bonuses_per_round.category_fts_bonus,
        fts_bonuses_per_round.gender_fts_bonus,
        fts_bonuses_per_round.category_gender_fts_bonus,
        pb_bonuses_per_round.pb_bonus,
    from route_ranks_per_round
        left join fts_bonuses_per_round using(rider_id, round_id)
        left join pb_bonuses_per_round using(rider_id, round_id)

),

derive_totals as (
    select *,
        route_rank + fts_bonus + pb_bonus as overall_score,
        category_route_rank + category_fts_bonus + pb_bonus as category_score,
        gender_route_rank + gender_fts_bonus + pb_bonus as gender_score,
        category_gender_route_rank + category_gender_fts_bonus + pb_bonus as category_gender_score,
    from merge_bonuses
),


rider_round_rankings as (
    select round_id, rider_id, rider, gender, category, type, overall_score, category_score, gender_score, category_gender_score,
        row_number() over (partition by rider_id, type order by overall_score) as overall_round_ranking,
        row_number() over (partition by rider_id, type order by category_score) as category_round_ranking,
        row_number() over (partition by rider_id, type order by gender_score) as gender_round_ranking,
        row_number() over (partition by rider_id, type order by category_gender_score) as category_gender_round_ranking,
    from derive_totals
),

best_seven as (
    select 
        rider_id, rider, gender, category,
        -- sum(case when type='flat' and overall_round_ranking<=4 then 1 else 0 end) as flat_races,
        -- sum(case when type='rolling' and overall_round_ranking<=2 then 1 else 0 end) as rolling_races,
        -- sum(case when type='mountain' and overall_round_ranking=1 then 1 else 0 end) as mountain_races,
        sum(case when 
            (type='flat' and overall_round_ranking<=4) or 
            (type='rolling' and overall_round_ranking<=2) or 
            (type='mountain' and overall_round_ranking=1) 
            then overall_score else 0 end) + 50 as overall_score,
        sum(case when 
            (type='flat' and category_round_ranking<=4) or 
            (type='rolling' and category_round_ranking<=2) or 
            (type='mountain' and category_round_ranking=1) 
            then category_score else 0 end) + 50 as category_score,
        sum(case when 
            (type='flat' and gender_round_ranking<=4) or 
            (type='rolling' and gender_round_ranking<=2) or 
            (type='mountain' and gender_round_ranking=1) 
            then gender_score else 0 end) + 50 as gender_score,
        sum(case when 
            (type='flat' and category_gender_round_ranking<=4) or 
            (type='rolling' and category_gender_round_ranking<=2) or 
            (type='mountain' and category_gender_round_ranking=1) 
            then category_gender_score else 0 end) + 50 as category_gender_score,
    from rider_round_rankings
    group by 1,2,3,4
)

select * from best_seven