with
results as (
    select *
    from {{ ref("int_results") }}
    --from ctt_winter_series_test.intermediate.int_results
),

rider_best_mixed_race_per_round_category as (
    select * exclude(mixed_category, womens_category),
      row_number() over (partition by round_id, rider_id, mixed_category order by race_seconds)=1 as best_race, 
      'Mixed' as gender_category,
      mixed_category as power_category,
    from results
),

rider_best_womens_race_per_round_category as (
    select * exclude(mixed_category, womens_category),
        row_number() over (partition by round_id, rider_id, womens_category order by race_seconds)=1 as best_race, 
        'Womens' as gender_category,
        womens_category as power_category
    from results
    where womens_category is not null
),

union_ranked_races as (
    select * from rider_best_mixed_race_per_round_category
    union all
    select * from rider_best_womens_race_per_round_category
),

rank_times as (
    select * exclude(best_race),
      row_number() over (partition by round_id, power_category, gender_category order by race_seconds) as race_rank,
      row_number() over (partition by round_id, power_category, gender_category order by segment_seconds) as segment_rank,
    from union_ranked_races
    where best_race
),

bonuses as (
    select *,
        -sum(case when new_pb then 1 else 0 end) over (partition by round_id, rider_id, gender_category) as pb_bonus,
        case when segment_rank<=5 then segment_rank-6 else 0 end as segment_bonus
    from rank_times
),

scores as (
    select 
        *, 
        50 + race_rank + segment_bonus + pb_bonus as score
    from bonuses
)

select * from scores

