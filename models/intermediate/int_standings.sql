with 
results as (
    select * from {{ ref("int_round_results") }}
),

add_fts_bonus as (
    select 
        *,
        case when overall_fts_position <= 5 then overall_fts_position - 6 else 0 end as overall_fts_bonus,
        case when category_fts_position <= 5 then category_fts_position - 6 else 0 end as category_fts_bonus,
        case when gender_fts_position <= 5 then gender_fts_position - 6 else 0 end as gender_fts_bonus,
        case when category_gender_fts_position <= 5 then category_gender_fts_position - 6 else 0 end as category_gender_fts_bonus,
    from results
),

derive_points as (
    select *,
        overall_position + overall_fts_bonus + pb_bonus as overall_points, 
        category_position + category_fts_bonus + pb_bonus as category_points, 
        gender_position + gender_fts_bonus + pb_bonus as gender_points, 
        category_gender_position + category_gender_fts_bonus + pb_bonus as category_gender_points,
    from add_fts_bonus
),

rank_race_performance_within_rider_type as (
    select *,
        row_number() over (partition by rider_id, type order by overall_points) as overall_performance_rank,
        row_number() over (partition by rider_id, type order by category_points) as category_performance_rank,
        row_number() over (partition by rider_id, type order by gender_points) as gender_performance_rank,
        row_number() over (partition by rider_id, type order by category_gender_points) as category_gender_performance_rank,
    from derive_points
),

best_seven as (
    select 
        rider_id, rider, gender, category,
        sum(case when 
            (type='flat' and overall_performance_rank<=4) or 
            (type='rolling' and overall_performance_rank<=2) or 
            (type='mountain' and overall_performance_rank=1) 
            then pb_bonus else 0 end) as pb_bonuses,
        sum(case when 
            (type='flat' and overall_performance_rank<=4) or 
            (type='rolling' and overall_performance_rank<=2) or 
            (type='mountain' and overall_performance_rank=1) 
            then overall_points else 0 end) + 50 as overall_score,
        sum(case when 
            (type='flat' and overall_performance_rank<=4) or 
            (type='rolling' and overall_performance_rank<=2) or 
            (type='mountain' and overall_performance_rank=1) 
            then overall_position else 0 end) as overall_positions,
        sum(case when 
            (type='flat' and overall_performance_rank<=4) or 
            (type='rolling' and overall_performance_rank<=2) or 
            (type='mountain' and overall_performance_rank=1) 
            then overall_fts_bonus else 0 end) as overall_fts_bonuses,
        sum(case when 
            (type='flat' and category_performance_rank<=4) or 
            (type='rolling' and category_performance_rank<=2) or 
            (type='mountain' and category_performance_rank=1) 
            then category_points else 0 end) + 50 as category_score,
        sum(case when 
            (type='flat' and category_performance_rank<=4) or 
            (type='rolling' and category_performance_rank<=2) or 
            (type='mountain' and category_performance_rank=1) 
            then category_position else 0 end) as category_positions,
        sum(case when 
            (type='flat' and category_performance_rank<=4) or 
            (type='rolling' and category_performance_rank<=2) or 
            (type='mountain' and category_performance_rank=1) 
            then category_fts_bonus else 0 end) as category_fts_bonuses,
        sum(case when 
            (type='flat' and gender_performance_rank<=4) or 
            (type='rolling' and gender_performance_rank<=2) or 
            (type='mountain' and gender_performance_rank=1) 
            then gender_points else 0 end) + 50 as gender_score,
        sum(case when 
            (type='flat' and gender_performance_rank<=4) or 
            (type='rolling' and gender_performance_rank<=2) or 
            (type='mountain' and gender_performance_rank=1) 
            then gender_position else 0 end) as gender_positions,
        sum(case when 
            (type='flat' and gender_performance_rank<=4) or 
            (type='rolling' and gender_performance_rank<=2) or 
            (type='mountain' and gender_performance_rank=1) 
            then gender_fts_bonus else 0 end) as gender_fts_bonuses,
        sum(case when 
            (type='flat' and category_gender_performance_rank<=4) or 
            (type='rolling' and category_gender_performance_rank<=2) or 
            (type='mountain' and category_gender_performance_rank=1) 
            then category_gender_points else 0 end) + 50 as category_gender_score,
        sum(case when 
            (type='flat' and category_gender_performance_rank<=4) or 
            (type='rolling' and category_gender_performance_rank<=2) or 
            (type='mountain' and category_gender_performance_rank=1) 
            then category_gender_position else 0 end) as category_gender_positions,
        sum(case when 
            (type='flat' and category_gender_performance_rank<=4) or 
            (type='rolling' and category_gender_performance_rank<=2) or 
            (type='mountain' and category_gender_performance_rank=1) 
            then category_gender_fts_bonus else 0 end) as category_gender_fts_bonuses,
    from rank_race_performance_within_rider_type
    group by 1,2,3,4
)

select * from best_seven