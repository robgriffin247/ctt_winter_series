with 
results as (
    select * from {{ ref("int_race_results") }}
),

identify_fastest_attempt as (
    select round_id, type, event_id, start_datetime_utc, rider_id, rider, club, gender, category, 
        time_seconds as race_time, 
        segment_seconds as fts_time,
        row_number() over (partition by round_id, rider_id order by race_time)=1 as fastest_attempt
    from results
),

pb_bonuses as (
    select round_id, rider_id, -sum(new_pb) as pb_bonus from results group by 1, 2
),

focal_race_per_rider_per_round as (
  select identify_fastest_attempt.*, pb_bonuses.pb_bonus
  from identify_fastest_attempt left join pb_bonuses using(round_id, rider_id)
  where fastest_attempt
),

add_ranks as (
  select *,
    rank() over (partition by round_id order by race_time) as overall_position,
    rank() over (partition by round_id, category order by race_time) as category_position,
    rank() over (partition by round_id, gender order by race_time) as gender_position,
    rank() over (partition by round_id, category, gender order by race_time) as category_gender_position,
    rank() over (partition by round_id order by fts_time) as overall_fts_position,
    rank() over (partition by round_id, category order by fts_time) as category_fts_position,
    rank() over (partition by round_id, gender order by fts_time) as gender_fts_position,
    rank() over (partition by round_id, category, gender order by fts_time) as category_gender_fts_position,
  from focal_race_per_rider_per_round
)

select * from add_ranks order by overall_position