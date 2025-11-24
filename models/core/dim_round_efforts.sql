with
results as (
    select
        round_id,
        route,
        route_type,
        event_id,
        start_datetime_utc,
        rider_id,
        rider,
        segments_rider,
        gender,
        gender_category,
        power_category,
        watts_average,
        wkg_average,
        race_seconds,
        new_pb,
        segment_seconds
    from {{ref("int_results")}}
),

mark_best_efforts as (
    select *,
        row_number() over (partition by round_id, rider_id, gender_category order by race_seconds)=1 as best_race
    from results
),

best_efforts as (
    select * from mark_best_efforts where best_race
),

add_round_ranks as (
    select *,
        rank() over (partition by round_id, gender_category, power_category order by race_seconds) as race_rank,
        rank() over (partition by round_id, gender_category, power_category order by race_seconds) as segment_rank,
    from best_efforts
)

select * from add_round_ranks