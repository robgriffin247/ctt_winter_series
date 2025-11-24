with source as (
    select
        round_id,
        route_type,
        event_id,
        rider_id,
        rider,
        gender,
        gender_category,
        power_category,
        watts_average,
        race_seconds,
        new_pb,
        segment_seconds,
        race_rank,
        pb_bonus,
        segment_bonus,
        score,
    from {{ ref("int_round_results") }}
    order by round_id, gender_category, power_category, score
)

select * from source
where round_id<=2
