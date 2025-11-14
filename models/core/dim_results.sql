with 

results as (
    select
        round_id,
        event_id,
        rider,
        club,
        gender,
        category,
        time_seconds,
        gap_seconds,
        category_position,
        category_gap_seconds,
        gender_position,
        gender_gap_seconds,
        category_gender_position,
        category_gender_gap_seconds,
        new_pb
    from {{ ref("int_results") }}
    order by event_id, time_seconds
)

select * from results