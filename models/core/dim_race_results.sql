with 

results as (
    select
        *
    from {{ ref("int_race_results") }}
    order by event_id, time_seconds
)

select * from results