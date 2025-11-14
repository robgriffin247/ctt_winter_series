with 

source as (select * from {{ ref("stg_segment_times") }}),

events as (select * from {{ ref("stg_events") }}),

add_round as (
    select source.*, events.round_id
    from source left join events using(event_id)
),

fts_ranks as (
    select 
        *,
        row_number() over (partition by event_id, category) as event_rank
    from add_round
),

add_times as (
    select *, fts_minutes*60 + fts_seconds as fts_time from fts_ranks
),

rank_rounds as (
    select *, row_number() over (partition by round_id, category order by fts_seconds, event_rank) as round_rank
    from add_times
)

select * from rank_rounds