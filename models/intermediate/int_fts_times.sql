with 
source as (select * from {{ref("stg_fts_times")}}),

races as (select * from {{ref("stg_races")}}),

select_columns as (
    select
    zid::int as event_id,
    zwid::int as rider_id,
    case
        when zid::int in (select event_id::int from races where round_id in (1, 5)) then msec___35::float
        when zid::int in (select event_id::int from races where round_id in (2, 6)) then msec___116::float
        when zid::int in (select event_id::int from races where round_id in (3, 7)) then msec___1::float
        when zid::int in (select event_id::int from races where round_id in (4)) then null
        when zid::int in (select event_id::int from races where round_id in (8, 12)) then null
        when zid::int in (select event_id::int from races where round_id in (9, 13)) then null
        when zid::int in (select event_id::int from races where round_id in (10, 14)) then null
        when zid::int in (select event_id::int from races where round_id in (11)) then null
        else null end as segment_seconds
    from source
)

select * from select_columns
