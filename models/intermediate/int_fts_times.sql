with 
source as (select * from {{ref("stg_fts_times")}}),

races as (select * from {{ref("stg_races")}}),

select_columns as (
    select event_id, rider_id,
        case
            when event_id in (select event_id from races where round_id in (1, 5)) then msec___35
            when event_id in (select event_id from races where round_id in (2, 6)) then msec___116
            when event_id in (select event_id from races where round_id in (3, 7)) then msec___1
            when event_id in (select event_id from races where round_id in (4)) then msec___20
            when event_id in (select event_id from races where round_id in (8, 12)) then msec___3
            when event_id in (select event_id from races where round_id in (9, 13)) then msec___76
            when event_id in (select event_id from races where round_id in (10, 14)) then msec___34
            when event_id in (select event_id from races where round_id in (11)) then null
            else null end as segment_seconds,
        case
            when event_id in (select event_id from races where round_id in (1, 5)) then watts___35
            when event_id in (select event_id from races where round_id in (2, 6)) then watts___116
            when event_id in (select event_id from races where round_id in (3, 7)) then watts___1
            when event_id in (select event_id from races where round_id in (4)) then watts___20
            when event_id in (select event_id from races where round_id in (8, 12)) then watts___3
            when event_id in (select event_id from races where round_id in (9, 13)) then watts___76
            when event_id in (select event_id from races where round_id in (10, 14)) then watts___34
            when event_id in (select event_id from races where round_id in (11)) then null
            else null end as segment_watts,
        case
            when event_id in (select event_id from races where round_id in (1, 5)) then wkg___35
            when event_id in (select event_id from races where round_id in (2, 6)) then wkg___116
            when event_id in (select event_id from races where round_id in (3, 7)) then wkg___1
            when event_id in (select event_id from races where round_id in (4)) then wkg___20
            when event_id in (select event_id from races where round_id in (8, 12)) then wkg___3
            when event_id in (select event_id from races where round_id in (9, 13)) then wkg___76
            when event_id in (select event_id from races where round_id in (10, 14)) then wkg___34
            when event_id in (select event_id from races where round_id in (11)) then null
            else null end as segment_wkg
    from source
)

select * from select_columns
