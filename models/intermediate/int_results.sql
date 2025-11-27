with

riders as (
    select 
        rider_id,
        rider, 
        club,
        club_id,
        gender, 
        mixed_category,
        womens_category
    from {{ref("int_riders")}}
),

race_results as (
    select 
        event_id, 
        rider_id, 
        race_seconds, 
        watts_average, 
        wkg_average, 
        zp_position
    from {{ref("stg_race_results")}}
),

fts_times as (
    select
        event_id,
        rider_id,
        segment_seconds
    from {{ref("int_fts_times")}}
),

rounds as (
    select 
        round_id,
        route,
        route_type,
        route_length,
        route_elevation,
    from {{ref("stg_rounds")}}
),

races as (
    select
        round_id,
        event_id,
        start_datetime_utc,
    from {{ref("stg_races")}}
),

races_rounds as (
    select 
        rounds.*,
        races.* exclude(round_id)
    from races left join rounds using(round_id)
),

combine_results as (
    select
        race_results.*,
        fts_times.segment_seconds
    from race_results left join fts_times using(event_id, rider_id)
),

add_rider_data as (
    select
        combine_results.event_id,
        combine_results.rider_id,
        riders.rider,
        riders.club_id,
        riders.club,
        riders.gender,
        riders.mixed_category,
        riders.womens_category,
        combine_results.watts_average,
        combine_results.wkg_average,
        combine_results.race_seconds,
        combine_results.segment_seconds,
    from combine_results left join riders using(rider_id)
),

add_round_data as (
    select
        races_rounds.round_id,
        races_rounds.route,
        races_rounds.route_type,
        races_rounds.route_length,
        races_rounds.route_elevation,
        races_rounds.start_datetime_utc,
        add_rider_data.*,
    from add_rider_data
        left join races_rounds using(event_id)
),

add_gender_category as (
    (select *, 'Mixed' as gender_category, mixed_category as power_category from add_round_data)
    union all 
    (select *, 'Womens' as gender_category, womens_category as power_category  from add_round_data where gender='F')
    union all 
    (select *, 'Mens' as gender_category, mixed_category as power_category  from add_round_data where gender='M')
),


-- Add PBs marker (divide on gender category!)
mark_pbs as (
    select *,
        case 
            when coalesce(lag(race_seconds) over (partition by rider_id, route, gender_category order by start_datetime_utc), race_seconds) > race_seconds then race_seconds 
            else coalesce(lag(race_seconds) over (partition by rider_id, route, gender_category order by start_datetime_utc), race_seconds) 
            end as pb,
        case 
            when coalesce(lag(race_seconds) over (partition by rider_id, route, gender_category order by start_datetime_utc), race_seconds) > race_seconds then true 
            else false
            end as new_pb
    from add_gender_category   
),

add_times as (
    select *, 
        route_length / (race_seconds / 3600) as race_speed,
        printf('%02d:%02d:%05.2f', 
            cast(floor(race_seconds / 3600) as integer),
            cast(floor((race_seconds % 3600) / 60) as integer),
            race_seconds % 60
        ) AS race_time, 
        printf('%02d:%05.2f', 
            cast(floor((segment_seconds % 3600) / 60) as integer),
            segment_seconds % 60
        ) AS segment_time
    from mark_pbs
),

select_columns as (
    select
        round_id,
        route,
        route_length,
        route_elevation,
        route_type,
        event_id,
        start_datetime_utc,
        rider_id,
        rider,
        club_id,
        club,
        gender,
        gender_category,
        power_category,
        watts_average,
        wkg_average,
        race_seconds,
        race_speed,
        race_time,
        new_pb,
        segment_seconds,
        segment_time
    from add_times
)

select * from select_columns
where round_id<=2