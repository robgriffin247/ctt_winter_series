with
select_columns as (
    select
        round_id,
        route,
        route_length,
        route_elevation,
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
        segment_time,
    from {{ref("int_results")}}
)

select * from select_columns