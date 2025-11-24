with
select_columns as (
    select
        route,
        start_datetime_utc,
        rider,
        gender,
        gender_category,
        power_category,
        watts_average,
        wkg_average,
        race_seconds,
        new_pb,
        segment_seconds
    from {{ref("int_results")}}
)

select * from select_columns