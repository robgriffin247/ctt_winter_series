with 

source as (
  select
    event_id,
    rider_id,
    rider,
    weight,
    category,
    club_id,
    club,
    gender,
    position,
    category_position,
    time_seconds,
    gap_seconds,
    watts_average,
    watts_normalised,
    watts_20m,
    watts_5m,
    watts_1m,
    watts_15s,
    _dlt_load_id,
  from {{ ref("stg_event_results") }}
),

add_formatted_times as (
  select *,
    case when time_seconds = 0 then null else time_seconds end as time_seconds,
    case 
        when time_seconds = 0 then null
        when time_seconds < 3600 then 
            lpad(floor(time_seconds / 60)::integer::varchar, 2, '0') || ':' ||
            lpad(printf('%.3f', time_seconds % 60), 6, '0')
        else 
            lpad(floor(time_seconds / 3600)::integer::varchar, 2, '0') || ':' ||
            lpad(floor((time_seconds % 3600) / 60)::integer::varchar, 2, '0') || ':' ||
            lpad(printf('%.3f', time_seconds % 60), 6, '0')
    end as time_formatted,
    case when gap_seconds = 0 then null else gap_seconds end as gap_seconds,
    case 
      when gap_seconds = 0 then null
      when gap_seconds < 3600 then 
          lpad(floor(gap_seconds / 60)::integer::varchar, 2, '0') || ':' ||
          lpad(printf('%.3f', gap_seconds % 60), 6, '0')
      else 
          lpad(floor(gap_seconds / 3600)::integer::varchar, 2, '0') || ':' ||
          lpad(floor((gap_seconds % 3600) / 60)::integer::varchar, 2, '0') || ':' ||
          lpad(printf('%.3f', gap_seconds % 60), 6, '0')
    end as gap_formatted
  from source
),

add_timestamp as (
  select 
    * exclude(_dlt_load_id),
    to_timestamp(_dlt_load_id) as load_timestamp_utc,
  from add_formatted_times
)

select * from add_timestamp