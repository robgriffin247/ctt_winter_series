with 
segment_times as (
    select *
    from {{ ref("stg_segment_times") }}
),

get_time_part as (
    select 
        event_id, 
        position, 
        rider_segments, 
        trim(str_split(time, chr(10))[1]) as time
    from segment_times
),

parse_seconds as (
  select * exclude(time),
    case 
      when contains(time, ':') then 
        ((str_split(time, ':')[1]::int * 60) + 
        (str_split(time, ':')[2]::int))::float8
      else 
        time::float8
      end as segment_seconds
  from get_time_part  
)

select * from parse_seconds