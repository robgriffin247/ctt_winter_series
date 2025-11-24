with 
segment_times as (
    select *
    from {{ ref("stg_segment_times") }}
),

get_time_part as (
    select 
        event_id, 
        zp_position, 
        segments_rider, 
        trim(str_split(segment_data, chr(10))[1]) as segment_data_timepart
    from segment_times
),

parse_seconds as (
  select * exclude(segment_data_timepart),
    case 
      when contains(segment_data_timepart, ':') then 
        ((str_split(segment_data_timepart, ':')[1]::int * 60) + 
        (str_split(segment_data_timepart, ':')[2]::int))::float8
      else 
        segment_data_timepart::float8
      end as segment_seconds
  from get_time_part  
)

select * from parse_seconds