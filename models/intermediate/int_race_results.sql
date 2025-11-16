with 
results as (
    select * 
    from {{ref("stg_results") }}),

segment_times as (
    select * 
    from {{ref("int_segment_times") }}),

events as (
    select * 
    from {{ref("stg_events") }}),

rounds as (
    select * 
    from {{ref("stg_rounds") }}),

add_round_details_to_events as (
    select events.*, rounds.type, rounds.route 
    from events 
        left join rounds using(round_id)),

add_round_details_to_results as (
  select add_round_details_to_events.* exclude(event_id), results.*
  from results 
    left join add_round_details_to_events using(event_id)
),

add_segment_times as (
  select add_round_details_to_results.*, segment_times.segment_seconds, segment_times.rider_segments
  from add_round_details_to_results 
    left join segment_times using(zp_position, event_id)
),
  
add_pb as (
  select *,
  case 
    when 
      coalesce(lag(time_seconds) over (partition by rider_id order by start_datetime_utc), time_seconds) > time_seconds 
        then time_seconds 
    else coalesce(lag(time_seconds) over (partition by rider_id order by start_datetime_utc), time_seconds) end as pb,
  case 
    when 
      coalesce(lag(time_seconds) over (partition by rider_id order by start_datetime_utc), time_seconds) > time_seconds 
        then true 
    else false end as new_pb,
     count(*) over (partition by rider_id order by start_datetime_utc) as n_races
  from add_segment_times
),

decoded_gender as (select * exclude(gender_numeric), 
    case gender_numeric when 1 then 'M' when 0 then 'F' else null end as gender
    from add_pb)

select * from decoded_gender order by round_id, time_seconds, segment_seconds