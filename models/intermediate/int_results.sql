with 
results as (
    select
      event_id,
      rider_id,
      zp_position,
      race_seconds,
      watts_average,
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

riders as (
    select * 
    from {{ref("int_riders") }}),

add_round_details_to_events as (
    select events.*, rounds.route_type, rounds.route 
    from events 
        left join rounds using(round_id)),

add_rider_details as (
  select
    results.event_id,
    riders.rider_id,
    riders.rider,
    riders.gender_numeric,
    riders.mixed_category,
    riders.womens_category,
    results.zp_position,
    results.race_seconds,
    results.watts_average,
  from results left join riders using(rider_id)
),

add_round_details_to_results as (
  select add_round_details_to_events.* exclude(event_id), add_rider_details.*
  from add_rider_details 
    left join add_round_details_to_events using(event_id)
),

add_segment_times as (
  select add_round_details_to_results.*, segment_times.segment_seconds, segment_times.segments_rider
  from add_round_details_to_results 
    left join segment_times using(zp_position, event_id)
),

add_pb as (
  select *,
  case 
    when 
      coalesce(lag(race_seconds) over (partition by rider_id, route order by start_datetime_utc), race_seconds) > race_seconds 
        then race_seconds 
    else coalesce(lag(race_seconds) over (partition by rider_id, route order by start_datetime_utc), race_seconds) end as pb,
  case 
    when 
      coalesce(lag(race_seconds) over (partition by rider_id, route order by start_datetime_utc), race_seconds) > race_seconds 
        then true 
    else false end as new_pb,
    -- count(*) over (partition by rider_id order by start_datetime_utc) as n_races
  from add_segment_times
),

decoded_gender as (select * exclude(gender_numeric), 
    case gender_numeric when 1 then 'M' when 0 then 'F' else null end as gender
    from add_pb)



select * from decoded_gender 

