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
    select events.*, rounds.route_type, rounds.route 
    from events 
        left join rounds using(round_id)),

add_round_details_to_results as (
  select add_round_details_to_events.* exclude(event_id), results.*
  from results 
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
      coalesce(lag(race_seconds) over (partition by rider_id order by start_datetime_utc), race_seconds) > race_seconds 
        then race_seconds 
    else coalesce(lag(race_seconds) over (partition by rider_id order by start_datetime_utc), race_seconds) end as pb,
  case 
    when 
      coalesce(lag(race_seconds) over (partition by rider_id order by start_datetime_utc), race_seconds) > race_seconds 
        then true 
    else false end as new_pb,
     count(*) over (partition by rider_id order by start_datetime_utc) as n_races
  from add_segment_times
),

decoded_gender as (select * exclude(gender_numeric), 
    case gender_numeric when 1 then 'M' when 0 then 'F' else null end as gender
    from add_pb),

add_wkg as (
  select *, 
    case when weight is not null and watts_average is not null then (watts_average / weight) else null end as watts_kg_average
  from decoded_gender
),
  
add_category as (
  select *,
    case 
      when watts_average >= 250 and watts_kg_average >= 4.2 then 'A'
      when watts_average >= 200 and watts_kg_average >= 3.36 then 'B'
      when watts_average >= 150 and watts_kg_average >= 2.63 then 'C'
      else 'D' end as mixed_category,
    case 
      when gender = 'F' and watts_kg_average >= 3.88 then 'A'
      when gender = 'F' and watts_kg_average >= 3.36 then 'B'
      when gender = 'F' and watts_kg_average >= 2.63 then 'C'
      when gender = 'F' and watts_kg_average < 2.63 then 'D'
      else null end as womens_category    
  from add_wkg  
)

select * from add_category 

