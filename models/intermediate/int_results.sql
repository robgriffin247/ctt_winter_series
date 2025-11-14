with 

results as (
  select
    event_id,
    rider_id,
    trim(rider) as rider,
    weight,
    category,
    club_id,
    club,
    gender_numeric,
    zp_position,
    time_seconds,
    gap_seconds,
    watts_average,
    watts_normalised,
    watts_1200s,
    watts_300s,
    watts_120s,
    watts_60s,
    watts_30s,
    watts_15s,
    watts_5s,
    _dlt_load_id,
  from {{ ref("stg_results") }}
),

rounds as (
  select * from {{ ref("stg_rounds") }}
),

events as (
  select * from {{ ref("stg_events") }}
),

segment_times as (
    select *
    from {{ ref("int_segment_times") }}
),

events_rounds as (
  select events.*, rounds.* exclude(round_id)
  from events left join rounds using(round_id)
),

results_events as (
  select 
    results.*,
    events_rounds.* exclude(event_id)
  from results left join events_rounds using(event_id)
),

decoded_gender as (
  select * exclude(gender_numeric),
      case gender_numeric when 1 then 'M' when 0 then 'F' else null end as gender,
  from results_events
),

add_position_and_gaps as (
  select *,
    row_number() over (partition by event_id order by time_seconds) as position,
    row_number() over (partition by event_id, category order by time_seconds) as category_position,
    row_number() over (partition by event_id, gender order by time_seconds) as gender_position,
    row_number() over (partition by event_id, category, gender order by time_seconds) as category_gender_position,
    coalesce(time_seconds - lag(time_seconds) over (partition by event_id order by time_seconds), 0) as diff_seconds,
    coalesce(time_seconds - lag(time_seconds) over (partition by event_id, category order by time_seconds), 0) as category_diff_seconds,
    coalesce(time_seconds - lag(time_seconds) over (partition by event_id, gender order by time_seconds), 0) as gender_diff_seconds,
    coalesce(time_seconds - lag(time_seconds) over (partition by event_id, category, gender order by time_seconds), 0) as category_gender_diff_seconds,
  from decoded_gender
),

rollsum_gaps as (
  select * exclude(diff_seconds, category_diff_seconds, gender_diff_seconds, category_gender_diff_seconds),
    sum(diff_seconds) over (partition by event_id order by time_seconds) as gap_seconds,
    sum(category_diff_seconds) over (partition by event_id, category order by time_seconds) as category_gap_seconds,
    sum(gender_diff_seconds) over (partition by event_id, gender order by time_seconds) as gender_gap_seconds,
    sum(category_gender_diff_seconds) over (partition by event_id, category, gender order by time_seconds) as category_gender_gap_seconds,
  from add_position_and_gaps
),

add_timestamp as (
  select 
    * exclude(_dlt_load_id),
    to_timestamp(_dlt_load_id) as load_timestamp_utc,
  from rollsum_gaps
),

add_segment_times as (
  select add_timestamp.*, segment_times.segment_seconds, segment_times.rider_segments
  from add_timestamp left join segment_times on add_timestamp.event_id=segment_times.event_id and add_timestamp.zp_position=segment_times.position
),

-- Work out if time was a new PB
add_rolling_pb as (
  select
    *,
    min(time_seconds) over (partition by rider_id, route order by start_datetime_utc) as rolling_pb
  from add_segment_times
),

add_new_pb as (
  select *,
    coalesce(rolling_pb - lag(rolling_pb) over (partition by rider_id, route order by start_datetime_utc), 0) as pb_change
  from add_rolling_pb
),

new_pb as (
  select * exclude(rolling_pb, pb_change), 
    pb_change<0 as new_pb,
  from add_new_pb
)



select * from new_pb