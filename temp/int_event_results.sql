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
    gender_numeric,
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

decoded_gender as (
  select * exclude(gender_numeric),
      case gender_numeric when 1 then 'M' when 0 then 'F' else null end as gender,
  from source
),

add_position_and_gaps as (
  select *,
    row_number() over (partition by event_id order by time_seconds) as position,
    row_number() over (partition by event_id, category order by time_seconds) as category_position,
    row_number() over (partition by event_id, gender order by time_seconds) as gender_position,
    row_number() over (partition by event_id, category, gender order by time_seconds) as category_gender_position,
    coalesce(time_seconds - lag(time_seconds) over (partition by event_id order by time_seconds), 0) as gap,
    coalesce(time_seconds - lag(time_seconds) over (partition by event_id, category order by time_seconds), 0) as gap_category,
    coalesce(time_seconds - lag(time_seconds) over (partition by event_id, gender order by time_seconds), 0) as gap_gender,
    coalesce(time_seconds - lag(time_seconds) over (partition by event_id, category, gender order by time_seconds), 0) as gap_category_gender,
  from decoded_gender
),

rollsum_gaps as (
  select * exclude(gap, gap_category, gap_gender, gap_category_gender),
    sum(gap) over (partition by event_id order by time_seconds) as gap,
    sum(gap) over (partition by event_id, category order by time_seconds) as gap_category,
    sum(gap) over (partition by event_id, gender order by time_seconds) as gap_gender,
    sum(gap) over (partition by event_id, category, gender order by time_seconds) as gap_category_gender,
  from add_position_and_gaps
),

add_timestamp as (
  select 
    * exclude(_dlt_load_id),
    to_timestamp(_dlt_load_id) as load_timestamp_utc,
  from rollsum_gaps
)

select * from add_timestamp