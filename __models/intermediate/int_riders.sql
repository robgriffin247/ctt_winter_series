with 
  
results as (
  select * 
  from {{ref("stg_results")}}
  ),

events as (
    select event_id, start_datetime_utc
    from {{ref("stg_events")}}
),

add_start as (
    select results.*, events.start_datetime_utc
    from results left join events using(event_id)
),

riders as (
  select 
    rider_id, 
    row_number() over (partition by rider_id order by start_datetime_utc)=1 as most_recent,
    last(rider) over (partition by rider_id order by start_datetime_utc) as rider, 
    max(gender_numeric) over (partition by rider_id) as gender_numeric
  from add_start 
),

distinct_riders as (
    select * exclude(most_recent) from riders where most_recent
),

derive_weight as (
    select * exclude(weight),
        case when weight is null or weight = 0 then 
            ( (watts_average/wkg_average) + 
                (watts_1200s/wkg_1200s) +
                (watts_300s/wkg_300s) + 
                (watts_120s/wkg_120s) + 
                (watts_60s/wkg_60s) +
                (watts_30s/wkg_30s) +
                (watts_15s/wkg_15s) +
                (watts_5s/wkg_5s) ) / 8 
            else weight end as weight
    from results
),

max_powers as (
  select 
    rider_id, 
    max(watts_average) as watts, 
    max(watts_average/weight) as wkg 
  from derive_weight group by 1),

combine_max_powers as (
    select
        distinct_riders.*,
        max_powers.watts,
        max_powers.wkg
    from distinct_riders left join max_powers using(rider_id)
),
  
add_categories as (
  select 
    rider_id,
    rider,
    gender_numeric,
    watts,
    wkg,
    case
      when watts>=250 or wkg>=4.2 then 'A'
      when watts>=200 or wkg>=3.36 then 'B'
      when watts>=150 or wkg>=2.63 then 'C'
      when watts>0 or wkg>0 then 'D'
      else null end as mixed_category,
    case
      when gender_numeric=0 and wkg>=3.88 then 'A'
      when gender_numeric=0 and wkg>=3.36 then 'B'
      when gender_numeric=0 and wkg>=2.63 then 'C'
      when gender_numeric=0 and wkg<2.63 then 'D'
      else null end as womens_category
  from combine_max_powers
)


select * from add_categories