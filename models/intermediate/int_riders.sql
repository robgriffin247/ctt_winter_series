with 

race_results as (
    select *
    from {{ref("stg_race_results")}}
),

races as (
    select *
    from {{ref("stg_races") }}
),

join_race_date as (
    select race_results.*, races.start_datetime_utc, races.round_id
    from race_results left join races using(event_id)
    where races.round_id<=2
),

rider_names as (
    select
        rider_id, rider, club_id, club,
        row_number() over (partition by rider_id order by start_datetime_utc)=1 as latest
    from join_race_date
),

latest_rider_names as (
    select rider_id, rider, club_id, club from rider_names where latest
),

rider_genders_and_power as (
    select
        rider_id,
        max(gender_numeric) as gender_numeric,
        max(watts_average) as watts_max,
        max(wkg_average) as wkg_max,
        max(watts_average/weight) as derived_wkg_max
    from join_race_date
    group by rider_id
),

join_rider_names as (
    select latest_rider_names.*, rider_genders_and_power.* exclude(rider_id)
    from latest_rider_names left join rider_genders_and_power using(rider_id)
),

add_categories as (
    select 
    *,
    case
      when watts_max>=250 and wkg_max>=4.2 then 'A'
      when watts_max>=200 and wkg_max>=3.36 then 'B'
      when watts_max>=150 and wkg_max>=2.63 then 'C'
      when watts_max>0 and wkg_max>0 then 'D'
      else null end as mixed_category,
    case
      when gender_numeric=0 and wkg_max>=3.88 then 'A'
      when gender_numeric=0 and wkg_max>=3.36 then 'B'
      when gender_numeric=0 and wkg_max>=2.63 then 'C'
      when gender_numeric=0 and wkg_max<2.63 then 'D'
      else null end as womens_category
  from join_rider_names
),

decode_gender as (
    select * exclude(gender_numeric),
        case gender_numeric when 1 then 'M' when 0 then 'F' else null end as gender 
    from add_categories
)

select rider_id, rider, club_id, club, watts_max, wkg_max, derived_wkg_max, gender, mixed_category, womens_category from decode_gender