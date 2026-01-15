with 

stg_race_results as (
    select
        *
    from {{ ref("stg_race_results") }}
),

stg_races as (
    select
        *
    from {{ ref("stg_races") }}
),

stg_rounds as (
    select
        *
    from {{ ref("stg_rounds") }}
),

int_fts_times as (
    select
        *
    from {{ ref("int_fts_times") }}
),


-- Combine data -----------------------------------------------------------------------------------
race_results_with_race_details as (
    select
        stg_race_results.*,
        stg_races.start_datetime_utc,
        stg_races.round_id
    from stg_race_results left join stg_races using(event_id)
),

race_results_with_round_details as (
    select
        race_results_with_race_details.*,
        stg_rounds.route_type,
        stg_rounds.route,
        stg_rounds.route_length,
        stg_rounds.route_elevation,
        stg_rounds.segment,
        stg_rounds.segment_length,
        stg_rounds.round_completed
    from race_results_with_race_details left join stg_rounds using(round_id)
),

race_results_with_segment_values as (
    select
        race_results_with_round_details.*,
        int_fts_times.segment_seconds, 
        int_fts_times.segment_watts,
        int_fts_times.segment_wkg
    from race_results_with_round_details left join int_fts_times using(event_id, rider_id)
),

input_results as (
    select
        round_id,
        round_completed,
        route,
        route_length,
        route_elevation,
        route_type,
        event_id,
        start_datetime_utc,
        rider_id,
        rider,
        case when country!='' then upper(left(country,2)) else null end as country,
        weight,
        case when gender_numeric=1 then 'M' when gender_numeric=0 then 'F' else null end as gender,
        age_category,
        club_id,
        club,
        watts_average,
        wkg_average,
        race_seconds,
        route_length / (race_seconds/3600) as race_speed,
        segment,
        segment_seconds,
        segment_length / (segment_seconds/3600) as segment_speed,
        segment_watts,
        segment_wkg
    from race_results_with_segment_values
),


-- Get latest rider details -----------------------------------------------------------------------
rider_latest_details as (
    select
        row_number() over (partition by rider_id order by start_datetime_utc desc)=1 as is_latest,
        last(rider_id) over (partition by rider_id order by start_datetime_utc) as rider_id,
        last(rider) over (partition by rider_id order by start_datetime_utc) as rider,
        last(gender) over (partition by rider_id order by start_datetime_utc) as gender,
        last(club) over (partition by rider_id order by start_datetime_utc) as club,
        first(age_category) over (partition by rider_id order by start_datetime_utc) as age_category,
        last(country) over (partition by rider_id order by start_datetime_utc) as country,
    from input_results
),

updated_rider_details as (
    select 
        input_results.* exclude(rider, gender, club, age_category, country),
        rider_latest_details.* exclude(rider_id)
    from input_results left join rider_latest_details using(rider_id)
    where rider_latest_details.is_latest
),

-- Get rider categories ---------------------------------------------------------------------------
rider_best_power as (
    select
        rider_id,
        rider,
        gender,
        max(watts_average) as watts_max,
        max(wkg_average) as wkg_max,
    from updated_rider_details
    group by all
),

rider_categories as (
    select
        rider_id,
        rider,
        gender,
        watts_max,
        wkg_max,
        case 
            when watts_max>=250 and wkg_max>=4.20 then 'A'
            when watts_max>=200 and wkg_max>=3.36 then 'B'
            when watts_max>=150 and wkg_max>=2.63 then 'C'
            when watts_max>0 and wkg_max>0 then 'D'
            else null end as mixed_category,
        case
            when gender='F' and wkg_max>=3.88 then 'A'
            when gender='F' and wkg_max>=3.36 then 'B'
            when gender='F' and wkg_max>=2.63 then 'C'
            when gender='F' and wkg_max<2.63 then 'D'
            else null end as womens_category
    from rider_best_power
),

categories_on_results as (
    select
        updated_rider_details.*,
        rider_categories.mixed_category,
        rider_categories.womens_category,
        rider_categories.watts_max,
        rider_categories.wkg_max
    from updated_rider_details left join rider_categories using(rider_id)
),


-- Mark riders fastest ride per round -------------------------------------------------------------
identified_best_race_per_round as (
    select
        *,
        row_number() over (partition by round_id, rider_id order by race_seconds)=1 as is_best_effort_in_round
    from categories_on_results
),


-- Mark riders new PBs ----------------------------------------------------------------------------
mark_pbs as (
    select *,
        case 
            when coalesce(lag(race_seconds) over (partition by rider_id, route order by start_datetime_utc), race_seconds) > race_seconds 
                then race_seconds 
            else coalesce(lag(race_seconds) over (partition by rider_id, route order by start_datetime_utc), race_seconds) 
            end as pb,
        case 
            when coalesce(lag(race_seconds) over (partition by rider_id, route order by start_datetime_utc), race_seconds) > race_seconds 
                then true 
            else false
            end as is_new_pb
    from identified_best_race_per_round   
),

-- Formatting -------------------------------------------------------------------------------------
add_times as (
    select *, 
        printf('%02d:%02d:%05.2f', 
            cast(floor(race_seconds / 3600) as integer),
            cast(floor((race_seconds % 3600) / 60) as integer),
            race_seconds % 60
        ) AS race_time, 
        case when segment_seconds<6000 then 
            printf('%02d:%05.2f', 
                cast(floor(segment_seconds / 60) as integer),
                segment_seconds % 60
            ) else '>=100 mins' end as segment_time
    from mark_pbs
),

joined_category as (
    select *,
        case when womens_category is not null then mixed_category || ' (' || womens_category || ')' else mixed_category end as categories
    from add_times
)

select * 
from joined_category
order by start_datetime_utc, race_seconds